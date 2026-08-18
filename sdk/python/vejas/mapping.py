"""The business surface: mappings, constants and lookup tables are data, not code.

A flow may declare:

    COUNTRY_CODES = {"France": "FR", "Germany": "DE"}      # transcoding table
    MAPPING = {
        "amount_eur": ["data.object.amount", "cents_to_eur"],
        "country":    ["shipping_address.country", "lookup:COUNTRY_CODES"],
        "customer":   "data.object.customer",
    }
    THRESHOLD_EUR = 500                                     # constant

Rules are dotted source paths plus named transforms; `lookup:<TABLE>` maps a
value through a module-level UPPERCASE literal dict (the agent seeds these
tables, the domain expert corrects them in the panel). Because everything
here is a pure literal, the platform extracts it statically
(`python -m vejas surface`), renders it for a non-technical user, and applies
constrained corrections in place (`python -m vejas set`), leaving the
algorithmic body of the flow untouched. The agent owns how; the human owns
what it means.
"""

import ast
import inspect
import json
import sys
from pathlib import Path

TRANSFORMS = {
    "cents_to_eur": lambda v: v / 100,
    "upper": lambda v: str(v).upper(),
    "lower": lambda v: str(v).lower(),
    "str": str,
    "int": int,
    "float": float,
}


def get_path(obj, dotted):
    cur = obj
    for part in dotted.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def _caller_tables():
    frame = inspect.currentframe()
    caller = frame.f_back.f_back if frame and frame.f_back else None
    if caller is None:
        return {}
    return {k: v for k, v in caller.f_globals.items() if k.isupper() and isinstance(v, dict)}


def apply_mapping(event, mapping, tables=None):
    """Apply a literal mapping table to an event. Returns a flat dict.

    `lookup:<NAME>` transforms resolve NAME against `tables`, or, by default,
    against the caller module's UPPERCASE dict literals."""
    if tables is None:
        tables = _caller_tables()
    out = {}
    for target, rule in mapping.items():
        out[target] = _apply_rule(event, rule, tables)
    return out


def _apply_rule(event, rule, tables):
    # array projection: {"each": "path.to.list", "map": {sub-mapping}}
    # element-wise only, recursion allowed; anything needing aggregation,
    # joins or reordering is code, on purpose.
    if isinstance(rule, dict):
        items = get_path(event, rule["each"])
        if not isinstance(items, list):
            return []
        submap = rule.get("map", {})
        return [
            {t: _apply_rule(item, r, tables) for t, r in submap.items()}
            for item in items
        ]
    if isinstance(rule, str):
        return get_path(event, rule)
    path, *transforms = rule
    value = get_path(event, path)
    for name in transforms:
        if name.startswith("split:"):
            _, sep, idx = name.split(":", 2)
            if value is not None:
                parts = str(value).split(sep)
                i = int(idx)
                value = parts[i] if -len(parts) <= i < len(parts) else None
            continue
        if name.startswith("lookup:"):
            tname = name.split(":", 1)[1]
            table = tables.get(tname)
            if not isinstance(table, dict):
                raise KeyError(f"unknown lookup table {tname!r}")
            value = table.get(value) if value is not None else None
            continue
        fn = TRANSFORMS.get(name)
        if fn is None:
            raise KeyError(f"unknown transform {name!r} (known: {sorted(TRANSFORMS)})")
        if value is not None:
            value = fn(value)
    return value


def _is_literal(node):
    try:
        ast.literal_eval(node)
        return True
    except (ValueError, TypeError):
        return False


def extract_surface(path):
    """Statically extract the business surface of a .py file.

    MAPPING* dict literals -> kind "mapping"
    other UPPERCASE dict literals -> kind "table" (transcoding tables)
    UPPERCASE scalar literals -> kind "constant"
    """
    tree = ast.parse(Path(path).read_text(), filename=str(path))
    found = []
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name) or not _is_literal(node.value):
            continue
        value = ast.literal_eval(node.value)
        entry = {"file": str(path), "name": target.id, "line": node.lineno, "value": value}
        if target.id.startswith("MAPPING") and isinstance(value, dict):
            found.append({**entry, "kind": "mapping"})
        elif target.id.isupper() and isinstance(value, dict):
            found.append({**entry, "kind": "table"})
        elif target.id.isupper() and isinstance(value, (int, float, str, bool)):
            found.append({**entry, "kind": "constant"})
        elif (
            target.id.isupper()
            and isinstance(value, list)
            and all(isinstance(v, (int, float, str, bool)) for v in value)
        ):
            found.append({**entry, "kind": "constant", "list": True})
    return found


def dump_surface(root):
    root = Path(root)
    files = [root] if root.is_file() else sorted(root.glob("*.py"))
    out = []
    for f in files:
        try:
            out.extend(extract_surface(f))
        except SyntaxError as exc:
            print(f"[vejas] {f}: {exc}", file=sys.stderr)
    json.dump(out, sys.stdout, indent=2)
    print()


def set_literal(path, name, key, value_json):
    """Rewrite ONE literal in place. `key` is a mapping/table entry, or '-'
    for a whole constant. The rest of the file is untouched; result must
    re-parse."""
    path = Path(path)
    src = path.read_text()
    tree = ast.parse(src)
    new_value = json.loads(value_json)
    target_node = None
    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == name
        ):
            if key in (None, "", "-"):
                target_node = node.value
            else:
                # nested descent: "positions/map/sku" walks dict literals
                current = node.value
                for part in key.split("/"):
                    if not isinstance(current, ast.Dict):
                        raise TypeError(f"{name}: {part!r} is not inside a dict literal")
                    nxt = None
                    for k, v in zip(current.keys, current.values):
                        if isinstance(k, ast.Constant) and k.value == part:
                            nxt = v
                            break
                    if nxt is None:
                        raise KeyError(f"{name} has no key {key!r} (missing {part!r})")
                    current = nxt
                target_node = current
            break
    if target_node is None:
        raise KeyError(f"no literal assignment {name!r} in {path}")
    if not _is_literal(target_node):
        raise TypeError(f"{name}[{key}] is not a pure literal; edit the code instead")

    lines = src.splitlines(keepends=True)
    start = sum(len(l) for l in lines[: target_node.lineno - 1]) + target_node.col_offset
    end = sum(len(l) for l in lines[: target_node.end_lineno - 1]) + target_node.end_col_offset
    new_src = src[:start] + repr(new_value) + src[end:]
    ast.parse(new_src)  # refuse to write a file that no longer parses
    path.write_text(new_src)
    return {"ok": True, "file": str(path), "name": name, "key": key or "-", "value": new_value}
