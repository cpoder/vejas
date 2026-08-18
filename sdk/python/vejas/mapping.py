"""The business surface: mappings and constants are data, not code.

A flow may declare literal tables and constants:

    MAPPING = {
        "amount_eur": ["data.object.amount", "cents_to_eur"],
        "customer":   "data.object.customer",
    }
    THRESHOLD_EUR = 500

Rules are dotted source paths, optionally followed by named transforms.
Because these are pure literals, the platform can extract them statically
(`python -m vejas surface flows/`), render them for a non-technical user,
and apply constrained corrections that rewrite ONLY the literal in place
(`python -m vejas set <file> <name> <key|-> <json-value>`), leaving the
algorithmic body of the flow untouched. The agent owns how; the human owns
what it means.
"""

import ast
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


def apply_mapping(event, mapping):
    """Apply a literal mapping table to an event. Returns a flat dict."""
    out = {}
    for target, rule in mapping.items():
        if isinstance(rule, str):
            out[target] = get_path(event, rule)
            continue
        path, *transforms = rule
        value = get_path(event, path)
        for name in transforms:
            fn = TRANSFORMS.get(name)
            if fn is None:
                raise KeyError(f"unknown transform {name!r} (known: {sorted(TRANSFORMS)})")
            if value is not None:
                value = fn(value)
        out[target] = value
    return out


def _is_literal(node):
    try:
        ast.literal_eval(node)
        return True
    except (ValueError, TypeError):
        return False


def extract_surface(path):
    """Statically extract the business surface of a .py file.

    Returns MAPPING* dict literals and UPPERCASE scalar constants."""
    tree = ast.parse(Path(path).read_text(), filename=str(path))
    found = []
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name) or not _is_literal(node.value):
            continue
        value = ast.literal_eval(node.value)
        if target.id.startswith("MAPPING") and isinstance(value, dict):
            found.append(
                {
                    "file": str(path),
                    "name": target.id,
                    "kind": "mapping",
                    "line": node.lineno,
                    "value": value,
                }
            )
        elif target.id.isupper() and isinstance(value, (int, float, str, bool)):
            found.append(
                {
                    "file": str(path),
                    "name": target.id,
                    "kind": "constant",
                    "line": node.lineno,
                    "value": value,
                }
            )
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
    """Rewrite ONE literal in place. `key` is a mapping entry, or '-' for a
    whole constant. The rest of the file is untouched; result must re-parse."""
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
                if not isinstance(node.value, ast.Dict):
                    raise TypeError(f"{name} is not a dict literal")
                for k, v in zip(node.value.keys, node.value.values):
                    if isinstance(k, ast.Constant) and k.value == key:
                        target_node = v
                        break
                if target_node is None:
                    raise KeyError(f"{name} has no key {key!r}")
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
