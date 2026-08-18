"""Static pipeline graph + flow previews.

Everything here is derived from the code itself (AST) or from running the
flow on its fixture, never from a registry: flow sources come from
@flow(source=...), emit targets from emit(...) calls (string literals or
module-level constants), connector subjects from their declared SUBJECTS_IN
/ SUBJECTS_OUT literals. If the code moves, the graph moves.

The preview has two halves:
  - the static mapping preview (raw value -> mapped value, per rule)
  - the sample run: the fixture event goes through the ACTUAL handler
    (custom parse helpers included) and the captured emits are returned.
    That second half is how a domain expert validates the parts of a flow
    that are deliberately code, without ever reading the code.
"""

import ast
import importlib.util
import inspect as _inspect
import json
import sys
from pathlib import Path

from .mapping import apply_mapping, extract_surface, get_path


def _module_consts(tree):
    consts = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            t = node.targets[0]
            if isinstance(t, ast.Name):
                try:
                    consts[t.id] = ast.literal_eval(node.value)
                except (ValueError, TypeError):
                    pass
    return consts


def _flow_info(path):
    tree = ast.parse(Path(path).read_text(), filename=str(path))
    consts = _module_consts(tree)
    sources, emits, flows = [], [], []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for deco in node.decorator_list:
                if isinstance(deco, ast.Call) and isinstance(deco.func, ast.Name) and deco.func.id == "flow":
                    flows.append(node.name)
                    for arg in deco.args[:1]:
                        if isinstance(arg, ast.Constant):
                            sources.append(arg.value)
                    for kw in deco.keywords:
                        if kw.arg == "source" and isinstance(kw.value, ast.Constant):
                            sources.append(kw.value.value)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "emit" and node.args:
            first = node.args[0]
            if isinstance(first, ast.Constant):
                emits.append(first.value)
            elif isinstance(first, ast.Name) and isinstance(consts.get(first.id), str):
                emits.append(consts[first.id])
    return {
        "file": str(path),
        "name": Path(path).stem,
        "functions": flows,
        "sources": sorted(set(sources)),
        "emits": sorted(set(emits)),
    }


def _connector_info(path):
    tree = ast.parse(Path(path).read_text(), filename=str(path))
    consts = _module_consts(tree)
    return {
        "file": str(path),
        "name": Path(path).stem,
        "subjects_in": consts.get("SUBJECTS_IN", []),
        "subjects_out": consts.get("SUBJECTS_OUT", []),
    }


def build_graph(root):
    root = Path(root)
    flows = [_flow_info(f) for f in sorted((root / "flows").glob("*.py"))]
    connectors = [_connector_info(f) for f in sorted((root / "connectors").glob("*.py"))]
    return {"flows": flows, "connectors": connectors}


def dump_graph(root):
    json.dump(build_graph(root), sys.stdout, indent=2)
    print()


def _sample_run(flow_path, event):
    """Run the fixture through the real handlers, capturing emits (no NATS)."""
    import vejas as sdk

    sdk._FLOWS.clear()
    spec = importlib.util.spec_from_file_location(f"vejas_preview_{Path(flow_path).stem}", flow_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    token = sdk._pending.set([])
    error = None
    try:
        for fn, _source, durable in list(sdk._FLOWS):
            try:
                result = fn(event)
                if _inspect.isawaitable(result):
                    import asyncio

                    asyncio.run(result)
            except Exception as exc:
                error = f"{durable}: {exc!r}"
        emits = [
            {"subject": subject, "payload": json.loads(data.decode())}
            for subject, data in sdk._pending.get()
        ]
    finally:
        sdk._pending.reset(token)
        sdk._FLOWS.clear()
    return emits, error


def preview(flow_path):
    flow_path = Path(flow_path)
    fixture_path = flow_path.parent / "fixtures" / f"{flow_path.stem}.json"
    if not fixture_path.exists():
        return {"fixture": None, "entries": {}, "emits": [], "error": None}
    event = json.loads(fixture_path.read_text())

    surface = extract_surface(flow_path)
    tables = {i["name"]: i["value"] for i in surface if i["kind"] == "table"}
    entries = {}
    for item in surface:
        if item["kind"] != "mapping":
            continue
        mapping = item["value"]
        mapped = apply_mapping(event, mapping, tables)
        for target, rule in mapping.items():
            if isinstance(rule, dict):
                items = get_path(event, rule.get("each", "")) or []
                raw = f"{len(items)} × {rule.get('each')}"
            else:
                path = rule if isinstance(rule, str) else rule[0]
                raw = get_path(event, path)
            entries.setdefault(item["name"], {})[target] = {"raw": raw, "out": mapped.get(target)}

    emits, error = _sample_run(flow_path, event)
    return {"fixture": str(fixture_path), "entries": entries, "emits": emits, "error": error}


def dump_preview(flow_path):
    json.dump(preview(flow_path), sys.stdout, indent=2, default=str)
    print()
