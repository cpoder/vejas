"""Prompt -> flow, through whatever agent CLI you already use.

`python -m vejas new "<request>"` asks the agent (default: the `claude` CLI
in print mode) to write a complete flow file against the SDK contract, then
validates it (parses, has @flow, mappings are literals) and drops it into
flows/. The platform never embeds a model or an API key: bring your own
agent, this is just the narrow prompt that makes its output land correctly.
"""

import ast
import json
import os
import re
import subprocess
import sys
from pathlib import Path

AGENT_CMD = os.environ.get("VEJAS_AGENT_CMD", "claude")

CONTRACT = """You write ONE Python file for the Vejas integration platform. Reply with ONLY the file content (no markdown fences, no commentary).

The SDK contract:
- from vejas import flow, emit
- from vejas.mapping import apply_mapping
- Call signature: apply_mapping(event, MAPPING) — event FIRST, mapping SECOND.
- @flow(source="vx.<domain>.<name>") decorates a function taking one JSON event (dict).
- emit("vx.<domain>.<name>", payload_dict) publishes; it is flushed before the ack.
- Subjects always start with "vx.". Known sinks: vx.slack.out (payload {"text": ...}).
- Field extraction MUST go through a module-level literal MAPPING dict:
  MAPPING = {"target": "dotted.source.path", "other": ["dotted.path", "transform"]}
  Available transforms: cents_to_eur, upper, lower, str, int, float,
  split:<sep>:<index> (e.g. ["ref", "split:-:1"]), and lookup:<TABLE>.
  NOTHING else is allowed in rules. Real parsing (person names, addresses,
  variable formats, regex territory) must be a small plain-Python helper
  function in this file, called from the handler; the human validates it on
  sample events, not in the mapping table.
- For value transcoding (names to codes, labels to ids...) declare a module-level
  UPPERCASE literal dict and reference it with the lookup transform:
  COUNTRY_CODES = {"France": "FR", "Germany": "DE"}
  MAPPING = {"country": ["shipping_address.country", "lookup:COUNTRY_CODES"]}
  Seed the table with the obvious values for the domain; the human expert will
  complete and correct it in the panel afterwards.
- For arrays of structures mapped element-wise, use the each construct inside MAPPING:
  MAPPING = {"positions": {"each": "line_items", "map": {"sku": "sku", "qty": ["quantity", "int"], "unit_eur": ["unit_price_cents", "cents_to_eur"]}}}
  Element-wise projection ONLY. Anything needing aggregation, grouping, joins,
  filtering or reordering of arrays: write it as plain Python in the handler
  (the human validates it on sample events, not in the mapping table).
- Business thresholds/queues MUST be module-level UPPERCASE literal constants.
- Keep the handler body minimal: apply_mapping, a condition on constants, emit.
- Start the file with a short docstring. The flow function name becomes the file name: make it a valid snake_case identifier describing the task.

Existing flows (do not duplicate their names): {existing}

Task: {task}"""


def _ask_agent(prompt):
    proc = subprocess.run(
        [AGENT_CMD, "-p", prompt],
        capture_output=True,
        text=True,
        timeout=300,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"agent failed: {proc.stderr.strip()[:300]}")
    return proc.stdout.strip()


def _strip_fences(text):
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\n", "", text)
        text = re.sub(r"\n```\s*$", "", text)
    return text.strip() + "\n"


def _validate(code):
    tree = ast.parse(code)
    flow_names = [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
        and any(
            isinstance(d, ast.Call) and isinstance(d.func, ast.Name) and d.func.id == "flow"
            for d in node.decorator_list
        )
    ]
    if not flow_names:
        raise ValueError("generated file has no @flow function")
    return flow_names[0]


def new_flow(task, root="."):
    root = Path(root)
    flows_dir = root / "flows"
    existing = sorted(p.stem for p in flows_dir.glob("*.py"))
    # .replace, not .format: the contract is full of literal braces (dict examples)
    prompt = CONTRACT.replace("{existing}", ", ".join(existing) or "none").replace("{task}", task)
    code = _strip_fences(_ask_agent(prompt))
    name = _validate(code)
    target = flows_dir / f"{name}.py"
    n = 2
    while target.exists():
        target = flows_dir / f"{name}_{n}.py"
        n += 1
    target.write_text(code)
    return {"ok": True, "file": str(target), "flow": name}


def main(task, root="."):
    try:
        print(json.dumps(new_flow(task, root)))
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)[:400]}))
        sys.exit(1)
