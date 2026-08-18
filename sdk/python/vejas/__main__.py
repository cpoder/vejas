import json
import sys

USAGE = """usage:
  python -m vejas run <flow.py>                       run a flow file
  python -m vejas surface <dir|file>                  dump the business surface as JSON
  python -m vejas mappings <dir|file>                 (alias of surface)
  python -m vejas set <file> <name> <key|-> <json>    rewrite one literal in place
  python -m vejas graph <root>                        dump the static pipeline graph
  python -m vejas preview <flow.py>                   mapping preview against its fixture
  python -m vejas new <request> [root]                ask the agent CLI for a new flow
"""

if __name__ == "__main__":
    argv = sys.argv[1:]
    if len(argv) == 2 and argv[0] == "run":
        from . import run

        run(argv[1])
    elif len(argv) == 2 and argv[0] in ("surface", "mappings"):
        from .mapping import dump_surface

        dump_surface(argv[1])
    elif len(argv) == 5 and argv[0] == "set":
        from .mapping import set_literal

        try:
            print(json.dumps(set_literal(argv[1], argv[2], argv[3], argv[4])))
        except Exception as exc:
            print(json.dumps({"ok": False, "error": str(exc)}))
            sys.exit(1)
    elif len(argv) == 2 and argv[0] == "graph":
        from .graph import dump_graph

        dump_graph(argv[1])
    elif len(argv) == 2 and argv[0] == "preview":
        from .graph import dump_preview

        dump_preview(argv[1])
    elif len(argv) in (2, 3) and argv[0] == "new":
        from .new import main

        main(argv[1], argv[2] if len(argv) == 3 else ".")
    else:
        print(USAGE, file=sys.stderr)
        sys.exit(2)
