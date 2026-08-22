#!/bin/bash
# Delete the VEJAS-* demo accounts created by the demo (Salesforce bulk delete).
set -u
SF="${BIN_DIR:-/opt/vejas}/vejas-salesforce"
: "${SF_INSTANCE_URL:?}" "${SF_ACCESS_TOKEN:?}"
env SF_INSTANCE_URL="$SF_INSTANCE_URL" SF_ACCESS_TOKEN="$SF_ACCESS_TOKEN" \
  SF_QUERY="SELECT Id FROM Account WHERE Name LIKE 'VEJAS-%'" SF_INTERVAL_SECS=0 "$SF" 2>/dev/null \
  | python3 -c 'import sys,json; ids=[json.loads(l)["row"]["Id"] for l in sys.stdin if l.strip()]; print(json.dumps([{"Id":i} for i in ids]))' \
  | env SF_INSTANCE_URL="$SF_INSTANCE_URL" SF_ACCESS_TOKEN="$SF_ACCESS_TOKEN" \
    SF_OBJECT=Account SF_OPERATION=delete "$SF" ingest 2>/dev/null
echo " (VEJAS-* demo accounts deleted)"
