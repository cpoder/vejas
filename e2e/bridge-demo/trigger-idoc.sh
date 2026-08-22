#!/bin/bash
# Send a big inbound IDoc (10 account segments) into SAP, addressed to the
# registered server's destination. Uses the SAP env exported by run.sh.
set -u
LD="${LD_LIBRARY_PATH:-/usr/sap/NPL/D00/exe}"
SAPRFC="${BIN_DIR:-/opt/vejas}/vejas-sap-rfc"
J=$(python3 - <<'PY'
import json
abap=[
 "REPORT ZBRIDGE.",
 "DATA: ctl TYPE STANDARD TABLE OF edi_dc40, wc TYPE edi_dc40.",
 "DATA: dat TYPE STANDARD TABLE OF edi_dd40, wd TYPE edi_dd40.",
 "DATA: nm(40), ix(10).",
 "wc-tabnam = 'EDI_DC40'. wc-idoctyp = 'MATMAS05'.",
 "wc-mestyp = 'MATMAS'. wc-sndprn = 'BIGSAP'. wc-sndprt = 'LS'.",
 "wc-rcvprn = 'NPLCLNT001'. wc-rcvprt = 'LS'. wc-direct = '2'.",
 "APPEND wc TO ctl.",
 "DO 10 TIMES.",
 "  ix = sy-index.",
 "  CONCATENATE 'VEJAS-IDOC-Account-' ix INTO nm.",
 "  CONDENSE nm.",
 "  wd-segnam = 'E1MARAM'. wd-sdata = nm. APPEND wd TO dat.",
 "ENDDO.",
 "CALL FUNCTION 'IDOC_INBOUND_ASYNCHRONOUS' DESTINATION 'WMETHODS_RFC'",
 "  TABLES idoc_control_rec_40 = ctl idoc_data_rec_40 = dat",
 "  EXCEPTIONS OTHERS = 1.",
 "WRITE: / 'SUBRC=', SY-SUBRC.",
]
print(json.dumps({"op":"call","func":"RFC_ABAP_INSTALL_AND_RUN","import":{"PROGRAM":[{"LINE":l} for l in abap]}}))
PY
)
printf '%s\n' "$J" | env SAP_ASHOST="${SAP_ASHOST:-localhost}" SAP_SYSNR="${SAP_SYSNR:-00}" \
  SAP_CLIENT="${SAP_CLIENT:-001}" SAP_USER="${SAP_USER:-DEVELOPER}" SAP_PASSWD="${SAP_PASSWD:-Down1oad}" \
  SAP_LANG=EN LD_LIBRARY_PATH="$LD" "$SAPRFC" 2>/dev/null
