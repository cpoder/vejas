"""Route urgent helpdesk tickets to Slack.

Tickets arriving on vx.helpdesk.tickets carry a French priority label; it is
transcoded to a severity code and only the configured severities are alerted.
"""

from vejas import flow, emit
from vejas.mapping import apply_mapping

# French priority label -> severity code. Complete/correct in the panel.
SEVERITY_CODES = {
    "critique": "P1",
    "haute": "P2",
    "normale": "P3",
    "basse": "P4",
}

# Severities that trigger a Slack alert.
ALERT_SEVERITIES = ["P1", "P2"]

ALERT_CHANNEL = "vx.slack.out"

MAPPING = {
    "severity": ["priority", "lookup:SEVERITY_CODES"],
    "subject": "subject",
    "requester_email": ["requester.email", "lower"],
}


@flow(source="vx.helpdesk.tickets")
def helpdesk_ticket_alerts(event):
    ticket = apply_mapping(event, MAPPING)
    if ticket["severity"] not in ALERT_SEVERITIES:
        return
    emit(
        ALERT_CHANNEL,
        {
            "text": "[{severity}] {subject} - {requester_email}".format(**ticket)
        },
    )
