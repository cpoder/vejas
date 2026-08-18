"""Alert Slack when a Stripe payment exceeds 500 EUR.

This file is the whole flow. An agent wrote the first version; the mapping
table below is the part a non-developer reviews (see docs/MAPPINGS.md).
"""

from vejas import flow, emit
from vejas.mapping import apply_mapping

MAPPING = {
    "amount_eur": ["data.object.amount", "cents_to_eur"],
    "currency": ["data.object.currency", "upper"],
    "customer": "data.object.customer",
}

THRESHOLD_EUR = 500


@flow(source="vx.stripe.events")
def stripe_alerts(event):
    m = apply_mapping(event, MAPPING)
    if m["amount_eur"] is not None and m["amount_eur"] > THRESHOLD_EUR:
        emit(
            "vx.slack.out",
            {"text": f"payment {m['amount_eur']:.2f} {m['currency']} from {m['customer']}"},
        )
