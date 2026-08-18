"""Sync paid shop orders into the ERP queue.

The shop sends country names ("France"); the ERP wants ISO codes ("FR"):
COUNTRY_CODES is the transcoding table. Order lines are projected
element-wise into ERP positions via the `each` construct. Both are part of
the business surface: seeded by the agent, corrected by the domain expert
in the panel.
"""

from vejas import flow, emit
from vejas.mapping import apply_mapping

COUNTRY_CODES = {
    "France": "FR",
    "Germany": "DE",
    "Italy": "IT",
    "Portugal": "PT",
    "Netherlands": "NL",
}

MAPPING = {
    "order_ref": ["id", "split:#:1"],
    "customer_email": ["email", "lower"],
    "total_eur": ["total_price", "float"],
    "country": ["shipping_address.country", "lookup:COUNTRY_CODES"],
    "positions": {
        "each": "line_items",
        "map": {
            "sku": "sku",
            "qty": ["quantity", "int"],
            "unit_eur": ["unit_price_cents", "cents_to_eur"],
        },
    },
}

MIN_TOTAL_EUR = 0
ERP_QUEUE = "vx.erp.orders.upsert"


@flow(source="vx.shop.orders")
def order_sync(order):
    m = apply_mapping(order, MAPPING)
    if m["total_eur"] is not None and m["total_eur"] >= MIN_TOTAL_EUR:
        emit(ERP_QUEUE, m)
