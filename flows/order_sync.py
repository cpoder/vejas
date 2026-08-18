"""Sync paid shop orders into the ERP queue.

The MAPPING table and the UPPERCASE constants below are the business
surface: they show up in the panel, where a domain expert can correct
them without touching the rest of this file.
"""

from vejas import flow, emit
from vejas.mapping import apply_mapping

MAPPING = {
    "order_id": "id",
    "customer_email": ["email", "lower"],
    "total_eur": ["total_price", "float"],
    "currency": ["currency", "upper"],
    "country": "shipping_address.country_code",
}

MIN_TOTAL_EUR = 0
ERP_QUEUE = "vx.erp.orders.upsert"


@flow(source="vx.shop.orders")
def order_sync(order):
    m = apply_mapping(order, MAPPING)
    if m["total_eur"] is not None and m["total_eur"] >= MIN_TOTAL_EUR:
        emit(ERP_QUEUE, m)
