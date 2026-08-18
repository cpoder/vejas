# Mappings are data, not code

The one screen business users ever touched in classic integration platforms
was the mapping table. Deleting the builder must not delete them.

The convention: a flow declares its field mappings as a **literal** table:

```python
MAPPING = {
    "amount_eur": ["data.object.amount", "cents_to_eur"],
    "currency":   ["data.object.currency", "upper"],
    "customer":   "data.object.customer",
}
```

Rules are dotted source paths plus optional named transforms
(`vejas/mapping.py` holds the registry). Because the table is a pure literal:

- it is statically extractable: `python -m vejas mappings flows/` dumps every
  table as JSON, which is what a monitoring UI renders as the familiar
  two-column view, filled with live sample values from the traces;
- it is safely correctable: a non-developer edit only ever touches literals
  (paths, transform names, constants), which round-trips into the file as a
  reviewable patch, shown as before/after on real sample events;
- anything beyond its expressiveness is deliberately code, written by the
  agent, reviewed like code.

The workflow for a non-developer: see the mapping with real values, spot the
error, either fix the literal or tell the agent what is wrong in plain
language. Both paths end in the same place, a patch in git that shows the
behavioral diff. They approve behavior; the artifact stays plain code.
