"""A composed decision's readings: each part connector's history, keyed by its allocation."""

from almanak.dashboard.history import prefixes
from almanak.dashboard.models import HistoryRow

MAX_FIELDS = 32


def part_rows(state, observed):
    """`(allocation, row)` from each part connector with a history reader, one per allocation:
    the part reads a view narrowed to that allocation and its own observation."""
    from almanak.connectors.composed.lifecycle import component_state
    from almanak.connectors.composition import connector_from_wire

    for identity in getattr(getattr(state, "config", None), "parameter_components", ()):
        binding = connector_from_wire({"connector": str(identity)})
        reader = getattr(binding.projection, "history", None)
        if reader is None:
            continue
        view = component_state(state, identity)
        for owned in view.owned:
            target = owned.target
            mine = tuple(o for o in observed if getattr(o, "target", None) == target)
            if not mine:
                continue
            narrowed = view.model_copy(
                update={
                    name: tuple(item for item in getattr(view, name) if item.target == target)
                    for name in ("owned", "baseline", "closure")
                }
            )
            row = reader(narrowed, mine)
            if row is not None:
                yield target.allocation, row


def history_row(state, observed) -> HistoryRow | None:
    """Every part's reading prefixed `<allocation>:` (`liquidity:price`), at most 32 fields in
    part order; time and block are the latest part reading's. Lending legs are read together
    (`loan:debt`, `collateral:health`, and the strategy's `value`/`hold`), since collateral and
    debt share one account."""
    from almanak.connectors.lending_dashboard import family, lending_fields, texts

    observed = tuple(observed or ())
    owned = getattr(state, "owned", ())
    lending = [o for o in observed if family(o)]
    rows = list(part_rows(state, tuple(o for o in observed if not family(o))))
    fields: dict[str, str] = {}
    if lending:
        baseline = [o for o in state.baseline if family(o)]
        lent = lending_fields(
            lending,
            baseline,
            prefixed=True,
            required=[o.target.allocation for o in owned if family(o)],
            allocations=[o.target.allocation for o in owned],
        )
        # With other parts beside the loans, the lending total is only part of the strategy.
        if any(not family(o) for o in owned):
            lent = {("lending_" + k if k in ("value", "hold") else k): v for k, v in lent.items()}
        fields |= texts(lent)
        block = lending[0].block
        rows.append(("", HistoryRow(time=block.timestamp, block=block.number, fields={})))
    if not rows or (not fields and not any(row.fields for _, row in rows)):
        return None
    # Lending legs were prefixed first, so the same names keep the same prefixes here.
    names = prefixes([o.target.allocation for o in owned] + [a for a, _ in rows if a])
    for allocation, row in rows:
        if not allocation:
            continue
        prefix = names[allocation]
        for key, value in row.fields.items():
            if len(fields) < MAX_FIELDS:
                fields[f"{prefix}:{key}"[:100]] = value
    latest = max((row for _, row in rows), key=lambda row: (row.block, row.time))
    return HistoryRow(time=latest.time, block=latest.block, fields=fields)
