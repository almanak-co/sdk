"""What a lending teardown still holds, named per allocation when closure is refused."""

from almanak.gateway.ethereum import DataError


def obligation(observation, held: dict, context: dict | None = None) -> dict | None:
    """The allocation's non-zero `held` amounts, with `context` amounts, or None if none.

    Only `held` decides whether the allocation is closed; `context` adds the same
    position in other units (asset base units beside scaled balances or shares).
    Amounts are decimal strings, so large integers survive any JSON reader.
    """
    remaining = {name: wire(value) for name, value in held.items() if value}
    if not remaining:
        return None
    extra = {name: wire(value) for name, value in (context or {}).items() if value}
    return {
        "allocation": observation.allocation,
        "asset": str(observation.target.asset),
        **remaining,
        **extra,
    }


def wire(value):
    return value if isinstance(value, bool) else str(value)


def require_closed(label: str, rows) -> None:
    """Refuse closure with each allocation's remaining obligations as `details.remaining`."""
    remaining = [row for row in rows if row is not None]
    if not remaining:
        return
    named = "; ".join(
        row["allocation"]
        + " "
        + ", ".join(f"{name} {value}" for name, value in row.items() if name not in IDENTITY)
        for row in remaining
    )
    raise DataError("remaining_obligations", f"{label}: {named}", details={"remaining": remaining})


IDENTITY = ("allocation", "asset")
