"""Economic exchange cash, independent of pool math.

The cumulative `swap` and `deposit` counters record inputs for reports; they bound
nothing (blueprint 27). Inventory is the only limit on an input.
"""

from almanak.models import AssetAmount


def moved(owned, deltas, **changes):
    inventory = tuple(
        AssetAmount(asset=old.asset, amount=old.amount + delta)
        for old, delta in zip((owned.inventory0, owned.inventory1), deltas, strict=True)
    )
    return type(owned)(
        **{**owned.model_dump(), "inventory0": inventory[0], "inventory1": inventory[1], **changes}
    )


def within_inventory(payload, requested):
    inventory = {v.asset: v.amount for v in (payload.owned.inventory0, payload.owned.inventory1)}
    if any(amount.amount > inventory[amount.asset] for amount in requested):
        raise ValueError("economic action exceeds attributed inventory")


def swap_fill(config, intent, payload, record, time, quote):
    within_inventory(payload, (intent.amount,))
    fill = quote(record, intent.target, intent.amount, time)
    if fill.amount_out.amount < intent.minimum_output.amount:
        raise ValueError("modeled exact-input fill fails the retained minimum")
    index = intent.target.assets.index(intent.amount.asset)
    deltas = [0, 0]
    deltas[index], deltas[1 - index] = -intent.amount.amount, fill.amount_out.amount
    owned = moved(
        payload.owned,
        deltas,
        entry_complete=payload.owned.entry_complete
        or intent.amount.asset == intent.target.funding_currency,
    )
    result = payload.model_copy(
        update={
            "owned": owned,
            f"swap{index}": getattr(payload, f"swap{index}") + intent.amount.amount,
        }
    )
    return result, tuple(deltas), (0, 0), 0, "swap", fill
