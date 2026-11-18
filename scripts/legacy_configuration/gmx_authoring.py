"""Human quantities normalize once before authoritative GMX allocation validation."""

from almanak.configuration.values import amount

# Archived allocations sized Roles quotas with these fields. Grants are scope-only
# (blueprint 27), so the frozen values are history and the current model refuses them.
RETIRED = (
    "maximum_input",
    "maximum_size_delta_usd_e30",
    "maximum_inputs",
    "maximum_market_token_input",
    "maximum_price_per_base_unit",
    "maximum_collateral_withdrawal_base_units",
)


def normalize(item, assets):
    for field in RETIRED:
        item.pop(field, None)
    item["execution_fee"] = amount(item["execution_fee"], assets)
    if item.get("kind") == "gmx_liquidity":
        normalize_liquidity(item, assets)
    elif item.get("kind") == "gmx_swap":
        for field in ("entry_amount", "entry_minimum_output", "exit_minimum_output"):
            item[field] = amount(item[field], assets)
        item["initial_inventory"] = [amount(value, assets) for value in item["initial_inventory"]]
    else:
        item["entry_collateral"] = amount(item["entry_collateral"], assets)


def normalize_liquidity(item, assets):
    for field in ("minimum_market_tokens", "partial_withdrawal"):
        item[field] = amount(item[field], assets)
    for field in (
        "initial_inventory",
        "entry_amounts",
        "additional_amounts",
        "withdrawal_minimums",
    ):
        item[field] = [amount(value, assets) for value in item[field]]
