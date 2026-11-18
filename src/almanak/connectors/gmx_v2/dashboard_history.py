"""GMX readings at each decision for the dashboard history: only what the snapshot measured.

Mark price, PnL, leverage and liquidation price are not observed, so they are never read here.
"""

from decimal import Decimal, localcontext

from almanak.dashboard.history import decimal_text
from almanak.dashboard.models import HistoryRow

USD_E30 = Decimal(10) ** 30


def readings(observation, decimals) -> dict[str, str] | None:
    """One GMX snapshot as decimal readings, in token units by `decimals(asset)`:
    `wallet0`.. in the allocation's inventory order. A position snapshot adds `size_usd`,
    `size_tokens`, `collateral`, `entry_price` (USD per index token: size_usd over size_tokens,
    the average entry), and `funding_long`/`funding_short` (claimable funding in the market's
    long and short assets); `size_usd` and `collateral` read 0 with no open position. A GM
    liquidity snapshot adds `gm_shares`. A reading whose asset decimals are unknown is left out."""
    fields: dict[str, Decimal] = {}
    with localcontext() as context:
        context.prec = 60

        def amount(value):
            places = decimals(value.asset)
            return None if places is None else Decimal(value.amount) / Decimal(10) ** places

        def put(key, value):
            if value is not None:
                fields[key] = value

        for index, wallet in enumerate(observation.wallets):
            put(f"wallet{index}", amount(wallet))
        if hasattr(observation, "market_tokens"):
            put("gm_shares", amount(observation.market_tokens))
        elif hasattr(observation, "claimable_funding"):
            long, short = observation.claimable_funding
            put("funding_long", amount(long))
            put("funding_short", amount(short))
            position = observation.position
            if position is None:
                fields |= {"size_usd": Decimal(0), "collateral": Decimal(0)}
            else:
                size = Decimal(position.size_usd_e30) / USD_E30
                tokens = amount(position.index_amount)
                fields["size_usd"] = size
                put("size_tokens", tokens)
                put("collateral", amount(position.collateral))
                if tokens:
                    fields["entry_price"] = size / tokens
        return {key: decimal_text(value) for key, value in fields.items()} or None


def history_row(state, observed) -> HistoryRow | None:
    """The decision's reading of the first owned GMX allocation, for the dashboard history."""

    def decimals(asset):
        metadata = state.config.asset_metadata.get(asset)
        return metadata.decimals if metadata else None

    targets = [owned.target for owned in state.owned]
    for observation in observed or ():
        if getattr(observation, "target", None) not in targets:
            continue
        fields = readings(observation, decimals)
        if fields is None:
            return None
        block = observation.block
        return HistoryRow(time=block.timestamp, block=block.number, fields=fields)
    return None
