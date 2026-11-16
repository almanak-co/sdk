"""Complete fund/Pool risk scope validation shared by compatible Pool connectors."""


def validate_positions(positions, user_configuration):
    first = positions[0]
    expected = first.fund, first.reserve.source.pool, first.reserve.block
    ids, assets = set(), set()
    covered = 0
    for position in positions:
        reserve = position.reserve
        if (position.fund, reserve.source.pool, reserve.block) != expected:
            raise ValueError("pool risk inputs must share fund, Pool and pinned block")
        if position.user_configuration != user_configuration:
            raise ValueError("pool risk observations disagree on the user configuration")
        if reserve.reserve_id in ids or reserve.source.asset in assets:
            raise ValueError("pool risk cannot count the same reserve twice")
        ids.add(reserve.reserve_id)
        assets.add(reserve.source.asset)
        flags = (user_configuration >> (2 * reserve.reserve_id)) & 3
        if bool(flags & 2) != position.collateral_enabled or bool(flags & 1) != bool(
            position.debt_scaled
        ):
            raise ValueError("pool risk positions disagree with the observed user bitmap")
        covered |= 3 << (2 * reserve.reserve_id)
    if user_configuration & ~covered:
        raise ValueError("pool risk is missing an externally held collateral or debt reserve")
