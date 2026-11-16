"""Independent checks for banded V3 references: positions stay inside the band."""

from types import SimpleNamespace

from almanak.connectors.composition import connector_for
from almanak.gateway.ethereum import EthereumReader
from tests.strategy_fork.aave_chain_assertions import settled
from tests.strategy_fork.v3_pool_assertions import (
    assert_closed_position,
    assert_payer,
    assert_successful_outcomes,
    assert_wallet_effects,
    position_effects,
)


def entered(state):
    return settled(state) and all(map(holding_band, state.owned))


def holding_band(owned):
    return owned.entry_complete and getattr(owned.position, "range", None) is not None


def assert_holding(node, state):
    assert (entered(state), state.mode) == (True, "running")
    assert_successful_outcomes(state)
    snapshot = connector_for(state.config).execution.snapshot
    with EthereumReader(state.owned[0].target.fund.chain, node.rpc.endpoint) as reader:
        block = reader.pin_block("latest")
        backend = SimpleNamespace(config=state.config, reader=reader)
        for owned in state.owned:
            observed = snapshot(backend, owned.target.allocation, block, owned, quotes=False)
            assert (observed.wallet0, observed.wallet1, observed.position) == (
                owned.inventory0,
                owned.inventory1,
                owned.position,
            )
            selected = owned.position.range
            assert selected.lower <= observed.pool.tick < selected.upper


def assert_closed(node, state):
    assert (state.mode, settled(state)) == ("closed", True)
    assert_successful_outcomes(state)
    for owned, before, after in zip(state.owned, state.baseline, state.closure, strict=True):
        assert_closed_position(owned, after)
        effects = position_effects(state, owned.target)
        assert_wallet_effects(effects, before, after)
        assert_payer(state, owned.target, before, after, effects)
        assert_band_operations(owned.target, effects)


def assert_band_operations(target, effects):
    assert_recentring_cycles(effects)
    for outcome in by_kind(effects, "mint", "close"):
        target.band.validate_range(outcome.operation.range, target.pool.tick_spacing)
    assert_funding_round_trip(target, by_kind(effects, "swap"))


def by_kind(effects, *kinds):
    return [o for o in effects if o.operation.kind in kinds]


def assert_recentring_cycles(effects):
    kinds = [o.operation.kind for o in effects if o.operation.kind not in ("approve", "swap")]
    assert kinds and kinds == ["mint", "close"] * (len(kinds) // 2)


def assert_funding_round_trip(target, swaps):
    field = ("delta0", "delta1")[target.assets.index(target.funding_asset)]
    assert getattr(swaps[0], field) < 0 < getattr(swaps[-1], field)
