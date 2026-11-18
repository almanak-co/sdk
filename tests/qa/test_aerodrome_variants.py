"""Stable and volatile pool targets retain distinct native and historical obligations."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from almanak.connectors.aerodrome.connector import connector
from almanak.connectors.aerodrome.economic_data import MODEL
from almanak.connectors.aerodrome.models import Parameters
from almanak.connectors.identifiers import ConnectorId, ProtocolVariant
from almanak.connectors.support import EconomicAction, RunMode
from almanak.models import StrategyAccounts
from tests.aerodrome_support import parameters
from tests.qa.intent_coverage import economic_direct, native_direct


def configured_variants():
    configs = {name: parameters(name, lp=True) for name in ("stable", "volatile")}
    first = configs["stable"]
    return Parameters(
        decision_trigger=first.decision_trigger,
        asset_metadata={
            key: value for c in configs.values() for key, value in c.asset_metadata.items()
        },
        accounts=StrategyAccounts(
            bindings=tuple(
                c.accounts.bindings[0].model_copy(update={"name": name})
                for name, c in configs.items()
            )
        ),
        allocations=tuple(
            c.allocations[0].model_copy(update={"name": name, "account": name})
            for name, c in configs.items()
        ),
    )


@pytest.mark.parametrize("kind", ("swap_exact_input", "close_liquidity", "collect_liquidity_fees"))
def test_configured_other_invariant_cannot_satisfy_native_or_prepared_coverage(kind):
    config = configured_variants()
    chain, identity = ChainId("eip155:8453"), ConnectorId("aerodrome")
    assert {config.target(p.name).fund.chain.chain_id for p in config.positions} == {chain}
    for observed, missing in (("stable", "volatile"), ("volatile", "stable")):
        result = SimpleNamespace(
            status="completed", intent=SimpleNamespace(target=config.target(observed), kind=kind)
        )
        native = SimpleNamespace(
            config=config,
            outcomes=[SimpleNamespace(strategy_result=lambda result=result: result)],
            delivered=1,
        )
        economic = SimpleNamespace(
            completed=[SimpleNamespace(transition=SimpleNamespace(result=result))], delivered=1
        )
        assert native_direct(
            native, chain, connector=identity, variant=ProtocolVariant(observed)
        ) == {kind}
        assert economic_direct(
            economic, config, chain, connector=identity, variant=ProtocolVariant(observed)
        ) == {kind}
        assert not native_direct(
            native, chain, connector=identity, variant=ProtocolVariant(missing)
        )
        assert not economic_direct(
            economic, config, chain, connector=identity, variant=ProtocolVariant(missing)
        )


def test_runtime_and_historical_support_require_each_actual_pool_variant():
    config = configured_variants()
    binding = connector()
    chain = ChainId("eip155:8453")
    support = binding.support_for(chain)
    assert set(support.variants) == {ProtocolVariant("stable"), ProtocolVariant("volatile")}
    assert support.require_model(MODEL).variants == support.variants
    for position in config.positions:
        target = config.target(position.name)
        assert binding.variant(target) == ProtocolVariant(position.name)
        for mode in (RunMode.FORK, RunMode.BACKTEST):
            selected_model = MODEL if mode == RunMode.BACKTEST else None
            binding.require_target(
                chain, target, mode, EconomicAction.SWAP, "eoa", model=selected_model
            )
            reduced = support.model_copy(update={"variants": (ProtocolVariant("different"),)})
            if mode == RunMode.BACKTEST:
                historical = support.require_model(MODEL).model_copy(
                    update={"variants": (ProtocolVariant("different"),)}
                )
                reduced = reduced.model_copy(update={"historical": (historical,)})
            with pytest.raises(ValueError, match="protocol variant"):
                replace(binding, chain_support=(reduced,)).require_target(
                    chain, target, mode, EconomicAction.SWAP, "eoa", model=selected_model
                )
