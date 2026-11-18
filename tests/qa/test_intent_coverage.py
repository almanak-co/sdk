"""Coverage counts completed author results inside their exact declared scopes."""

from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from almanak.caip import ChainId
from almanak.connectors.identifiers import ConnectorId, ProtocolVariant
from almanak.connectors.support import EconomicAction
from tests.qa import intent_coverage


@dataclass(frozen=True)
class Target:
    name: str
    chain: ChainId = ChainId("eip155:1")

    @property
    def fund(self):
        return SimpleNamespace(chain=SimpleNamespace(chain_id=self.chain))

    def model_dump(self, **kwargs):
        return {"name": self.name, "chain": str(self.chain)}


def setup_result(*, status="completed", target=None, kind="close_liquidity"):
    target = Target("configured") if target is None else target
    config = SimpleNamespace(
        positions=[SimpleNamespace(name="main")], target=lambda name: Target("configured")
    )
    result = SimpleNamespace(status=status, intent=SimpleNamespace(target=target, kind=kind))
    return config, result


@pytest.mark.parametrize("status", ("failed", "rejected", "intermediate", "pending"))
def test_only_completed_author_results_count(status):
    config, result = setup_result(status=status)
    assert intent_coverage.direct_results([result], config, ChainId("eip155:1")) == set()


@pytest.mark.parametrize("target", (Target("foreign"), Target("configured", ChainId("eip155:56"))))
def test_foreign_target_or_chain_cannot_establish_coverage(target):
    config, result = setup_result(target=target)
    assert intent_coverage.direct_results([result], config, ChainId("eip155:1")) == set()


def test_undelivered_native_and_economic_results_do_not_count():
    config, result = setup_result()
    native = SimpleNamespace(
        config=config, outcomes=[SimpleNamespace(strategy_result=lambda: result)], delivered=0
    )
    economic = SimpleNamespace(
        completed=[SimpleNamespace(transition=SimpleNamespace(result=result))], delivered=0
    )
    chain = ChainId("eip155:1")
    assert intent_coverage.native_direct(native, chain) == set()
    assert intent_coverage.economic_direct(economic, config, chain) == set()
    native.delivered = economic.delivered = 1
    assert intent_coverage.native_direct(native, chain) == {"close_liquidity"}
    assert intent_coverage.economic_direct(economic, config, chain) == {"close_liquidity"}


def test_nested_close_operation_does_not_establish_direct_close_intent():
    config, result = setup_result(kind="exit_exchange")
    result.operations = [SimpleNamespace(kind="close_liquidity")]
    assert intent_coverage.direct_results([result], config, ChainId("eip155:1")) == {
        "exit_exchange"
    }


def test_variant_matching_remains_connector_owned():
    config, result = setup_result()
    connector, variant = ConnectorId("curve"), ProtocolVariant("stableswap_ng")
    with (
        patch.object(intent_coverage, "target_connector", return_value=connector),
        patch.object(intent_coverage, "matches", return_value=False) as matcher,
    ):
        assert not intent_coverage.direct_results(
            [result], config, ChainId("eip155:1"), connector=connector, variant=variant
        )
        matcher.assert_called_once_with(connector, result.intent.target.model_dump(), variant)


def test_revocation_retains_its_separate_transaction_cleanup_obligation():
    required = intent_coverage.required_direct(EconomicAction)
    assert EconomicAction.REVOKE not in required
    assert EconomicAction.CLOSE_LIQUIDITY in required


@pytest.mark.parametrize("kind", ("swap_exact_input", "close_liquidity"))
def test_another_configured_connector_cannot_fill_the_requested_connectors_coverage(kind):
    from tests.pancakeswap_v3_support import parameters as pancake_parameters
    from tests.uniswap_v3_support import parameters as uniswap_parameters

    pancake = pancake_parameters(lp=True, network="ethereum")
    uniswap = uniswap_parameters(lp=True, fund=pancake.accounts.bindings[0].profile.fund)
    targets = {
        name: config.target(config.allocations[0].name).model_copy(update={"allocation": name})
        for name, config in (("pancake", pancake), ("uniswap", uniswap))
    }
    config = SimpleNamespace(
        positions=[SimpleNamespace(name=name) for name in targets], target=targets.__getitem__
    )
    assert {target.fund.chain.chain_id for target in targets.values()} == {ChainId("eip155:1")}
    result = SimpleNamespace(
        status="completed", intent=SimpleNamespace(target=targets["uniswap"], kind=kind)
    )
    requested = ConnectorId("pancakeswap_v3")
    assert not intent_coverage.direct_results(
        [result], config, ChainId("eip155:1"), connector=requested
    )
    result.intent.target = targets["pancake"]
    assert intent_coverage.direct_results(
        [result], config, ChainId("eip155:1"), connector=requested
    ) == {kind}
