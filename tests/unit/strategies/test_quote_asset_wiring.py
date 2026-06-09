"""Integration tests for the ``quote_asset`` strategy declaration (PR-1).

Covers the wiring that connects the :class:`QuoteAsset` value type to the places
a strategy author / operator declares it and the places the hosted platform
reads it back:

* ``@almanak_strategy(quote_asset=...)`` -> ``StrategyMetadata.quote_asset``
* ``StrategyConfig`` load-time validation / normalisation
* ``IntentStrategy`` boot resolution (decorator default), per-deployment override,
  and ``to_dict()`` exposure
* ``DemoSpec.quote_asset`` (config.json override else decorator default)

Definition-only: there is deliberately no valuation/accounting behaviour to assert.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from almanak.config.strategy import StrategyConfig
from almanak.core.models.quote_asset import QuoteAsset
from almanak.framework.demos.spec import DemoSpec, QaConfig
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.strategies.metadata import StrategyMetadata

WETH_ETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
WETH_ARB = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"


class _MinimalConfig:
    """Smallest config object IntentStrategy.__init__ accepts (needs ``to_dict``)."""

    def to_dict(self) -> dict:
        return {}


def _make_strategy(name: str, **decorator_kwargs):
    @almanak_strategy(name=name, supported_chains=["ethereum"], **decorator_kwargs)
    class _Strategy(IntentStrategy):
        def decide(self, market):
            return None

        def get_open_positions(self):
            return []

        def generate_teardown_intents(self, mode, market=None):
            return []

    return _Strategy


# --------------------------------------------------------------------------- #
# Decorator -> StrategyMetadata
# --------------------------------------------------------------------------- #


def test_metadata_defaults_to_usd_when_omitted():
    meta = StrategyMetadata(name="m")
    assert meta.quote_asset == QuoteAsset.usd()
    assert meta.to_dict()["quote_asset"] == {"type": "fiat_usd"}


def test_decorator_omitted_is_usd():
    cls = _make_strategy("qa_wire_omitted")
    assert cls.STRATEGY_METADATA.quote_asset.is_usd


@pytest.mark.parametrize(
    "value, expected",
    [
        ("USD", QuoteAsset.usd()),
        ({"type": "token", "chain_id": 42161, "address": WETH_ARB}, QuoteAsset.token(42161, WETH_ARB)),
        (QuoteAsset.token(1, WETH_ETH), QuoteAsset.token(1, WETH_ETH)),
    ],
)
def test_decorator_accepts_str_dict_and_typed(value, expected):
    cls = _make_strategy("qa_wire_" + str(abs(hash(str(value)))), quote_asset=value)
    assert cls.STRATEGY_METADATA.quote_asset == expected


def test_metadata_to_dict_exposes_token_quote_asset():
    cls = _make_strategy("qa_wire_to_dict", quote_asset=QuoteAsset.token(1, WETH_ETH))
    assert cls.STRATEGY_METADATA.to_dict()["quote_asset"] == {
        "type": "token",
        "chain_id": 1,
        "address": WETH_ETH.lower(),
    }


# --------------------------------------------------------------------------- #
# StrategyConfig (config.json) validation / normalisation
# --------------------------------------------------------------------------- #


def test_config_normalises_usd_and_token():
    assert StrategyConfig.model_validate({"quote_asset": "USD"}).quote_asset == {"type": "fiat_usd"}
    cfg = StrategyConfig.model_validate(
        {"quote_asset": {"type": "token", "chain_id": 42161, "address": WETH_ARB}}
    )
    assert cfg.quote_asset == {"type": "token", "chain_id": 42161, "address": WETH_ARB.lower()}


def test_config_omitted_is_none():
    # None -> the loader emits nothing -> the decorator default applies downstream.
    assert StrategyConfig.model_validate({"chain": "ethereum"}).quote_asset is None


def test_config_rejects_chain_name_string():
    with pytest.raises(ValueError, match="chain_id"):
        StrategyConfig.model_validate(
            {"quote_asset": {"type": "token", "chain": "arbitrum", "address": WETH_ARB}}
        )


# --------------------------------------------------------------------------- #
# IntentStrategy boot resolution / override / exposure
# --------------------------------------------------------------------------- #


def _instantiate(cls):
    return cls(config=_MinimalConfig(), chain="ethereum", wallet_address="0x" + "00" * 20)


def test_instance_resolves_decorator_default():
    cls = _make_strategy("qa_wire_inst", quote_asset=QuoteAsset.token(1, WETH_ETH))
    strat = _instantiate(cls)
    assert strat.quote_asset == QuoteAsset.token(1, WETH_ETH)
    assert strat.to_dict()["quote_asset"] == {"type": "token", "chain_id": 1, "address": WETH_ETH.lower()}


def test_instance_defaults_to_usd():
    strat = _instantiate(_make_strategy("qa_wire_inst_usd"))
    assert strat.quote_asset.is_usd
    assert strat.to_dict()["quote_asset"] == {"type": "fiat_usd"}


def test_config_override_applies_and_none_keeps_current():
    cls = _make_strategy("qa_wire_override", quote_asset=QuoteAsset.token(1, WETH_ETH))
    strat = _instantiate(cls)
    strat.apply_quote_asset_override({"type": "token", "chain_id": 42161, "address": WETH_ARB})
    assert strat.quote_asset == QuoteAsset.token(42161, WETH_ARB)
    # None is a no-op (keeps the already-resolved value).
    strat.apply_quote_asset_override(None)
    assert strat.quote_asset == QuoteAsset.token(42161, WETH_ARB)


def test_quote_asset_frozen_across_hot_reload():
    cls = _make_strategy("qa_wire_freeze", quote_asset=QuoteAsset.token(1, WETH_ETH))
    strat = _instantiate(cls)
    # The resolved quote asset is boot-frozen: it is NOT part of the hot-reloadable
    # config surface, so a config update cannot change it.
    result = strat.update_config({"quote_asset": {"type": "token", "chain_id": 42161, "address": WETH_ARB}})
    assert not result.success  # quote_asset is not a hot-reloadable field
    assert strat.quote_asset == QuoteAsset.token(1, WETH_ETH)


# --------------------------------------------------------------------------- #
# CLI run-helper seam: _instantiate_strategy applies the config override
# --------------------------------------------------------------------------- #


def test_instantiate_strategy_applies_config_override():
    from types import SimpleNamespace

    from almanak.framework.cli.run_helpers import _instantiate_strategy

    cls = _make_strategy("qa_wire_run_helpers", quote_asset="USD")  # decorator default USD
    runtime = SimpleNamespace(chain="ethereum", execution_address="0x" + "00" * 20)
    strat = _instantiate_strategy(
        strategy_class=cls,
        strategy_config={"quote_asset": {"type": "token", "chain_id": 42161, "address": WETH_ARB}},
        runtime_config=runtime,
        multi_chain=False,
        strategy_chains=["ethereum"],
        chain_wallets={},
    )
    # config.json override beat the decorator's USD default through the boot seam.
    assert strat.quote_asset == QuoteAsset.token(42161, WETH_ARB)


# --------------------------------------------------------------------------- #
# DemoSpec exposure
# --------------------------------------------------------------------------- #


def _demo_spec(metadata: StrategyMetadata, config: dict) -> DemoSpec:
    return DemoSpec(
        name="d",
        directory=Path("."),
        metadata=metadata,
        config=config,
        qa=QaConfig.from_mapping(None),
        sidecar=None,
    )


def test_demospec_uses_decorator_default():
    spec = _demo_spec(StrategyMetadata(name="d", quote_asset=QuoteAsset.token(1, WETH_ETH)), {})
    assert spec.quote_asset == {"type": "token", "chain_id": 1, "address": WETH_ETH.lower()}


def test_demospec_config_override_wins():
    spec = _demo_spec(
        StrategyMetadata(name="d"),  # decorator default USD
        {"quote_asset": {"type": "token", "chain_id": 42161, "address": WETH_ARB}},
    )
    assert spec.quote_asset == {"type": "token", "chain_id": 42161, "address": WETH_ARB.lower()}
