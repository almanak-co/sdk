"""Strategy-configurable teardown config (VIB-5844 / Exp-19 F14).

The framework's token-consolidation dust floor is a hard
``DEFAULT_MIN_SWAP_VALUE_USD = $5`` (``almanak/framework/teardown/config.py``):
residuals at or below it are deliberately never swapped, because the swap gas
would eat the proceeds.

That default is sound for production size, but it is **above the entire small-cap
test regime** ($3-5 per strategy), which makes teardown's token-consolidation
phase a structural no-op for every such run — the residual is simply stranded in
the volatile leg. A strategy that knowingly runs at $4 needs to be able to say
"consolidate anyway, my floor is $0.01".

``TeardownConfig.from_dict`` already existed; these tests pin the wiring that lets
a strategy's own ``config.json`` reach it via ``get_config("teardown")``, and pin
the backwards-compatible default for every strategy that says nothing.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from almanak.framework.runner._teardown_helpers import _teardown_config_from_request
from almanak.framework.teardown.config import DEFAULT_MIN_SWAP_VALUE_USD
from almanak.framework.teardown.models import TeardownAssetPolicy, TeardownRequest


class _Strategy:
    """Minimal stand-in exposing the ``get_config`` surface the helper reads."""

    def __init__(self, cfg: dict[str, Any] | None) -> None:
        self._cfg = cfg

    def get_config(self, key: str, default: Any = None) -> Any:
        if self._cfg is None:
            return default
        return self._cfg.get(key, default)


def _request() -> TeardownRequest:
    return TeardownRequest(
        deployment_id="deployment:test",
        mode="graceful",
        asset_policy=TeardownAssetPolicy.TARGET_TOKEN,
        target_token="USDC",
    )


def test_strategy_can_lower_the_consolidation_dust_floor() -> None:
    """A $4 strategy may opt into a floor below the $5 default (Exp-19 F14)."""
    strategy = _Strategy(
        {"teardown": {"token_consolidation": {"enabled": True, "min_swap_value_usd": "0.01"}}}
    )
    cfg = _teardown_config_from_request(_request(), strategy=strategy)

    assert cfg.token_consolidation.enabled is True
    assert Decimal(str(cfg.token_consolidation.min_swap_value_usd)) == Decimal("0.01")
    # the request's asset policy/target token must still win — the strategy config
    # supplies defaults, it does not override the operator's explicit request
    assert cfg.token_consolidation.target_token == "USDC"


def test_no_strategy_config_keeps_the_default_floor() -> None:
    """Backwards compatibility: say nothing, get exactly today's behaviour."""
    for strategy in (None, _Strategy(None), _Strategy({})):
        cfg = _teardown_config_from_request(_request(), strategy=strategy)
        assert Decimal(str(cfg.token_consolidation.min_swap_value_usd)) == Decimal(
            str(DEFAULT_MIN_SWAP_VALUE_USD)
        ), f"strategy={strategy!r} must fall back to the ${DEFAULT_MIN_SWAP_VALUE_USD} default"


def test_non_dict_teardown_config_is_ignored_not_fatal() -> None:
    """A malformed ``teardown`` key must not crash the teardown path."""
    cfg = _teardown_config_from_request(_request(), strategy=_Strategy({"teardown": "nonsense"}))
    assert Decimal(str(cfg.token_consolidation.min_swap_value_usd)) == Decimal(
        str(DEFAULT_MIN_SWAP_VALUE_USD)
    )


def test_request_none_still_disables_consolidation_regardless_of_strategy_config() -> None:
    """The `request=None` consent rule outranks strategy config.

    Consolidation swaps are wallet-scoped (``amount="all"``); on a shared wallet
    that would sweep sibling strategies' balances. An explicit TeardownRequest is
    the operator's consent for that semantic, so a strategy must not be able to
    grant it to itself by setting `enabled: true` in its own config.
    """
    strategy = _Strategy(
        {"teardown": {"token_consolidation": {"enabled": True, "min_swap_value_usd": "0.01"}}}
    )
    cfg = _teardown_config_from_request(None, strategy=strategy)
    assert cfg.token_consolidation.enabled is False


def test_explicit_request_forces_consolidation_enabled_over_strategy_disabled() -> None:
    """A strategy's ``enabled: false`` must NOT veto an explicit operator request.

    The explicit ``TeardownRequest`` (with a target-token asset policy) is the
    operator's consent to consolidate. Pre-VIB-5844 the request lane always ran
    with ``enabled=True``; letting a strategy's own ``enabled: false`` carry
    through would close positions but strand residual tokens despite the operator
    asking for a target-token teardown (VIB-5844 review, codex P2). Strategy config
    governs the dust FLOOR only — not operator consent. The lowered floor must
    still survive so the small-cap consolidation actually fires.
    """
    strategy = _Strategy(
        {"teardown": {"token_consolidation": {"enabled": False, "min_swap_value_usd": "0.01"}}}
    )
    cfg = _teardown_config_from_request(_request(), strategy=strategy)
    assert cfg.token_consolidation.enabled is True
    assert Decimal(str(cfg.token_consolidation.min_swap_value_usd)) == Decimal("0.01")


def test_strategy_config_with_unknown_keys_does_not_crash_teardown() -> None:
    """Unknown keys under ``teardown`` must not abort the risk-reducing path.

    ``TeardownConfig.from_dict`` ends in ``cls(**data)``, so any extra key raises
    ``TypeError``. Teardown must survive that by falling back to the production
    default rather than crashing and stranding on-chain risk (VIB-5844 review, gemini).
    """
    strategy = _Strategy(
        {
            "teardown": {
                "token_consolidation": {"enabled": True, "min_swap_value_usd": "0.01"},
                "some_custom_metadata": "not a TeardownConfig field",
            }
        }
    )
    cfg = _teardown_config_from_request(_request(), strategy=strategy)
    # malformed → production default floor, teardown still proceeds enabled
    assert cfg.token_consolidation.enabled is True
    assert Decimal(str(cfg.token_consolidation.min_swap_value_usd)) == Decimal(
        str(DEFAULT_MIN_SWAP_VALUE_USD)
    )


def test_null_nested_consolidation_config_is_coerced_not_fatal() -> None:
    """``token_consolidation: null`` must not AttributeError on later attribute writes."""
    strategy = _Strategy({"teardown": {"token_consolidation": None, "chain_consolidation": None}})
    # request present → the code writes cfg.token_consolidation.target_token etc.,
    # which would crash on a None nested config without coercion.
    cfg = _teardown_config_from_request(_request(), strategy=strategy)
    assert cfg.token_consolidation is not None
    assert cfg.chain_consolidation is not None
    assert cfg.token_consolidation.target_token == "USDC"
    assert cfg.token_consolidation.enabled is True


def test_request_none_also_disables_chain_consolidation() -> None:
    """A self-signalled teardown must not bridge assets, even if the strategy asked.

    ``request=None`` is close-only with no operator consent. A strategy config
    enabling cross-chain consolidation must be overridden — bridging without
    consent is a stronger violation than a token sweep (VIB-5844 review, coderabbit).
    """
    strategy = _Strategy(
        {
            "teardown": {
                "token_consolidation": {"enabled": True},
                "chain_consolidation": {"enabled": True, "target_chain": "base"},
            }
        }
    )
    cfg = _teardown_config_from_request(None, strategy=strategy)
    assert cfg.token_consolidation.enabled is False
    assert cfg.chain_consolidation.enabled is False


def test_invalid_dust_floor_falls_back_to_default() -> None:
    """A negative or NaN dust floor must not drive consolidation — reset to the $5 default.

    ``from_dict`` coerces the floor to a Decimal but does not range-check it, so
    ``"-1"`` (every residual becomes swap-worthy) and ``"NaN"`` (breaks the
    ``<= floor`` comparison) parse without raising (VIB-5844 review, coderabbit).
    """
    for bad in ("-1", "-0.01", "NaN", "Infinity"):
        strategy = _Strategy(
            {"teardown": {"token_consolidation": {"enabled": True, "min_swap_value_usd": bad}}}
        )
        cfg = _teardown_config_from_request(_request(), strategy=strategy)
        assert Decimal(str(cfg.token_consolidation.min_swap_value_usd)) == Decimal(
            str(DEFAULT_MIN_SWAP_VALUE_USD)
        ), f"floor={bad!r} must reset to the production default"


def test_valid_low_floor_still_honoured() -> None:
    """A finite non-negative floor below $5 is still accepted (the feature)."""
    for good in ("0", "0.01", "4.99"):
        strategy = _Strategy(
            {"teardown": {"token_consolidation": {"enabled": True, "min_swap_value_usd": good}}}
        )
        cfg = _teardown_config_from_request(_request(), strategy=strategy)
        assert Decimal(str(cfg.token_consolidation.min_swap_value_usd)) == Decimal(good)
