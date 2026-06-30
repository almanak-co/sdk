"""Unit tests for MorphoBlueAdapter placeholder-pricing guard (VIB-5527 / ALM-2895).

Covers the Defect-C fix: an unauthorized silent placeholder-price fallback must
fail loud on the hosted perimeter, but only at the *point of use* — never in the
constructor, because the lending intent compilers build the adapter purely to
assemble transactions (no pricing) and a constructor raise would break all hosted
morpho_blue SUPPLY/BORROW/REPAY/WITHDRAW compilation.

Cases:

(a) hosted + allow_placeholder_prices=False + no oracle/provider
    → construction SUCCEEDS (no compile-path regression)
    → consuming the placeholder price RAISES ValueError
(b) local  + same → placeholder fallback works, consumption returns 1.0
(c) any mode + allow_placeholder_prices=True → explicit opt-in; consumption returns 1.0
(d) any mode + explicit price_oracle / price_provider → real prices, no placeholder
(e) empty price_provider ({}) is treated as "no provider" (would otherwise price
    every asset at 0 — another silent mis-valuation).

Tests mock is_hosted() from almanak.framework.deployment.mode so no env vars are set.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.connectors.morpho_blue.adapter import MorphoBlueAdapter, MorphoBlueConfig

TEST_WALLET = "0x1234567890123456789012345678901234567890"

_HOSTED_PATH = "almanak.connectors.morpho_blue.adapter.is_hosted"


def _make_config(
    allow_placeholder: bool = False,
    price_provider: dict[str, Decimal] | None = None,
) -> MorphoBlueConfig:
    return MorphoBlueConfig(
        chain="arbitrum",
        wallet_address=TEST_WALLET,
        allow_placeholder_prices=allow_placeholder,
        price_provider=price_provider,
    )


class TestPlaceholderPriceGuard:
    """adapter.py placeholder-price point-of-use guard."""

    def test_hosted_unauthorized_placeholder_construction_succeeds(self) -> None:
        """(a) hosted + allow=False + no oracle → construction does NOT raise.

        Regression guard for the compiler hot path: the four lending compilers
        construct the adapter with default no-price config; a constructor raise
        would take down all hosted morpho_blue lending compilation.
        """
        config = _make_config(allow_placeholder=False)
        with patch(_HOSTED_PATH, return_value=True):
            adapter = MorphoBlueAdapter(config)
        assert adapter._using_placeholder_prices is True
        assert adapter._placeholder_prices_authorized is False

    def test_compiler_style_construction_hosted_does_not_raise(self) -> None:
        """(a) mirrors aave_helpers compiler construction under hosted mode.

        The lending compilers build `MorphoBlueConfig(chain, wallet_address,
        gateway_client=...)` with no price source. This must construct cleanly
        in hosted mode so transaction-building (which never prices) still works.
        """
        config = MorphoBlueConfig(
            chain="arbitrum",
            wallet_address=TEST_WALLET,
            gateway_client=None,
        )
        with patch(_HOSTED_PATH, return_value=True):
            adapter = MorphoBlueAdapter(config)
        assert adapter._using_placeholder_prices is True

    def test_hosted_unauthorized_placeholder_consumption_raises(self) -> None:
        """(a) hosted + unauthorized placeholder → consuming a price RAISES."""
        config = _make_config(allow_placeholder=False)
        with patch(_HOSTED_PATH, return_value=True):
            adapter = MorphoBlueAdapter(config)
            with pytest.raises(ValueError, match="Production deployments must supply"):
                adapter._price_oracle("USDC")

    def test_local_unauthorized_placeholder_consumption_returns_one(self) -> None:
        """(b) local + allow=False + no oracle → placeholder works, returns 1.0."""
        config = _make_config(allow_placeholder=False)
        with patch(_HOSTED_PATH, return_value=False):
            adapter = MorphoBlueAdapter(config)
            assert adapter._using_placeholder_prices is True
            assert adapter._price_oracle("USDC") == Decimal("1.0")

    def test_local_no_oracle_no_provider_emits_warning(self) -> None:
        """(b) local fallback path emits the legacy warning message."""
        config = _make_config(allow_placeholder=False)
        with patch(_HOSTED_PATH, return_value=False):
            with patch("almanak.connectors.morpho_blue.adapter.logger") as mock_log:
                MorphoBlueAdapter(config)
        mock_log.warning.assert_any_call(
            "MorphoBlueAdapter: No price_oracle or price_provider provided. "
            "Using placeholder prices. For production, use create_adapter_with_prices()."
        )

    def test_allow_placeholder_true_local_works(self) -> None:
        """(c) allow_placeholder_prices=True in local mode → authorized placeholder."""
        config = _make_config(allow_placeholder=True)
        with patch(_HOSTED_PATH, return_value=False):
            adapter = MorphoBlueAdapter(config)
        assert adapter._using_placeholder_prices is True
        assert adapter._placeholder_prices_authorized is True
        assert adapter._price_oracle("USDC") == Decimal("1.0")

    def test_allow_placeholder_true_hosted_works(self) -> None:
        """(c) allow_placeholder_prices=True on hosted → explicit opt-in, NOT blocked."""
        config = _make_config(allow_placeholder=True)
        with patch(_HOSTED_PATH, return_value=True):
            adapter = MorphoBlueAdapter(config)
            assert adapter._using_placeholder_prices is True
            assert adapter._placeholder_prices_authorized is True
            # Explicit opt-in: consumption returns the placeholder, does not raise.
            assert adapter._price_oracle("USDC") == Decimal("1.0")

    def test_price_oracle_supplied_bypasses_guard(self) -> None:
        """(d) price_oracle supplied → real oracle; no placeholder, no raise."""
        config = _make_config(allow_placeholder=False)

        def my_oracle(token: str) -> Decimal:
            return Decimal("1.0")

        with patch(_HOSTED_PATH, return_value=True):
            adapter = MorphoBlueAdapter(config, price_oracle=my_oracle)
            assert adapter._using_placeholder_prices is False
            # Uses the supplied oracle, not the guarded placeholder.
            assert adapter._price_oracle("USDC") == Decimal("1.0")

    def test_price_provider_dict_bypasses_guard(self) -> None:
        """(d) config.price_provider dict supplied → real oracle; no placeholder."""
        config = _make_config(
            allow_placeholder=False,
            price_provider={"USDC": Decimal("1"), "wstETH": Decimal("2000")},
        )
        with patch(_HOSTED_PATH, return_value=True):
            adapter = MorphoBlueAdapter(config)
            assert adapter._using_placeholder_prices is False
            assert adapter._price_oracle("wstETH") == Decimal("2000")

    def test_empty_price_provider_hosted_treated_as_missing(self) -> None:
        """(e) empty price_provider {} → treated as no provider; guarded in hosted.

        An empty dict would otherwise price every asset at 0 — a silent
        mis-valuation. It must route to the placeholder path and fail loud.
        """
        config = _make_config(allow_placeholder=False, price_provider={})
        with patch(_HOSTED_PATH, return_value=True):
            adapter = MorphoBlueAdapter(config)
            assert adapter._using_placeholder_prices is True
            assert adapter._placeholder_prices_authorized is False
            with pytest.raises(ValueError, match="Production deployments must supply"):
                adapter._price_oracle("USDC")

    def test_empty_price_provider_local_uses_placeholder(self) -> None:
        """(e) empty price_provider {} in local mode → placeholder, returns 1.0."""
        config = _make_config(allow_placeholder=False, price_provider={})
        with patch(_HOSTED_PATH, return_value=False):
            adapter = MorphoBlueAdapter(config)
            assert adapter._using_placeholder_prices is True
            assert adapter._price_oracle("USDC") == Decimal("1.0")
