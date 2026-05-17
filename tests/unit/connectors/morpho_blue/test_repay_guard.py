"""Tests for Morpho Blue repay guards.

VIB-648: Morpho Blue panics (0x11 underflow) if repay amount > actual on-chain
debt. The adapter caps asset-based repay amounts at the actual debt.

VIB-4531: the cap path silently produces ``repay(0, 0)`` calldata when the
SDK returns ``actual_debt_wei=0`` (stale or wrong-owner view) but the
on-chain position still has ``borrow_shares > 0``. ``repay(0, 0)`` violates
Morpho's ``exactlyOneZero(assets, shares)`` invariant and reverts with
``INCONSISTENT_INPUT``. Caught on the 2026-05-17 post-merge Morpho teardown
(intent 2 of 7 always reverts; full details in
``docs/internal/MorphoStatusMay17.md``).
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.morpho_blue.adapter import (
    MORPHO_BLUE_ADDRESSES,
    MORPHO_MARKETS,
    MORPHO_REPAY_SELECTOR,
    MorphoBlueAdapter,
    MorphoBlueConfig,
    MorphoBluePosition,
)

WSTETH_USDC_MARKET_ID = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"


def _extract_repay_assets(calldata: str) -> int:
    """Extract the assets_wei value from encoded repay calldata."""
    # Layout: selector(10) + MarketParams(5*64) + assets(64) + ...
    payload = calldata[len(MORPHO_REPAY_SELECTOR) :]
    assets_offset = 64 * 5  # 5 MarketParams slots
    return int(payload[assets_offset : assets_offset + 64], 16)


def _extract_repay_shares(calldata: str) -> int:
    """Extract the shares_wei value from encoded repay calldata.

    Layout: selector(10) + MarketParams(5*64) + assets(64) + shares(64) + …
    """
    payload = calldata[len(MORPHO_REPAY_SELECTOR) :]
    shares_offset = 64 * 6  # 5 MarketParams slots + assets
    return int(payload[shares_offset : shares_offset + 64], 16)


@pytest.fixture
def adapter() -> MorphoBlueAdapter:
    """Create Morpho Blue adapter for Ethereum."""
    config = MorphoBlueConfig(
        chain="ethereum",
        wallet_address="0x1234567890123456789012345678901234567890",
    )
    return MorphoBlueAdapter(config)


class TestRepayOverRepayGuard:
    """Tests for the over-repay guard that prevents 0x11 underflow."""

    def test_repay_caps_at_actual_debt(self, adapter: MorphoBlueAdapter) -> None:
        """Repay amount exceeding actual debt should be capped."""
        adapter._sdk_enabled = True
        adapter._sdk = MagicMock()
        # Actual debt: 500 USDC (500 * 1e6 = 500_000_000 wei)
        adapter._sdk.get_borrow_assets.return_value = 500_000_000

        # Request 505 USDC (exceeds debt)
        result = adapter.repay(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("505"),
        )
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["data"].startswith(MORPHO_REPAY_SELECTOR)

        # Verify the repay amount was capped to actual debt (500 USDC = 500_000_000 wei)
        assert _extract_repay_assets(result.tx_data["data"]) == 500_000_000

        # Verify SDK was called to check debt
        adapter._sdk.get_borrow_assets.assert_called_once()

    def test_repay_no_cap_when_under_debt(self, adapter: MorphoBlueAdapter) -> None:
        """Repay amount under actual debt should pass through unchanged."""
        adapter._sdk_enabled = True
        adapter._sdk = MagicMock()
        adapter._sdk.get_borrow_assets.return_value = 500_000_000  # 500 USDC

        result = adapter.repay(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("400"),
        )
        assert result.success is True
        assert result.tx_data is not None

        # Verify the repay amount is the original requested amount (400 USDC = 400_000_000 wei)
        assert _extract_repay_assets(result.tx_data["data"]) == 400_000_000

    def test_repay_sdk_error_proceeds_with_requested_amount(self, adapter: MorphoBlueAdapter) -> None:
        """SDK error during debt query should not block repay."""
        adapter._sdk_enabled = True
        adapter._sdk = MagicMock()
        adapter._sdk.get_borrow_assets.side_effect = Exception("RPC error")

        result = adapter.repay(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("505"),
        )
        assert result.success is True
        assert result.tx_data is not None

        # Verify the original amount is used when SDK query fails (505 USDC = 505_000_000 wei)
        assert _extract_repay_assets(result.tx_data["data"]) == 505_000_000

    def test_repay_sdk_disabled_skips_guard(self, adapter: MorphoBlueAdapter) -> None:
        """When SDK is disabled, repay should proceed without guard."""
        adapter._sdk_enabled = False

        result = adapter.repay(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("505"),
        )
        assert result.success is True
        assert result.tx_data is not None

        # Verify the original amount is used when SDK is disabled (505 USDC = 505_000_000 wei)
        assert _extract_repay_assets(result.tx_data["data"]) == 505_000_000

    def test_repay_shares_mode_skips_guard(self, adapter: MorphoBlueAdapter) -> None:
        """Shares-mode repay should not trigger the guard."""
        adapter._sdk_enabled = True
        adapter._sdk = MagicMock()

        result = adapter.repay(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("1000000000000000000"),
            shares_mode=True,
        )
        assert result.success is True
        # SDK should NOT be called for shares-mode repay
        adapter._sdk.get_borrow_assets.assert_not_called()


class TestRepayCapToZero:
    """VIB-4531: cap-to-actual-debt must not silently produce repay(0, 0).

    When ``sdk.get_borrow_assets`` returns 0 but ``borrow_shares > 0`` on
    chain (the stale-view shape that fires in production), the original
    cap rewrote ``assets_wei = 0`` while ``shares_wei`` was already 0,
    producing ``repay(0, 0, …)`` calldata. Morpho's ``exactlyOneZero``
    invariant rejects with ``INCONSISTENT_INPUT``.

    Fix design: when the cap would force assets_wei=0, consult the
    on-chain position. If borrow_shares > 0, fall back to shares-mode
    repay (assets=0, shares=borrow_shares). If on-chain agrees there's
    no debt, refuse the call up front with an actionable error rather
    than ship invalid calldata.
    """

    def test_sdk_returns_zero_does_not_emit_inconsistent_calldata(
        self, adapter: MorphoBlueAdapter
    ) -> None:
        """The production failure mode: SDK reports debt=0 (stale view) but
        real on-chain debt exists. Adapter must NOT emit (assets=0, shares=0)
        — the (0, 0) shape violates Morpho's ``exactlyOneZero`` invariant.

        VIB-4531 contract (revised audit PR #2343 — CI repro): skip the cap
        and proceed with the caller's originally requested amount. Three
        possible outcomes from on-chain Morpho, all preferable to silent
        (0, 0): SDK was wrong + real debt covers (accept), SDK was wrong +
        real debt is smaller (clean Morpho underflow revert), SDK was right
        (clean Morpho no-debt revert). The earlier "refuse at compile-time"
        shape broke synthetic-intent discovery for the Zodiac manifest
        builder (synthetic owners have no debt by construction; refusing
        dropped the repay selector from manifests).
        """
        adapter._sdk_enabled = True
        adapter._sdk = MagicMock()
        adapter._sdk.get_borrow_assets.return_value = 0  # stale view

        position = MorphoBluePosition(
            market_id=WSTETH_USDC_MARKET_ID,
            borrow_shares=Decimal(10**18),
            collateral=Decimal(int(0.014 * 10**18)),
        )
        # get_position_on_chain is no longer called from this branch (the
        # refuse-at-compile-time draft used it); patch it anyway as a guard.
        with patch.object(adapter, "get_position_on_chain", return_value=position):
            result = adapter.repay(
                market_id=WSTETH_USDC_MARKET_ID,
                amount=Decimal("9.45758900781777"),
            )

        # Calldata is produced with the originally-requested assets value.
        # shares_wei stays 0 (assets-mode); the assets value carries the
        # request through to Morpho which decides whether it's valid.
        assert result.success is True
        assert result.tx_data is not None
        assets_wei = _extract_repay_assets(result.tx_data["data"])
        shares_wei = _extract_repay_shares(result.tx_data["data"])
        # 9.45758900781777 USDC at 6 decimals = 9_457_589 wei (after Decimal
        # quantization via int() — see _MorphoBlueAdapter.repay).
        assert assets_wei == int(Decimal("9.45758900781777") * Decimal(10**6))
        assert shares_wei == 0
        # exactlyOneZero invariant holds.
        assert (assets_wei == 0) != (shares_wei == 0)

    def test_sdk_returns_below_amount_caps_to_actual_nonzero(
        self, adapter: MorphoBlueAdapter
    ) -> None:
        """Sanity / regression: when SDK returns 5 USDC and caller asks 9,
        the cap still fires and produces (assets=5e6, shares=0). The
        VIB-4531 fix only kicks in when ``actual_debt_wei == 0``."""
        adapter._sdk_enabled = True
        adapter._sdk = MagicMock()
        adapter._sdk.get_borrow_assets.return_value = 5_000_000  # 5 USDC

        result = adapter.repay(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("9.0"),
        )
        assert result.success is True
        assets_wei = _extract_repay_assets(result.tx_data["data"])
        shares_wei = _extract_repay_shares(result.tx_data["data"])
        assert assets_wei == 5_000_000
        assert shares_wei == 0

    def test_repay_all_path_unaffected_by_zero_cap_fix(
        self, adapter: MorphoBlueAdapter
    ) -> None:
        """Regression: ``repay_all=True`` (the working shares-only path)
        must continue to work. It calls ``adapter.get_position_on_chain``
        (not ``sdk.get_borrow_assets``) — different code path; VIB-4531
        must not touch it.

        Also verifies that ``sdk.get_borrow_assets`` is never invoked on
        this path — a regression that started querying it (e.g. by mis-
        routing the cap into the ``repay_all`` branch) would still
        produce the right calldata here but would burn an extra RPC and
        change error semantics. CodeRabbit review on PR #2343."""
        adapter._sdk_enabled = True
        adapter._sdk = MagicMock()

        position = MorphoBluePosition(
            market_id=WSTETH_USDC_MARKET_ID,
            borrow_shares=Decimal(28 * 10**18),
            collateral=Decimal(int(0.02 * 10**18)),
        )
        with patch.object(adapter, "get_position_on_chain", return_value=position):
            result = adapter.repay(
                market_id=WSTETH_USDC_MARKET_ID,
                amount=Decimal("0"),
                repay_all=True,
            )
        assert result.success is True
        assets_wei = _extract_repay_assets(result.tx_data["data"])
        shares_wei = _extract_repay_shares(result.tx_data["data"])
        assert assets_wei == 0
        assert shares_wei == 28 * 10**18
        # repay_all uses the on-chain position directly; the SDK debt
        # query is only on the partial-assets path. Guard against a
        # future refactor that conflates them.
        adapter._sdk.get_borrow_assets.assert_not_called()

    def test_sdk_zero_and_onchain_zero_proceeds_with_requested_amount(
        self, adapter: MorphoBlueAdapter
    ) -> None:
        """When the SDK reports debt=0, the adapter no longer consults the
        on-chain position at compile time — it just skips the cap and ships
        the originally-requested ``assets_wei`` (see VIB-4531 revised
        contract in the sibling test). If on-chain truly has no debt,
        Morpho will revert at execute-time with a clear no-debt error,
        which is the right place to surface that state. Compile-time
        refusal broke synthetic-intent discovery (see CI repro in audit
        PR #2343)."""
        adapter._sdk_enabled = True
        adapter._sdk = MagicMock()
        adapter._sdk.get_borrow_assets.return_value = 0

        # No need to patch get_position_on_chain — the revised adapter
        # doesn't call it from this branch.
        result = adapter.repay(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("9.0"),
        )
        assert result.success is True
        assert result.tx_data is not None
        assets_wei = _extract_repay_assets(result.tx_data["data"])
        shares_wei = _extract_repay_shares(result.tx_data["data"])
        assert assets_wei == 9_000_000  # 9 USDC at 6 decimals
        assert shares_wei == 0
