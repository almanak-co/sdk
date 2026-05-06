"""VIB-3770: Polymarket pUSD as a first-class collateral token.

Polymarket V2 uses pUSD as the spendable trading collateral. Wallets are
funded with one of three on-chain assets: pUSD directly, USDC.e (legacy
bridged USDC, the original Onramp source), or native Circle USDC (the
new Onramp source). The connector must:

1. Recognise pUSD via ``MarketSnapshot.balance("USDC", protocol="polymarket")``
   AND ``MarketSnapshot.balance("PUSD")``.
2. Read every source asset's on-chain balance via ``CtfSDK.get_collateral_breakdown``.
3. Pick the right source for wrap → pUSD via ``CtfSDK.select_source_for_wrap``
   so a user funded with native USDC instead of USDC.e doesn't get a misleading
   "Insufficient source asset" error.
4. Approve native USDC → Onramp lazily — only when the wallet actually holds
   native USDC, so USDC.e-only wallets keep the same approval footprint.

These tests pin all four properties.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from web3 import Web3

from almanak.framework.connectors.polymarket.ctf_sdk import (
    MAX_UINT256,
    AllowanceStatus,
    CollateralBreakdown,
    CtfSDK,
)
from almanak.framework.connectors.polymarket.models import (
    PUSD,
    USDC_NATIVE_POLYGON,
    USDCE_POLYGON,
)
from almanak.framework.market import MarketSnapshot, TokenBalance

# =============================================================================
# (1) MarketSnapshot resolves pUSD for polymarket
# =============================================================================


class TestPusdBalanceWiring:
    """``balance("USDC", protocol="polymarket")`` and ``balance("PUSD")``
    both flow through to the wallet's pUSD balance — the spendable collateral
    for V2 trading."""

    def test_balance_usdc_with_polymarket_resolves_to_pusd(self) -> None:
        market = MarketSnapshot(chain="polygon", wallet_address="0xtest")
        market._balances["USDC"] = TokenBalance(
            symbol="USDC", balance=Decimal("0.08"), balance_usd=Decimal("0.08")
        )
        market._balances["PUSD"] = TokenBalance(
            symbol="PUSD", balance=Decimal("11.00"), balance_usd=Decimal("11.00")
        )

        result = market.balance("USDC", protocol="polymarket")
        assert result.symbol == "PUSD"
        assert result.balance == Decimal("11.00")

    def test_balance_pusd_directly_returns_pusd_balance(self) -> None:
        """A strategy author who explicitly asks for PUSD should still get the
        pUSD wallet balance, no protocol kwarg required."""
        market = MarketSnapshot(chain="polygon", wallet_address="0xtest")
        market._balances["PUSD"] = TokenBalance(
            symbol="PUSD", balance=Decimal("4.20"), balance_usd=Decimal("4.20")
        )

        result = market.balance("PUSD")
        assert result.symbol == "PUSD"
        assert result.balance == Decimal("4.20")


# =============================================================================
# (2) CollateralBreakdown — every funded source surfaced
# =============================================================================


class TestCollateralBreakdown:
    """``get_collateral_breakdown`` returns balances for pUSD + USDC.e + native
    USDC. ``total_spendable`` aggregates them under the 1:1 Onramp parity."""

    def test_breakdown_reads_three_balances(self) -> None:
        sdk = CtfSDK()
        web3 = MagicMock()

        pusd_contract = MagicMock()
        pusd_contract.functions.balanceOf.return_value.call.return_value = 1_500_000  # 1.5 pUSD

        usdce_contract = MagicMock()
        usdce_contract.functions.balanceOf.return_value.call.return_value = 80_000  # 0.08 USDC.e

        native_contract = MagicMock()
        native_contract.functions.balanceOf.return_value.call.return_value = 11_000_000  # 11 native USDC

        web3.eth.contract.side_effect = [pusd_contract, usdce_contract, native_contract]

        breakdown = sdk.get_collateral_breakdown("0x" + "ab" * 20, web3)

        assert isinstance(breakdown, CollateralBreakdown)
        assert breakdown.pusd == 1_500_000
        assert breakdown.usdce == 80_000
        assert breakdown.usdc_native == 11_000_000
        assert breakdown.total_spendable == 12_580_000  # all three sources
        assert breakdown.pusd_address == PUSD
        assert breakdown.usdce_address == USDCE_POLYGON
        assert breakdown.usdc_native_address == USDC_NATIVE_POLYGON

    def test_breakdown_native_read_failure_is_non_fatal(self) -> None:
        """If reading native USDC reverts (e.g. Polygon RPC blip), the
        breakdown should still surface pUSD + USDC.e rather than failing
        the whole call. Symmetric with check_allowances' behaviour."""
        sdk = CtfSDK()
        web3 = MagicMock()

        pusd_contract = MagicMock()
        pusd_contract.functions.balanceOf.return_value.call.return_value = 5_000_000

        usdce_contract = MagicMock()
        usdce_contract.functions.balanceOf.return_value.call.return_value = 7_000_000

        # Third contract call (native USDC) raises — simulate transient RPC failure.
        web3.eth.contract.side_effect = [pusd_contract, usdce_contract, RuntimeError("rpc")]

        breakdown = sdk.get_collateral_breakdown("0x" + "cd" * 20, web3)

        # Pin the failure path: the native USDC contract must have actually
        # been instantiated (3rd web3.eth.contract call) — otherwise this
        # test would silently pass even if we accidentally skipped the
        # native read entirely.
        assert web3.eth.contract.call_count == 3  # pUSD, USDC.e, native (raises)
        assert breakdown.pusd == 5_000_000
        assert breakdown.usdce == 7_000_000
        assert breakdown.usdc_native == 0

    def test_breakdown_pinned_to_canonical_usdce_when_source_reconfigured(self) -> None:
        """If a future deploy reconfigures ``source_asset`` to native USDC,
        the breakdown must still report each pile under its semantic
        bucket: native USDC under ``usdc_native`` (not ``usdce``), and
        ``usdce_address`` must remain the canonical USDC.e address. This
        keeps ``almanak ax balance --protocol polymarket`` truthful about
        which collateral the wallet actually holds.
        """
        sdk = CtfSDK(source_asset=USDC_NATIVE_POLYGON)
        web3 = MagicMock()

        pusd_contract = MagicMock()
        pusd_contract.functions.balanceOf.return_value.call.return_value = 1_000_000

        usdce_contract = MagicMock()
        usdce_contract.functions.balanceOf.return_value.call.return_value = 2_000_000  # actual USDC.e

        native_contract = MagicMock()
        native_contract.functions.balanceOf.return_value.call.return_value = 9_000_000  # actual native USDC

        web3.eth.contract.side_effect = [pusd_contract, usdce_contract, native_contract]

        breakdown = sdk.get_collateral_breakdown("0x" + "ef" * 20, web3)

        # USDC.e bucket holds the USDC.e contract's balance, NOT what
        # source_asset happens to point at.
        assert breakdown.usdce == 2_000_000
        assert breakdown.usdc_native == 9_000_000
        assert breakdown.usdce_address == Web3.to_checksum_address(USDCE_POLYGON)
        assert breakdown.usdc_native_address == Web3.to_checksum_address(USDC_NATIVE_POLYGON)


# =============================================================================
# (3) select_source_for_wrap — picks the asset that actually covers the deficit
# =============================================================================


def _status(*, usdce: int, native: int) -> AllowanceStatus:
    """Build an AllowanceStatus for source-selection tests.

    Allowances are full on every leg — the selection decision is purely
    about *which asset has enough on-chain balance*, not approvals.
    """
    return AllowanceStatus(
        source_asset_balance=usdce,
        pusd_balance=0,
        source_asset_allowance_onramp=MAX_UINT256,
        pusd_allowance_ctf_exchange=MAX_UINT256,
        pusd_allowance_neg_risk_exchange=MAX_UINT256,
        pusd_allowance_neg_risk_adapter=MAX_UINT256,
        ctf_approved_for_ctf_exchange=True,
        ctf_approved_for_neg_risk_adapter=True,
        native_usdc_balance=native,
        native_usdc_allowance_onramp=MAX_UINT256 if native > 0 else 0,
    )


class TestSelectSourceForWrap:
    """The deficit-aware source picker: USDC.e first when it covers, native
    USDC fallback when USDC.e is short, larger pile when neither covers solo."""

    def test_prefers_usdce_when_it_covers_alone(self) -> None:
        sdk = CtfSDK()
        chosen = sdk.select_source_for_wrap(deficit=5_000_000, status=_status(usdce=10_000_000, native=20_000_000))
        assert chosen == sdk.source_asset  # USDC.e wins despite having less

    def test_falls_back_to_native_usdc_when_usdce_short(self) -> None:
        """The case the original ticket calls out: user funded native USDC
        only, USDC.e balance is dust. Wrap must go through native USDC."""
        sdk = CtfSDK()
        chosen = sdk.select_source_for_wrap(deficit=5_000_000, status=_status(usdce=80_000, native=11_000_000))
        assert chosen == sdk.native_usdc

    def test_picks_larger_pile_when_neither_covers_solo(self) -> None:
        """Wallet is short but partially-funded. Pick the larger source so
        the eventual deficit message reflects the most-funded asset."""
        sdk = CtfSDK()
        chosen = sdk.select_source_for_wrap(deficit=20_000_000, status=_status(usdce=3_000_000, native=8_000_000))
        assert chosen == sdk.native_usdc

    def test_defaults_to_usdce_when_native_balance_zero(self) -> None:
        """Wallets that never funded native USDC must keep the legacy path."""
        sdk = CtfSDK()
        chosen = sdk.select_source_for_wrap(deficit=5_000_000, status=_status(usdce=0, native=0))
        assert chosen == sdk.source_asset


# =============================================================================
# (4) ensure_allowances — native USDC approval is conditional on holding it
# =============================================================================


def _allowance_status_mocks(
    *,
    source_balance: int,
    source_allowance: int,
    pusd_allowance: int,
    ctf_approved: bool,
    native_balance: int,
    native_allowance: int,
) -> tuple[MagicMock, MagicMock, MagicMock, MagicMock]:
    """Build the four web3.eth.contract returns ``check_allowances`` consumes.

    The original tuple order — source asset, pUSD, CTF, native USDC — is kept
    for legacy callers, but new tests should drive the mock through
    :func:`_install_contract_dispatch` so assertions stay behaviour-focused
    even if ``check_allowances`` reorders its internal calls (CodeRabbit
    PR #2080 nitpick).
    """
    source_contract = MagicMock()
    source_contract.functions.balanceOf.return_value.call.return_value = source_balance
    source_contract.functions.allowance.return_value.call.return_value = source_allowance

    pusd_contract = MagicMock()
    pusd_contract.functions.balanceOf.return_value.call.return_value = 0
    pusd_contract.functions.allowance.return_value.call.return_value = pusd_allowance

    ctf_contract = MagicMock()
    ctf_contract.functions.isApprovedForAll.return_value.call.return_value = ctf_approved

    native_contract = MagicMock()
    native_contract.functions.balanceOf.return_value.call.return_value = native_balance
    native_contract.functions.allowance.return_value.call.return_value = native_allowance

    return source_contract, pusd_contract, ctf_contract, native_contract


def _install_contract_dispatch(
    web3: MagicMock,
    sdk: CtfSDK,
    *,
    source_contract: MagicMock,
    pusd_contract: MagicMock,
    ctf_contract: MagicMock,
    native_contract: MagicMock | None,
) -> None:
    """Wire ``web3.eth.contract`` to dispatch by ERC-20 address.

    ``check_allowances`` / ``ensure_allowances`` may reorder their internal
    contract reads in the future without changing observable behaviour. A
    positional ``side_effect=[...]`` mock would fail those harmless reorders;
    routing by ``address`` keeps the tests pinned to behaviour, not call
    sequence. Pass ``native_contract=None`` to simulate a native-USDC RPC
    failure (the dispatcher will raise ``RuntimeError`` on that lookup, the
    same class real RPC blips surface to the SDK).
    """
    by_address = {
        sdk.source_asset.lower(): source_contract,
        sdk.pusd.lower(): pusd_contract,
        sdk.conditional_tokens.lower(): ctf_contract,
    }
    if native_contract is not None:
        by_address[sdk.native_usdc.lower()] = native_contract

    def _dispatch(*args, **kwargs):
        address = kwargs.get("address")
        if address is None and args:
            address = args[0]
        try:
            return by_address[address.lower()]
        except KeyError as exc:
            raise RuntimeError(f"no mock contract for {address}") from exc

    web3.eth.contract.side_effect = _dispatch


class TestEnsureAllowancesNativeUsdc:
    """The native-USDC approval is gated on the wallet actually holding native
    USDC. USDC.e-only wallets keep the same 6-tx approval footprint."""

    def test_usdce_only_wallet_emits_six_approvals_no_native(self) -> None:
        sdk = CtfSDK()
        web3 = MagicMock()
        source_c, pusd_c, ctf_c, native_c = _allowance_status_mocks(
            source_balance=10_000_000,
            source_allowance=0,
            pusd_allowance=0,
            ctf_approved=False,
            native_balance=0,
            native_allowance=0,
        )
        _install_contract_dispatch(
            web3, sdk,
            source_contract=source_c, pusd_contract=pusd_c,
            ctf_contract=ctf_c, native_contract=native_c,
        )

        txs = sdk.ensure_allowances("0x" + "11" * 20, web3)

        # Pre-VIB-3770 baseline: 1 source + 3 pUSD + 2 CTF = 6 txs.
        assert len(txs) == 6
        # No tx targets the native USDC address.
        assert all(tx.to.lower() != USDC_NATIVE_POLYGON.lower() for tx in txs)

    def test_native_usdc_holder_gets_extra_approval(self) -> None:
        """Wallet holds native USDC → ensure_allowances emits the native →
        Onramp approval after the standard 6 txs (idempotent: only when the
        allowance is below the sufficiency threshold)."""
        sdk = CtfSDK()
        web3 = MagicMock()
        source_c, pusd_c, ctf_c, native_c = _allowance_status_mocks(
            source_balance=10_000_000,
            source_allowance=0,
            pusd_allowance=0,
            ctf_approved=False,
            native_balance=11_000_000,  # user funded native USDC
            native_allowance=0,
        )
        _install_contract_dispatch(
            web3, sdk,
            source_contract=source_c, pusd_contract=pusd_c,
            ctf_contract=ctf_c, native_contract=native_c,
        )

        txs = sdk.ensure_allowances("0x" + "11" * 20, web3)

        assert len(txs) == 7
        assert txs[-1].to.lower() == USDC_NATIVE_POLYGON.lower()

    def test_native_usdc_already_approved_no_extra_tx(self) -> None:
        """Idempotency guard: if native USDC is already approved at MAX,
        we don't re-emit the approval just because the wallet still holds
        native USDC."""
        sdk = CtfSDK()
        web3 = MagicMock()
        source_c, pusd_c, ctf_c, native_c = _allowance_status_mocks(
            source_balance=10_000_000,
            source_allowance=MAX_UINT256,
            pusd_allowance=MAX_UINT256,
            ctf_approved=True,
            native_balance=11_000_000,
            native_allowance=MAX_UINT256,
        )
        _install_contract_dispatch(
            web3, sdk,
            source_contract=source_c, pusd_contract=pusd_c,
            ctf_contract=ctf_c, native_contract=native_c,
        )

        txs = sdk.ensure_allowances("0x" + "11" * 20, web3)

        assert txs == []


# =============================================================================
# (5) AllowanceStatus surfaces native USDC fields
# =============================================================================


class TestAllowanceStatusNativeFields:
    """Smoke test that the new fields default to 0 and the property reflects
    sufficiency (matching ``source_asset_approved_onramp`` semantics)."""

    def test_native_fields_default_zero(self) -> None:
        status = AllowanceStatus(
            source_asset_balance=0,
            pusd_balance=0,
            source_asset_allowance_onramp=0,
            pusd_allowance_ctf_exchange=0,
            pusd_allowance_neg_risk_exchange=0,
            pusd_allowance_neg_risk_adapter=0,
            ctf_approved_for_ctf_exchange=False,
            ctf_approved_for_neg_risk_adapter=False,
        )
        assert status.native_usdc_balance == 0
        assert status.native_usdc_allowance_onramp == 0
        assert status.native_usdc_approved_onramp is False

    def test_native_approval_uses_sufficiency_threshold(self) -> None:
        """A dust allowance is NOT 'approved' even though it is non-zero —
        same trap that bit ``source_asset_approved_onramp`` originally."""
        dust_allowance = AllowanceStatus(
            source_asset_balance=0,
            pusd_balance=0,
            source_asset_allowance_onramp=0,
            pusd_allowance_ctf_exchange=0,
            pusd_allowance_neg_risk_exchange=0,
            pusd_allowance_neg_risk_adapter=0,
            ctf_approved_for_ctf_exchange=False,
            ctf_approved_for_neg_risk_adapter=False,
            native_usdc_balance=1_000_000,
            native_usdc_allowance_onramp=1,  # leftover dust from a partial approve
        )
        assert dust_allowance.native_usdc_approved_onramp is False

        full_allowance = AllowanceStatus(
            source_asset_balance=0,
            pusd_balance=0,
            source_asset_allowance_onramp=0,
            pusd_allowance_ctf_exchange=0,
            pusd_allowance_neg_risk_exchange=0,
            pusd_allowance_neg_risk_adapter=0,
            ctf_approved_for_ctf_exchange=False,
            ctf_approved_for_neg_risk_adapter=False,
            native_usdc_balance=1_000_000,
            native_usdc_allowance_onramp=MAX_UINT256,
        )
        assert full_allowance.native_usdc_approved_onramp is True


# =============================================================================
# (6) check_allowances populates native USDC fields
# =============================================================================


class TestCheckAllowancesReadsNative:
    """End-to-end: ``check_allowances`` queries the native USDC contract and
    populates the new AllowanceStatus fields."""

    def test_check_allowances_includes_native_usdc(self) -> None:
        sdk = CtfSDK()
        web3 = MagicMock()
        source_c, pusd_c, ctf_c, native_c = _allowance_status_mocks(
            source_balance=80_000,
            source_allowance=MAX_UINT256,
            pusd_allowance=MAX_UINT256,
            ctf_approved=True,
            native_balance=11_000_000,
            native_allowance=MAX_UINT256,
        )
        _install_contract_dispatch(
            web3, sdk,
            source_contract=source_c, pusd_contract=pusd_c,
            ctf_contract=ctf_c, native_contract=native_c,
        )

        status = sdk.check_allowances("0x" + "11" * 20, web3)

        assert status.source_asset_balance == 80_000
        assert status.native_usdc_balance == 11_000_000
        assert status.native_usdc_approved_onramp is True

    def test_native_usdc_read_failure_falls_back_to_zero(self) -> None:
        """A transient RPC failure on the native USDC read must not break
        the whole approval check — the rest of the status is still useful."""
        sdk = CtfSDK()
        web3 = MagicMock()
        source_contract = MagicMock()
        source_contract.functions.balanceOf.return_value.call.return_value = 80_000
        source_contract.functions.allowance.return_value.call.return_value = MAX_UINT256

        pusd_contract = MagicMock()
        pusd_contract.functions.balanceOf.return_value.call.return_value = 0
        pusd_contract.functions.allowance.return_value.call.return_value = MAX_UINT256

        ctf_contract = MagicMock()
        ctf_contract.functions.isApprovedForAll.return_value.call.return_value = True

        # Address-routed dispatch with no native_contract entry — the
        # dispatcher raises RuntimeError when ``check_allowances`` reaches
        # the native USDC read, the same error class a real RPC blip surfaces.
        _install_contract_dispatch(
            web3, sdk,
            source_contract=source_contract, pusd_contract=pusd_contract,
            ctf_contract=ctf_contract, native_contract=None,
        )

        status = sdk.check_allowances("0x" + "11" * 20, web3)

        # Pin the failure path: the native USDC contract must have actually
        # been instantiated (4th web3.eth.contract call) — otherwise this
        # test would silently pass even if we accidentally skipped the
        # native read entirely.
        assert web3.eth.contract.call_count == 4  # source, pUSD, CTF, native (raises)
        assert status.source_asset_balance == 80_000
        assert status.native_usdc_balance == 0
        assert status.native_usdc_allowance_onramp == 0


# =============================================================================
# (7) build_wrap_to_pusd_tx accepts a source_asset override
# =============================================================================


class TestWrapAcceptsSourceOverride:
    """``select_source_for_wrap`` returns an address; the wrap builder must
    accept that address and target the right ``wrap(asset, to, amount)`` call.
    """

    def test_wrap_targets_native_usdc_when_overridden(self) -> None:
        from eth_abi import decode as abi_decode

        sdk = CtfSDK()
        wallet = "0x" + "ab" * 20

        tx = sdk.build_wrap_to_pusd_tx(wallet, amount=5_000_000, source_asset=USDC_NATIVE_POLYGON)

        # Calldata starts with the 4-byte selector, then ABI-encoded args.
        payload = bytes.fromhex(tx.data[10:])
        asset, to, amount = abi_decode(["address", "address", "uint256"], payload)
        assert asset.lower() == USDC_NATIVE_POLYGON.lower()
        assert to.lower() == wallet.lower()
        assert amount == 5_000_000

    def test_wrap_defaults_to_configured_source_asset(self) -> None:
        """No override → wrap targets the SDK's configured source_asset
        (USDC.e by default). Pre-VIB-3770 callers stay unbroken."""
        from eth_abi import decode as abi_decode

        sdk = CtfSDK()
        tx = sdk.build_wrap_to_pusd_tx("0x" + "ab" * 20, amount=5_000_000)
        payload = bytes.fromhex(tx.data[10:])
        asset, _to, _amount = abi_decode(["address", "address", "uint256"], payload)
        assert asset.lower() == USDCE_POLYGON.lower()


# =============================================================================
# (8) Constructor accepts a custom native USDC override
# =============================================================================


class TestCustomNativeUsdc:
    """``CtfSDK(native_usdc=...)`` allows tests / future redeploys to swap the
    native USDC address without monkeypatching the constant."""

    def test_native_usdc_override(self) -> None:
        custom = "0x" + "ee" * 20
        sdk = CtfSDK(native_usdc=custom)
        assert sdk.native_usdc.lower() == custom.lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
