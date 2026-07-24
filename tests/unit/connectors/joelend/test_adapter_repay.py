"""Unit tests for JoeLendAdapter.repay (joelend/adapter.py).

The connector is dormant (construction raises JoeLendDeprecatedError), so
instances are built via ``object.__new__`` — the pure calldata-builder
methods have no instance state and remain testable until the VIB-3963
full removal. Covers every repay branch: unsupported asset, ERC20 exact
and repay-all (MAX_UINT256), and — via a patched market info, since every
registry entry is ERC20 after the jAVAX wind-down remap — the native
payable path with its interest buffer and zero-amount refusal.
"""

from decimal import Decimal
from unittest.mock import patch

from almanak.connectors.joelend.adapter import (
    DEFAULT_GAS_ESTIMATES,
    JOELEND_J_TOKENS,
    JOELEND_REPAY_BORROW_NATIVE_SELECTOR,
    JOELEND_REPAY_BORROW_SELECTOR,
    MAX_UINT256,
    JoeLendAdapter,
    JoeLendMarketInfo,
)

USDC_E_J_TOKEN = JOELEND_J_TOKENS["USDC.e"]["j_token"]

NATIVE_AVAX_MARKET = JoeLendMarketInfo(
    asset="AVAX",
    j_token_address="0xC22F01ddc8010Ee05574028528614634684EC29e",
    underlying_address=None,
    decimals=18,
    is_native=True,
)


def _make_adapter() -> JoeLendAdapter:
    """Build an adapter without __init__ (construction raises since VIB-3960)."""
    return object.__new__(JoeLendAdapter)


class TestRepayErc20:
    """repayBorrow(uint256) path for ERC20 jTokens."""

    def test_unsupported_asset_fails_with_supported_list(self):
        adapter = _make_adapter()
        result = adapter.repay("DOGE", Decimal("1"))
        assert result.success is False
        assert result.tx_data is None
        assert "Unsupported asset: DOGE" in result.error
        assert "USDC.e" in result.error

    def test_exact_amount_encodes_repay_borrow(self):
        adapter = _make_adapter()
        result = adapter.repay("USDC.e", Decimal("123.45"))
        assert result.success is True
        expected_wei = int(Decimal("123.45") * Decimal(10**6))
        assert result.tx_data == {
            "to": USDC_E_J_TOKEN,
            "data": JOELEND_REPAY_BORROW_SELECTOR + f"{expected_wei:064x}",
            "value": 0,
        }
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["repay"]
        assert result.description == "Repay 123.45 USDC.e to Joe Lend"

    def test_repay_all_uses_max_uint256(self):
        adapter = _make_adapter()
        result = adapter.repay("USDC.e", Decimal("0"), repay_all=True)
        assert result.success is True
        assert result.tx_data["data"] == JOELEND_REPAY_BORROW_SELECTOR + f"{MAX_UINT256:064x}"
        assert result.tx_data["value"] == 0
        assert result.description == "Repay all USDC.e debt on Joe Lend"

    def test_asset_resolution_is_case_insensitive(self):
        adapter = _make_adapter()
        result = adapter.repay("usdc.E", Decimal("5"))
        assert result.success is True
        # Resolves to the canonical USDC.e jToken despite the mixed-case input
        assert result.tx_data["to"] == USDC_E_J_TOKEN


class TestRepayNativeAvax:
    """repayBorrow() payable path (jAVAX).

    The live registry maps AVAX to WAVAX (is_native=False) since jAVAX
    rejects raw native deposits, so the native branch is driven through a
    patched market info — it must stay correct for historical receipts.
    """

    def _repay_native(self, amount: Decimal, *, repay_all: bool = False):
        adapter = _make_adapter()
        with patch.object(JoeLendAdapter, "get_market_info", return_value=NATIVE_AVAX_MARKET):
            return adapter.repay("AVAX", amount, repay_all=repay_all)

    def test_exact_amount_sends_value_with_bare_selector(self):
        result = self._repay_native(Decimal("2"))
        assert result.success is True
        assert result.tx_data == {
            "to": NATIVE_AVAX_MARKET.j_token_address,
            "data": JOELEND_REPAY_BORROW_NATIVE_SELECTOR,
            "value": 2 * 10**18,
        }
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["repay_native"]
        assert result.description == "Repay 2 AVAX to Joe Lend"

    def test_repay_all_adds_interest_accrual_buffer(self):
        result = self._repay_native(Decimal("1"), repay_all=True)
        assert result.success is True
        # 0.1% buffer over the queried debt; excess is returned by the protocol
        assert result.tx_data["value"] == int(10**18 * Decimal("1.001"))
        assert result.tx_data["data"] == JOELEND_REPAY_BORROW_NATIVE_SELECTOR
        assert result.description == "Repay all AVAX to Joe Lend"

    def test_repay_all_zero_amount_refused(self):
        result = self._repay_native(Decimal("0"), repay_all=True)
        assert result.success is False
        assert result.tx_data is None
        assert "requires a positive amount" in result.error
