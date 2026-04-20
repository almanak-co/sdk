"""IntentCompiler dispatch for protocol='aster_perps' vs 'pancakeswap_perps' (VIB-3045).

Both protocol keys must compile to the same adapter class; only the broker_id
attribution on the resulting ActionBundle differs.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.intents.compiler import CompilationStatus, IntentCompiler
from almanak.framework.intents.perp_intents import PerpOpenIntent


_PRICE_ORACLE: dict[str, Decimal] = {
    "BTC": Decimal("95000"),
    "WBTC": Decimal("95000"),
    "ETH": Decimal("3500"),
    "WETH": Decimal("3500"),
    "BNB": Decimal("600"),
    "WBNB": Decimal("600"),
    "USDT": Decimal("1"),
    "USDC": Decimal("1"),
}

_WALLET = "0x0000000000000000000000000000000000000001"


def _compile(protocol: str) -> tuple[CompilationStatus, dict]:
    compiler = IntentCompiler(
        chain="bsc",
        wallet_address=_WALLET,
        price_oracle=_PRICE_ORACLE,
    )
    intent = PerpOpenIntent(
        market="BTC/USD",
        collateral_token="BNB",
        collateral_amount=Decimal("0.3"),
        size_usd=Decimal("500"),
        is_long=True,
        max_slippage=Decimal("0.01"),
        protocol=protocol,
        leverage=Decimal("3"),
    )
    result = compiler.compile(intent)
    return result.status, (result.action_bundle.metadata if result.action_bundle else {})


class TestPerpOpenDispatch:
    def test_aster_perps_compiles_with_broker_id_0(self) -> None:
        status, metadata = _compile("aster_perps")
        assert status == CompilationStatus.SUCCESS
        assert metadata["broker_id"] == 0, "aster_perps must attribute to raw Aster"
        assert metadata["protocol"] == "aster_perps"
        assert metadata["chain"] == "bsc"

    def test_pancakeswap_perps_compiles_with_broker_id_2(self) -> None:
        status, metadata = _compile("pancakeswap_perps")
        assert status == CompilationStatus.SUCCESS
        assert metadata["broker_id"] == 2, "pancakeswap_perps must attribute to PCS"
        assert metadata["protocol"] == "pancakeswap_perps"

    def test_both_protocol_keys_produce_same_router_target(self) -> None:
        """aster_perps and pancakeswap_perps compile to the same on-chain target (Aster Diamond)."""
        _, aster_meta = _compile("aster_perps")
        _, pcs_meta = _compile("pancakeswap_perps")
        assert aster_meta["pair_base"] == pcs_meta["pair_base"]
        assert aster_meta["qty_1e10"] == pcs_meta["qty_1e10"]
        assert aster_meta["limit_price_1e8"] == pcs_meta["limit_price_1e8"]


class TestPerpClosePrecondition:
    """PERP_CLOSE dispatch — both keys route through the same close flow."""

    @pytest.mark.parametrize("protocol", ["aster_perps", "pancakeswap_perps"])
    def test_missing_position_id_rejected(self, protocol: str) -> None:
        from almanak.framework.intents.perp_intents import PerpCloseIntent

        compiler = IntentCompiler(chain="bsc", wallet_address=_WALLET, price_oracle=_PRICE_ORACLE)
        intent = PerpCloseIntent(
            market="BTC/USD",
            collateral_token="BNB",
            is_long=True,
            max_slippage=Decimal("0.01"),
            protocol=protocol,
            position_id=None,  # missing — must fail
        )
        result = compiler.compile(intent)
        assert result.status == CompilationStatus.FAILED
        assert "position_id" in (result.error or "")

    @pytest.mark.parametrize("protocol", ["aster_perps", "pancakeswap_perps"])
    def test_close_with_valid_trade_hash_compiles(self, protocol: str) -> None:
        from almanak.framework.intents.perp_intents import PerpCloseIntent

        compiler = IntentCompiler(chain="bsc", wallet_address=_WALLET, price_oracle=_PRICE_ORACLE)
        trade_hash = "0x" + "ab" * 32
        intent = PerpCloseIntent(
            market="BTC/USD",
            collateral_token="BNB",
            is_long=True,
            max_slippage=Decimal("0.01"),
            protocol=protocol,
            position_id=trade_hash,
        )
        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["position_id"] == trade_hash
        expected_broker = 0 if protocol == "aster_perps" else 2
        assert result.action_bundle.metadata["broker_id"] == expected_broker


class TestReceiptRegistry:
    def test_both_keys_resolve_to_aster_parser(self) -> None:
        from almanak.framework.connectors.aster_perps.receipt_parser import AsterPerpsReceiptParser
        from almanak.framework.execution.receipt_registry import ReceiptParserRegistry

        registry = ReceiptParserRegistry()
        aster_parser = registry.get("aster_perps", chain="bsc")
        pcs_parser = registry.get("pancakeswap_perps", chain="bsc")
        assert isinstance(aster_parser, AsterPerpsReceiptParser)
        assert isinstance(pcs_parser, AsterPerpsReceiptParser)
