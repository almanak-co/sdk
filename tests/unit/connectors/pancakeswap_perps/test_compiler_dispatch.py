"""IntentCompiler dispatch for the legacy Aster Diamond (``pancakeswap_perps``).

``pancakeswap_perps`` is the only protocol key the Diamond compiler owns and it
always attributes to the PancakeSwap broker. ``aster_perps`` names Aster Pro, an
off-chain order book, and must never compile to a Diamond transaction.
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


def _compile(protocol: str, collateral_token: str = "BNB") -> tuple[CompilationStatus, dict, list]:
    compiler = IntentCompiler(
        chain="bsc",
        wallet_address=_WALLET,
        price_oracle=_PRICE_ORACLE,
    )
    intent = PerpOpenIntent(
        market="BTC/USD",
        collateral_token=collateral_token,
        collateral_amount=Decimal("0.3"),
        size_usd=Decimal("500"),
        is_long=True,
        max_slippage=Decimal("0.01"),
        protocol=protocol,
        leverage=Decimal("3"),
    )
    result = compiler.compile(intent)
    metadata = result.action_bundle.metadata if result.action_bundle else {}
    return result.status, metadata, list(result.transactions or [])


class TestPerpOpenDispatch:
    def test_pancakeswap_perps_compiles_with_broker_id_2(self) -> None:
        status, metadata, transactions = _compile("pancakeswap_perps")
        assert status == CompilationStatus.SUCCESS
        assert metadata["broker_id"] == 2, "pancakeswap_perps must attribute to PCS"
        assert metadata["protocol"] == "pancakeswap_perps"
        assert metadata["chain"] == "bsc"
        assert transactions, "the Diamond lane is on-chain"

    def test_diamond_compiler_owns_only_pancakeswap_perps(self) -> None:
        from almanak.connectors._aster_perps_core.compiler import AsterDiamondPerpsCompiler

        assert AsterDiamondPerpsCompiler.protocols == frozenset({"pancakeswap_perps"})

    def test_aster_perps_never_compiles_to_a_diamond_transaction(self) -> None:
        status, metadata, transactions = _compile("aster_perps", collateral_token="USDT")
        assert status == CompilationStatus.SUCCESS
        assert transactions == []
        assert "broker_id" not in metadata
        assert "pair_base" not in metadata


class TestBSCPerpPriceAliasFallback:
    """The compiler routes BSC perp market base symbols (BTC, ETH, BNB)
    through ``_PERP_PRICE_ALIAS_BY_CHAIN`` to the canonical registry symbol
    (BTCB, WETH, WBNB) before querying the price oracle.

    This exercises the alias fallback specifically — the price oracle here
    is seeded with ONLY the wrapped symbols (no "BTC"/"ETH"/"BNB"), so
    compilation can only succeed when the alias mapping fires."""

    _ALIAS_ONLY_ORACLE: dict[str, Decimal] = {
        # Note: the bare base symbols (BTC, ETH, BNB) are deliberately absent
        # so the alias path is the only way the compiler reaches a price.
        "BTCB": Decimal("95000"),
        "WETH": Decimal("3500"),
        "WBNB": Decimal("600"),
        "USDT": Decimal("1"),
        "USDC": Decimal("1"),
    }

    def _compile_with_alias_only_oracle(self, market: str) -> CompilationStatus:
        compiler = IntentCompiler(
            chain="bsc",
            wallet_address=_WALLET,
            price_oracle=self._ALIAS_ONLY_ORACLE,
        )
        intent = PerpOpenIntent(
            market=market,
            collateral_token="BNB",
            collateral_amount=Decimal("0.3"),
            size_usd=Decimal("500"),
            is_long=True,
            max_slippage=Decimal("0.01"),
            protocol="pancakeswap_perps",
            leverage=Decimal("3"),
        )
        return compiler.compile(intent).status

    @pytest.mark.parametrize(
        "market",
        ["BTC/USD", "ETH/USD", "BNB/USD"],
    )
    def test_bsc_base_symbol_routes_through_alias_to_registered_wrapper(
        self, market: str
    ) -> None:
        """BTC → BTCB, ETH → WETH, BNB → WBNB. The oracle only has the
        wrappers, so a SUCCESS verdict proves the alias dict fired."""
        assert (
            self._compile_with_alias_only_oracle(market) == CompilationStatus.SUCCESS
        )

    def test_unknown_base_symbol_with_no_alias_fails_loudly(self) -> None:
        """A symbol with no chain-alias entry must propagate the underlying
        ValueError as a failed compile — the helper must NOT silently
        substitute a default price."""
        # XYZ is not in any oracle, not in any alias map.
        status = self._compile_with_alias_only_oracle("XYZ/USD")
        assert status == CompilationStatus.FAILED


class TestPerpClosePrecondition:
    """PERP_CLOSE dispatch through the Diamond close flow."""

    def test_missing_position_id_rejected(self) -> None:
        from almanak.framework.intents.perp_intents import PerpCloseIntent

        compiler = IntentCompiler(chain="bsc", wallet_address=_WALLET, price_oracle=_PRICE_ORACLE)
        intent = PerpCloseIntent(
            market="BTC/USD",
            collateral_token="BNB",
            is_long=True,
            max_slippage=Decimal("0.01"),
            protocol="pancakeswap_perps",
            position_id=None,  # missing — must fail
        )
        result = compiler.compile(intent)
        assert result.status == CompilationStatus.FAILED
        assert "position_id" in (result.error or "")

    def test_close_with_valid_trade_hash_compiles(self) -> None:
        from almanak.framework.intents.perp_intents import PerpCloseIntent

        compiler = IntentCompiler(chain="bsc", wallet_address=_WALLET, price_oracle=_PRICE_ORACLE)
        trade_hash = "0x" + "ab" * 32
        intent = PerpCloseIntent(
            market="BTC/USD",
            collateral_token="BNB",
            is_long=True,
            max_slippage=Decimal("0.01"),
            protocol="pancakeswap_perps",
            position_id=trade_hash,
        )
        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS
        assert result.action_bundle.metadata["position_id"] == trade_hash
        assert result.action_bundle.metadata["broker_id"] == 2


class TestReceiptRegistry:
    def test_pancakeswap_perps_resolves_to_diamond_parser(self) -> None:
        from almanak.connectors._aster_perps_core.receipt_parser import AsterPerpsReceiptParser
        from almanak.framework.execution.receipt_registry import ReceiptParserRegistry

        assert isinstance(ReceiptParserRegistry().get("pancakeswap_perps", chain="bsc"), AsterPerpsReceiptParser)

    def test_aster_perps_has_no_diamond_receipt_parser(self) -> None:
        from almanak.framework.execution.receipt_registry import ReceiptParserRegistry

        with pytest.raises(ValueError, match="Unknown protocol: aster_perps"):
            ReceiptParserRegistry().get("aster_perps", chain="bsc")
