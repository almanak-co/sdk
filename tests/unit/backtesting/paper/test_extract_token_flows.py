"""Tests for _extract_token_flows with correct token decimals.

This test ensures that token amounts are correctly parsed from receipts
using the actual token decimals (e.g., 6 for USDC, 18 for ETH).

The critical bug this validates: Previously, _extract_token_flows used
hardcoded 10**18 for all tokens, causing USDC amounts to be ~1 million
times too small (since USDC has 6 decimals, not 18).
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.backtesting.paper.engine import (
    CHAIN_ID_ARBITRUM,
    CHAIN_ID_ETHEREUM,
    PaperTrader,
)

# Token addresses (lowercase for registry lookup)
USDC_ETHEREUM = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"  # 6 decimals
WETH_ETHEREUM = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"  # 18 decimals
USDC_ARBITRUM = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"  # 6 decimals
WETH_ARBITRUM = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"  # 18 decimals


def make_transfer_log(token_address: str, from_addr: str, to_addr: str, value: int) -> dict:
    """Create a mock ERC-20 Transfer event log.

    Args:
        token_address: Token contract address
        from_addr: Sender address (without 0x prefix)
        to_addr: Recipient address (without 0x prefix)
        value: Amount in smallest units (wei for 18 decimals, 10^-6 for USDC)

    Returns:
        Log dict in the format expected by receipt parsers
    """
    # ERC-20 Transfer(address,address,uint256) event signature
    transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

    # Pad addresses to 32 bytes (64 hex chars)
    from_padded = from_addr.lower().replace("0x", "").zfill(64)
    to_padded = to_addr.lower().replace("0x", "").zfill(64)

    # Value encoded as 32-byte hex
    value_hex = hex(value)[2:].zfill(64)

    return {
        "address": token_address,
        "topics": [transfer_topic, f"0x{from_padded}", f"0x{to_padded}"],
        "data": f"0x{value_hex}",
    }


def create_mock_receipt(logs: list[dict], status: int = 1) -> MagicMock:
    """Create a mock TransactionReceipt.

    Args:
        logs: List of log dicts (from make_transfer_log)
        status: Transaction status (1 = success, 0 = failure)

    Returns:
        Mock receipt object with to_dict() method
    """
    receipt = MagicMock()
    receipt.to_dict.return_value = {
        "status": status,
        "logs": logs,
        "block_number": 12345678,
        "gas_used": 150000,
    }
    return receipt


def create_mock_paper_trader(chain_id: int, is_running: bool = True) -> MagicMock:
    """Create a mock PaperTrader with fork manager.

    Args:
        chain_id: Chain ID for the fork
        is_running: Whether the fork is running

    Returns:
        Mock PaperTrader with configured fork_manager
    """
    # Create mock fork manager
    fork_manager = MagicMock()
    fork_manager.chain_id = chain_id
    fork_manager.is_running = is_running
    fork_manager.get_rpc_url.return_value = "http://localhost:8545"

    # Create mock paper trader
    trader = MagicMock(spec=PaperTrader)
    trader.fork_manager = fork_manager
    trader._backtest_id = "test-backtest-id"

    return trader


class TestExtractTokenFlowsDecimals:
    """Tests for _extract_token_flows with correct decimal handling."""

    @pytest.mark.asyncio
    async def test_usdc_swap_correct_decimals(self):
        """Test that USDC amounts are parsed with 6 decimals, not 18.

        This is the critical integration test: swapping 1000 USDC for ETH.
        With 6 decimals, 1000 USDC = 1000 * 10^6 = 1_000_000_000 smallest units.
        With 18 decimals (the old bug), it would be interpreted as:
            1_000_000_000 / 10^18 = 0.000000001 USDC (wrong by ~1 million x)
        """
        wallet = "0x1234567890123456789012345678901234567890"

        # 1000 USDC out = 1000 * 10^6 = 1_000_000_000
        usdc_amount_raw = 1_000_000_000  # 1000 USDC in 6-decimal units
        # 0.5 ETH in = 0.5 * 10^18 = 500_000_000_000_000_000
        weth_amount_raw = 500_000_000_000_000_000  # 0.5 WETH in 18-decimal units

        logs = [
            # USDC out: from wallet to DEX
            make_transfer_log(
                USDC_ETHEREUM,
                from_addr=wallet,
                to_addr="0xDEXADDRESS000000000000000000000000000001",
                value=usdc_amount_raw,
            ),
            # WETH in: from DEX to wallet
            make_transfer_log(
                WETH_ETHEREUM,
                from_addr="0xDEXADDRESS000000000000000000000000000001",
                to_addr=wallet,
                value=weth_amount_raw,
            ),
        ]

        receipt = create_mock_receipt(logs)
        trader = create_mock_paper_trader(CHAIN_ID_ETHEREUM)

        # Call the method under test
        # We need to bind the method to our mock and call it
        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        # Create a bound method by using the real class's method with our mock
        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        tokens_in, tokens_out = await method(
            intent=MagicMock(),  # Intent not used when receipt is provided
            receipt=receipt,
            wallet_address=wallet,
        )

        # Verify USDC (6 decimals): 1_000_000_000 / 10^6 = 1000
        # Note: US-065c changed keys from addresses to symbols
        assert "USDC" in tokens_out, f"Expected USDC symbol in tokens_out, got: {tokens_out}"
        usdc_out = tokens_out["USDC"]
        assert usdc_out == Decimal("1000"), f"Expected 1000 USDC, got {usdc_out}"

        # Verify WETH (18 decimals): 500_000_000_000_000_000 / 10^18 = 0.5
        assert "WETH" in tokens_in, f"Expected WETH symbol in tokens_in, got: {tokens_in}"
        weth_in = tokens_in["WETH"]
        assert weth_in == Decimal("0.5"), f"Expected 0.5 WETH, got {weth_in}"

    @pytest.mark.asyncio
    async def test_usdc_amount_not_off_by_million(self):
        """Regression test: Verify USDC is NOT interpreted with 18 decimals.

        Before the fix, 1_000_000_000 would be divided by 10^18 instead of 10^6,
        resulting in 0.000000001 instead of 1000.

        The difference factor is 10^12 (approximately 1 million x).
        """
        wallet = "0x1234567890123456789012345678901234567890"
        usdc_amount_raw = 1_000_000_000  # 1000 USDC in 6-decimal units

        logs = [
            make_transfer_log(
                USDC_ETHEREUM,
                from_addr=wallet,
                to_addr="0xDEXADDRESS000000000000000000000000000001",
                value=usdc_amount_raw,
            ),
        ]

        receipt = create_mock_receipt(logs)
        trader = create_mock_paper_trader(CHAIN_ID_ETHEREUM)

        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        _, tokens_out = await method(
            intent=MagicMock(),
            receipt=receipt,
            wallet_address=wallet,
        )

        # US-065c: Keys are now symbols, not addresses
        usdc_out = tokens_out.get("USDC", Decimal("0"))

        # This should be 1000, not 0.000000001
        assert usdc_out > Decimal("0.001"), (
            f"USDC amount {usdc_out} is too small - likely using 18 decimals instead of 6"
        )
        assert usdc_out == Decimal("1000"), f"Expected 1000 USDC, got {usdc_out}"

    @pytest.mark.asyncio
    async def test_arbitrum_usdc_swap(self):
        """Test USDC swap on Arbitrum chain."""
        wallet = "0x1234567890123456789012345678901234567890"

        # 500 USDC out = 500 * 10^6 = 500_000_000
        usdc_amount_raw = 500_000_000
        # 0.25 ETH in = 0.25 * 10^18
        weth_amount_raw = 250_000_000_000_000_000

        logs = [
            make_transfer_log(
                USDC_ARBITRUM,
                from_addr=wallet,
                to_addr="0xDEXADDRESS000000000000000000000000000001",
                value=usdc_amount_raw,
            ),
            make_transfer_log(
                WETH_ARBITRUM,
                from_addr="0xDEXADDRESS000000000000000000000000000001",
                to_addr=wallet,
                value=weth_amount_raw,
            ),
        ]

        receipt = create_mock_receipt(logs)
        trader = create_mock_paper_trader(CHAIN_ID_ARBITRUM)

        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        tokens_in, tokens_out = await method(
            intent=MagicMock(),
            receipt=receipt,
            wallet_address=wallet,
        )

        # Verify USDC (6 decimals): 500_000_000 / 10^6 = 500
        # US-065c: Keys are now symbols, not addresses
        assert tokens_out["USDC"] == Decimal("500")

        # Verify WETH (18 decimals): 250_000_000_000_000_000 / 10^18 = 0.25
        assert tokens_in["WETH"] == Decimal("0.25")

    @pytest.mark.asyncio
    async def test_unknown_token_skipped_not_defaulted_to_18(self):
        """VIB-3164: an unknown token with unresolved decimals is SKIPPED, not 18.

        Previously the flow was emitted assuming 18 decimals, silently
        miscounting any non-18-decimal token (a 10^12x error for USDC).
        Empty != Zero: with no RPC and no registry entry the decimals are
        unmeasured, so the receipt path must not emit a fabricated amount.
        """
        wallet = "0x1234567890123456789012345678901234567890"
        unknown_token = "0x1111111111111111111111111111111111111111"

        # 1 token with 18 decimals = 10^18
        amount_raw = 1_000_000_000_000_000_000

        logs = [
            make_transfer_log(
                unknown_token,
                from_addr="0xSOMEADDRESS00000000000000000000000000001",
                to_addr=wallet,
                value=amount_raw,
            ),
        ]

        receipt = create_mock_receipt(logs)
        # Fork not running = no RPC to query decimals
        trader = create_mock_paper_trader(CHAIN_ID_ETHEREUM, is_running=False)

        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        tokens_in, _tokens_out = await method(
            intent=MagicMock(),
            receipt=receipt,
            wallet_address=wallet,
        )

        # The receipt path cannot resolve decimals -> the unknown token must not
        # be emitted with a silently-defaulted (wrong) amount.
        for key in tokens_in:
            assert unknown_token.lower() not in key.lower(), (
                f"unresolved-decimal token must be skipped, not emitted: {tokens_in}"
            )

    @pytest.mark.asyncio
    async def test_one_unresolved_leg_aborts_whole_trade(self):
        """VIB-3164 (CodeRabbit critical): one unresolved leg => ATOMIC skip.

        A swap with one resolvable leg (WETH=18) and one unresolvable-decimal
        leg must NOT emit a one-sided flow. Recording only the WETH leg would
        let record_trade apply half the swap and corrupt balances/PnL. The
        whole receipt-based extraction is aborted and ({}, {}) returned, so the
        caller falls back to intent-based estimation (Empty != Zero: an
        unmeasurable trade is omitted, never half-recorded).
        """
        wallet = "0x1234567890123456789012345678901234567890"
        unknown_token = "0x1111111111111111111111111111111111111111"

        # Resolvable leg: 0.5 WETH out (18 decimals)
        weth_amount_raw = 500_000_000_000_000_000
        # Unresolvable leg: unknown token in (decimals cannot be resolved)
        unknown_amount_raw = 1_000_000_000_000_000_000

        logs = [
            make_transfer_log(
                WETH_ETHEREUM,
                from_addr=wallet,
                to_addr="0xDEXADDRESS000000000000000000000000000001",
                value=weth_amount_raw,
            ),
            make_transfer_log(
                unknown_token,
                from_addr="0xDEXADDRESS000000000000000000000000000001",
                to_addr=wallet,
                value=unknown_amount_raw,
            ),
        ]

        receipt = create_mock_receipt(logs)
        # Fork not running = no RPC, so the unknown token's decimals are unresolved.
        trader = create_mock_paper_trader(CHAIN_ID_ETHEREUM, is_running=False)

        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        tokens_in, tokens_out = await method(
            intent=MagicMock(),
            receipt=receipt,
            wallet_address=wallet,
        )

        # Atomic skip: NOTHING is recorded from the receipt path -- not even the
        # resolvable WETH leg -- to avoid a one-sided trade.
        assert tokens_in == {}, f"Expected empty tokens_in (atomic skip), got: {tokens_in}"
        assert tokens_out == {}, f"Expected empty tokens_out (atomic skip), got: {tokens_out}"

    @pytest.mark.asyncio
    async def test_both_legs_resolvable_records_full_trade(self):
        """VIB-3164: both legs resolvable => full two-sided trade (no regression).

        Companion to test_one_unresolved_leg_aborts_whole_trade: when every leg
        resolves, the happy path is unchanged and both sides are recorded.
        """
        wallet = "0x1234567890123456789012345678901234567890"

        usdc_amount_raw = 1_000_000_000  # 1000 USDC out (6 decimals)
        weth_amount_raw = 500_000_000_000_000_000  # 0.5 WETH in (18 decimals)

        logs = [
            make_transfer_log(
                USDC_ETHEREUM,
                from_addr=wallet,
                to_addr="0xDEXADDRESS000000000000000000000000000001",
                value=usdc_amount_raw,
            ),
            make_transfer_log(
                WETH_ETHEREUM,
                from_addr="0xDEXADDRESS000000000000000000000000000001",
                to_addr=wallet,
                value=weth_amount_raw,
            ),
        ]

        receipt = create_mock_receipt(logs)
        trader = create_mock_paper_trader(CHAIN_ID_ETHEREUM)

        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        tokens_in, tokens_out = await method(
            intent=MagicMock(),
            receipt=receipt,
            wallet_address=wallet,
        )

        assert tokens_out["USDC"] == Decimal("1000")
        assert tokens_in["WETH"] == Decimal("0.5")

    @pytest.mark.asyncio
    async def test_failed_transaction_returns_empty_flows(self):
        """Test that failed transactions don't have token flows extracted."""
        wallet = "0x1234567890123456789012345678901234567890"

        logs = [
            make_transfer_log(
                USDC_ETHEREUM,
                from_addr=wallet,
                to_addr="0xDEXADDRESS000000000000000000000000000001",
                value=1_000_000_000,
            ),
        ]

        # Failed transaction (status = 0)
        receipt = create_mock_receipt(logs, status=0)
        trader = create_mock_paper_trader(CHAIN_ID_ETHEREUM)

        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        tokens_in, tokens_out = await method(
            intent=MagicMock(),
            receipt=receipt,
            wallet_address=wallet,
        )

        # Failed transactions should have empty flows
        # (extract_token_flows in receipt_utils handles this)
        assert tokens_in == {}, f"Failed tx should have empty tokens_in, got: {tokens_in}"
        assert tokens_out == {}, f"Failed tx should have empty tokens_out, got: {tokens_out}"


class TestDecimalPrecision:
    """Tests for decimal precision in token flow calculations."""

    @pytest.mark.asyncio
    async def test_small_usdc_amount(self):
        """Test small USDC amounts (e.g., $0.01)."""
        wallet = "0x1234567890123456789012345678901234567890"

        # 0.01 USDC = 10_000 (6 decimal units)
        usdc_amount_raw = 10_000

        logs = [
            make_transfer_log(
                USDC_ETHEREUM,
                from_addr="0xSOMEADDRESS00000000000000000000000000001",
                to_addr=wallet,
                value=usdc_amount_raw,
            ),
        ]

        receipt = create_mock_receipt(logs)
        trader = create_mock_paper_trader(CHAIN_ID_ETHEREUM)

        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        tokens_in, _ = await method(
            intent=MagicMock(),
            receipt=receipt,
            wallet_address=wallet,
        )

        # US-065c: Keys are now symbols, not addresses
        assert tokens_in["USDC"] == Decimal("0.01")

    @pytest.mark.asyncio
    async def test_large_usdc_amount(self):
        """Test large USDC amounts (e.g., $1,000,000)."""
        wallet = "0x1234567890123456789012345678901234567890"

        # 1,000,000 USDC = 1_000_000 * 10^6 = 1_000_000_000_000
        usdc_amount_raw = 1_000_000_000_000

        logs = [
            make_transfer_log(
                USDC_ETHEREUM,
                from_addr="0xSOMEADDRESS00000000000000000000000000001",
                to_addr=wallet,
                value=usdc_amount_raw,
            ),
        ]

        receipt = create_mock_receipt(logs)
        trader = create_mock_paper_trader(CHAIN_ID_ETHEREUM)

        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        tokens_in, _ = await method(
            intent=MagicMock(),
            receipt=receipt,
            wallet_address=wallet,
        )

        # US-065c: Keys are now symbols, not addresses
        assert tokens_in["USDC"] == Decimal("1000000")

    @pytest.mark.asyncio
    async def test_wei_precision_preserved(self):
        """Test that wei-level precision is preserved for 18-decimal tokens."""
        wallet = "0x1234567890123456789012345678901234567890"

        # 1 wei = smallest ETH unit
        weth_amount_raw = 1

        logs = [
            make_transfer_log(
                WETH_ETHEREUM,
                from_addr="0xSOMEADDRESS00000000000000000000000000001",
                to_addr=wallet,
                value=weth_amount_raw,
            ),
        ]

        receipt = create_mock_receipt(logs)
        trader = create_mock_paper_trader(CHAIN_ID_ETHEREUM)

        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        method = RealPaperTrader._extract_token_flows.__get__(trader, type(trader))

        tokens_in, _ = await method(
            intent=MagicMock(),
            receipt=receipt,
            wallet_address=wallet,
        )

        # 1 wei = 10^-18 ETH
        # US-065c: Keys are now symbols, not addresses
        expected = Decimal("1") / Decimal(10**18)
        assert tokens_in["WETH"] == expected


# Registry symbol -> address map used to drive _resolve_token_address in the
# balance-delta tests. USDC/WETH resolve from the registry without RPC.
_SYMBOL_TO_ADDRESS = {
    "USDC": USDC_ETHEREUM,
    "WETH": WETH_ETHEREUM,
}


def _make_balance_delta_trader(
    chain_id: int = CHAIN_ID_ETHEREUM,
    *,
    address_map: dict[str, str] | None = None,
) -> MagicMock:
    """Mock PaperTrader configured for _compute_balance_deltas / discovery tests.

    ``_resolve_token_address`` maps known symbols to registry addresses (so
    decimals resolve without RPC); unknown symbols return ``None`` (unmeasurable).
    """
    trader = create_mock_paper_trader(chain_id)
    trader.config = MagicMock()
    trader.config.chain = "ethereum"
    amap = _SYMBOL_TO_ADDRESS if address_map is None else address_map

    def _resolve(symbol: str) -> str | None:
        if symbol.upper() == "ETH":
            return None
        return amap.get(symbol.upper())

    trader._resolve_token_address = _resolve
    return trader


class TestComputeBalanceDeltas:
    """Direct tests for PaperTrader._compute_balance_deltas (VIB-3164).

    Pins the Empty != Zero atomic-skip contract on the balance-snapshot path:
    sign-routed inflows/outflows, native-ETH 18-decimal invariant, and the
    atomic abort (return empty flows) when any token's address/decimals are
    unresolved. Reached in production only via _build_and_record_paper_trade,
    so direct coverage here keeps its CRAP score down.
    """

    @staticmethod
    def _method(trader: MagicMock):
        from almanak.framework.backtesting.paper.engine import PaperTrader as RealPaperTrader

        return RealPaperTrader._compute_balance_deltas.__get__(trader, type(trader))

    @pytest.mark.asyncio
    async def test_two_sided_swap_sign_routing_and_decimals(self):
        """USDC down / WETH up -> outflow USDC (6 dec), inflow WETH (18 dec)."""
        trader = _make_balance_delta_trader()
        before = {"USDC": 1_000_000_000, "WETH": 0}  # 1000 USDC
        after = {"USDC": 0, "WETH": 500_000_000_000_000_000}  # 0.5 WETH
        intent = MagicMock(spec=[])  # no token attributes -> no discovery

        tokens_in, tokens_out = await self._method(trader)(before, after, intent)

        assert tokens_out == {"USDC": Decimal("1000")}
        assert tokens_in == {"WETH": Decimal("0.5")}

    @pytest.mark.asyncio
    async def test_native_eth_uses_18_decimals_no_lookup(self):
        """Native ETH delta converts at 18 decimals by chain invariant."""
        trader = _make_balance_delta_trader()
        before = {"ETH": 0}
        after = {"ETH": 2_000_000_000_000_000_000}  # +2 ETH
        intent = MagicMock(spec=[])

        tokens_in, tokens_out = await self._method(trader)(before, after, intent)

        assert tokens_in == {"ETH": Decimal("2")}
        assert tokens_out == {}

    @pytest.mark.asyncio
    async def test_zero_delta_token_dropped(self):
        """A token whose balance did not change is not recorded."""
        trader = _make_balance_delta_trader()
        before = {"USDC": 1_000_000_000, "WETH": 500_000_000_000_000_000}
        after = {"USDC": 0, "WETH": 500_000_000_000_000_000}  # WETH unchanged
        intent = MagicMock(spec=[])

        tokens_in, tokens_out = await self._method(trader)(before, after, intent)

        assert tokens_out == {"USDC": Decimal("1000")}
        assert "WETH" not in tokens_in and "WETH" not in tokens_out

    @pytest.mark.asyncio
    async def test_unresolvable_address_aborts_whole_flow(self):
        """One token with no resolvable address -> atomic skip (empty flows)."""
        trader = _make_balance_delta_trader()  # only USDC/WETH known
        before = {"WETH": 500_000_000_000_000_000, "MYSTERY": 0}
        after = {"WETH": 0, "MYSTERY": 1_000_000}  # MYSTERY address unresolvable
        intent = MagicMock(spec=[])

        tokens_in, tokens_out = await self._method(trader)(before, after, intent)

        # Atomic skip: NOT even the resolvable WETH leg is recorded.
        assert tokens_in == {}
        assert tokens_out == {}

    @pytest.mark.asyncio
    async def test_unresolved_decimals_aborts_whole_flow(self):
        """Address resolves but decimals are None (no RPC) -> atomic skip."""
        # UNKNOWN maps to an address absent from the registry; fork not running
        # so there is no RPC fallback -> decimals None.
        amap = {"WETH": WETH_ETHEREUM, "UNKNOWN": "0x1111111111111111111111111111111111111111"}
        trader = _make_balance_delta_trader(address_map=amap)
        trader.fork_manager.is_running = False
        before = {"WETH": 500_000_000_000_000_000, "UNKNOWN": 0}
        after = {"WETH": 0, "UNKNOWN": 1_000_000_000_000_000_000}
        intent = MagicMock(spec=[])

        tokens_in, tokens_out = await self._method(trader)(before, after, intent)

        assert tokens_in == {}
        assert tokens_out == {}

    @pytest.mark.asyncio
    async def test_intent_token_discovery_seeds_balance(self):
        """An intent token not in the snapshots is discovered and measured."""
        trader = _make_balance_delta_trader()
        trader._orchestrator = MagicMock()
        trader._orchestrator.signer.address = "0xWALLET"

        async def _fake_balance(address: str, holder: str) -> int:
            return 1_000_000_000  # 1000 USDC discovered post-trade

        trader.fork_manager._get_token_balance = _fake_balance

        before: dict[str, int] = {}
        after: dict[str, int] = {}
        intent = MagicMock(spec=["from_token"])
        intent.from_token = "USDC"

        tokens_in, tokens_out = await self._method(trader)(before, after, intent)

        # Discovery seeded after["USDC"]=1e9, before=0 -> inflow 1000 USDC.
        assert tokens_in == {"USDC": Decimal("1000")}


class TestDiscoverIntentTokenBalances:
    """Direct tests for the discover_intent_token_balances helper (VIB-3164)."""

    @pytest.mark.asyncio
    async def test_seeds_untracked_intent_token(self):
        from almanak.framework.backtesting.paper import _engine_helpers

        trader = _make_balance_delta_trader()
        trader._orchestrator = MagicMock()
        trader._orchestrator.signer.address = "0xWALLET"

        async def _fake_balance(address: str, holder: str) -> int:
            return 42

        trader.fork_manager._get_token_balance = _fake_balance

        before: dict[str, int] = {}
        after: dict[str, int] = {}
        all_symbols: set[str] = set()
        intent = MagicMock(spec=["to_token"])
        intent.to_token = "WETH"

        await _engine_helpers.discover_intent_token_balances(trader, intent, before, after, all_symbols)

        assert after["WETH"] == 42
        assert before["WETH"] == 0
        assert "WETH" in all_symbols

    @pytest.mark.asyncio
    async def test_skips_eth_and_already_tracked_and_unresolvable(self):
        from almanak.framework.backtesting.paper import _engine_helpers

        trader = _make_balance_delta_trader()
        trader._orchestrator = MagicMock()
        trader._orchestrator.signer.address = "0xWALLET"
        called = False

        async def _fake_balance(address: str, holder: str) -> int:
            nonlocal called
            called = True
            return 1

        trader.fork_manager._get_token_balance = _fake_balance

        before = {"USDC": 5}
        after = {"USDC": 5}
        all_symbols = {"USDC"}
        # ETH (skipped), USDC (already tracked), MYSTERY (unresolvable address).
        intent = MagicMock(spec=["token", "asset", "from_token"])
        intent.token = "ETH"
        intent.asset = "USDC"
        intent.from_token = "MYSTERY"

        await _engine_helpers.discover_intent_token_balances(trader, intent, before, after, all_symbols)

        # No new symbols seeded; balance query never fired (all three skipped).
        assert all_symbols == {"USDC"}
        assert called is False

    @pytest.mark.asyncio
    async def test_balance_query_failure_is_swallowed(self):
        from almanak.framework.backtesting.paper import _engine_helpers

        trader = _make_balance_delta_trader()
        trader._orchestrator = MagicMock()
        trader._orchestrator.signer.address = "0xWALLET"

        async def _boom(address: str, holder: str) -> int:
            raise RuntimeError("rpc down")

        trader.fork_manager._get_token_balance = _boom

        before: dict[str, int] = {}
        after: dict[str, int] = {}
        all_symbols: set[str] = set()
        intent = MagicMock(spec=["from_token"])
        intent.from_token = "WETH"

        # Must not raise; failed discovery leaves snapshots untouched.
        await _engine_helpers.discover_intent_token_balances(trader, intent, before, after, all_symbols)

        assert "WETH" not in after
        assert all_symbols == set()
