"""Integration-style unit tests for the full copy trading pipeline.

Validates the complete flow: WalletMonitor -> CopySignalEngine ->
WalletActivityProvider -> CopySizer -> sizing/cap decisions.
All gateway interactions are mocked.
"""

import json
import time
from decimal import Decimal
from unittest.mock import MagicMock

from almanak.framework.connectors.contract_registry import ContractInfo, ContractRegistry
from almanak.framework.data.wallet_activity import WalletActivityProvider
from almanak.framework.services.copy_signal_engine import CopySignalEngine
from almanak.framework.services.copy_sizer import CopySizer, CopySizingConfig
from almanak.framework.services.copy_trading_models import SizingMode
from almanak.framework.services.wallet_monitor import WalletMonitor, WalletMonitorConfig

# -- Constants ----------------------------------------------------------------

LEADER = "0xABCDef0123456789AbcDEF0123456789abCdEf01"
ROUTER = "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45"
CHAIN = "arbitrum"
BLOCK_NUMBER = 200
TX_HASH_1 = "0xaaa1111111111111111111111111111111111111111111111111111111111111"
TX_HASH_2 = "0xbbb2222222222222222222222222222222222222222222222222222222222222"
TX_HASH_3 = "0xccc3333333333333333333333333333333333333333333333333333333333333"


# -- Mock helpers -------------------------------------------------------------


def _make_block(
    block_number: int = BLOCK_NUMBER - 1,
    timestamp: int | None = None,
    transactions: list[dict] | None = None,
):
    """Create a mock block response from eth_getBlockByNumber."""
    if timestamp is None:
        timestamp = int(time.time())  # Fresh timestamp to avoid stale-age filtering
    if transactions is None:
        transactions = [_make_tx(TX_HASH_1)]
    return {
        "number": hex(block_number),
        "timestamp": hex(timestamp),
        "transactions": transactions,
    }


def _make_tx(tx_hash: str, from_addr: str = LEADER, to_addr: str = ROUTER):
    """Create a mock transaction object inside a block."""
    return {
        "hash": tx_hash,
        "from": from_addr,
        "to": to_addr,
    }


def _make_receipt(tx_hash: str, from_addr: str = LEADER):
    """Create a mock transaction receipt."""
    return {
        "transactionHash": tx_hash,
        "from": from_addr,
        "to": ROUTER,
        "status": "0x1",
        "blockNumber": hex(BLOCK_NUMBER - 1),
        "logs": [
            {
                "logIndex": "0x0",
                "transactionHash": tx_hash,
                "address": ROUTER,
                "topics": ["0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"],
                "data": "0x",
            }
        ],
    }


def _make_mock_gateway(
    block_number: int,
    blocks: dict[int, dict] | None = None,
    receipts: dict[str, dict] | None = None,
):
    """Create a mock gateway_client with canned RPC responses for block-scanning."""
    if blocks is None:
        blocks = {}
    if receipts is None:
        receipts = {}

    gateway = MagicMock()

    def rpc_call(request, timeout=30.0):
        resp = MagicMock()
        if request.method == "eth_blockNumber":
            resp.success = True
            resp.result = json.dumps(hex(block_number))
            resp.error = ""
        elif request.method == "eth_getBlockByNumber":
            params = json.loads(request.params)
            block_num = int(params[0], 16)
            block = blocks.get(block_num)
            resp.success = block is not None
            resp.result = json.dumps(block) if block else ""
            resp.error = "" if block else json.dumps({"code": -32000, "message": "not found"})
        elif request.method == "eth_getTransactionReceipt":
            params = json.loads(request.params)
            tx_hash = params[0]
            receipt = receipts.get(tx_hash)
            resp.success = receipt is not None
            resp.result = json.dumps(receipt) if receipt else ""
            resp.error = "" if receipt else json.dumps({"code": -32000, "message": "not found"})
        else:
            resp.success = False
            resp.result = ""
            resp.error = json.dumps({"code": -32601, "message": "method not found"})
        return resp

    gateway.rpc.Call = MagicMock(side_effect=rpc_call)
    return gateway


def _make_mock_swap_amounts():
    """Create a mock SwapAmounts returned by the receipt parser."""
    sa = MagicMock()
    sa.amount_in_decimal = Decimal("1000")
    sa.amount_out_decimal = Decimal("0.5")
    sa.token_in = "USDC"
    sa.token_out = "WETH"
    sa.effective_price = Decimal("2000")
    return sa


def _make_registry():
    """Create a ContractRegistry with one Uniswap V3 entry."""
    reg = ContractRegistry()
    reg.register(
        CHAIN,
        ROUTER,
        ContractInfo(
            protocol="uniswap_v3",
            contract_type="swap_router",
            parser_module="almanak.framework.connectors.uniswap_v3.receipt_parser",
            parser_class_name="UniswapV3ReceiptParser",
        ),
    )
    return reg


def _make_engine(registry, mock_parser):
    """Create a CopySignalEngine with a mock parser pre-cached."""
    engine = CopySignalEngine(registry=registry, max_age_seconds=300, retention_days=7)
    cache_key = (
        "almanak.framework.connectors.uniswap_v3.receipt_parser"
        f".UniswapV3ReceiptParser:{CHAIN}"
    )
    engine._parser_cache[cache_key] = mock_parser
    return engine


def _build_pipeline(
    block_number: int = BLOCK_NUMBER,
    blocks: dict[int, dict] | None = None,
    receipts: dict[str, dict] | None = None,
):
    """Build the full pipeline with mocked gateway and parser.

    Returns (provider, sizer, gateway, mock_parser).
    """
    if blocks is None:
        # Default: one block containing one leader tx
        blocks = {BLOCK_NUMBER - 1: _make_block()}
    if receipts is None:
        receipts = {TX_HASH_1: _make_receipt(TX_HASH_1)}

    gateway = _make_mock_gateway(block_number, blocks, receipts)

    monitor_config = WalletMonitorConfig(
        leader_addresses=[LEADER],
        chain=CHAIN,
        poll_interval_seconds=12,
        lookback_blocks=50,
        confirmation_depth=1,
    )
    monitor = WalletMonitor(config=monitor_config, gateway_client=gateway)

    registry = _make_registry()

    mock_parser = MagicMock()
    mock_parser.extract_swap_amounts.return_value = _make_mock_swap_amounts()

    engine = _make_engine(registry, mock_parser)

    provider = WalletActivityProvider(wallet_monitor=monitor, signal_engine=engine)

    sizer_config = CopySizingConfig(
        mode=SizingMode.FIXED_USD,
        fixed_usd=Decimal("100"),
        max_trade_usd=Decimal("500"),
        min_trade_usd=Decimal("10"),
        max_daily_notional_usd=Decimal("5000"),
        max_open_positions=5,
    )
    sizer = CopySizer(config=sizer_config)

    return provider, sizer, gateway, mock_parser


# -- Tests --------------------------------------------------------------------


class TestFullPipeline:
    """Tests for the complete copy trading pipeline."""

    def test_full_pipeline_detects_and_sizes_swap(self):
        """Full flow: poll -> decode -> signal -> size."""
        provider, sizer, _, _ = _build_pipeline()

        provider.poll_and_process()

        signals = provider.get_signals()
        assert len(signals) == 1

        signal = signals[0]
        assert signal.action_type == "SWAP"
        assert signal.protocol == "uniswap_v3"
        assert signal.chain == CHAIN
        assert signal.tokens == ["USDC", "WETH"]
        assert signal.leader_address == LEADER

        size = sizer.compute_size(signal)
        assert size == Decimal("100")  # FIXED_USD mode

        assert sizer.check_daily_cap(size) is True
        assert sizer.check_position_cap() is True

    def test_dedup_prevents_double_processing(self):
        """Second poll with same data produces no new signals."""
        provider, _, _, _ = _build_pipeline()

        provider.poll_and_process()
        signals_first = provider.get_signals()
        assert len(signals_first) == 1

        # Second poll -- same gateway returns same blocks but signal engine
        # deduplicates by event_id
        provider.poll_and_process()

        # Only the original signal should be pending (signal engine skips dups,
        # but provider still holds unconsumed ones from first poll)
        signals_second = provider.get_signals()
        assert len(signals_second) == 1
        assert signals_second[0].event_id == signals_first[0].event_id

    def test_cursor_advances(self):
        """Block cursor advances after each poll."""
        provider, _, _, _ = _build_pipeline(block_number=201)

        # First poll
        provider.poll_and_process()
        state = provider.get_state()
        # safe_block = 201 - 1 = 200
        assert state["last_processed_block"] == 200

        # Build new pipeline at higher block to simulate advancement
        provider2, _, gateway2, mock_parser2 = _build_pipeline(
            block_number=215,
            blocks={213: _make_block(block_number=213, transactions=[_make_tx(TX_HASH_2)])},
            receipts={TX_HASH_2: _make_receipt(TX_HASH_2)},
        )
        # Restore cursor from first poll
        provider2.set_state(state)
        provider2.poll_and_process()

        state2 = provider2.get_state()
        # safe_block = 215 - 1 = 214
        assert state2["last_processed_block"] == 214
        assert state2["last_processed_block"] > state["last_processed_block"]

    def test_unknown_protocol_skipped(self):
        """Events sent to an unknown contract produce no signals (fail closed)."""
        unknown_contract = "0x0000000000000000000000000000000000099999"
        block = _make_block(transactions=[_make_tx(TX_HASH_1, to_addr=unknown_contract)])
        receipt = _make_receipt(TX_HASH_1)
        receipt["to"] = unknown_contract

        provider, _, _, _ = _build_pipeline(
            blocks={BLOCK_NUMBER - 1: block},
            receipts={TX_HASH_1: receipt},
        )

        provider.poll_and_process()

        signals = provider.get_signals()
        assert len(signals) == 0

    def test_daily_cap_blocks_excess(self):
        """Daily cap blocks trades after notional threshold is exceeded."""
        sizer_config = CopySizingConfig(
            mode=SizingMode.FIXED_USD,
            fixed_usd=Decimal("100"),
            max_trade_usd=Decimal("500"),
            min_trade_usd=Decimal("10"),
            max_daily_notional_usd=Decimal("250"),  # Low cap: 2.5 trades
            max_open_positions=10,
        )
        sizer = CopySizer(config=sizer_config)

        provider, _, _, _ = _build_pipeline()
        provider.poll_and_process()
        signals = provider.get_signals()
        assert len(signals) == 1
        signal = signals[0]

        size = sizer.compute_size(signal)
        assert size == Decimal("100")

        # First execution: 100 used, 150 remaining
        assert sizer.check_daily_cap(size) is True
        sizer.record_execution(size)

        # Second execution: 200 used, 50 remaining
        assert sizer.check_daily_cap(size) is True
        sizer.record_execution(size)

        # Third execution: would reach 300, exceeds 250 cap
        assert sizer.check_daily_cap(size) is False

        # Verify skip reason
        assert sizer.get_skip_reason(signal) == "daily_cap_reached"

    def test_position_cap_blocks_excess(self):
        """Position cap blocks new trades when max_open_positions reached."""
        sizer_config = CopySizingConfig(
            mode=SizingMode.FIXED_USD,
            fixed_usd=Decimal("100"),
            max_trade_usd=Decimal("500"),
            min_trade_usd=Decimal("10"),
            max_daily_notional_usd=Decimal("50000"),
            max_open_positions=2,  # Only allow 2 positions
        )
        sizer = CopySizer(config=sizer_config)

        provider, _, _, _ = _build_pipeline()
        provider.poll_and_process()
        signals = provider.get_signals()
        assert len(signals) == 1
        signal = signals[0]

        size = sizer.compute_size(signal)
        assert size == Decimal("100")

        # Open 2 positions to hit the cap
        assert sizer.check_position_cap() is True
        sizer.record_execution(size)
        assert sizer.check_position_cap() is True
        sizer.record_execution(size)

        # Position cap reached
        assert sizer.check_position_cap() is False
        assert sizer.get_skip_reason(signal) == "position_cap_reached"

        # Close one position -- cap should open up
        sizer.record_close()
        assert sizer.check_position_cap() is True
        assert sizer.get_skip_reason(signal) is None

    def test_state_persistence_roundtrip(self):
        """Cursor state survives save/restore across provider instances."""
        provider1, _, _, _ = _build_pipeline(block_number=300)

        provider1.poll_and_process()
        state = provider1.get_state()
        assert "last_processed_block" in state
        assert state["last_processed_block"] == 299  # 300 - 1 (confirmation_depth)

        # Create a new provider and restore state
        provider2, _, _, _ = _build_pipeline(block_number=300)
        provider2.set_state(state)

        restored_state = provider2.get_state()
        assert restored_state["last_processed_block"] == state["last_processed_block"]

        # Poll with same block number -- from_block (300) > safe_block (299) -> no new events
        provider2.poll_and_process()
        signals = provider2.get_signals()
        assert len(signals) == 0

        # State should still have the cursor
        assert provider2.get_state()["last_processed_block"] == 299

    def test_signal_consumed_after_processing(self):
        """Signals are removed from pending list when consumed by event_id."""
        provider, _, _, _ = _build_pipeline()

        provider.poll_and_process()
        signals = provider.get_signals()
        assert len(signals) == 1

        # Consume the signal
        provider.consume_signals([signals[0].event_id])

        # Verify no pending signals remain
        assert provider.get_signals() == []

    def test_block_timestamp_used_for_age_filtering(self):
        """Block timestamp is used for event age, not receipt timestamp."""
        # Create a block with a very old timestamp -- events should be filtered as stale
        old_timestamp = 1000  # Very old
        block = _make_block(timestamp=old_timestamp)
        receipt = _make_receipt(TX_HASH_1)

        provider, _, _, _ = _build_pipeline(
            blocks={BLOCK_NUMBER - 1: block},
            receipts={TX_HASH_1: receipt},
        )

        provider.poll_and_process()
        signals = provider.get_signals()
        # The signal engine should filter out the stale event
        assert len(signals) == 0
