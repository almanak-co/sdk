"""Tests for WalletMonitor block-scanning RPC polling."""

import json
from unittest.mock import MagicMock

import pytest

from almanak.framework.services.wallet_monitor import WalletMonitor, WalletMonitorConfig

# -- Helpers ------------------------------------------------------------------

LEADER = "0xABCDef0123456789AbcDEF0123456789abCdEf01"
CONTRACT = "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45"
TX_HASH = "0xaaa1111111111111111111111111111111111111111111111111111111111111"
BLOCK_NUM = 100
BLOCK_TIMESTAMP = 1700000000


def _make_block(
    block_number: int = BLOCK_NUM,
    timestamp: int = BLOCK_TIMESTAMP,
    transactions: list[dict] | None = None,
    block_hash: str | None = None,
):
    """Create a mock block response from eth_getBlockByNumber."""
    if transactions is None:
        transactions = [_make_tx()]
    return {
        "hash": block_hash or f"0xhash{block_number}",
        "number": hex(block_number),
        "timestamp": hex(timestamp),
        "transactions": transactions,
    }


def _make_tx(tx_hash: str = TX_HASH, from_addr: str = LEADER, to_addr: str = CONTRACT):
    """Create a mock transaction object inside a block."""
    return {
        "hash": tx_hash,
        "from": from_addr,
        "to": to_addr,
    }


def _make_receipt(tx_hash: str = TX_HASH, from_addr: str = LEADER, num_logs: int = 1):
    """Create a mock transaction receipt."""
    logs = [
        {
            "logIndex": hex(i),
            "transactionHash": tx_hash,
            "address": CONTRACT,
            "topics": ["0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"],
            "data": "0x",
        }
        for i in range(num_logs)
    ]
    return {
        "transactionHash": tx_hash,
        "from": from_addr,
        "to": CONTRACT,
        "status": "0x1",
        "blockNumber": hex(BLOCK_NUM),
        "logs": logs,
    }


def _make_mock_gateway(
    block_number: int,
    blocks: dict[int, dict] | None = None,
    receipts: dict[str, dict] | None = None,
):
    """Create a mock gateway_client that handles block-scanning RPC methods.

    Args:
        block_number: Current chain head block number (eth_blockNumber).
        blocks: Mapping of block_number -> block dict for eth_getBlockByNumber.
        receipts: Mapping of tx_hash -> receipt dict for eth_getTransactionReceipt.
    """
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


# -- Config -------------------------------------------------------------------


@pytest.fixture
def config():
    return WalletMonitorConfig(
        leader_addresses=[LEADER],
        chain="arbitrum",
        poll_interval_seconds=12,
        lookback_blocks=50,
        confirmation_depth=1,
    )


# -- Tests --------------------------------------------------------------------


class TestWalletMonitorPoll:
    """Tests for WalletMonitor.poll()."""

    def test_poll_returns_leader_events(self, config):
        """Poll returns LeaderEvents for matching leader transactions."""
        block = _make_block()
        receipt = _make_receipt()
        gateway = _make_mock_gateway(
            block_number=101,
            blocks={BLOCK_NUM: block},
            receipts={TX_HASH: receipt},
        )
        monitor = WalletMonitor(config=config, gateway_client=gateway)

        events, state = monitor.poll({})

        assert len(events) == 1
        event = events[0]
        assert event.chain == "arbitrum"
        assert event.tx_hash == TX_HASH
        assert event.log_index == 0
        assert event.from_address == LEADER
        assert event.to_address == CONTRACT
        assert event.receipt == receipt
        assert event.timestamp == BLOCK_TIMESTAMP
        assert event.event_id == f"arbitrum:{TX_HASH}:0"

    def test_poll_advances_cursor(self, config):
        """Cursor (last_processed_block) advances after each poll."""
        gateway = _make_mock_gateway(block_number=101, blocks={}, receipts={})
        monitor = WalletMonitor(config=config, gateway_client=gateway)

        _, state = monitor.poll({})
        # safe_block = 101 - 1 = 100
        assert state["last_processed_block"] == 100

    def test_cursor_used_on_subsequent_poll(self, config):
        """Second poll with same block number produces no new events."""
        block = _make_block()
        receipt = _make_receipt()
        gateway = _make_mock_gateway(
            block_number=101,
            blocks={BLOCK_NUM: block},
            receipts={TX_HASH: receipt},
        )
        monitor = WalletMonitor(config=config, gateway_client=gateway)

        _, state1 = monitor.poll({})
        assert state1["last_processed_block"] == 100

        # Second poll: from_block = 101, safe_block = 100 -> no new blocks
        events2, state2 = monitor.poll(state1)
        assert len(events2) == 0

    def test_confirmation_depth_respected(self, config):
        """Only processes blocks at (latest - confirmation_depth)."""
        config.confirmation_depth = 3
        gateway = _make_mock_gateway(block_number=110, blocks={}, receipts={})
        monitor = WalletMonitor(config=config, gateway_client=gateway)

        _, state = monitor.poll({})
        # safe_block = 110 - 3 = 107
        assert state["last_processed_block"] == 107

    def test_lookback_blocks_on_first_poll(self, config):
        """First poll scans back lookback_blocks from safe_block."""
        config.lookback_blocks = 10
        # safe_block = 100, from_block = 100 - 10 = 90
        # We need blocks 90-100 (11 blocks); provide only the relevant one
        block_90 = _make_block(block_number=90)
        gateway = _make_mock_gateway(
            block_number=101,
            blocks={90: block_90},
            receipts={TX_HASH: _make_receipt()},
        )
        monitor = WalletMonitor(config=config, gateway_client=gateway)

        events, _ = monitor.poll({})
        # Should have attempted to fetch blocks 90 through 100
        block_calls = [
            c for c in gateway.rpc.Call.call_args_list if "eth_getBlockByNumber" in str(c)
        ]
        assert len(block_calls) == 11  # blocks 90..100 inclusive

    def test_empty_result_when_no_new_blocks(self, config):
        """Returns empty when from_block > safe_block (no new blocks)."""
        gateway = _make_mock_gateway(block_number=101, blocks={}, receipts={})
        monitor = WalletMonitor(config=config, gateway_client=gateway)

        state = {"last_processed_block": 100}
        events, new_state = monitor.poll(state)

        assert len(events) == 0

    def test_non_leader_tx_filtered_out(self, config):
        """Transactions from non-leader addresses are filtered out."""
        non_leader = "0x1111111111111111111111111111111111111111"
        block = _make_block(transactions=[_make_tx(from_addr=non_leader)])
        gateway = _make_mock_gateway(
            block_number=101,
            blocks={BLOCK_NUM: block},
            receipts={TX_HASH: _make_receipt(from_addr=non_leader)},
        )
        monitor = WalletMonitor(config=config, gateway_client=gateway)

        events, _ = monitor.poll({})
        assert len(events) == 0

    def test_single_event_per_tx_regardless_of_log_count(self, config):
        """One LeaderEvent per leader tx, even if the receipt has multiple logs."""
        block = _make_block()
        receipt = _make_receipt(num_logs=3)
        gateway = _make_mock_gateway(
            block_number=101,
            blocks={BLOCK_NUM: block},
            receipts={TX_HASH: receipt},
        )
        monitor = WalletMonitor(config=config, gateway_client=gateway)

        events, _ = monitor.poll({})
        assert len(events) == 1
        assert events[0].log_index == 0
        # Receipt still has all 3 logs for downstream parsing
        assert len(events[0].receipt["logs"]) == 3

    def test_case_insensitive_leader_matching(self, config):
        """Leader address matching is case-insensitive."""
        block = _make_block(transactions=[_make_tx(from_addr=LEADER.lower())])
        receipt = _make_receipt(from_addr=LEADER.lower())
        gateway = _make_mock_gateway(
            block_number=101,
            blocks={BLOCK_NUM: block},
            receipts={TX_HASH: receipt},
        )
        monitor = WalletMonitor(config=config, gateway_client=gateway)

        events, _ = monitor.poll({})
        assert len(events) == 1

    def test_rpc_failure_returns_empty(self, config):
        """If eth_blockNumber fails, returns empty events and unchanged state."""
        gateway = MagicMock()
        resp = MagicMock()
        resp.success = False
        resp.error = json.dumps({"code": -32000, "message": "connection failed"})
        gateway.rpc.Call.return_value = resp
        monitor = WalletMonitor(config=config, gateway_client=gateway)

        initial_state = {"last_processed_block": 50}
        events, state = monitor.poll(initial_state)
        assert len(events) == 0
        assert state == initial_state

    def test_block_timestamp_propagated_to_events(self, config):
        """Block timestamp is correctly propagated to LeaderEvents."""
        ts = 1700001234
        block = _make_block(timestamp=ts)
        receipt = _make_receipt()
        gateway = _make_mock_gateway(
            block_number=101,
            blocks={BLOCK_NUM: block},
            receipts={TX_HASH: receipt},
        )
        monitor = WalletMonitor(config=config, gateway_client=gateway)

        events, _ = monitor.poll({})
        assert len(events) == 1
        assert events[0].timestamp == ts


class TestReorgDetection:
    """Tests for chain reorganization detection."""

    def test_reorg_detected_when_hash_changes(self, config):
        """When stored block hash doesn't match chain, cursor rolls back."""
        # Block 100 on chain has hash "0xhash100" (from _make_block)
        # But state stores a different hash -> reorg detected
        gateway = _make_mock_gateway(
            block_number=110,
            blocks={BLOCK_NUM: _make_block(block_number=BLOCK_NUM)},
            receipts={},
        )
        monitor = WalletMonitor(config=config, gateway_client=gateway)

        state = {
            "last_processed_block": BLOCK_NUM,
            "last_block_hash": "0xOLDhash",  # Differs from "0xhash100"
        }
        events, new_state = monitor.poll(state)

        assert events == []
        # Rolled back by confirmation_depth * 2 = 1 * 2 = 2
        assert new_state["last_processed_block"] == BLOCK_NUM - 2
        assert "last_block_hash" not in new_state

    def test_no_reorg_when_hash_matches(self, config):
        """When stored hash matches chain, normal polling continues."""
        config.lookback_blocks = 5
        # Provide blocks 101-109 for polling after cursor at 100
        blocks = {}
        for i in range(101, 110):
            blocks[i] = _make_block(block_number=i, transactions=[])
        # Also need block 100 for reorg check
        blocks[BLOCK_NUM] = _make_block(block_number=BLOCK_NUM, transactions=[])

        gateway = _make_mock_gateway(
            block_number=110,
            blocks=blocks,
            receipts={},
        )
        monitor = WalletMonitor(config=config, gateway_client=gateway)

        state = {
            "last_processed_block": BLOCK_NUM,
            "last_block_hash": f"0xhash{BLOCK_NUM}",  # Matches chain
        }
        events, new_state = monitor.poll(state)

        # safe_block = 110 - 1 = 109
        assert new_state["last_processed_block"] == 109

    def test_no_stored_hash_skips_reorg_check(self, config):
        """Without stored hash, reorg detection is skipped."""
        config.lookback_blocks = 5
        blocks = {}
        for i in range(96, 101):
            blocks[i] = _make_block(block_number=i, transactions=[])

        gateway = _make_mock_gateway(
            block_number=101,
            blocks=blocks,
            receipts={},
        )
        monitor = WalletMonitor(config=config, gateway_client=gateway)

        state = {"last_processed_block": 95}
        events, new_state = monitor.poll(state)

        assert new_state["last_processed_block"] == 100

    def test_block_hash_stored_after_poll(self, config):
        """After polling blocks, last_block_hash is stored in state."""
        config.lookback_blocks = 3
        blocks = {
            98: _make_block(block_number=98, transactions=[]),
            99: _make_block(block_number=99, transactions=[]),
            100: _make_block(block_number=100, transactions=[]),
        }
        gateway = _make_mock_gateway(
            block_number=101,
            blocks=blocks,
            receipts={},
        )
        monitor = WalletMonitor(config=config, gateway_client=gateway)

        state = {"last_processed_block": 97}
        _, new_state = monitor.poll(state)

        assert new_state["last_block_hash"] == "0xhash100"


class TestCursorIntegrity:
    """Tests for cursor validation."""

    def test_corrupted_cursor_returns_empty(self, config):
        """Cursor far ahead of chain returns empty without advancing."""
        gateway = _make_mock_gateway(block_number=100, blocks={}, receipts={})
        monitor = WalletMonitor(config=config, gateway_client=gateway)

        # safe_block = 99, cursor at 300 -> 300 > 99 + 100 = corrupted
        state = {"last_processed_block": 300}
        events, new_state = monitor.poll(state)

        assert events == []
        assert new_state == state  # Unchanged

    def test_normal_cursor_passes_validation(self, config):
        """Normal cursor within range works correctly."""
        config.lookback_blocks = 5
        blocks = {}
        for i in range(96, 100):
            blocks[i] = _make_block(block_number=i, transactions=[])

        gateway = _make_mock_gateway(
            block_number=100,
            blocks=blocks,
            receipts={},
        )
        monitor = WalletMonitor(config=config, gateway_client=gateway)

        state = {"last_processed_block": 95}
        _, new_state = monitor.poll(state)

        assert new_state["last_processed_block"] == 99
