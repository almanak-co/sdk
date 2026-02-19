"""Tests for CopySignalEngine."""

import time
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.contract_registry import ContractInfo, ContractRegistry
from almanak.framework.services.copy_signal_engine import CopySignalEngine
from almanak.framework.services.copy_trading_models import CopySignal, LeaderEvent


@pytest.fixture()
def registry():
    """ContractRegistry with one Uniswap V3 entry on arbitrum."""
    reg = ContractRegistry()
    reg.register(
        "arbitrum",
        "0xRouter1234",
        ContractInfo(
            protocol="uniswap_v3",
            contract_type="swap_router",
            parser_module="almanak.framework.connectors.uniswap_v3.receipt_parser",
            parser_class_name="UniswapV3ReceiptParser",
            supported_actions=["SWAP"],
        ),
    )
    return reg


@pytest.fixture()
def mock_swap_amounts():
    """A mock SwapAmounts object returned by the parser."""
    sa = MagicMock()
    sa.amount_in_decimal = Decimal("1000")
    sa.amount_out_decimal = Decimal("0.5")
    sa.token_in = "USDC"
    sa.token_out = "WETH"
    sa.effective_price = Decimal("2000")
    return sa


@pytest.fixture()
def mock_parser(mock_swap_amounts):
    """A mock receipt parser."""
    parser = MagicMock()
    parser.extract_swap_amounts.return_value = mock_swap_amounts
    return parser


@pytest.fixture()
def leader_event():
    """A sample LeaderEvent."""
    now = int(time.time())
    return LeaderEvent(
        chain="arbitrum",
        block_number=100,
        tx_hash="0xabc123",
        log_index=0,
        timestamp=now,
        from_address="0xLeader",
        to_address="0xRouter1234",
        receipt={"logs": [{"topics": ["0xswap_topic"], "data": "0x1234"}]},
    )


def _make_engine(registry, mock_parser, price_fn=None):
    """Create a CopySignalEngine with mock parser injected."""
    engine = CopySignalEngine(registry=registry, max_age_seconds=300, retention_days=7, price_fn=price_fn)
    # Pre-populate the parser cache to avoid real imports
    engine._parser_cache["almanak.framework.connectors.uniswap_v3.receipt_parser.UniswapV3ReceiptParser:arbitrum"] = (
        mock_parser
    )
    return engine


class TestCopySignalEngineProcessEvents:
    def test_successful_signal_production(self, registry, mock_parser, leader_event):
        engine = _make_engine(registry, mock_parser)
        signals = engine.process_events([leader_event])

        assert len(signals) == 1
        signal = signals[0]
        assert isinstance(signal, CopySignal)
        assert signal.event_id == leader_event.event_id
        assert signal.action_type == "SWAP"
        assert signal.protocol == "uniswap_v3"
        assert signal.chain == "arbitrum"
        assert signal.tokens == ["USDC", "WETH"]
        assert signal.amounts == {"USDC": Decimal("1000"), "WETH": Decimal("0.5")}
        assert signal.amounts_usd == {}
        assert signal.leader_address == "0xLeader"
        assert signal.block_number == 100
        assert signal.timestamp == leader_event.timestamp
        assert signal.metadata["effective_price"] == "2000"

    def test_dedup_skips_duplicate_event_ids(self, registry, mock_parser, leader_event):
        engine = _make_engine(registry, mock_parser)

        signals_first = engine.process_events([leader_event])
        assert len(signals_first) == 1

        signals_second = engine.process_events([leader_event])
        assert len(signals_second) == 0

    def test_age_filter_skips_stale_events(self, registry, mock_parser):
        old_event = LeaderEvent(
            chain="arbitrum",
            block_number=50,
            tx_hash="0xold",
            log_index=0,
            timestamp=1000,
            from_address="0xLeader",
            to_address="0xRouter1234",
            receipt={"logs": []},
        )
        engine = _make_engine(registry, mock_parser)
        signals = engine.process_events([old_event], current_time=2000)
        assert len(signals) == 0

    def test_unknown_protocol_skipped(self, registry, mock_parser):
        event = LeaderEvent(
            chain="arbitrum",
            block_number=100,
            tx_hash="0xunknown",
            log_index=0,
            timestamp=int(time.time()),
            from_address="0xLeader",
            to_address="0xUnknownContract",
            receipt={"logs": []},
        )
        engine = _make_engine(registry, mock_parser)
        signals = engine.process_events([event])
        assert len(signals) == 0

    def test_parser_returning_none_skipped(self, registry, leader_event):
        parser = MagicMock()
        parser.extract_swap_amounts.return_value = None

        engine = _make_engine(registry, parser)
        signals = engine.process_events([leader_event])
        assert len(signals) == 0

    def test_multiple_events_processed(self, registry, mock_parser):
        now = int(time.time())
        events = [
            LeaderEvent(
                chain="arbitrum",
                block_number=100,
                tx_hash="0xtx1",
                log_index=0,
                timestamp=now,
                from_address="0xLeader",
                to_address="0xRouter1234",
                receipt={"logs": []},
            ),
            LeaderEvent(
                chain="arbitrum",
                block_number=101,
                tx_hash="0xtx2",
                log_index=0,
                timestamp=now,
                from_address="0xLeader",
                to_address="0xRouter1234",
                receipt={"logs": []},
            ),
        ]
        engine = _make_engine(registry, mock_parser)
        signals = engine.process_events(events)
        assert len(signals) == 2


class TestCopySignalEnginePruneSeen:
    def test_prune_removes_old_entries(self, registry, mock_parser, leader_event):
        engine = _make_engine(registry, mock_parser)
        # Process an event at time=1000
        engine.process_events([leader_event], current_time=1000)
        assert leader_event.event_id in engine._seen_event_ids

        # Prune at a time well past retention
        prune_time = 1000 + (8 * 86400)  # 8 days later
        engine.prune_seen(prune_time)
        assert leader_event.event_id not in engine._seen_event_ids

    def test_prune_keeps_recent_entries(self, registry, mock_parser, leader_event):
        engine = _make_engine(registry, mock_parser)
        now = int(time.time())
        engine.process_events([leader_event], current_time=now)

        engine.prune_seen(now + 3600)  # 1 hour later
        assert leader_event.event_id in engine._seen_event_ids


class TestCopySignalEngineGetSkipReason:
    def test_returns_none_for_valid_event(self, registry, mock_parser, leader_event):
        engine = _make_engine(registry, mock_parser)
        assert engine.get_skip_reason(leader_event) is None

    def test_returns_duplicate(self, registry, mock_parser, leader_event):
        engine = _make_engine(registry, mock_parser)
        engine.process_events([leader_event])
        assert engine.get_skip_reason(leader_event) == "duplicate"

    def test_returns_stale(self, registry, mock_parser):
        old_event = LeaderEvent(
            chain="arbitrum",
            block_number=50,
            tx_hash="0xold",
            log_index=0,
            timestamp=0,
            from_address="0xLeader",
            to_address="0xRouter1234",
            receipt={"logs": []},
        )
        engine = _make_engine(registry, mock_parser)
        assert engine.get_skip_reason(old_event) == "stale"

    def test_returns_unknown_protocol(self, registry, mock_parser):
        event = LeaderEvent(
            chain="arbitrum",
            block_number=100,
            tx_hash="0xunknown",
            log_index=0,
            timestamp=int(time.time()),
            from_address="0xLeader",
            to_address="0xNoSuchContract",
            receipt={"logs": []},
        )
        engine = _make_engine(registry, mock_parser)
        assert engine.get_skip_reason(event) == "unknown_protocol"


class TestTokenResolution:
    def test_non_address_tokens_returned_as_is(self, registry, mock_parser, leader_event):
        """Tokens that are already symbols (not 0x addresses) are returned directly."""
        engine = _make_engine(registry, mock_parser)
        signals = engine.process_events([leader_event])
        assert signals[0].tokens == ["USDC", "WETH"]

    def test_address_tokens_resolved_via_resolver(self, registry, leader_event):
        """Token addresses (0x...) are resolved to symbols via token resolver."""
        sa = MagicMock()
        sa.amount_in_decimal = Decimal("1000")
        sa.amount_out_decimal = Decimal("0.5")
        sa.token_in = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
        sa.token_out = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
        sa.effective_price = Decimal("2000")

        parser = MagicMock()
        parser.extract_swap_amounts.return_value = sa

        engine = _make_engine(registry, parser)

        mock_resolver = MagicMock()
        mock_resolver.resolve.side_effect = lambda token, chain: MagicMock(
            symbol="USDC" if "af88" in token.lower() else "WETH"
        )

        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            return_value=mock_resolver,
        ):
            signals = engine.process_events([leader_event])
            assert signals[0].tokens == ["USDC", "WETH"]

    def test_address_resolution_failure_returns_address(self, registry, leader_event):
        """If token resolver fails, the raw address is used as-is."""
        sa = MagicMock()
        sa.amount_in_decimal = Decimal("1000")
        sa.amount_out_decimal = Decimal("0.5")
        sa.token_in = "0xUnknownToken"
        sa.token_out = "WETH"
        sa.effective_price = Decimal("2000")

        parser = MagicMock()
        parser.extract_swap_amounts.return_value = sa

        engine = _make_engine(registry, parser)

        with patch(
            "almanak.framework.data.tokens.get_token_resolver",
            side_effect=Exception("resolver unavailable"),
        ):
            signals = engine.process_events([leader_event])
            assert signals[0].tokens[0] == "0xUnknownToken"
            assert signals[0].tokens[1] == "WETH"


class TestAmountsUsdPopulation:
    """Tests for the price_fn-driven amounts_usd population."""

    def test_amounts_usd_populated_when_price_fn_provided(self, registry, mock_parser, leader_event):
        """When price_fn returns prices, amounts_usd is populated."""

        def price_fn(token: str, chain: str) -> Decimal | None:
            prices = {"USDC": Decimal("1"), "WETH": Decimal("2000")}
            return prices.get(token)

        engine = _make_engine(registry, mock_parser, price_fn=price_fn)
        signals = engine.process_events([leader_event])

        assert len(signals) == 1
        assert signals[0].amounts_usd == {
            "USDC": Decimal("1000"),  # 1000 * 1
            "WETH": Decimal("1000"),  # 0.5 * 2000
        }

    def test_amounts_usd_empty_when_price_fn_is_none(self, registry, mock_parser, leader_event):
        """When no price_fn, amounts_usd is empty (existing behavior)."""
        engine = _make_engine(registry, mock_parser, price_fn=None)
        signals = engine.process_events([leader_event])

        assert len(signals) == 1
        assert signals[0].amounts_usd == {}

    def test_amounts_usd_partial_when_price_fn_fails(self, registry, mock_parser, leader_event):
        """When price_fn returns None for some tokens, only resolved tokens get USD."""

        def price_fn(token: str, chain: str) -> Decimal | None:
            if token == "USDC":
                return Decimal("1")
            return None  # WETH lookup fails

        engine = _make_engine(registry, mock_parser, price_fn=price_fn)
        signals = engine.process_events([leader_event])

        assert len(signals) == 1
        assert signals[0].amounts_usd == {"USDC": Decimal("1000")}
        assert "WETH" not in signals[0].amounts_usd


class TestMultiActionExtraction:
    """Tests for multi-action decoding (LP, lending, perps)."""

    def _make_lp_registry(self):
        reg = ContractRegistry()
        reg.register(
            "arbitrum",
            "0xPosManager",
            ContractInfo(
                protocol="uniswap_v3",
                contract_type="position_manager",
                parser_module="almanak.framework.connectors.uniswap_v3.receipt_parser",
                parser_class_name="UniswapV3ReceiptParser",
                supported_actions=["LP_OPEN", "LP_CLOSE"],
            ),
        )
        return reg

    def _make_lending_registry(self):
        reg = ContractRegistry()
        reg.register(
            "arbitrum",
            "0xAavePool",
            ContractInfo(
                protocol="aave_v3",
                contract_type="pool",
                parser_module="almanak.framework.connectors.aave_v3.receipt_parser",
                parser_class_name="AaveV3ReceiptParser",
                supported_actions=["SUPPLY", "WITHDRAW", "BORROW", "REPAY"],
            ),
        )
        return reg

    def _make_perp_registry(self):
        reg = ContractRegistry()
        reg.register(
            "arbitrum",
            "0xGmxRouter",
            ContractInfo(
                protocol="gmx_v2",
                contract_type="exchange_router",
                parser_module="almanak.framework.connectors.gmx_v2.receipt_parser",
                parser_class_name="GMXv2ReceiptParser",
                supported_actions=["PERP_OPEN", "PERP_CLOSE"],
            ),
        )
        return reg

    def _make_event(self, to_address: str) -> LeaderEvent:
        return LeaderEvent(
            chain="arbitrum",
            block_number=100,
            tx_hash="0xabc123",
            log_index=0,
            timestamp=int(time.time()),
            from_address="0xLeader",
            to_address=to_address,
            receipt={"logs": [{"topics": ["0xtopic"], "data": "0x1234"}]},
        )

    def test_lp_open_extraction(self):
        """LP open produces LP_OPEN signal with position_id in metadata."""
        registry = self._make_lp_registry()
        parser = MagicMock()
        parser.extract_position_id.return_value = 12345
        parser.extract_liquidity.return_value = 999

        engine = CopySignalEngine(registry=registry)
        cache_key = "almanak.framework.connectors.uniswap_v3.receipt_parser.UniswapV3ReceiptParser:arbitrum"
        engine._parser_cache[cache_key] = parser

        signals = engine.process_events([self._make_event("0xPosManager")])
        assert len(signals) == 1
        assert signals[0].action_type == "LP_OPEN"
        assert signals[0].protocol == "uniswap_v3"
        assert signals[0].metadata["position_id"] == 12345
        assert signals[0].metadata["liquidity"] == "999"

    def test_lp_close_extraction(self):
        """LP close produces LP_CLOSE signal when no position_id but lp_close_data exists."""
        registry = self._make_lp_registry()
        parser = MagicMock()
        parser.extract_position_id.return_value = None
        parser.extract_lp_close_data.return_value = {"amount0": 100, "amount1": 200}

        engine = CopySignalEngine(registry=registry)
        cache_key = "almanak.framework.connectors.uniswap_v3.receipt_parser.UniswapV3ReceiptParser:arbitrum"
        engine._parser_cache[cache_key] = parser

        signals = engine.process_events([self._make_event("0xPosManager")])
        assert len(signals) == 1
        assert signals[0].action_type == "LP_CLOSE"

    def test_supply_extraction(self):
        """Lending supply produces SUPPLY signal."""
        registry = self._make_lending_registry()
        parser = MagicMock()
        parser.extract_supply_amount.return_value = 1000000
        # Ensure BORROW is not also triggered
        parser.extract_borrow_amount.return_value = None

        engine = CopySignalEngine(registry=registry)
        cache_key = "almanak.framework.connectors.aave_v3.receipt_parser.AaveV3ReceiptParser:arbitrum"
        engine._parser_cache[cache_key] = parser

        signals = engine.process_events([self._make_event("0xAavePool")])
        assert len(signals) == 1
        assert signals[0].action_type == "SUPPLY"
        assert signals[0].protocol == "aave_v3"

    def test_borrow_extraction(self):
        """Lending borrow produces BORROW signal when supply returns None."""
        registry = self._make_lending_registry()
        parser = MagicMock()
        parser.extract_supply_amount.return_value = None
        parser.extract_withdraw_amount.return_value = None
        parser.extract_borrow_amount.return_value = 500000

        engine = CopySignalEngine(registry=registry)
        cache_key = "almanak.framework.connectors.aave_v3.receipt_parser.AaveV3ReceiptParser:arbitrum"
        engine._parser_cache[cache_key] = parser

        signals = engine.process_events([self._make_event("0xAavePool")])
        assert len(signals) == 1
        assert signals[0].action_type == "BORROW"

    def test_perp_open_extraction(self):
        """Perp open produces PERP_OPEN signal."""
        registry = self._make_perp_registry()
        parser = MagicMock()
        parser.extract_perp_open.return_value = {"size": 1000, "leverage": 5}

        engine = CopySignalEngine(registry=registry)
        cache_key = "almanak.framework.connectors.gmx_v2.receipt_parser.GMXv2ReceiptParser:arbitrum"
        engine._parser_cache[cache_key] = parser

        signals = engine.process_events([self._make_event("0xGmxRouter")])
        assert len(signals) == 1
        assert signals[0].action_type == "PERP_OPEN"
        assert signals[0].protocol == "gmx_v2"

    def test_no_extraction_returns_empty(self):
        """When all extraction methods return None, no signal is produced."""
        registry = self._make_lending_registry()
        parser = MagicMock()
        parser.extract_supply_amount.return_value = None
        parser.extract_withdraw_amount.return_value = None
        parser.extract_borrow_amount.return_value = None
        parser.extract_repay_amount.return_value = None

        engine = CopySignalEngine(registry=registry)
        cache_key = "almanak.framework.connectors.aave_v3.receipt_parser.AaveV3ReceiptParser:arbitrum"
        engine._parser_cache[cache_key] = parser

        signals = engine.process_events([self._make_event("0xAavePool")])
        assert len(signals) == 0

    def test_backward_compat_empty_actions_uses_swap(self):
        """Registry entries with empty supported_actions fall back to swap extraction."""
        reg = ContractRegistry()
        reg.register(
            "arbitrum",
            "0xLegacy",
            ContractInfo(
                protocol="legacy_dex",
                contract_type="router",
                parser_module="test.module",
                parser_class_name="TestParser",
                supported_actions=[],  # No actions declared
            ),
        )
        sa = MagicMock()
        sa.amount_in_decimal = Decimal("100")
        sa.amount_out_decimal = Decimal("0.05")
        sa.token_in = "USDC"
        sa.token_out = "WETH"
        sa.effective_price = Decimal("2000")

        parser = MagicMock()
        parser.extract_swap_amounts.return_value = sa

        engine = CopySignalEngine(registry=reg)
        engine._parser_cache["test.module.TestParser:arbitrum"] = parser

        event = self._make_event("0xLegacy")
        signals = engine.process_events([event])
        assert len(signals) == 1
        assert signals[0].action_type == "SWAP"


class TestLeaderLagBlocks:
    """Tests for leader_lag_blocks metadata population."""

    def test_leader_lag_blocks_populated_when_current_block_provided(self, registry, mock_parser, leader_event):
        """When current_block is provided, leader_lag_blocks appears in signal metadata."""
        engine = _make_engine(registry, mock_parser)
        # leader_event.block_number is 100, current_block is 110 => lag = 10
        signals = engine.process_events([leader_event], current_block=110)
        assert len(signals) == 1
        assert signals[0].metadata["leader_lag_blocks"] == 10

    def test_leader_lag_blocks_absent_without_current_block(self, registry, mock_parser, leader_event):
        """When current_block is None, leader_lag_blocks is NOT in metadata."""
        engine = _make_engine(registry, mock_parser)
        signals = engine.process_events([leader_event], current_block=None)
        assert len(signals) == 1
        assert "leader_lag_blocks" not in signals[0].metadata

    def test_leader_lag_blocks_zero_when_same_block(self, registry, mock_parser, leader_event):
        """When current_block equals event block_number, lag is 0."""
        engine = _make_engine(registry, mock_parser)
        signals = engine.process_events([leader_event], current_block=100)
        assert len(signals) == 1
        assert signals[0].metadata["leader_lag_blocks"] == 0
