"""Tests for token_prices_json and wallet_balances_json in portfolio snapshots.

Validates Phase 1c of the Dashboard Accounting PRD:
- token_prices_json persists chain:address-keyed prices
- wallet_balances_json persists TokenBalance data
- Round-trip through SQLite save/read preserves the data
- Migration adds columns to existing databases
"""

import asyncio
import json
import sqlite3
import tempfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from almanak.framework.portfolio.models import (
    PortfolioSnapshot,
    PositionValue,
    TokenBalance,
    ValueConfidence,
)


class TestPortfolioSnapshotTokenPrices:
    """Test that token_prices field is preserved through serialization."""

    def test_to_dict_includes_token_prices(self):
        snapshot = PortfolioSnapshot(
            timestamp=datetime.now(UTC),
            strategy_id="test",
            total_value_usd=Decimal("1000"),
            available_cash_usd=Decimal("500"),
            token_prices={
                "avalanche:0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7": {
                    "price_usd": "25.50",
                    "symbol": "WAVAX",
                    "decimals": 18,
                },
                "avalanche:0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e": {
                    "price_usd": "1.0001",
                    "symbol": "USDC",
                    "decimals": 6,
                },
            },
        )

        d = snapshot.to_dict()
        assert "token_prices" in d
        assert len(d["token_prices"]) == 2
        assert d["token_prices"]["avalanche:0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7"]["price_usd"] == "25.50"

    def test_from_dict_restores_token_prices(self):
        data = {
            "timestamp": datetime.now(UTC).isoformat(),
            "strategy_id": "test",
            "total_value_usd": "1000",
            "available_cash_usd": "500",
            "value_confidence": "HIGH",
            "positions": [],
            "wallet_balances": [],
            "token_prices": {
                "ethereum:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": {
                    "price_usd": "3450.00",
                    "symbol": "WETH",
                    "decimals": 18,
                }
            },
        }

        snapshot = PortfolioSnapshot.from_dict(data)
        assert snapshot.token_prices
        assert "ethereum:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2" in snapshot.token_prices

    def test_empty_token_prices_default(self):
        snapshot = PortfolioSnapshot(
            timestamp=datetime.now(UTC),
            strategy_id="test",
            total_value_usd=Decimal("0"),
            available_cash_usd=Decimal("0"),
        )
        assert snapshot.token_prices == {}


class TestPortfolioSnapshotWalletBalances:
    """Test that wallet_balances survives serialization round-trip."""

    def test_wallet_balances_to_dict_and_back(self):
        snapshot = PortfolioSnapshot(
            timestamp=datetime.now(UTC),
            strategy_id="test",
            total_value_usd=Decimal("1500"),
            available_cash_usd=Decimal("1500"),
            wallet_balances=[
                TokenBalance(
                    symbol="USDC",
                    balance=Decimal("1000"),
                    value_usd=Decimal("1000"),
                    address="0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
                    price_usd=Decimal("1.0001"),
                ),
                TokenBalance(
                    symbol="WAVAX",
                    balance=Decimal("20"),
                    value_usd=Decimal("500"),
                    address="0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7",
                    price_usd=Decimal("25.00"),
                ),
            ],
        )

        d = snapshot.to_dict()
        restored = PortfolioSnapshot.from_dict(d)
        assert len(restored.wallet_balances) == 2
        assert restored.wallet_balances[0].symbol == "USDC"
        assert restored.wallet_balances[0].price_usd == Decimal("1.0001")
        assert restored.wallet_balances[1].symbol == "WAVAX"


class TestSQLiteSnapshotPersistence:
    """Test SQLite save/read round-trip for new snapshot fields."""

    @pytest.fixture
    def db_path(self, tmp_path):
        return str(tmp_path / "test_state.db")

    @pytest.fixture
    def store(self, db_path):
        from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

        config = SQLiteConfig(db_path=db_path)
        return SQLiteStore(config)

    def test_save_and_read_snapshot_with_prices_and_balances(self, store):
        """Full round-trip: save snapshot with prices and balances, read it back."""

        async def _test():
            await store.initialize()

            snapshot = PortfolioSnapshot(
                timestamp=datetime.now(UTC),
                strategy_id="test-strategy",
                total_value_usd=Decimal("12890.50"),
                available_cash_usd=Decimal("5000.00"),
                value_confidence=ValueConfidence.HIGH,
                wallet_balances=[
                    TokenBalance(
                        symbol="USDC",
                        balance=Decimal("5000"),
                        value_usd=Decimal("5000"),
                        address="0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
                        price_usd=Decimal("1.0001"),
                    ),
                ],
                token_prices={
                    "avalanche:0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e": {
                        "price_usd": "1.0001",
                        "symbol": "USDC",
                        "decimals": 6,
                    },
                    "avalanche:0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7": {
                        "price_usd": "25.50",
                        "symbol": "WAVAX",
                        "decimals": 18,
                    },
                },
                chain="avalanche",
            )

            row_id = await store.save_portfolio_snapshot(snapshot)
            assert row_id > 0

            restored = await store.get_latest_snapshot("test-strategy")
            assert restored is not None
            assert restored.total_value_usd == Decimal("12890.50")

            # Verify token_prices round-tripped
            assert len(restored.token_prices) == 2
            usdc_key = "avalanche:0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e"
            assert restored.token_prices[usdc_key]["price_usd"] == "1.0001"
            assert restored.token_prices[usdc_key]["symbol"] == "USDC"

            # Verify wallet_balances round-tripped
            assert len(restored.wallet_balances) == 1
            assert restored.wallet_balances[0].symbol == "USDC"
            assert restored.wallet_balances[0].price_usd == Decimal("1.0001")

        asyncio.get_event_loop().run_until_complete(_test())

    def test_migration_adds_columns_to_existing_db(self, db_path):
        """Verify migration adds token_prices_json and wallet_balances_json to old DBs."""

        # Create a legacy DB with only the old columns
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                iteration_number INTEGER DEFAULT 0,
                total_value_usd TEXT NOT NULL,
                available_cash_usd TEXT NOT NULL,
                value_confidence TEXT DEFAULT 'HIGH',
                positions_json TEXT NOT NULL,
                chain TEXT,
                created_at TEXT NOT NULL
            )
        """)
        # Insert a legacy row
        conn.execute(
            """
            INSERT INTO portfolio_snapshots
            (strategy_id, timestamp, iteration_number, total_value_usd,
             available_cash_usd, positions_json, chain, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("legacy", "2026-04-10T12:00:00", 1, "1000", "500", "[]", "ethereum", "2026-04-10T12:00:00"),
        )
        conn.commit()
        conn.close()

        # Now initialize store — should run migration
        from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

        config = SQLiteConfig(db_path=db_path)
        store = SQLiteStore(config)

        async def _test():
            await store.initialize()

            # Verify columns exist
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("PRAGMA table_info(portfolio_snapshots)")
            columns = {row["name"] for row in cursor.fetchall()}
            assert "token_prices_json" in columns
            assert "wallet_balances_json" in columns

            # Legacy row should still be readable
            restored = await store.get_latest_snapshot("legacy")
            assert restored is not None
            assert restored.total_value_usd == Decimal("1000")
            assert restored.token_prices == {}
            assert restored.wallet_balances == []

            conn.close()

        asyncio.get_event_loop().run_until_complete(_test())

    def test_extracted_data_json_migration(self, db_path):
        """Verify migration adds extracted_data_json to old transaction_ledger."""

        # Create a legacy DB
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS transaction_ledger (
                id TEXT PRIMARY KEY,
                cycle_id TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                intent_type TEXT NOT NULL,
                token_in TEXT, amount_in TEXT,
                token_out TEXT, amount_out TEXT,
                effective_price TEXT, slippage_bps REAL,
                gas_used INTEGER, gas_usd TEXT,
                tx_hash TEXT, chain TEXT, protocol TEXT,
                success BOOLEAN NOT NULL DEFAULT 1,
                error TEXT
            )
        """)
        conn.commit()
        conn.close()

        from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

        config = SQLiteConfig(db_path=db_path)
        store = SQLiteStore(config)

        async def _test():
            await store.initialize()
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("PRAGMA table_info(transaction_ledger)")
            columns = {row["name"] for row in cursor.fetchall()}
            assert "extracted_data_json" in columns
            conn.close()

        asyncio.get_event_loop().run_until_complete(_test())


class TestBuildTokenPriceRecords:
    """Test the PortfolioValuer._build_token_price_records static method."""

    def test_builds_chain_address_keyed_records(self):
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        prices = {"USDC": Decimal("1.0001"), "WETH": Decimal("3450")}
        records = PortfolioValuer._build_token_price_records(
            chain="ethereum",
            prices=prices,
            tracked_tokens=["USDC", "WETH"],
        )

        assert len(records) >= 1  # At least some tokens resolved
        # Check structure of records
        for key, val in records.items():
            assert key.startswith("ethereum:")
            assert "price_usd" in val
            assert "symbol" in val

    def test_skips_zero_price_tokens(self):
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        prices = {"USDC": Decimal("1.0"), "BROKEN": Decimal("0")}
        records = PortfolioValuer._build_token_price_records(
            chain="ethereum",
            prices=prices,
            tracked_tokens=["USDC", "BROKEN"],
        )

        # BROKEN should not be in records (price <= 0)
        symbols = [v["symbol"] for v in records.values()]
        assert "BROKEN" not in symbols

    def test_handles_missing_resolver(self):
        from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

        prices = {"UNKNOWN_TOKEN": Decimal("42.0")}
        # Should not crash even with unknown tokens
        records = PortfolioValuer._build_token_price_records(
            chain="testchain",
            prices=prices,
            tracked_tokens=["UNKNOWN_TOKEN"],
        )
        assert len(records) >= 1
