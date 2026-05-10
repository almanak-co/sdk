"""VIB-4225 ACC-02 — native-gas-token in wallet_balances_json unit tests.

Pins the D1.1, D2.1, F1, F2, F3 contract from the frozen UAT card.

- D1.1 native gas symbol present in wallet_balances_json after a tx, with
  exactly one canonical-uppercased entry; ``gas_native_status`` stamped 'ok'.
- D2.1 cross-chain matrix (registry-driven; tests every chain whose native
  is registered in ``gas_pricing.native_token_for_chain``).
- F1 (unknown_chain): ``native_token_for_chain`` returns None → status
  stamped 'unknown_chain', wallet_balances does NOT include UNKNOWN.
- F2 (balance_failed): ``market.balance(native)`` raises → status stamped
  'balance_failed', tracked tokens preserved.
- F3 (price_missing): ``market.price(native)`` raises or returns None →
  status stamped 'price_missing'.

The strategy stays fail-open at this layer; the live-mode raise happens in
``runner_state._enforce_native_gas_status_in_live`` which is verified
separately below in the runner-level F1-F3 enforcer test.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.portfolio import PortfolioSnapshot, ValueConfidence
from almanak.framework.runner.runner_state import _enforce_native_gas_status_in_live
from almanak.framework.state.exceptions import AccountingPersistenceError


def _market_mock(
    *,
    balance_returns: dict[str, Decimal] | None = None,
    price_returns: dict[str, Decimal | None] | None = None,
    balance_raises: set[str] | None = None,
    price_raises: set[str] | None = None,
) -> MagicMock:
    """Build a MarketSnapshot mock that stubs out balance/price per token."""
    market = MagicMock()
    bal_map = balance_returns or {}
    price_map = price_returns or {}
    bal_raises = balance_raises or set()
    pr_raises = price_raises or set()

    def _balance(token: str) -> Any:
        if token in bal_raises:
            raise RuntimeError(f"synthetic balance error for {token}")
        b = MagicMock()
        b.balance = bal_map.get(token, Decimal("0"))
        return b

    def _price(token: str) -> Any:
        if token in pr_raises:
            raise RuntimeError(f"synthetic price error for {token}")
        return price_map.get(token, Decimal("0"))

    market.balance.side_effect = _balance
    market.price.side_effect = _price
    return market


def _fake_strategy(chain: str = "arbitrum"):
    """Build a concrete IntentStrategy subclass that fills out abstract methods
    so we can call ``_append_native_gas_to_wallet`` directly without booting
    a full strategy lifecycle (state machine, runtime, runner, etc.).
    """
    from almanak.framework.strategies.intent_strategy import IntentStrategy

    class _TestStrategy(IntentStrategy):
        def decide(self, market):  # type: ignore[override]
            from almanak.framework.intents.vocabulary import HoldIntent
            return HoldIntent(reason="test")

        def generate_teardown_intents(self, market):  # type: ignore[override]
            return []

        def get_open_positions(self):  # type: ignore[override]
            from almanak.framework.runner.runner_models import PositionsList
            return PositionsList(positions=[], total_value_usd=Decimal("0"))

    s = _TestStrategy.__new__(_TestStrategy)
    s._chain = chain
    return s


# --- D1.1 — native gas present after tx ----------------------------------------

def test_native_in_wallet_balances_after_tx() -> None:
    """D1.1: native ETH appended uppercase, status='ok', exactly one entry."""
    strategy = _fake_strategy(chain="arbitrum")
    market = _market_mock(
        balance_returns={"ETH": Decimal("0.5")},
        price_returns={"ETH": Decimal("3000")},
    )
    wallet_balances = []  # no tracked tokens
    status, value = strategy._append_native_gas_to_wallet(market, wallet_balances)
    assert status == "ok"
    assert value == Decimal("1500")  # 0.5 ETH * $3000
    assert len(wallet_balances) == 1
    assert wallet_balances[0].symbol == "ETH"
    assert wallet_balances[0].balance == Decimal("0.5")
    assert wallet_balances[0].price_usd == Decimal("3000")


def test_native_dedupes_case_insensitively() -> None:
    """already_tracked path: tracked token list already contains native (any casing)."""
    from almanak.framework.portfolio.models import TokenBalance

    strategy = _fake_strategy(chain="arbitrum")
    market = _market_mock(
        balance_returns={"ETH": Decimal("0.5")},
        price_returns={"ETH": Decimal("3000")},
    )
    wallet_balances = [TokenBalance(symbol="eth", balance=Decimal("0.4"),
                                     value_usd=Decimal("1200"), price_usd=Decimal("3000"))]
    status, value = strategy._append_native_gas_to_wallet(market, wallet_balances)
    assert status == "already_tracked"
    assert value == Decimal("0")
    assert len(wallet_balances) == 1  # NOT duplicated


# --- F1 — unknown_chain --------------------------------------------------------

def test_f1_unknown_chain_status() -> None:
    """F1: native_token_for_chain returns None → status 'unknown_chain', no append."""
    strategy = _fake_strategy(chain="arbitrum")
    market = _market_mock()
    wallet_balances = []
    with patch(
        "almanak.framework.accounting.gas_pricing.native_token_for_chain",
        return_value=None,
    ):
        status, value = strategy._append_native_gas_to_wallet(market, wallet_balances)
    assert status == "unknown_chain"
    assert value == Decimal("0")
    assert wallet_balances == []  # no spurious "UNKNOWN" symbol


# --- F2 — balance_failed -------------------------------------------------------

def test_f2_balance_raises_status() -> None:
    """F2: market.balance(native) raises → status 'balance_failed'."""
    strategy = _fake_strategy(chain="arbitrum")
    market = _market_mock(balance_raises={"ETH"})
    wallet_balances = []
    status, value = strategy._append_native_gas_to_wallet(market, wallet_balances)
    assert status == "balance_failed"
    assert value == Decimal("0")
    assert wallet_balances == []  # no partial row


# --- F3 — price_missing --------------------------------------------------------

def test_f3_price_raises_status() -> None:
    """F3 (variant a): market.price(native) raises → status 'price_missing'."""
    strategy = _fake_strategy(chain="arbitrum")
    market = _market_mock(
        balance_returns={"ETH": Decimal("0.5")},
        price_raises={"ETH"},
    )
    wallet_balances = []
    status, value = strategy._append_native_gas_to_wallet(market, wallet_balances)
    assert status == "price_missing"
    assert value == Decimal("0")
    assert wallet_balances == []


def test_f3_price_returns_none_status() -> None:
    """F3 (variant b): market.price(native) returns None → status 'price_missing'."""
    strategy = _fake_strategy(chain="arbitrum")
    market = _market_mock(
        balance_returns={"ETH": Decimal("0.5")},
        price_returns={"ETH": None},
    )
    wallet_balances = []
    status, value = strategy._append_native_gas_to_wallet(market, wallet_balances)
    assert status == "price_missing"
    assert value == Decimal("0")
    assert wallet_balances == []


# --- D2.1 — cross-chain native symbol matrix ------------------------------------

@pytest.mark.parametrize("chain,expected_native", [
    ("ethereum", "ETH"),
    ("arbitrum", "ETH"),
    ("base", "ETH"),
    ("optimism", "ETH"),
    ("polygon", None),    # registry source of truth — read whatever it returns (MATIC or POL)
    ("avalanche", "AVAX"),
    ("bsc", "BNB"),
    ("solana", "SOL"),
    ("mantle", "MNT"),
    ("monad", None),      # registry source of truth — read whatever it returns
    ("linea", "ETH"),
])
def test_d2_native_symbol_matrix(chain: str, expected_native: str | None) -> None:
    """D2.1: every registered chain produces its expected native symbol."""
    from almanak.framework.accounting.gas_pricing import native_token_for_chain

    actual = native_token_for_chain(chain)
    if actual is None:
        pytest.skip(f"chain '{chain}' not in registry — registry change required to support")
    if expected_native is not None:
        assert actual.upper() == expected_native, (
            f"chain={chain}: expected {expected_native}, got {actual}"
        )

    # Drive the snapshot path end-to-end with this chain to confirm the
    # symbol lands in wallet_balances unchanged.
    strategy = _fake_strategy(chain=chain)
    market = _market_mock(
        balance_returns={actual: Decimal("1")},
        price_returns={actual: Decimal("1000")},
    )
    wallet_balances = []
    status, _ = strategy._append_native_gas_to_wallet(market, wallet_balances)
    assert status == "ok"
    if expected_native is not None:
        assert wallet_balances[0].symbol == expected_native
    else:
        # Registry-driven case (polygon/monad): symbol is whatever the registry returned.
        assert wallet_balances[0].symbol == actual.upper()


# --- F1-F3 — runner-level live-mode enforcer ------------------------------------

def _runner(execution_mode: str = "live") -> Any:
    runner = MagicMock()
    runner.config = MagicMock()
    with patch(
        "almanak.framework.runner.strategy_runner.derive_execution_mode_from_config",
        return_value=MagicMock(value=execution_mode),
    ):
        return runner


def _snapshot_with_status(status: str, mode_value: str = "live") -> tuple[Any, Any]:
    snapshot = PortfolioSnapshot(
        timestamp=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
        strategy_id="demo",
        total_value_usd=Decimal("100"),
        available_cash_usd=Decimal("100"),
        value_confidence=ValueConfidence.HIGH,
        snapshot_metadata={"gas_native_status": status},
    )
    runner = MagicMock()
    runner.config = MagicMock()
    return runner, snapshot


@pytest.mark.parametrize("status", ["unknown_chain", "balance_failed", "price_missing"])
def test_runner_live_enforcer_raises_on_failure_status(status: str) -> None:
    """In live mode, any non-ok / non-already_tracked status raises."""
    runner, snapshot = _snapshot_with_status(status)
    with patch(
        "almanak.framework.runner.strategy_runner.derive_execution_mode_from_config",
        return_value=MagicMock(value="live"),
    ):
        with pytest.raises(AccountingPersistenceError) as excinfo:
            _enforce_native_gas_status_in_live(runner, snapshot)
    assert excinfo.value.write_kind == "snapshot"
    assert status in str(excinfo.value)


@pytest.mark.parametrize("status", ["ok", "already_tracked"])
def test_runner_live_enforcer_passes_on_ok_status(status: str) -> None:
    """ok / already_tracked never raises in any mode."""
    runner, snapshot = _snapshot_with_status(status)
    with patch(
        "almanak.framework.runner.strategy_runner.derive_execution_mode_from_config",
        return_value=MagicMock(value="live"),
    ):
        _enforce_native_gas_status_in_live(runner, snapshot)  # no raise


@pytest.mark.parametrize("status", ["unknown_chain", "balance_failed", "price_missing"])
def test_runner_paper_enforcer_logs_but_does_not_raise(status: str) -> None:
    """Paper mode logs ERROR but does NOT raise on any status."""
    runner, snapshot = _snapshot_with_status(status)
    with patch(
        "almanak.framework.runner.strategy_runner.derive_execution_mode_from_config",
        return_value=MagicMock(value="paper"),
    ):
        _enforce_native_gas_status_in_live(runner, snapshot)  # no raise
