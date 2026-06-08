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

    def _balance(token: str, protocol: Any = None, *, chain: str | None = None, price: Any = None) -> Any:
        if token in bal_raises:
            raise RuntimeError(f"synthetic balance error for {token}")
        b = MagicMock()
        b.balance = bal_map.get(token, Decimal("0"))
        return b

    def _price(token: str, quote: str = "USD", *, chain: str | None = None) -> Any:
        if token in pr_raises:
            raise RuntimeError(f"synthetic price error for {token}")
        return price_map.get(token, Decimal("0"))

    market.balance.side_effect = _balance
    market.price.side_effect = _price
    return market


def _multichain_market_mock(
    *,
    chains: tuple[str, ...],
    balance_returns: dict[str, Decimal] | None = None,
    price_returns: dict[str, Decimal | None] | None = None,
) -> MagicMock:
    """Build a MarketSnapshot mock that behaves like a MULTI-chain snapshot:

    ``balance``/``price`` raise ``AmbiguousChainError`` when called with
    ``chain=None`` (the default), exactly as the real snapshot does, and only
    resolve when an explicit ``chain=`` is threaded through. This pins the
    VIB-5001 Fix 2 contract: ``_append_native_gas_to_wallet`` must pass
    ``chain=self._chain`` so native-gas accounting succeeds on a multi-chain
    snapshot instead of failing with ``balance_failed``.
    """
    from almanak.framework.market.errors import AmbiguousChainError

    market = MagicMock()
    bal_map = balance_returns or {}
    price_map = price_returns or {}

    def _balance(token: str, protocol: Any = None, *, chain: str | None = None, price: Any = None) -> Any:
        if chain is None:
            raise AmbiguousChainError(chains=chains)
        b = MagicMock()
        b.balance = bal_map.get(token, Decimal("0"))
        return b

    def _price(token: str, quote: str = "USD", *, chain: str | None = None) -> Any:
        if chain is None:
            raise AmbiguousChainError(chains=chains)
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


# --- VIB-5001 — native gas on a MULTI-chain snapshot ---------------------------

def test_native_gas_on_multichain_snapshot_passes_chain() -> None:
    """VIB-5001 Fix 2: native-gas accounting must succeed on a MULTI-chain snapshot.

    A multi-chain snapshot raises ``AmbiguousChainError`` for any
    ``balance``/``price`` call made with ``chain=None``. The helper must thread
    ``chain=self._chain`` through both reads so the native symbol is appended
    and status is 'ok' — NOT 'balance_failed' (which the live-mode enforcer
    escalates to ACCOUNTING_FAILED). Without the fix this returns
    'balance_failed' and the wallet stays empty.
    """
    strategy = _fake_strategy(chain="base")
    market = _multichain_market_mock(
        chains=("base", "arbitrum"),
        balance_returns={"ETH": Decimal("0.5")},
        price_returns={"ETH": Decimal("3000")},
    )
    wallet_balances = []
    status, value = strategy._append_native_gas_to_wallet(market, wallet_balances)
    assert status == "ok"
    assert value == Decimal("1500")  # 0.5 ETH * $3000
    assert len(wallet_balances) == 1
    assert wallet_balances[0].symbol == "ETH"
    # Confirm the chain was actually threaded through (not chain=None).
    assert market.balance.call_args.kwargs.get("chain") == "base"
    assert market.price.call_args.kwargs.get("chain") == "base"


def test_native_gas_falsy_chain_returns_unknown_chain_not_balance_failed() -> None:
    """VIB-5001 (CodeRabbit/gemini): a falsy ``self._chain`` must yield 'unknown_chain'.

    ``native_token_for_chain('')`` defaults to 'ETH' (truthy), so an empty/None
    ``self._chain`` slips past the ``if not native_symbol`` guard. Without an
    explicit ``self._chain`` guard the helper would call
    ``market.balance('ETH', chain=None)`` on a multi-chain snapshot, raise
    ``AmbiguousChainError``, and misclassify it as 'balance_failed' — a hard
    live-mode halt. The guard must short-circuit to 'unknown_chain' before any
    balance read.
    """
    strategy = _fake_strategy(chain="")
    market = _multichain_market_mock(
        chains=("base", "arbitrum"),
        balance_returns={"ETH": Decimal("0.5")},
        price_returns={"ETH": Decimal("3000")},
    )
    wallet_balances = []
    status, value = strategy._append_native_gas_to_wallet(market, wallet_balances)
    assert status == "unknown_chain"
    assert value == Decimal("0")
    assert wallet_balances == []
    # Guard must trip BEFORE any balance read (no AmbiguousChainError raised).
    market.balance.assert_not_called()
    market.price.assert_not_called()


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
        deployment_id="demo",
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
