"""Unit tests for Morpho Blue account-state reads (VIB-3483).

VIB-4929 PR-3a: the per-protocol ``read_morpho_blue_account_state`` is gone;
these tests now drive the generic
``read_lending_account_state(protocol="morpho_blue", ...)``, which resolves the
collateral/loan symbols + decimals + lltv from the connector market table (so
the explicit per-protocol args these tests used to pass are no longer needed).

Tests follow the same pattern as the Aave V3 equivalent tests in
tests/intents/arbitrum/test_accounting_e2e.py but are pure unit tests
that mock the gateway eth_call — no Anvil fork required.

Mock ABI encoding notes
-----------------------
Morpho Blue `position(bytes32, address)` returns 3 uint256 words (ABI standard):
  Word 0: supplyShares (uint256)
  Word 1: borrowShares (uint128 in uint256)
  Word 2: collateral   (uint128 in uint256)

Morpho Blue `market(bytes32)` returns 6 uint128 values packed as 6 uint256 words:
  Word 0: totalSupplyAssets
  Word 1: totalSupplyShares
  Word 2: totalBorrowAssets
  Word 3: totalBorrowShares
  Word 4: lastUpdate
  Word 5: fee
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

from web3 import Web3


# ─── Selector verification ────────────────────────────────────────────────────


class TestMorphoBlueSelectors:
    """Verify that the ABI function selector constants match keccak256 of their signatures.

    Any accidental drift (e.g. copy-paste of a wrong selector) will fail here.
    """

    def test_position_selector(self) -> None:
        """_MORPHO_POSITION_SELECTOR == first 4 bytes of keccak256('position(bytes32,address)')."""
        from almanak.connectors._strategy_base.lending_read_base import _MORPHO_POSITION_SELECTOR

        expected = "0x" + Web3.keccak(text="position(bytes32,address)").hex()[:8]
        assert _MORPHO_POSITION_SELECTOR == expected, (
            f"_MORPHO_POSITION_SELECTOR mismatch: got {_MORPHO_POSITION_SELECTOR!r}, expected {expected!r}"
        )

    def test_market_selector(self) -> None:
        """_MORPHO_MARKET_SELECTOR == first 4 bytes of keccak256('market(bytes32)')."""
        from almanak.connectors._strategy_base.lending_read_base import _MORPHO_MARKET_SELECTOR

        expected = "0x" + Web3.keccak(text="market(bytes32)").hex()[:8]
        assert _MORPHO_MARKET_SELECTOR == expected, (
            f"_MORPHO_MARKET_SELECTOR mismatch: got {_MORPHO_MARKET_SELECTOR!r}, expected {expected!r}"
        )

    def test_calldata_uses_position_selector(self) -> None:
        """read_morpho_blue_account_state() builds calldata that starts with _MORPHO_POSITION_SELECTOR."""
        from almanak.connectors._strategy_base.lending_read_base import _MORPHO_POSITION_SELECTOR
        from almanak.framework.accounting.lending_accounting import read_lending_account_state

        pos_response = _mock_position_response(0, 100_000_000, 1 * 10**18)
        mkt_response = _mock_market_response(
            total_supply_assets=10_000 * 10**6,
            total_supply_shares=10_000 * 10**18,
            total_borrow_assets=10_000 * 10**6,
            total_borrow_shares=100_000_000,
        )
        gateway = _make_mock_gateway(pos_response, mkt_response)

        read_lending_account_state(
            protocol="morpho_blue",
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            market_id=_MARKET_ID,
            price_oracle=_PRICE_ORACLE,
        )

        # First eth_call must have used the position() selector
        first_call_args = gateway.eth_call.call_args_list[0]
        # eth_call(chain, address, calldata) — calldata is the third positional arg
        if first_call_args[0]:
            calldata = first_call_args[0][2]
        else:
            kwargs = first_call_args[1]
            calldata = kwargs.get("calldata") or kwargs.get("data")
        assert calldata is not None
        assert calldata.startswith(_MORPHO_POSITION_SELECTOR), (
            f"First eth_call calldata {calldata[:12]!r} must start with position() selector {_MORPHO_POSITION_SELECTOR!r}"
        )

    def test_calldata_uses_market_selector(self) -> None:
        """read_morpho_blue_account_state() builds calldata that starts with _MORPHO_MARKET_SELECTOR."""
        from almanak.connectors._strategy_base.lending_read_base import _MORPHO_MARKET_SELECTOR
        from almanak.framework.accounting.lending_accounting import read_lending_account_state

        pos_response = _mock_position_response(0, 100_000_000, 1 * 10**18)
        mkt_response = _mock_market_response(
            total_supply_assets=10_000 * 10**6,
            total_supply_shares=10_000 * 10**18,
            total_borrow_assets=10_000 * 10**6,
            total_borrow_shares=100_000_000,
        )
        gateway = _make_mock_gateway(pos_response, mkt_response)

        read_lending_account_state(
            protocol="morpho_blue",
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            market_id=_MARKET_ID,
            price_oracle=_PRICE_ORACLE,
        )

        # Second eth_call must have used the market() selector
        second_call_args = gateway.eth_call.call_args_list[1]
        if second_call_args[0]:
            calldata = second_call_args[0][2]
        else:
            kwargs = second_call_args[1]
            calldata = kwargs.get("calldata") or kwargs.get("data")
        assert calldata is not None
        assert calldata.startswith(_MORPHO_MARKET_SELECTOR), (
            f"Second eth_call calldata {calldata[:12]!r} must start with market() selector {_MORPHO_MARKET_SELECTOR!r}"
        )


# ─── Helpers to build mock ABI return data ────────────────────────────────────


def _encode_word(value: int) -> str:
    """Encode a single uint256 value as 64 hex chars (no 0x)."""
    return hex(value)[2:].zfill(64)


def _mock_position_response(supply_shares: int, borrow_shares: int, collateral: int) -> str:
    """Build a hex string matching position() ABI return."""
    return "0x" + _encode_word(supply_shares) + _encode_word(borrow_shares) + _encode_word(collateral)


def _mock_market_response(
    total_supply_assets: int,
    total_supply_shares: int,
    total_borrow_assets: int,
    total_borrow_shares: int,
    last_update: int = 0,
    fee: int = 0,
) -> str:
    """Build a hex string matching market() ABI return (6 uint128 as 6 uint256 words)."""
    return (
        "0x"
        + _encode_word(total_supply_assets)
        + _encode_word(total_supply_shares)
        + _encode_word(total_borrow_assets)
        + _encode_word(total_borrow_shares)
        + _encode_word(last_update)
        + _encode_word(fee)
    )


def _make_mock_gateway(position_response: str, market_response: str) -> MagicMock:
    """Create a mock gateway that returns the given hex strings for successive eth_call() calls."""
    gateway = MagicMock()
    gateway.eth_call.side_effect = [position_response, market_response]
    return gateway


# ─── Constants shared across tests ────────────────────────────────────────────

_MARKET_ID = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"
_WALLET = "0x1234567890123456789012345678901234567890"
_CHAIN = "ethereum"

# wstETH/USDC market: 86% LLTV
_LLTV_RAW = 860_000_000_000_000_000  # 0.86e18

_PRICE_ORACLE = {
    "wstETH": Decimal("3500"),
    "USDC": Decimal("1"),
    "WSTETH": Decimal("3500"),
}

# 1 wstETH collateral = 18 decimals
_COLLATERAL_DECIMALS = 18
# USDC loan = 6 decimals
_LOAN_DECIMALS = 6


# ─── Test Cases ───────────────────────────────────────────────────────────────


class TestMorphoBlueAccountStateReadViaMockGatewayAfterBorrow:
    """Mock the gateway eth_call to return position data; assert HF fields populated."""

    def test_basic_borrow_populates_all_hf_fields(self) -> None:
        """Borrow 100 USDC against 1 wstETH collateral; verify HF, collateral_usd, debt_usd."""
        from almanak.framework.accounting.lending_accounting import read_lending_account_state

        # 1 wstETH = 1e18 raw units
        collateral_raw = 1 * 10**18

        # 100 USDC borrowed as 100_000_000 raw borrow shares (exact 1:1 for simplicity)
        # Market has 10_000 USDC total borrow assets and 10_000 shares → 1:1 ratio
        borrow_shares = 100_000_000  # 100 USDC in share units
        total_borrow_assets = 10_000 * 10**6  # 10_000 USDC
        total_borrow_shares = 10_000 * 10**6  # same for 1:1

        pos_response = _mock_position_response(
            supply_shares=0,
            borrow_shares=borrow_shares,
            collateral=collateral_raw,
        )
        mkt_response = _mock_market_response(
            total_supply_assets=20_000 * 10**6,
            total_supply_shares=20_000 * 10**6,
            total_borrow_assets=total_borrow_assets,
            total_borrow_shares=total_borrow_shares,
        )
        gateway = _make_mock_gateway(pos_response, mkt_response)

        state = read_lending_account_state(
            protocol="morpho_blue",
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            market_id=_MARKET_ID,
            price_oracle=_PRICE_ORACLE,
        )

        assert state is not None, "State must not be None when gateway returns valid data"

        # collateral: 1 wstETH * $3500 = $3500
        assert state.collateral_usd == Decimal("3500"), f"collateral_usd expected $3500, got {state.collateral_usd}"

        # debt: 100 USDC * $1 = $100
        assert state.debt_usd == Decimal("100"), f"debt_usd expected $100, got {state.debt_usd}"

        # lltv: 86%
        assert state.lltv == Decimal("0.86"), f"lltv expected 0.86, got {state.lltv}"

        # health_factor = (3500 * 0.86) / 100 = 30.1
        expected_hf = (Decimal("3500") * Decimal("0.86")) / Decimal("100")
        assert abs(state.health_factor - expected_hf) < Decimal("0.001"), (
            f"health_factor expected ~{expected_hf}, got {state.health_factor}"
        )

        # Gateway was called exactly twice: position() then market()
        assert gateway.eth_call.call_count == 2

    def test_hf_field_wired_into_lending_accounting_event(self) -> None:
        """Full pipeline test: build_lending_accounting_event() populates HF fields for Morpho BORROW."""
        from almanak.framework.accounting.basis import FIFOBasisStore
        from almanak.framework.accounting.lending_accounting import build_lending_accounting_event
        from almanak.framework.accounting.models import AccountingConfidence

        collateral_raw = 1 * 10**18  # 1 wstETH
        borrow_shares = 100_000_000
        total_borrow = 10_000 * 10**6

        pos_response = _mock_position_response(0, borrow_shares, collateral_raw)
        mkt_response = _mock_market_response(
            total_supply_assets=20_000 * 10**6,
            total_supply_shares=20_000 * 10**6,
            total_borrow_assets=total_borrow,
            total_borrow_shares=total_borrow,
        )
        gateway = _make_mock_gateway(pos_response, mkt_response)

        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        intent.protocol = "morpho_blue"
        intent.market_id = _MARKET_ID
        intent.borrow_token = "USDC"
        intent.collateral_token = "wstETH"
        intent.token = None

        result = MagicMock()
        result.tx_hash = "0xdeadbeef"
        result.extracted_data = {"borrow_amount": 100_000_000}  # 100 USDC in 6-dec
        result.total_gas_cost_wei = None

        event = build_lending_accounting_event(
            intent=intent,
            result=result,
            deployment_id="strat-1",
            cycle_id="cycle-001",
            execution_mode="paper",
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=gateway,
            basis_store=FIFOBasisStore(),
            price_oracle=_PRICE_ORACLE,
            ledger_entry_id=None,
        )

        assert event is not None

        assert event.health_factor_after is not None, "health_factor_after must be populated for morpho_blue BORROW"
        assert event.collateral_value_after_usd is not None, "collateral_value_after_usd must be populated"
        assert event.debt_value_after_usd is not None, "debt_value_after_usd must be populated"
        assert event.net_equity_after_usd is not None, (
            "net_equity_after_usd must be populated (collateral_value_after - debt_value_after)"
        )
        assert event.liquidation_threshold is not None, "liquidation_threshold must be populated from Morpho lltv"
        assert event.lltv is not None, "lltv must be populated for morpho_blue BORROW"
        assert event.confidence == AccountingConfidence.HIGH, "Confidence must be HIGH when after-state read succeeds"
        assert event.unavailable_reason == "", "unavailable_reason must be empty when after-state read succeeds"


class TestMorphoBlueAccountStateReadFailsGracefully:
    """Mock gateway eth_call to raise an exception; assert fields are None with non-empty reason."""

    def test_gateway_exception_returns_none(self) -> None:
        """When gateway.eth_call raises, read_morpho_blue_account_state returns None."""
        from almanak.framework.accounting.lending_accounting import read_lending_account_state

        gateway = MagicMock()
        gateway.eth_call.side_effect = RuntimeError("gateway connection refused")

        state = read_lending_account_state(
            protocol="morpho_blue",
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            market_id=_MARKET_ID,
            price_oracle=_PRICE_ORACLE,
        )

        assert state is None, "read_morpho_blue_account_state must return None when gateway raises"

    def test_gateway_returns_empty_string_returns_none(self) -> None:
        """When gateway.eth_call returns empty string, read_morpho_blue_account_state returns None."""
        from almanak.framework.accounting.lending_accounting import read_lending_account_state

        gateway = MagicMock()
        gateway.eth_call.return_value = ""

        state = read_lending_account_state(
            protocol="morpho_blue",
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            market_id=_MARKET_ID,
            price_oracle=_PRICE_ORACLE,
        )

        assert state is None, "read_morpho_blue_account_state must return None on empty response"

    def test_gateway_returns_malformed_market_payload_returns_none(self) -> None:
        """When market() returns an undersized payload (5 words instead of 6), return None."""
        from almanak.framework.accounting.lending_accounting import read_lending_account_state

        # Correct position() response so the first call succeeds
        pos_response = _mock_position_response(0, 100_000_000, 1 * 10**18)
        # Truncated market() — only 5 words (320 hex chars) instead of 6 (384 hex chars)
        five_word_market = "0x" + _encode_word(10_000 * 10**6) * 5  # 5 × 64 hex = 320 chars

        gateway = MagicMock()
        gateway.eth_call.side_effect = [pos_response, five_word_market]

        state = read_lending_account_state(
            protocol="morpho_blue",
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            market_id=_MARKET_ID,
            price_oracle=_PRICE_ORACLE,
        )

        assert state is None, "read_morpho_blue_account_state must return None when market() payload is too short"

    def test_build_event_graceful_when_gateway_fails(self) -> None:
        """build_lending_accounting_event sets ESTIMATED confidence + non-empty reason when read fails."""
        from almanak.framework.accounting.basis import FIFOBasisStore
        from almanak.framework.accounting.lending_accounting import build_lending_accounting_event
        from almanak.framework.accounting.models import AccountingConfidence

        gateway = MagicMock()
        gateway.eth_call.side_effect = RuntimeError("gateway connection refused")

        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        intent.protocol = "morpho_blue"
        intent.market_id = _MARKET_ID
        intent.borrow_token = "USDC"
        intent.collateral_token = "wstETH"
        intent.token = None

        result = MagicMock()
        result.tx_hash = "0xdeadbeef"
        result.extracted_data = {}
        result.total_gas_cost_wei = None

        event = build_lending_accounting_event(
            intent=intent,
            result=result,
            deployment_id="strat-1",
            cycle_id="cycle-001",
            execution_mode="paper",
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=gateway,
            basis_store=FIFOBasisStore(),
            price_oracle=_PRICE_ORACLE,
            ledger_entry_id=None,
        )

        assert event is not None
        assert event.health_factor_after is None, "health_factor_after must be None when gateway raises"
        assert event.collateral_value_after_usd is None
        assert event.debt_value_after_usd is None
        assert event.confidence == AccountingConfidence.ESTIMATED, (
            "Confidence must be ESTIMATED when after-state read fails"
        )
        assert event.unavailable_reason != "", "unavailable_reason must be non-empty when after-state read fails"


class TestMorphoBlueAccountStateZeroBorrow:
    """borrow_shares = 0; assert no division-by-zero, health_factor = 999999 (infinite sentinel)."""

    def test_zero_borrow_shares_returns_infinite_hf_sentinel(self) -> None:
        """With no borrow position, HF should be the infinite sentinel (999999)."""
        from almanak.framework.accounting.lending_accounting import read_lending_account_state

        collateral_raw = 1 * 10**18  # 1 wstETH

        pos_response = _mock_position_response(
            supply_shares=0,
            borrow_shares=0,  # no borrow
            collateral=collateral_raw,
        )
        mkt_response = _mock_market_response(
            total_supply_assets=20_000 * 10**6,
            total_supply_shares=20_000 * 10**6,
            total_borrow_assets=0,
            total_borrow_shares=0,
        )
        gateway = _make_mock_gateway(pos_response, mkt_response)

        state = read_lending_account_state(
            protocol="morpho_blue",
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            market_id=_MARKET_ID,
            price_oracle=_PRICE_ORACLE,
        )

        assert state is not None, "State must be returned even with zero borrow"

        # No debt
        assert state.debt_usd == Decimal("0"), f"debt_usd must be $0 with no borrow. Got {state.debt_usd}"

        # HF is the infinite sentinel (999999) — not None, not a real number
        assert state.health_factor == Decimal("999999"), (
            f"health_factor must be 999999 (infinite sentinel) with no debt. Got {state.health_factor}"
        )

        # Collateral still present
        assert state.collateral_usd == Decimal("3500"), (
            f"collateral_usd must be $3500 for 1 wstETH. Got {state.collateral_usd}"
        )

    def test_zero_borrow_shares_no_division_by_zero_on_zero_total_shares(self) -> None:
        """Market with totalBorrowShares = 0 must not raise ZeroDivisionError."""
        from almanak.framework.accounting.lending_accounting import read_lending_account_state

        collateral_raw = 1 * 10**18

        pos_response = _mock_position_response(
            supply_shares=500 * 10**18,
            borrow_shares=0,
            collateral=collateral_raw,
        )
        # Market with no borrows at all
        mkt_response = _mock_market_response(
            total_supply_assets=500 * 10**6,
            total_supply_shares=500 * 10**18,
            total_borrow_assets=0,
            total_borrow_shares=0,
        )
        gateway = _make_mock_gateway(pos_response, mkt_response)

        # Should not raise
        state = read_lending_account_state(
            protocol="morpho_blue",
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            market_id=_MARKET_ID,
            price_oracle=_PRICE_ORACLE,
        )

        assert state is not None
        assert state.debt_usd == Decimal("0")
        assert state.health_factor == Decimal("999999")

    def test_nonzero_borrow_shares_but_zero_total_borrow_assets_gives_zero_debt(self) -> None:
        """If borrow shares > 0 but market has no borrow assets, borrow_assets resolves to 0."""
        from almanak.framework.accounting.lending_accounting import read_lending_account_state

        collateral_raw = 1 * 10**18

        pos_response = _mock_position_response(
            supply_shares=0,
            borrow_shares=100 * 10**6,  # some shares recorded
            collateral=collateral_raw,
        )
        mkt_response = _mock_market_response(
            total_supply_assets=10_000 * 10**6,
            total_supply_shares=10_000 * 10**18,
            total_borrow_assets=0,  # edge case: assets are 0
            total_borrow_shares=100 * 10**6,
        )
        gateway = _make_mock_gateway(pos_response, mkt_response)

        state = read_lending_account_state(
            protocol="morpho_blue",
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            market_id=_MARKET_ID,
            price_oracle=_PRICE_ORACLE,
        )

        assert state is not None
        # borrow_assets = 100e6 * 0 / 100e6 = 0 → debt_usd = 0
        assert state.debt_usd == Decimal("0")
        assert state.health_factor == Decimal("999999")


class TestMorphoBlueAccountStateAdditionalCoverage:
    """Additional edge-case coverage."""

    def test_missing_price_oracle_returns_none(self) -> None:
        """When price_oracle is None, read_morpho_blue_account_state returns None."""
        from almanak.framework.accounting.lending_accounting import read_lending_account_state

        collateral_raw = 1 * 10**18
        borrow_shares = 100_000_000
        total_borrow = 10_000 * 10**6

        pos_response = _mock_position_response(0, borrow_shares, collateral_raw)
        mkt_response = _mock_market_response(
            total_supply_assets=total_borrow,
            total_supply_shares=total_borrow,
            total_borrow_assets=total_borrow,
            total_borrow_shares=total_borrow,
        )
        gateway = _make_mock_gateway(pos_response, mkt_response)

        state = read_lending_account_state(
            protocol="morpho_blue",
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            market_id=_MARKET_ID,
            price_oracle=None,  # no prices
        )

        assert state is None, "State must be None when price_oracle is None"

    def test_unsupported_chain_returns_none(self) -> None:
        """When chain has no Morpho Blue address, return None."""
        from almanak.framework.accounting.lending_accounting import read_lending_account_state

        gateway = MagicMock()

        state = read_lending_account_state(
            protocol="morpho_blue",
            gateway_client=gateway,
            chain="fantom",  # not in MORPHO_BLUE_ADDRESSES
            wallet_address=_WALLET,
            market_id=_MARKET_ID,
            price_oracle=_PRICE_ORACLE,
        )

        assert state is None, "State must be None for chain with no Morpho Blue deployment"
        # Should not have called eth_call at all
        gateway.eth_call.assert_not_called()

    def test_repay_intent_also_populates_hf(self) -> None:
        """REPAY intents also trigger Morpho Blue HF read."""
        from almanak.framework.accounting.basis import FIFOBasisStore
        from almanak.framework.accounting.lending_accounting import build_lending_accounting_event
        from almanak.framework.accounting.models import AccountingConfidence

        # After repay: 0.5 wstETH collateral, 50 USDC debt remaining
        collateral_raw = int(0.5 * 10**18)
        borrow_shares = 50_000_000
        total_borrow = 10_000 * 10**6

        pos_response = _mock_position_response(0, borrow_shares, collateral_raw)
        mkt_response = _mock_market_response(
            total_supply_assets=total_borrow,
            total_supply_shares=total_borrow,
            total_borrow_assets=total_borrow,
            total_borrow_shares=total_borrow,
        )
        gateway = _make_mock_gateway(pos_response, mkt_response)

        intent = MagicMock()
        intent.intent_type.value = "REPAY"
        intent.protocol = "morpho_blue"
        intent.market_id = _MARKET_ID
        intent.borrow_token = "USDC"
        intent.collateral_token = "wstETH"
        intent.token = "USDC"

        result = MagicMock()
        result.tx_hash = "0xdeadbeef"
        result.extracted_data = {"repay_amount": 50_000_000}  # 50 USDC
        result.total_gas_cost_wei = None

        event = build_lending_accounting_event(
            intent=intent,
            result=result,
            deployment_id="strat-1",
            cycle_id="cycle-001",
            execution_mode="paper",
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=gateway,
            basis_store=FIFOBasisStore(),
            price_oracle=_PRICE_ORACLE,
            ledger_entry_id=None,
        )

        assert event is not None
        assert event.health_factor_after is not None, "health_factor_after must be populated for morpho_blue REPAY"
        assert event.collateral_value_after_usd is not None
        assert event.debt_value_after_usd is not None
        assert event.lltv is not None
        assert event.confidence == AccountingConfidence.HIGH

    def test_supply_intent_triggers_morpho_hf_read(self) -> None:
        """VIB-4432: SUPPLY/WITHDRAW now trigger the Morpho Blue post-state HF
        read in parity with the pre-state arm (and with the Compound V3 branch
        at line 1570, which already handles SUPPLY/WITHDRAW).

        Previously this test asserted ``eth_call.assert_not_called()`` for
        SUPPLY — that encoded the pre-VIB-4432 guard ``intent_type_str in
        ("BORROW", "REPAY", "DELEVERAGE")`` which Gemini flagged as a
        consistency gap (post-state events for SUPPLY/WITHDRAW would ship
        with ESTIMATED confidence because no HF/debt was read). The
        behaviour change is intentional and now matches Compound V3.
        """
        from almanak.framework.accounting.basis import FIFOBasisStore
        from almanak.framework.accounting.lending_accounting import build_lending_accounting_event

        gateway = MagicMock()
        # Gateway will be called for SUPPLY post-state; return empty so the
        # read does not actually attempt to decode a market reply. The
        # important assertion is *that* it was called, not what it returned.
        gateway.eth_call.return_value = ""

        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"
        intent.protocol = "morpho_blue"
        intent.market_id = _MARKET_ID
        intent.borrow_token = None
        intent.collateral_token = "wstETH"
        intent.token = "wstETH"  # for SUPPLY-as-collateral, intent.token IS the collateral
        intent.use_as_collateral = True

        result = MagicMock()
        result.tx_hash = "0xdeadbeef"
        result.extracted_data = {"supply_collateral_amount": 1_000 * 10**18}
        result.total_gas_cost_wei = None

        event = build_lending_accounting_event(
            intent=intent,
            result=result,
            deployment_id="strat-1",
            cycle_id="cycle-001",
            execution_mode="paper",
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=gateway,
            basis_store=FIFOBasisStore(),
            price_oracle=_PRICE_ORACLE,
            ledger_entry_id=None,
        )

        assert event is not None
        # Post-VIB-4432: SUPPLY does trigger the Morpho HF post-state read,
        # in parity with BORROW/REPAY/DELEVERAGE.
        assert gateway.eth_call.called, (
            "SUPPLY must trigger the Morpho Blue post-state HF read (VIB-4432 "
            "parity with the pre-state arm and Compound V3's post-state branch). "
            f"Got called={gateway.eth_call.called}, call_count={gateway.eth_call.call_count}"
        )

    def test_deleverage_intent_populates_hf(self) -> None:
        """DELEVERAGE intents trigger Morpho Blue HF read, identical to REPAY."""
        from almanak.framework.accounting.basis import FIFOBasisStore
        from almanak.framework.accounting.lending_accounting import build_lending_accounting_event
        from almanak.framework.accounting.models import AccountingConfidence

        # After deleverage: 0.5 wstETH collateral, 50 USDC debt remaining
        collateral_raw = int(0.5 * 10**18)
        borrow_shares = 50_000_000
        total_borrow = 10_000 * 10**6

        pos_response = _mock_position_response(0, borrow_shares, collateral_raw)
        mkt_response = _mock_market_response(
            total_supply_assets=total_borrow,
            total_supply_shares=total_borrow,
            total_borrow_assets=total_borrow,
            total_borrow_shares=total_borrow,
        )
        gateway = _make_mock_gateway(pos_response, mkt_response)

        intent = MagicMock()
        intent.intent_type.value = "DELEVERAGE"
        intent.protocol = "morpho_blue"
        intent.market_id = _MARKET_ID
        intent.borrow_token = "USDC"
        intent.collateral_token = "wstETH"
        intent.token = "USDC"

        result = MagicMock()
        result.tx_hash = "0xdeadbeef"
        result.extracted_data = {"repay_amount": 50_000_000}  # 50 USDC repaid via deleverage
        result.total_gas_cost_wei = None

        event = build_lending_accounting_event(
            intent=intent,
            result=result,
            deployment_id="strat-1",
            cycle_id="cycle-001",
            execution_mode="paper",
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=gateway,
            basis_store=FIFOBasisStore(),
            price_oracle=_PRICE_ORACLE,
            ledger_entry_id=None,
        )

        assert event is not None, "DELEVERAGE must produce an accounting event"
        assert event.health_factor_after is not None, "health_factor_after must be populated for morpho_blue DELEVERAGE"
        assert event.collateral_value_after_usd is not None
        assert event.debt_value_after_usd is not None
        assert event.lltv is not None
        assert event.confidence == AccountingConfidence.HIGH
        # Gateway was called exactly twice: position() then market()
        assert gateway.eth_call.call_count == 2
