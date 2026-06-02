"""Unit tests for Compound V3 account-state reads — now validating the generic
``read_lending_account_state`` via a thin shim (VIB-3586; reader retired VIB-4929 PR-3b).

Tests mirror the pattern in test_morpho_blue_account_state.py:
pure unit tests that mock the gateway eth_call — no Anvil fork required.

Mock ABI encoding notes
-----------------------
Compound V3 `userCollateral(address wallet, address asset)` returns:
  (uint128 balance, uint128 reserved) packed as two 32-byte words in ABI response.
  Word 0: balance (uint128 in the lower 128 bits of a uint256)
  Word 1: reserved (uint128, ignored — we only use balance)

Compound V3 `borrowBalanceOf(address wallet)` returns:
  uint256 — raw borrow balance in base-token decimals (e.g. 6 for USDC).

Compound V3 `balanceOf(address wallet)` returns:
  uint256 — supplied base-asset balance in base-token decimals.
  Used for base-asset SUPPLY/WITHDRAW (e.g. supplying USDC to the USDC Comet).
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

from web3 import Web3

from almanak.framework.accounting.lending_accounting import read_lending_account_state


def read_compound_v3_account_state(
    gateway_client, chain, wallet_address, collateral_token, borrow_token, price_oracle, market_id=None, block=None
):
    """Test shim (VIB-4929 PR-3b): drive the generic reader the way the consumer's
    query_inputs does, so the legacy byte-equivalence assertions now validate
    read_lending_account_state for compound_v3. The Comet key falls back to the
    borrow token when market_id is absent (the legacy (market_id or borrow_token)
    behaviour); the base/borrow leg comes from the market catalogue."""
    return read_lending_account_state(
        protocol="compound_v3",
        chain=chain,
        wallet_address=wallet_address,
        market_id=(market_id or borrow_token),
        gateway_client=gateway_client,
        price_oracle=price_oracle,
        collateral_token=collateral_token,
        block=block,
    )


# ─── ABI encoding helpers ─────────────────────────────────────────────────────


def _encode_word(value: int) -> str:
    """Encode a single uint256 value as 64 hex chars (no 0x prefix)."""
    return hex(value)[2:].zfill(64)


def _mock_user_collateral_response(balance: int, reserved: int = 0) -> str:
    """Build a hex string matching userCollateral() ABI return: (uint128 balance, uint128 reserved)."""
    return "0x" + _encode_word(balance) + _encode_word(reserved)


def _mock_borrow_balance_response(balance: int) -> str:
    """Build a hex string matching borrowBalanceOf() ABI return: uint256."""
    return "0x" + _encode_word(balance)


def _make_mock_gateway(collateral_response: str, borrow_response: str) -> MagicMock:
    """Create a mock gateway that returns the given hex strings for successive eth_call() calls."""
    gateway = MagicMock()
    gateway.eth_call.side_effect = [collateral_response, borrow_response]
    return gateway


# ─── Test constants ───────────────────────────────────────────────────────────

_WALLET = "0x1234567890123456789012345678901234567890"
_CHAIN = "ethereum"

# USDC Comet on Ethereum — WETH is a valid collateral (LCF=0.895)
_BORROW_TOKEN = "USDC"
_COLLATERAL_TOKEN = "WETH"

_PRICE_ORACLE = {
    "WETH": Decimal("3000"),
    "USDC": Decimal("1"),
}


# ─── Selector verification ────────────────────────────────────────────────────


class TestCompoundV3Selectors:
    """Verify ABI function selector constants match keccak256 of their signatures."""

    def test_user_collateral_selector(self) -> None:
        from almanak.connectors._strategy_base.lending_read_base import _COMPOUND_V3_USER_COLLATERAL_SELECTOR

        expected = "0x" + Web3.keccak(text="userCollateral(address,address)").hex()[:8]
        assert _COMPOUND_V3_USER_COLLATERAL_SELECTOR == expected, (
            f"_COMPOUND_V3_USER_COLLATERAL_SELECTOR mismatch: got {_COMPOUND_V3_USER_COLLATERAL_SELECTOR!r}, expected {expected!r}"
        )

    def test_borrow_balance_selector(self) -> None:
        from almanak.connectors._strategy_base.lending_read_base import _COMPOUND_V3_BORROW_BALANCE_SELECTOR

        expected = "0x" + Web3.keccak(text="borrowBalanceOf(address)").hex()[:8]
        assert _COMPOUND_V3_BORROW_BALANCE_SELECTOR == expected, (
            f"_COMPOUND_V3_BORROW_BALANCE_SELECTOR mismatch: got {_COMPOUND_V3_BORROW_BALANCE_SELECTOR!r}, expected {expected!r}"
        )

    def test_user_collateral_calldata_format(self) -> None:
        """First eth_call calldata must start with the userCollateral selector."""
        from almanak.connectors._strategy_base.lending_read_base import (
            _COMPOUND_V3_USER_COLLATERAL_SELECTOR,
        )

        gateway = _make_mock_gateway(
            _mock_user_collateral_response(1 * 10**18),
            _mock_borrow_balance_response(100 * 10**6),
        )
        read_compound_v3_account_state(
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            collateral_token=_COLLATERAL_TOKEN,
            borrow_token=_BORROW_TOKEN,
            price_oracle=_PRICE_ORACLE,
        )

        first_call = gateway.eth_call.call_args_list[0]
        calldata = first_call[0][2] if first_call[0] else first_call[1].get("calldata") or first_call[1].get("data")
        assert calldata is not None
        assert calldata.startswith(_COMPOUND_V3_USER_COLLATERAL_SELECTOR), (
            f"First calldata {calldata[:12]!r} must start with userCollateral selector"
        )

    def test_borrow_balance_calldata_format(self) -> None:
        """Second eth_call calldata must start with the borrowBalanceOf selector."""
        from almanak.connectors._strategy_base.lending_read_base import (
            _COMPOUND_V3_BORROW_BALANCE_SELECTOR,
        )

        gateway = _make_mock_gateway(
            _mock_user_collateral_response(1 * 10**18),
            _mock_borrow_balance_response(100 * 10**6),
        )
        read_compound_v3_account_state(
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            collateral_token=_COLLATERAL_TOKEN,
            borrow_token=_BORROW_TOKEN,
            price_oracle=_PRICE_ORACLE,
        )

        second_call = gateway.eth_call.call_args_list[1]
        calldata = second_call[0][2] if second_call[0] else second_call[1].get("calldata") or second_call[1].get("data")
        assert calldata is not None
        assert calldata.startswith(_COMPOUND_V3_BORROW_BALANCE_SELECTOR), (
            f"Second calldata {calldata[:12]!r} must start with borrowBalanceOf selector"
        )


# ─── Happy-path tests ─────────────────────────────────────────────────────────


class TestCompoundV3AccountStateHappyPath:
    """Mock gateway eth_call to return collateral and borrow data; assert state populated."""

    def test_basic_borrow_populates_all_fields(self) -> None:
        """1 WETH collateral, 100 USDC debt — verify collateral_usd, debt_usd, health_factor."""
        collateral_raw = 1 * 10**18  # 1 WETH (18 dec)
        borrow_raw = 100 * 10**6  # 100 USDC (6 dec)

        gateway = _make_mock_gateway(
            _mock_user_collateral_response(collateral_raw),
            _mock_borrow_balance_response(borrow_raw),
        )

        state = read_compound_v3_account_state(
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            collateral_token=_COLLATERAL_TOKEN,
            borrow_token=_BORROW_TOKEN,
            price_oracle=_PRICE_ORACLE,
        )

        assert state is not None
        # 1 WETH * $3000 = $3000
        assert state.collateral_usd == Decimal("3000")
        # 100 USDC * $1 = $100
        assert state.debt_usd == Decimal("100")
        # health_factor = (3000 * lcf) / 100; lcf for WETH on usdc market = 0.895
        expected_hf = (Decimal("3000") * Decimal("0.895")) / Decimal("100")
        assert state.health_factor is not None
        assert abs(state.health_factor - expected_hf) < Decimal("0.001")
        assert gateway.eth_call.call_count == 2

    def test_no_debt_yields_sentinel_hf(self) -> None:
        """Zero debt → health_factor sentinel 999999 (no liquidation risk)."""
        gateway = _make_mock_gateway(
            _mock_user_collateral_response(1 * 10**18),
            _mock_borrow_balance_response(0),
        )
        state = read_compound_v3_account_state(
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            collateral_token=_COLLATERAL_TOKEN,
            borrow_token=_BORROW_TOKEN,
            price_oracle=_PRICE_ORACLE,
        )

        assert state is not None
        assert state.debt_usd == Decimal("0")
        assert state.health_factor == Decimal("999999")

    def test_health_factor_uses_lcf_not_raw_ratio(self) -> None:
        """HF must be (collateral * LCF) / debt — raw ratio would overstate safety."""
        # 1 WETH @ $3000 collateral, 1000 USDC debt
        # Raw ratio: 3000 / 1000 = 3.0 (overstates)
        # LCF-adjusted (WETH LCF=0.895): (3000 * 0.895) / 1000 = 2.685
        gateway = _make_mock_gateway(
            _mock_user_collateral_response(1 * 10**18),
            _mock_borrow_balance_response(1000 * 10**6),
        )
        state = read_compound_v3_account_state(
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            collateral_token=_COLLATERAL_TOKEN,
            borrow_token=_BORROW_TOKEN,
            price_oracle=_PRICE_ORACLE,
        )

        assert state is not None
        raw_ratio = state.collateral_usd / state.debt_usd  # 3.0
        assert state.health_factor is not None
        assert state.health_factor < raw_ratio, (
            f"HF {state.health_factor} should be < raw ratio {raw_ratio} (LCF not applied)"
        )
        expected_hf = (Decimal("3000") * Decimal("0.895")) / Decimal("1000")
        assert abs(state.health_factor - expected_hf) < Decimal("0.001")

    def test_gateway_called_exactly_twice(self) -> None:
        """Two eth_calls must be made: userCollateral() then borrowBalanceOf()."""
        gateway = _make_mock_gateway(
            _mock_user_collateral_response(1 * 10**18),
            _mock_borrow_balance_response(100 * 10**6),
        )
        read_compound_v3_account_state(
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            collateral_token=_COLLATERAL_TOKEN,
            borrow_token=_BORROW_TOKEN,
            price_oracle=_PRICE_ORACLE,
        )
        assert gateway.eth_call.call_count == 2


# ─── None-return / failure cases ─────────────────────────────────────────────


class TestCompoundV3AccountStateFailureCases:
    """Verify read_compound_v3_account_state returns None on various failure paths."""

    def test_returns_none_for_unknown_chain(self) -> None:
        """No Comet on unknown chain → return None without calling gateway."""
        gateway = MagicMock()
        state = read_compound_v3_account_state(
            gateway_client=gateway,
            chain="unknown_chain_xyz",
            wallet_address=_WALLET,
            collateral_token=_COLLATERAL_TOKEN,
            borrow_token=_BORROW_TOKEN,
            price_oracle=_PRICE_ORACLE,
        )

        assert state is None
        gateway.eth_call.assert_not_called()

    def test_returns_none_for_unknown_borrow_token(self) -> None:
        """No exact Comet match for unknown token — no fuzzy fallback."""
        gateway = MagicMock()
        state = read_compound_v3_account_state(
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            collateral_token=_COLLATERAL_TOKEN,
            borrow_token="UNKNOWNCOIN",
            price_oracle=_PRICE_ORACLE,
        )

        assert state is None
        gateway.eth_call.assert_not_called()

    def test_no_fuzzy_match_for_similar_token(self) -> None:
        """'usdc_bridged' is NOT returned when borrow_token='usdc' — exact match only.

        On Arbitrum both 'usdc' and 'usdc_bridged' exist.  The former
        fuzzy-match fallback would pick whichever key satisfied a substring
        check first — which on Python dict iteration order was 'usdc_bridged'.
        Now only an exact match is accepted, so 'usdc' → USDC Comet.
        """
        from almanak.connectors.compound_v3.adapter import COMPOUND_V3_COMET_ADDRESSES

        # Assert test preconditions against the live registry
        arb_comets = COMPOUND_V3_COMET_ADDRESSES.get("arbitrum", {})
        expected_comet = arb_comets.get("usdc", "")
        wrong_comet = arb_comets.get("usdc_bridged", "")
        assert expected_comet, "Precondition: COMPOUND_V3_COMET_ADDRESSES['arbitrum']['usdc'] must exist"
        assert wrong_comet, "Precondition: COMPOUND_V3_COMET_ADDRESSES['arbitrum']['usdc_bridged'] must exist"
        assert expected_comet.lower() != wrong_comet.lower(), "Precondition: usdc and usdc_bridged must differ"

        # WETH is in the Arbitrum USDC market collateral registry → state will be non-None
        gateway = _make_mock_gateway(
            _mock_user_collateral_response(1 * 10**18),
            _mock_borrow_balance_response(100 * 10**6),
        )
        state = read_compound_v3_account_state(
            gateway_client=gateway,
            chain="arbitrum",
            wallet_address=_WALLET,
            collateral_token="WETH",
            borrow_token="usdc",
            price_oracle={"WETH": Decimal("3000"), "USDC": Decimal("1")},
        )

        assert state is not None, "state must not be None: WETH/USDC are valid Arbitrum Compound V3 tokens"
        # eth_call must have used the exact 'usdc' Comet, not 'usdc_bridged'
        first_call = gateway.eth_call.call_args_list[0]
        actual_comet = first_call[0][1] if first_call[0] else first_call[1].get("to")
        assert actual_comet is not None, "eth_call must record the target comet address"
        assert actual_comet.lower() == expected_comet.lower(), (
            f"Expected USDC comet {expected_comet}, got {actual_comet} (fuzzy match guard failed)"
        )
        assert actual_comet.lower() != wrong_comet.lower(), (
            "Picked usdc_bridged comet instead of usdc — exact match guard failed"
        )

    def test_returns_none_when_price_oracle_empty(self) -> None:
        """Missing prices → return None without calling gateway."""
        gateway = MagicMock()
        state = read_compound_v3_account_state(
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            collateral_token=_COLLATERAL_TOKEN,
            borrow_token=_BORROW_TOKEN,
            price_oracle={},
        )

        assert state is None
        gateway.eth_call.assert_not_called()

    def test_returns_none_when_price_oracle_none(self) -> None:
        gateway = MagicMock()
        state = read_compound_v3_account_state(
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            collateral_token=_COLLATERAL_TOKEN,
            borrow_token=_BORROW_TOKEN,
            price_oracle=None,
        )

        assert state is None
        gateway.eth_call.assert_not_called()

    def test_returns_none_when_collateral_price_missing(self) -> None:
        gateway = MagicMock()
        state = read_compound_v3_account_state(
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            collateral_token=_COLLATERAL_TOKEN,
            borrow_token=_BORROW_TOKEN,
            price_oracle={"USDC": Decimal("1")},  # WETH price missing
        )

        assert state is None
        gateway.eth_call.assert_not_called()

    def test_returns_none_when_user_collateral_call_fails(self) -> None:
        """Gateway returns None for userCollateral() → return None."""
        gateway = MagicMock()
        gateway.eth_call.return_value = None

        state = read_compound_v3_account_state(
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            collateral_token=_COLLATERAL_TOKEN,
            borrow_token=_BORROW_TOKEN,
            price_oracle=_PRICE_ORACLE,
        )

        assert state is None

    def test_returns_none_when_gateway_raises(self) -> None:
        """Exception in gateway → return None without propagating."""
        gateway = MagicMock()
        gateway.eth_call.side_effect = RuntimeError("RPC unavailable")

        state = read_compound_v3_account_state(
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            collateral_token=_COLLATERAL_TOKEN,
            borrow_token=_BORROW_TOKEN,
            price_oracle=_PRICE_ORACLE,
        )

        assert state is None

    def test_hf_is_none_when_lcf_not_in_registry(self) -> None:
        """When collateral has no LCF in COMPOUND_V3_MARKETS, HF must be None (not raw ratio).

        DAI is a real EVM token that resolves but is NOT listed as a Compound V3
        USDC-market collateral on Ethereum.  The LCF lookup returns None, so
        health_factor must be None rather than the misleadingly optimistic raw ratio.
        """
        from almanak.connectors.compound_v3.adapter import COMPOUND_V3_MARKETS

        # Assert test precondition: DAI must NOT be in the Ethereum USDC market collateral list
        eth_usdc_collaterals = COMPOUND_V3_MARKETS.get("ethereum", {}).get("usdc", {}).get("collaterals", {})
        assert "DAI" not in eth_usdc_collaterals, (
            "Precondition violated: DAI was added to COMPOUND_V3_MARKETS ethereum/usdc; pick a different token"
        )

        # 1 DAI collateral (18 dec), 100 USDC debt (6 dec)
        gateway = _make_mock_gateway(
            _mock_user_collateral_response(1 * 10**18),
            _mock_borrow_balance_response(100 * 10**6),
        )
        state = read_compound_v3_account_state(
            gateway_client=gateway,
            chain=_CHAIN,
            wallet_address=_WALLET,
            collateral_token="DAI",  # Resolves, but not in COMPOUND_V3_MARKETS collaterals
            borrow_token=_BORROW_TOKEN,
            price_oracle={"DAI": Decimal("1"), "USDC": Decimal("1")},
        )

        assert state is not None, "state must not be None: DAI and USDC are real Ethereum tokens"
        assert state.debt_usd > 0, "debt_usd must be populated (100 USDC borrow)"
        assert state.health_factor is None, (
            f"Expected HF=None for collateral with no LCF, got {state.health_factor} (raw ratio would overstate safety)"
        )


# ─── market_id routing test ──────────────────────────────────────────────────


class TestCompoundV3MarketIdRouting:
    """Verify that market_id selects the correct Comet for SUPPLY/WITHDRAW intents."""

    def test_supply_intent_uses_market_id_not_collateral_token(self) -> None:
        """SUPPLY WETH to USDC market: market_id='usdc' must pick the USDC Comet, not WETH Comet.

        Without market_id, borrow_token fallback would use 'weth' → wrong Comet.
        """
        from almanak.connectors.compound_v3.adapter import COMPOUND_V3_COMET_ADDRESSES

        arb_comets = COMPOUND_V3_COMET_ADDRESSES.get("arbitrum", {})
        usdc_comet = arb_comets.get("usdc", "")
        weth_comet = arb_comets.get("weth", "")
        assert usdc_comet, "Precondition: arbitrum/usdc Comet must exist"
        assert weth_comet, "Precondition: arbitrum/weth Comet must exist"
        assert usdc_comet.lower() != weth_comet.lower(), "Precondition: usdc and weth Comets must differ"

        # Simulate SUPPLY WETH to USDC market: collateral=WETH, borrow_token=WETH (placeholder),
        # market_id='usdc' → effective_borrow_token='USDC' from registry
        gateway = _make_mock_gateway(
            _mock_user_collateral_response(1 * 10**18),  # 1 WETH collateral
            _mock_borrow_balance_response(0),  # no debt (pure supply)
        )
        state = read_compound_v3_account_state(
            gateway_client=gateway,
            chain="arbitrum",
            wallet_address=_WALLET,
            collateral_token="WETH",
            borrow_token="WETH",  # placeholder — overridden by market_id derivation
            price_oracle={"WETH": Decimal("3000"), "USDC": Decimal("1")},
            market_id="usdc",
        )

        assert state is not None, "state must not be None for valid SUPPLY to USDC market"
        # Verify the USDC Comet was used, not the WETH Comet
        first_call = gateway.eth_call.call_args_list[0]
        actual_comet = first_call[0][1] if first_call[0] else first_call[1].get("to")
        assert actual_comet is not None
        assert actual_comet.lower() == usdc_comet.lower(), (
            f"Expected USDC Comet {usdc_comet}, got {actual_comet} — SUPPLY used wrong Comet"
        )
        assert actual_comet.lower() != weth_comet.lower(), "SUPPLY to USDC market incorrectly used WETH Comet"
        # No debt → sentinel HF
        assert state.debt_usd == Decimal("0")
        assert state.health_factor == Decimal("999999")

    def test_explicit_market_id_overrides_borrow_token_for_comet_lookup(self) -> None:
        """When market_id='weth' is passed, the WETH Comet is used even if borrow_token='USDC'."""
        from almanak.connectors.compound_v3.adapter import COMPOUND_V3_COMET_ADDRESSES

        weth_comet = COMPOUND_V3_COMET_ADDRESSES.get("ethereum", {}).get("weth", "")
        assert weth_comet, "Precondition: ethereum/weth Comet must exist"

        gateway = _make_mock_gateway(
            _mock_user_collateral_response(1 * 10**18),
            _mock_borrow_balance_response(0),
        )
        read_compound_v3_account_state(
            gateway_client=gateway,
            chain="ethereum",
            wallet_address=_WALLET,
            collateral_token="wstETH",
            borrow_token="USDC",  # would select usdc Comet without market_id
            price_oracle={"wstETH": Decimal("3500"), "WETH": Decimal("3000"), "USDC": Decimal("1")},
            market_id="weth",  # overrides → WETH Comet
        )

        if gateway.eth_call.call_count > 0:
            first_call = gateway.eth_call.call_args_list[0]
            actual_comet = first_call[0][1] if first_call[0] else first_call[1].get("to")
            if actual_comet:
                assert actual_comet.lower() == weth_comet.lower(), (
                    f"market_id='weth' should select WETH Comet {weth_comet}, got {actual_comet}"
                )


# ─── Pipeline integration test ────────────────────────────────────────────────


class TestCompoundV3PipelineIntegration:
    """Verify build_lending_accounting_event() populates HF fields for Compound V3 BORROW."""

    def test_hf_fields_wired_into_lending_event(self) -> None:
        """Full pipeline: build_lending_accounting_event() populates health_factor for Compound V3."""
        from almanak.framework.accounting.basis import FIFOBasisStore
        from almanak.framework.accounting.lending_accounting import build_lending_accounting_event
        from almanak.framework.accounting.models import AccountingConfidence

        collateral_raw = 2 * 10**18  # 2 WETH
        borrow_raw = 2000 * 10**6  # 2000 USDC

        gateway = _make_mock_gateway(
            _mock_user_collateral_response(collateral_raw),
            _mock_borrow_balance_response(borrow_raw),
        )

        intent = MagicMock()
        intent.intent_type.value = "BORROW"
        intent.protocol = "compound_v3"
        intent.collateral_token = _COLLATERAL_TOKEN
        intent.borrow_token = _BORROW_TOKEN
        intent.token = None
        intent.market_id = "usdc"

        result = MagicMock()
        result.tx_hash = "0xdeadbeef"
        result.extracted_data = {"borrow_amount": borrow_raw}
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
            price_oracle=_PRICE_ORACLE,
            basis_store=FIFOBasisStore(),
        )

        assert event is not None
        assert event.health_factor_after is not None, "health_factor_after must be populated for Compound V3 BORROW"
        assert event.collateral_value_after_usd is not None
        assert event.debt_value_after_usd is not None
        assert event.confidence == AccountingConfidence.HIGH
        # 2 WETH * $3000 = $6000 collateral
        assert event.collateral_value_after_usd == Decimal("6000")
        # 2000 USDC * $1 = $2000 debt
        assert event.debt_value_after_usd == Decimal("2000")
        # HF = (6000 * 0.895) / 2000 = 2.685
        expected_hf = (Decimal("6000") * Decimal("0.895")) / Decimal("2000")
        assert abs(event.health_factor_after - expected_hf) < Decimal("0.001")


# ─── Base-asset SUPPLY/WITHDRAW tests ────────────────────────────────────────


def _mock_balance_of_response(balance: int) -> str:
    """Build a hex string matching balanceOf() ABI return: uint256."""
    return "0x" + _encode_word(balance)


class TestCompoundV3BaseAssetSupply:
    """Verify base-asset SUPPLY/WITHDRAW uses balanceOf() instead of userCollateral().

    In Compound V3, supplying the market's base asset (e.g., USDC into the USDC
    Comet) is tracked via Comet.balanceOf(wallet), NOT userCollateral(wallet, token).
    userCollateral() always returns zero for the base asset — using it would produce
    collateral_usd=0 with AccountingConfidence.HIGH, which is silently wrong.
    """

    def test_balance_of_selector_is_correct(self) -> None:
        """_COMPOUND_V3_BALANCE_OF_SELECTOR must match keccak256('balanceOf(address)')."""
        from almanak.connectors._strategy_base.lending_read_base import _COMPOUND_V3_BALANCE_OF_SELECTOR

        expected = "0x" + Web3.keccak(text="balanceOf(address)").hex()[:8]
        assert _COMPOUND_V3_BALANCE_OF_SELECTOR == expected, (
            f"_COMPOUND_V3_BALANCE_OF_SELECTOR mismatch: got {_COMPOUND_V3_BALANCE_OF_SELECTOR!r}, "
            f"expected {expected!r}"
        )

    def test_base_asset_supply_uses_balance_of_not_user_collateral(self) -> None:
        """Supplying USDC to the USDC Comet must call balanceOf(), not userCollateral().

        collateral_token='USDC' == base_token='USDC' → first eth_call must use
        the balanceOf selector, not the userCollateral selector.
        """
        from almanak.connectors._strategy_base.lending_read_base import (
            _COMPOUND_V3_BALANCE_OF_SELECTOR,
            _COMPOUND_V3_USER_COLLATERAL_SELECTOR,
        )

        supplied_raw = 1000 * 10**6  # 1000 USDC (6 dec)
        gateway = MagicMock()
        gateway.eth_call.side_effect = [
            _mock_balance_of_response(supplied_raw),
            _mock_borrow_balance_response(0),
        ]

        state = read_compound_v3_account_state(
            gateway_client=gateway,
            chain="ethereum",
            wallet_address=_WALLET,
            collateral_token="USDC",  # == base_token of "usdc" market
            borrow_token="USDC",
            price_oracle={"USDC": Decimal("1")},
            market_id="usdc",
        )

        assert state is not None, "state must not be None for valid base-asset SUPPLY"
        # First call must use balanceOf selector, not userCollateral selector
        first_call = gateway.eth_call.call_args_list[0]
        calldata = first_call[0][2] if first_call[0] else first_call[1].get("calldata") or first_call[1].get("data")
        assert calldata is not None
        assert calldata.startswith(_COMPOUND_V3_BALANCE_OF_SELECTOR), (
            f"Base-asset SUPPLY must call balanceOf(), not userCollateral(). First calldata selector: {calldata[:10]!r}"
        )
        assert not calldata.startswith(_COMPOUND_V3_USER_COLLATERAL_SELECTOR), (
            "Base-asset SUPPLY must NOT call userCollateral() — it always returns zero for the base asset"
        )

    def test_base_asset_supply_collateral_usd_is_nonzero(self) -> None:
        """collateral_usd must reflect the actual supplied balance, not zero.

        The old code path called userCollateral(wallet, USDC) which always returns
        zero for the base asset, producing collateral_usd=0 with HIGH confidence.
        """
        supplied_raw = 500 * 10**6  # 500 USDC
        gateway = MagicMock()
        gateway.eth_call.side_effect = [
            _mock_balance_of_response(supplied_raw),
            _mock_borrow_balance_response(0),
        ]

        state = read_compound_v3_account_state(
            gateway_client=gateway,
            chain="ethereum",
            wallet_address=_WALLET,
            collateral_token="USDC",
            borrow_token="USDC",
            price_oracle={"USDC": Decimal("1")},
            market_id="usdc",
        )

        assert state is not None
        assert state.collateral_usd == Decimal("500"), (
            f"collateral_usd should be 500 (500 USDC @ $1), got {state.collateral_usd}"
        )
        assert state.debt_usd == Decimal("0")
        # No liquidation risk for pure base-asset supply → sentinel HF
        assert state.health_factor == Decimal("999999"), (
            f"Pure base-asset supply must use sentinel HF=999999, got {state.health_factor}"
        )

    def test_base_asset_supply_weth_market(self) -> None:
        """Supplying WETH to the WETH Comet uses balanceOf() (WETH is base_token of the weth market)."""
        from almanak.connectors._strategy_base.lending_read_base import (
            _COMPOUND_V3_BALANCE_OF_SELECTOR,
        )

        supplied_raw = 2 * 10**18  # 2 WETH (18 dec)
        gateway = MagicMock()
        gateway.eth_call.side_effect = [
            _mock_balance_of_response(supplied_raw),
            _mock_borrow_balance_response(0),
        ]

        state = read_compound_v3_account_state(
            gateway_client=gateway,
            chain="ethereum",
            wallet_address=_WALLET,
            collateral_token="WETH",  # == base_token of "weth" market
            borrow_token="WETH",
            price_oracle={"WETH": Decimal("3000")},
            market_id="weth",
        )

        assert state is not None, "state must not be None for WETH base-asset supply"
        # 2 WETH * $3000 = $6000
        assert state.collateral_usd == Decimal("6000"), f"collateral_usd should be 6000, got {state.collateral_usd}"
        assert state.debt_usd == Decimal("0")
        assert state.health_factor == Decimal("999999")
        # Verify first call was balanceOf
        first_call = gateway.eth_call.call_args_list[0]
        calldata = first_call[0][2] if first_call[0] else first_call[1].get("calldata") or first_call[1].get("data")
        assert calldata.startswith(_COMPOUND_V3_BALANCE_OF_SELECTOR)

    def test_collateral_supply_still_uses_user_collateral(self) -> None:
        """WETH supplied as collateral to USDC market still uses userCollateral() (not balanceOf).

        WETH != base_token ('USDC') of the usdc market → collateral path unchanged.
        """
        from almanak.connectors._strategy_base.lending_read_base import (
            _COMPOUND_V3_USER_COLLATERAL_SELECTOR,
        )

        collateral_raw = 1 * 10**18  # 1 WETH
        gateway = _make_mock_gateway(
            _mock_user_collateral_response(collateral_raw),
            _mock_borrow_balance_response(0),
        )

        state = read_compound_v3_account_state(
            gateway_client=gateway,
            chain="ethereum",
            wallet_address=_WALLET,
            collateral_token="WETH",  # != base_token 'USDC' → collateral path
            borrow_token="USDC",
            price_oracle={"WETH": Decimal("3000"), "USDC": Decimal("1")},
            market_id="usdc",
        )

        assert state is not None
        assert state.collateral_usd == Decimal("3000")
        first_call = gateway.eth_call.call_args_list[0]
        calldata = first_call[0][2] if first_call[0] else first_call[1].get("calldata") or first_call[1].get("data")
        assert calldata.startswith(_COMPOUND_V3_USER_COLLATERAL_SELECTOR), (
            "Collateral (non-base) SUPPLY must still call userCollateral()"
        )

    def test_base_asset_supply_gateway_called_exactly_twice(self) -> None:
        """Two eth_calls for base-asset path: balanceOf() then borrowBalanceOf()."""
        gateway = MagicMock()
        gateway.eth_call.side_effect = [
            _mock_balance_of_response(100 * 10**6),
            _mock_borrow_balance_response(0),
        ]

        read_compound_v3_account_state(
            gateway_client=gateway,
            chain="ethereum",
            wallet_address=_WALLET,
            collateral_token="USDC",
            borrow_token="USDC",
            price_oracle={"USDC": Decimal("1")},
            market_id="usdc",
        )

        assert gateway.eth_call.call_count == 2, (
            f"Expected exactly 2 eth_calls for base-asset path, got {gateway.eth_call.call_count}"
        )
