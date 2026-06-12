"""Unit tests for the Fluid vault account-state read spec (VIB-5031, ADR §3.2).

Pins every branch of the pure planner/reducer pair against synthetic
``eth_abi``-encoded ``positionsByUser`` blobs of the exact verified shapes:
the single-call plan, the market_id filter, the protocol-truth HF from the
vault's OWN oracle data, the measured-empty position (Decimal("0") amounts +
``health_factor=None``), and the fail-closed reductions (Empty ≠ Zero — a
read failure must NEVER masquerade as a measured-empty position). The live
read is covered by the chain intent tests.
"""

from __future__ import annotations

from decimal import Decimal

from eth_abi import encode as abi_encode

from almanak.connectors._strategy_base.lending_read_base import AccountStateQuery
from almanak.connectors.fluid.vault_lending_read import (
    ACCOUNT_STATE_READ_SPEC,
    _build_fluid_vault_account_state_calls,
    _reduce_fluid_vault_account_state,
)
from almanak.connectors.fluid.vault_sdk import USER_POSITION_TYPE, VAULT_ENTIRE_DATA_TYPE

RESOLVER = "0xA5C3E16523eeeDDcC34706b0E6bE88b4c6EA95cC"
VAULT = "0xeAbBfca72F8a8bf14C4ac59e69ECB2eB69F0811C"
OTHER_VAULT = "0x1111111111111111111111111111111111111111"
ARB_USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
NATIVE = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
ZERO = "0x" + "0" * 40

# ETH(18dp) -> USDC(6dp) at $2500 in the vault oracle's 1e27 convention.
ORACLE_PRICE = 2500 * 10**6 * 10**27 // 10**18
SUPPLY = 10**18  # 1 ETH
BORROW = 500_000_000  # 500 USDC
LIQ_THRESHOLD = 9200  # bps


def _position_tuple(nft_id: int = 12542, supply: int = SUPPLY, borrow: int = BORROW):
    return (nft_id, WALLET, False, borrow == 0, -14338, 1, supply, borrow, 0, supply, borrow, 0)


def _vault_tuple(
    vault: str = VAULT,
    oracle_price: int = ORACLE_PRICE,
    liq_threshold: int = LIQ_THRESHOLD,
    oracle_price_liquidate: int | None = None,
):
    constants = (
        ZERO,
        ZERO,
        ZERO,
        ZERO,
        ZERO,
        ZERO,
        ZERO,
        ZERO,
        (NATIVE, ZERO),
        (ARB_USDC, ZERO),
        1,
        10000,
        b"\x00" * 32,
        b"\x00" * 32,
        b"\x00" * 32,
        b"\x00" * 32,
    )
    if oracle_price_liquidate is None:
        oracle_price_liquidate = oracle_price
    configs = (0, 0, 8700, liq_threshold, 9500, 0, 100, 0, ZERO, oracle_price, oracle_price_liquidate, ZERO, 0)
    return (
        vault,
        False,
        False,
        constants,
        configs,
        (0,) * 14,
        (0,) * 6,
        (0,) * 8,
        (1, 0, 0, 0, 0, 0, (0, 0, 0, 0, 0, 0, 0)),
        (True,) + (0,) * 10,
        (True,) + (0,) * 10,
    )


def _blob(pairs: list[tuple]) -> str:
    positions = [pair[0] for pair in pairs]
    vaults = [pair[1] for pair in pairs]
    return "0x" + abi_encode([f"{USER_POSITION_TYPE}[]", f"{VAULT_ENTIRE_DATA_TYPE}[]"], [positions, vaults]).hex()


def _query(**overrides) -> AccountStateQuery:
    defaults = {
        "chain": "arbitrum",
        "wallet_address": WALLET,
        "position_manager_address": RESOLVER,
        "market_id": VAULT.lower(),
        "collateral_token": "ETH",
        "loan_token": "USDC",
        "prices": {"ETH": Decimal("2500"), "USDC": Decimal("1")},
        "decimals": {"ETH": 18, "USDC": 6},
    }
    defaults.update(overrides)
    return AccountStateQuery(**defaults)


class TestBuildCalls:
    def test_plans_single_positions_by_user_call(self):
        calls = _build_fluid_vault_account_state_calls(_query())
        assert len(calls) == 1, "ADR §3.2: ONE wallet-scoped static call"
        assert calls[0].to == RESOLVER
        assert calls[0].data.startswith("0x347ca8bb")  # positionsByUser(address)
        assert calls[0].data.endswith(WALLET[2:].lower())

    def test_unbound_resolver_target_fails_closed(self):
        assert _build_fluid_vault_account_state_calls(_query(position_manager_address="")) == []

    def test_missing_market_id_fails_closed(self):
        # Per-market account state: a missing market id has no well-defined
        # read (Morpho precedent) — no calls, never an unscoped read.
        assert _build_fluid_vault_account_state_calls(_query(market_id=None)) == []


class TestReduceHappyPath:
    def test_hf_uses_liquidation_oracle_price_not_operate_price(self):
        # The on-chain liquidation boundary keys on oraclePriceLiquidate
        # (configs[10]); some vaults run it apart from oraclePriceOperate
        # (configs[9]). HF computed from the operate price would over/
        # understate liquidation risk (Codex audit finding, VIB-5031).
        # Liquidate price at 80% of operate: HF must scale with IT.
        state = _reduce_fluid_vault_account_state(
            _query(),
            [_blob([(_position_tuple(), _vault_tuple(oracle_price_liquidate=ORACLE_PRICE * 8 // 10))])],
        )
        assert state is not None
        # col_in_debt = 1e18 * 2.0e18 / 1e27 = 2000e6; HF = 2000 * 0.92 / 500 = 3.68
        assert state.health_factor == Decimal("3.68")

    def test_position_present_protocol_truth_hf_and_usd_legs(self):
        state = _reduce_fluid_vault_account_state(_query(), [_blob([(_position_tuple(), _vault_tuple())])])
        assert state is not None
        # USD legs from the injected valuation seam:
        assert state.collateral_usd == Decimal("2500")  # 1 ETH @ $2500
        assert state.debt_usd == Decimal("500")
        # HF is PROTOCOL TRUTH from the vault's own oracle:
        # col_in_debt = 1e18 * 2.5e18 / 1e27 = 2500e6; HF = 2500 * 0.92 / 500 = 4.6
        assert state.health_factor == Decimal("4.6")
        # Morpho-family serialization shape: lltv set, bps derived by the serializer.
        assert state.lltv == Decimal("0.92")
        assert state.liquidation_threshold_bps is None
        assert state.e_mode_category is None

    def test_other_vault_positions_filtered_out(self):
        pairs = [
            (_position_tuple(nft_id=7, supply=5 * 10**18, borrow=0), _vault_tuple(vault=OTHER_VAULT)),
            (_position_tuple(), _vault_tuple()),
        ]
        state = _reduce_fluid_vault_account_state(_query(), [_blob(pairs)])
        assert state is not None
        assert state.collateral_usd == Decimal("2500")  # only the market_id vault counts

    def test_zero_debt_position_hf_sentinel(self):
        state = _reduce_fluid_vault_account_state(_query(), [_blob([(_position_tuple(borrow=0), _vault_tuple())])])
        assert state is not None
        assert state.debt_usd == Decimal("0")
        assert state.health_factor == Decimal("999999")  # Morpho-convention sentinel

    def test_multiple_nfts_lowest_id_selected(self):
        # One-NFT invariant violated externally: deterministic lowest-id
        # selection mirrors FluidVaultSDK.resolve_user_nft_for_vault.
        pairs = [
            (_position_tuple(nft_id=900, supply=2 * 10**18, borrow=0), _vault_tuple()),
            (_position_tuple(nft_id=12542), _vault_tuple()),
        ]
        state = _reduce_fluid_vault_account_state(_query(), [_blob(pairs)])
        assert state is not None
        assert state.collateral_usd == Decimal("5000")  # nft 900's 2 ETH


class TestReducePositionAbsent:
    def test_no_position_measured_zero_amounts_hf_none(self):
        # Empty ≠ Zero, the MEASURED side: the wallet provably holds no NFT
        # on the vault — amounts are measured Decimal("0"), HF is None
        # (the HF of an absent position is undefined, NOT zero).
        state = _reduce_fluid_vault_account_state(_query(), [_blob([])])
        assert state is not None
        assert state.collateral_usd == Decimal("0")
        assert state.debt_usd == Decimal("0")
        assert state.health_factor is None
        assert state.lltv is None

    def test_only_foreign_vault_positions_measured_zero(self):
        pairs = [(_position_tuple(), _vault_tuple(vault=OTHER_VAULT))]
        state = _reduce_fluid_vault_account_state(_query(), [_blob(pairs)])
        assert state is not None
        assert state.collateral_usd == Decimal("0")
        assert state.health_factor is None


class TestReduceClosedShell:
    """A fully-closed NFT shell (supply=0 AND borrow=0) is measured zeros."""

    def test_closed_shell_measured_zero_hf_none(self):
        blob = _blob([(_position_tuple(supply=0, borrow=0), _vault_tuple())])
        state = _reduce_fluid_vault_account_state(_query(), [blob])
        assert state is not None
        assert state.collateral_usd == Decimal("0")
        assert state.debt_usd == Decimal("0")
        assert state.health_factor is None
        assert state.lltv is None

    def test_closed_shell_without_valuation_seam_still_measured_zero(self):
        # ORDERING regression guard: the closed-shell early return must run
        # BEFORE the injected-valuation fail-closed gate — both legs are
        # provably zero and need NO prices to value, so a missing valuation
        # seam must not turn a measured-closed position into a read failure.
        blob = _blob([(_position_tuple(supply=0, borrow=0), _vault_tuple())])
        for query in (
            _query(prices=None),
            _query(decimals=None),
            _query(collateral_token=None),
            _query(loan_token=None),
        ):
            state = _reduce_fluid_vault_account_state(query, [blob])
            assert state is not None
            assert state.collateral_usd == Decimal("0")
            assert state.debt_usd == Decimal("0")
            assert state.health_factor is None
            assert state.lltv is None


class TestReduceReadFailureFailsClosed:
    """Empty ≠ Zero, the UNMEASURED side — never fabricated zeros."""

    def test_read_failure_missing_blob_fails_closed(self):
        assert _reduce_fluid_vault_account_state(_query(), []) is None
        assert _reduce_fluid_vault_account_state(_query(), [None]) is None
        assert _reduce_fluid_vault_account_state(_query(), [""]) is None

    def test_read_failure_truncated_blob_fails_closed(self):
        good = _blob([(_position_tuple(), _vault_tuple())])
        truncated = good[: len(good) // 2]
        assert _reduce_fluid_vault_account_state(_query(), [truncated]) is None

    def test_read_failure_garbage_blob_fails_closed(self):
        assert _reduce_fluid_vault_account_state(_query(), ["0xzzzz"]) is None
        assert _reduce_fluid_vault_account_state(_query(), ["0x1234"]) is None

    def test_missing_injected_valuation_fails_closed(self):
        blob = _blob([(_position_tuple(), _vault_tuple())])
        assert _reduce_fluid_vault_account_state(_query(prices=None), [blob]) is None
        assert _reduce_fluid_vault_account_state(_query(decimals=None), [blob]) is None
        assert _reduce_fluid_vault_account_state(_query(collateral_token=None), [blob]) is None
        assert _reduce_fluid_vault_account_state(_query(loan_token=None), [blob]) is None

    def test_none_price_fails_closed(self):
        blob = _blob([(_position_tuple(), _vault_tuple())])
        assert (
            _reduce_fluid_vault_account_state(
                _query(prices={"ETH": None, "USDC": Decimal("1")}),
                [blob],
            )
            is None
        )

    def test_missing_market_id_fails_closed(self):
        blob = _blob([(_position_tuple(), _vault_tuple())])
        assert _reduce_fluid_vault_account_state(_query(market_id=None), [blob]) is None

    def test_unpriceable_vault_risk_params_fail_closed(self):
        # liquidationThreshold == 0 / oraclePrice == 0 → unmeasured, not HF=0.
        blob = _blob([(_position_tuple(), _vault_tuple(liq_threshold=0))])
        assert _reduce_fluid_vault_account_state(_query(), [blob]) is None
        blob = _blob([(_position_tuple(), _vault_tuple(oracle_price=0))])
        assert _reduce_fluid_vault_account_state(_query(), [blob]) is None


class TestSpecDeclaration:
    def test_spec_targets_vault_resolver_kind(self):
        assert ACCOUNT_STATE_READ_SPEC.contract_kinds == ("vault_resolver",)

    def test_spec_declares_both_valuation_roles(self):
        assert ACCOUNT_STATE_READ_SPEC.valuation_role_keys == (
            ("collateral_token", "collateral_token"),
            ("loan_token", "loan_token"),
        )

    def test_spec_normalizes_address_market_ids_lowercase(self):
        # Vault market ids are 20-byte ADDRESSES — the default Morpho
        # zfill(64) normalisation would mangle them.
        assert ACCOUNT_STATE_READ_SPEC.normalize_market_id is not None
        assert ACCOUNT_STATE_READ_SPEC.normalize_market_id(VAULT) == VAULT.lower()

    def test_spec_query_inputs_read_intent_market_id(self):
        from types import SimpleNamespace

        inputs = ACCOUNT_STATE_READ_SPEC.query_inputs_from_intent(SimpleNamespace(market_id=VAULT))
        assert inputs == {"market_id": VAULT}
