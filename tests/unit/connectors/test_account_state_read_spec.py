"""Byte-equivalence + contract tests for the aggregate account-state read seam.

PR-1 of VIB-4929 added the strategy-side **aggregate account-state** read
capability (total collateral / total debt / health factor / liquidation
threshold / e-mode) mirroring the existing single-reserve
:class:`~almanak.connectors._strategy_base.lending_read_base.LendingReadSpec`
seam. VIB-4929 PR-3a then delivered the generic framework reader
(:func:`~almanak.framework.accounting.lending_accounting.read_lending_account_state`)
that drives this spec — it is the **oracle** the spec must reproduce
byte-for-byte for the Aave family.

The gate: ``ACCOUNT_STATE_READ_SPEC.reduce_calls(query, recorded)`` must produce
a :class:`LendingAccountState` whose fields equal the state that
``read_lending_account_state(protocol="aave_v3", ...)`` decodes from the *same*
recorded ``getUserAccountData`` / ``getUserEMode`` return blobs. If those two
decoders ever diverge, an accounting auditor would see different valuation
inputs — so we pin them here.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from almanak.connectors._strategy_base.address_registry import AddressRegistry
from almanak.connectors._strategy_base.lending_read_base import (
    _AAVE_GET_ACCOUNT_DATA_SELECTOR,
    _AAVE_GET_USER_EMODE_SELECTOR,
    AAVE_FORK_ACCOUNT_STATE_READ,
    AccountStateQuery,
    AccountStateReadSpec,
    EthCall,
    LendingAccountState,
)
from almanak.connectors._strategy_base.lending_read_registry import (
    AccountStatePlan,
    LendingReadRegistry,
)
from almanak.connectors.aave_v3.lending_read import ACCOUNT_STATE_READ_SPEC
from almanak.framework.accounting.lending_accounting import read_lending_account_state

_WALLET = "0xABCDEF0123456789abcdef0123456789ABCDEF01"
_ARBITRUM_AAVE_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"


# ---------------------------------------------------------------------------
# Recorded-blob builders (the on-chain return shapes the oracle decodes)
# ---------------------------------------------------------------------------


def _account_data_hex(
    *,
    collateral_base: int,
    debt_base: int,
    available_borrows_base: int = 0,
    liquidation_threshold_bps: int,
    ltv_bps: int = 0,
    health_factor_raw: int,
) -> str:
    """ABI-encode a ``getUserAccountData`` return blob (6 uint256 words)."""
    words = [
        collateral_base,
        debt_base,
        available_borrows_base,
        liquidation_threshold_bps,
        ltv_bps,
        health_factor_raw,
    ]
    return "0x" + "".join(format(w, "064x") for w in words)


def _emode_hex(category: int) -> str:
    """ABI-encode a ``getUserEMode`` return blob (single uint256 word)."""
    return "0x" + format(category, "064x")


def _mock_gateway(account_hex: str | None, emode_hex: str | None) -> Any:
    """Gateway whose ``eth_call`` routes by selector to a recorded blob.

    The generic reader (Aave) issues two calls against the pool — first
    ``getUserAccountData``, then ``getUserEMode`` — through
    ``gateway_client.eth_call(chain, to, data, block=...)``. This mock returns
    the matching recorded blob so the oracle decodes the *same* bytes the spec's
    ``reduce_calls`` is handed directly.
    """

    class _G:
        def eth_call(self, chain: str, to: str, data: str, block: Any = None) -> str | None:
            if data.startswith(_AAVE_GET_ACCOUNT_DATA_SELECTOR):
                return account_hex
            if data.startswith(_AAVE_GET_USER_EMODE_SELECTOR):
                return emode_hex
            raise AssertionError(f"unexpected selector in calldata: {data[:10]}")

    return _G()


def _oracle_state(account_hex: str | None, emode_hex: str | None) -> Any:
    """Run the generic reader (Aave family, whole-account) over recorded blobs.

    ``market_id=None`` for the Aave family (whole-account); the reader declares
    no valuation roles, so no oracle is touched — the Aave on-chain reads are
    USD-denominated already.
    """
    return read_lending_account_state(
        protocol="aave_v3",
        chain="arbitrum",
        wallet_address=_WALLET,
        market_id=None,
        gateway_client=_mock_gateway(account_hex, emode_hex),
        price_oracle=None,
    )


def _spec_state(account_hex: str | None, emode_hex: str | None) -> LendingAccountState | None:
    """Run the new spec's pure ``reduce_calls`` over the same recorded blobs."""
    query = AccountStateQuery(
        chain="arbitrum",
        wallet_address=_WALLET,
        position_manager_address=_ARBITRUM_AAVE_POOL,
    )
    return ACCOUNT_STATE_READ_SPEC.reduce_calls(query, [account_hex, emode_hex])


def _assert_equivalent(account_hex: str | None, emode_hex: str | None) -> None:
    """Assert the spec reducer and the oracle agree field-for-field."""
    oracle = _oracle_state(account_hex, emode_hex)
    spec = _spec_state(account_hex, emode_hex)

    if oracle is None:
        assert spec is None
        return

    assert spec is not None
    # Aave denominates collateral/debt in USD on-chain; both decoders must yield
    # identical Decimals (no float drift), HF, threshold, and e-mode.
    assert spec.collateral_usd == oracle.collateral_usd
    assert spec.debt_usd == oracle.debt_usd
    assert spec.health_factor == oracle.health_factor
    assert spec.liquidation_threshold_bps == oracle.liquidation_threshold_bps
    assert spec.e_mode_category == oracle.e_mode_category


# ---------------------------------------------------------------------------
# THE PR-1 GATE — reduce_calls is byte-identical to read_aave_account_state
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("collateral_base", "debt_base", "lt_bps", "hf_raw", "emode"),
    [
        # Healthy leveraged position, e-mode 0 (stables disabled).
        (1_500_000_000, 250_000_000, 8500, 6_000_000_000_000_000_000, 0),
        # In an e-mode category (1 = stables on Aave V3).
        (10_000_000_000, 7_000_000_000, 9300, 1_400_000_000_000_000_000, 1),
        # Supply-only (no debt) → HF sentinel (uint256 max) → capped at 999999.
        (5_000_000_000, 0, 8000, 2**256 - 1, 0),
        # Empty position (all zero); HF raw 0.
        (0, 0, 0, 0, 0),
        # Large e-mode id at the uint8 boundary.
        (3_300_000_000, 1_100_000_000, 7700, 3_000_000_000_000_000_000, 255),
        # Sub-dollar dust collateral (exercises the 1e8 Decimal scaling exactly).
        (12_345_678, 1, 8500, 12_500_000_000_000_000_000, 0),
    ],
)
def test_reduce_calls_byte_identical_to_read_aave_account_state(
    collateral_base: int, debt_base: int, lt_bps: int, hf_raw: int, emode: int
) -> None:
    account_hex = _account_data_hex(
        collateral_base=collateral_base,
        debt_base=debt_base,
        liquidation_threshold_bps=lt_bps,
        health_factor_raw=hf_raw,
    )
    _assert_equivalent(account_hex, _emode_hex(emode))


def test_reduce_calls_concrete_decode_values() -> None:
    # Pin the absolute decoded values (not just oracle-equality) so a change to
    # the scaling constants is caught even if the oracle changed in lockstep.
    account_hex = _account_data_hex(
        collateral_base=1_500_000_000,  # 15.00000000 USD
        debt_base=250_000_000,  # 2.50000000 USD
        liquidation_threshold_bps=8500,
        health_factor_raw=6_000_000_000_000_000_000,  # 6.0
    )
    state = _spec_state(account_hex, _emode_hex(1))
    assert state is not None
    assert state.collateral_usd == Decimal("15")
    assert state.debt_usd == Decimal("2.5")
    assert state.liquidation_threshold_bps == 8500
    assert state.health_factor == Decimal("6")
    assert state.e_mode_category == 1


def test_hf_sentinel_capped_at_999999_like_oracle() -> None:
    account_hex = _account_data_hex(
        collateral_base=5_000_000_000,
        debt_base=0,
        liquidation_threshold_bps=8000,
        health_factor_raw=2**256 - 1,
    )
    state = _spec_state(account_hex, _emode_hex(0))
    assert state is not None
    assert state.health_factor == Decimal("999999")
    # Equivalence with the oracle still holds at the cap.
    _assert_equivalent(account_hex, _emode_hex(0))


# ---------------------------------------------------------------------------
# Empty ≠ Zero — failure semantics match the oracle
# ---------------------------------------------------------------------------


def test_missing_account_data_blob_reduces_to_none() -> None:
    # Primary read failing ⇒ None (oracle returns None too).
    assert _spec_state(None, _emode_hex(0)) is None
    _assert_equivalent(None, _emode_hex(0))


def test_short_account_data_blob_reduces_to_none() -> None:
    short = "0x" + "00" * 32  # one word, < 6 words
    assert _spec_state(short, _emode_hex(0)) is None
    _assert_equivalent(short, _emode_hex(0))


def test_failed_emode_read_yields_none_category_not_zero() -> None:
    # e-mode best-effort: a missing/failed e-mode read ⇒ None (UNMEASURED),
    # never a fabricated 0 (which would mean "measured: not in e-mode").
    account_hex = _account_data_hex(
        collateral_base=1_000_000_000,
        debt_base=0,
        liquidation_threshold_bps=8500,
        health_factor_raw=10_000_000_000_000_000_000,
    )
    state = _spec_state(account_hex, None)
    assert state is not None
    assert state.e_mode_category is None
    _assert_equivalent(account_hex, None)


def test_out_of_uint8_range_emode_treated_as_unmeasured() -> None:
    # A 256+ e-mode value is a wrong-shape response ⇒ None, matching the oracle.
    account_hex = _account_data_hex(
        collateral_base=1_000_000_000,
        debt_base=500_000_000,
        liquidation_threshold_bps=8500,
        health_factor_raw=2_000_000_000_000_000_000,
    )
    bad_emode = _emode_hex(256)
    state = _spec_state(account_hex, bad_emode)
    assert state is not None
    assert state.e_mode_category is None
    _assert_equivalent(account_hex, bad_emode)


def test_uppercase_0x_prefix_handled_like_oracle() -> None:
    account_hex = _account_data_hex(
        collateral_base=2_222_222_222,
        debt_base=1_111_111_111,
        liquidation_threshold_bps=8500,
        health_factor_raw=2_000_000_000_000_000_000,
    ).replace("0x", "0X", 1)
    emode = _emode_hex(2).replace("0x", "0X", 1)
    _assert_equivalent(account_hex, emode)


# ---------------------------------------------------------------------------
# build_calls — emits the oracle's two reads against the resolved pool
# ---------------------------------------------------------------------------


def test_build_calls_emits_account_data_then_emode_against_pool() -> None:
    query = AccountStateQuery(
        chain="arbitrum",
        wallet_address=_WALLET,
        position_manager_address=_ARBITRUM_AAVE_POOL,
    )
    calls = ACCOUNT_STATE_READ_SPEC.build_calls(query)
    assert len(calls) == 2
    assert all(isinstance(c, EthCall) for c in calls)
    # Both target the pool; first is getUserAccountData, second getUserEMode.
    assert calls[0].to == _ARBITRUM_AAVE_POOL
    assert calls[1].to == _ARBITRUM_AAVE_POOL
    assert calls[0].data.startswith(_AAVE_GET_ACCOUNT_DATA_SELECTOR)
    assert calls[1].data.startswith(_AAVE_GET_USER_EMODE_SELECTOR)
    # Wallet padded to 32 bytes, lower-cased, no extra args.
    padded = _WALLET.lower().replace("0x", "").zfill(64)
    assert calls[0].data == _AAVE_GET_ACCOUNT_DATA_SELECTOR + padded
    assert calls[1].data == _AAVE_GET_USER_EMODE_SELECTOR + padded


# ---------------------------------------------------------------------------
# Registry dispatch contract (parallel to the single-reserve registry tests)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("protocol", ["aave_v3", "spark", "aave", "AAVE_V3", "morpho_blue", "MORPHO_BLUE"])
def test_supports_account_state_for_supported_protocols(protocol: str) -> None:
    # Morpho Blue joined in VIB-4929 PR-3a (via the injected-price seam).
    assert LendingReadRegistry.supports_account_state(protocol)


@pytest.mark.parametrize("protocol", ["compound_v3", "uniswap_v3", "unknown"])
def test_account_state_unsupported_for_non_supported_protocols(protocol: str) -> None:
    # Compound V3 is still deferred (routes through the registry in PR-3b).
    assert not LendingReadRegistry.supports_account_state(protocol)


def test_all_aave_fork_account_state_specs_share_the_canonical_read() -> None:
    for protocol in ("aave_v3", "spark"):
        spec = LendingReadRegistry._load_account_state_spec(protocol)
        assert isinstance(spec, AccountStateReadSpec)
        assert spec is AAVE_FORK_ACCOUNT_STATE_READ
        assert spec.contract_kinds == ("pool",)


def test_position_manager_address_resolves_aave_pool() -> None:
    addr = LendingReadRegistry.position_manager_address("aave_v3", "arbitrum")
    assert addr == AddressRegistry.addresses_for("aave_v3", "arbitrum")["pool"]


def test_position_manager_address_resolves_via_alias() -> None:
    via_alias = LendingReadRegistry.position_manager_address("aave", "ethereum")
    via_canon = LendingReadRegistry.position_manager_address("aave_v3", "ethereum")
    assert via_alias == via_canon
    assert via_alias is not None


def test_position_manager_address_none_for_unsupported_protocol() -> None:
    assert LendingReadRegistry.position_manager_address("compound_v3", "arbitrum") is None


def test_position_manager_address_none_for_unsupported_chain() -> None:
    assert LendingReadRegistry.position_manager_address("aave_v3", "solana") is None


def test_resolve_account_state_plan_binds_resolved_pool_and_calls() -> None:
    # A placeholder target on the query must be overwritten by the registry.
    query = AccountStateQuery(
        chain="arbitrum",
        wallet_address=_WALLET,
        position_manager_address="0xPLACEHOLDER",
    )
    plan = LendingReadRegistry.resolve_account_state_plan("aave_v3", query)
    assert isinstance(plan, AccountStatePlan)
    expected_pool = AddressRegistry.addresses_for("aave_v3", "arbitrum")["pool"]
    assert plan.query.position_manager_address == expected_pool
    assert len(plan.calls) == 2
    assert all(c.to == expected_pool for c in plan.calls)
    # The plan's reducer is the connector's pure reducer.
    assert plan.reduce is AAVE_FORK_ACCOUNT_STATE_READ.reduce_calls


def test_resolve_account_state_plan_alias_matches_canonical() -> None:
    base = AccountStateQuery(
        chain="ethereum", wallet_address=_WALLET, position_manager_address="0x0"
    )
    plan_alias = LendingReadRegistry.resolve_account_state_plan("aave", base)
    plan_canon = LendingReadRegistry.resolve_account_state_plan("aave_v3", base)
    assert plan_alias is not None and plan_canon is not None
    assert plan_alias.query.position_manager_address == plan_canon.query.position_manager_address


def test_resolve_account_state_plan_none_for_unsupported_chain() -> None:
    query = AccountStateQuery(
        chain="solana", wallet_address=_WALLET, position_manager_address="0x0"
    )
    assert LendingReadRegistry.resolve_account_state_plan("aave_v3", query) is None


def test_resolve_account_state_plan_none_for_unsupported_protocol() -> None:
    query = AccountStateQuery(
        chain="arbitrum", wallet_address=_WALLET, position_manager_address="0x0"
    )
    assert LendingReadRegistry.resolve_account_state_plan("compound_v3", query) is None


def test_plan_round_trip_decodes_via_reducer() -> None:
    # End-to-end on the strategy side: build a plan, feed its calls' "results"
    # (recorded blobs) back through the reducer — same output as direct decode.
    account_hex = _account_data_hex(
        collateral_base=4_000_000_000,
        debt_base=1_000_000_000,
        liquidation_threshold_bps=8500,
        health_factor_raw=3_400_000_000_000_000_000,
    )
    query = AccountStateQuery(
        chain="arbitrum", wallet_address=_WALLET, position_manager_address="0x0"
    )
    plan = LendingReadRegistry.resolve_account_state_plan("aave_v3", query)
    assert plan is not None
    results: list[str | None] = [account_hex, _emode_hex(1)]
    state = plan.reduce(plan.query, results)
    assert state is not None
    assert state.collateral_usd == Decimal("40")
    assert state.debt_usd == Decimal("10")
    assert state.e_mode_category == 1
