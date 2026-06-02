"""Byte-equivalence + contract tests for the Morpho Blue account-state spec.

VIB-4929 PR-3a delivers the generic
:func:`~almanak.framework.accounting.lending_accounting.read_lending_account_state`
that drives the connector-owned account-state spec
(:data:`~almanak.connectors._strategy_base.lending_read_base.MORPHO_BLUE_ACCOUNT_STATE_READ`)
through :class:`LendingReadRegistry`. Morpho is **not USD-native**, so the spec
values the position from the price/decimals/market-params seam the framework
reader injects onto the :class:`AccountStateQuery`; the framework reader keeps
the oracle resolution + gateway round-trip. The registry's ``market_params`` /
``valuation_roles`` are the LIVE path that names the valued tokens + lltv.

The gate: ``MORPHO_BLUE_ACCOUNT_STATE_READ.reduce_calls(query, [position, market])``
must produce a :class:`LendingAccountState` whose collateral/debt/HF/lltv equal
the state the generic reader (``protocol="morpho_blue"``) decodes from the SAME
recorded ``position`` / ``market`` blobs (fed the SAME prices). If the two
decoders ever diverge, an accounting auditor would see different valuation
inputs — so we pin them here, plus absolute decoded values, the zero-shares
no-debt sentinel, the missing-price / short-blob fail-closed paths, and the
``build_calls`` selectors / targets / order.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from almanak.connectors._strategy_base.address_registry import AddressRegistry
from almanak.connectors._strategy_base.lending_read_base import (
    _MORPHO_MARKET_SELECTOR,
    _MORPHO_POSITION_SELECTOR,
    MORPHO_BLUE_ACCOUNT_STATE_READ,
    AccountStateQuery,
    EthCall,
    LendingAccountState,
)
from almanak.connectors._strategy_base.lending_read_registry import (
    AccountStatePlan,
    LendingReadRegistry,
)
from almanak.framework.accounting.lending_accounting import read_lending_account_state

# wstETH/USDC market id (matches the Ethereum MORPHO_MARKETS entry).
_MARKET_ID = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"
_WALLET = "0xABCDEF0123456789abcdef0123456789ABCDEF01"
_CHAIN = "ethereum"
_ETHEREUM_MORPHO = "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb"

_COLLATERAL = "wstETH"
_LOAN = "USDC"
_COLLATERAL_DECIMALS = 18
_LOAN_DECIMALS = 6
_LLTV_RAW = 860_000_000_000_000_000  # 0.86e18
_PRICES = {"wstETH": Decimal("3500"), "USDC": Decimal("1")}


# ---------------------------------------------------------------------------
# Recorded-blob builders (the on-chain return shapes the oracle decodes)
# ---------------------------------------------------------------------------


def _word(value: int) -> str:
    return format(value, "064x")


def _position_hex(supply_shares: int, borrow_shares: int, collateral: int) -> str:
    """ABI-encode a ``position(id, user)`` return blob (3 uint256 words)."""
    return "0x" + _word(supply_shares) + _word(borrow_shares) + _word(collateral)


def _market_hex(
    total_supply_assets: int,
    total_supply_shares: int,
    total_borrow_assets: int,
    total_borrow_shares: int,
    last_update: int = 0,
    fee: int = 0,
) -> str:
    """ABI-encode a ``market(id)`` return blob (6 uint128 as 6 uint256 words)."""
    return "0x" + "".join(
        _word(w)
        for w in (
            total_supply_assets,
            total_supply_shares,
            total_borrow_assets,
            total_borrow_shares,
            last_update,
            fee,
        )
    )


def _mock_gateway(position_hex: str | None, market_hex: str | None) -> Any:
    """Gateway whose ``eth_call`` routes by selector to a recorded blob.

    ``read_lending_account_state`` (Morpho) issues two calls against the Morpho
    singleton — first ``position``, then ``market`` — through
    ``gateway_client.eth_call(chain, to, data, block=...)``. This returns the
    matching recorded blob so the reader decodes the *same* bytes the spec's
    ``reduce_calls`` is handed directly.
    """

    class _G:
        def eth_call(self, chain: str, to: str, data: str, block: Any = None) -> str | None:
            if data.startswith(_MORPHO_POSITION_SELECTOR):
                return position_hex
            if data.startswith(_MORPHO_MARKET_SELECTOR):
                return market_hex
            raise AssertionError(f"unexpected selector in calldata: {data[:10]}")

    return _G()


def _oracle_state(position_hex: str | None, market_hex: str | None) -> Any:
    """Run the generic reader over recorded blobs (its price + gateway seam).

    The generic reader resolves the Morpho singleton + the (collateral, loan)
    valuation tokens + lltv through ``LendingReadRegistry`` — i.e. through the
    LIVE ``market_params`` / ``valuation_roles`` path. The gateway mock routes by
    selector, so the registry-resolved address still hits the recorded blobs.
    """
    return read_lending_account_state(
        protocol="morpho_blue",
        chain=_CHAIN,
        wallet_address=_WALLET,
        market_id=_MARKET_ID,
        gateway_client=_mock_gateway(position_hex, market_hex),
        price_oracle=_PRICES,
    )


def _spec_query() -> AccountStateQuery:
    return AccountStateQuery(
        chain=_CHAIN,
        wallet_address=_WALLET,
        position_manager_address=_ETHEREUM_MORPHO,
        market_id=_MARKET_ID,
        prices=_PRICES,
        decimals={_COLLATERAL: _COLLATERAL_DECIMALS, _LOAN: _LOAN_DECIMALS},
        market_params={"lltv": _LLTV_RAW},
        collateral_token=_COLLATERAL,
        loan_token=_LOAN,
    )


def _spec_state(position_hex: str | None, market_hex: str | None) -> LendingAccountState | None:
    """Run the spec's pure ``reduce_calls`` over the same recorded blobs."""
    return MORPHO_BLUE_ACCOUNT_STATE_READ.reduce_calls(_spec_query(), [position_hex, market_hex])


def _assert_equivalent(position_hex: str | None, market_hex: str | None) -> None:
    """Assert the spec reducer and the unchanged reader agree field-for-field."""
    oracle = _oracle_state(position_hex, market_hex)
    spec = _spec_state(position_hex, market_hex)

    if oracle is None:
        assert spec is None
        return

    assert spec is not None
    assert spec.collateral_usd == oracle.collateral_usd
    assert spec.debt_usd == oracle.debt_usd
    assert spec.health_factor == oracle.health_factor
    assert spec.lltv == oracle.lltv


# ---------------------------------------------------------------------------
# THE GATE — reduce_calls is byte-identical to read_morpho_blue_account_state
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("borrow_shares", "collateral_raw", "total_borrow_assets", "total_borrow_shares"),
    [
        # Healthy leveraged position (100 USDC debt vs 1 wstETH collateral, 1:1 shares).
        (100_000_000, 1 * 10**18, 10_000 * 10**6, 10_000 * 10**6),
        # Share ratio != 1:1 — exercises the ceil(shares * assets / shares) round-up.
        (333_333_333, 5 * 10**17, 7_777 * 10**6, 9_999 * 10**6),
        # Tiny dust borrow vs large collateral.
        (1, 12_345_678_900_000_000, 1_000_000 * 10**6, 1_000_000 * 10**6),
        # Large 128-bit-ish share totals — guards the int (not Decimal) ceil-div.
        (10**20 + 7, 3 * 10**18, 5 * 10**17, 10**20 + 11),
    ],
)
def test_reduce_calls_byte_identical_to_reader(
    borrow_shares: int, collateral_raw: int, total_borrow_assets: int, total_borrow_shares: int
) -> None:
    position_hex = _position_hex(0, borrow_shares, collateral_raw)
    market_hex = _market_hex(
        total_supply_assets=20_000 * 10**6,
        total_supply_shares=20_000 * 10**6,
        total_borrow_assets=total_borrow_assets,
        total_borrow_shares=total_borrow_shares,
    )
    _assert_equivalent(position_hex, market_hex)


def test_reduce_calls_concrete_decode_values() -> None:
    # Pin the absolute decoded values (not just reader-equality) so a change to
    # the scaling / lltv constants is caught even if the reader changed in lockstep.
    position_hex = _position_hex(0, 100_000_000, 1 * 10**18)  # 1 wstETH collateral
    market_hex = _market_hex(
        total_supply_assets=20_000 * 10**6,
        total_supply_shares=20_000 * 10**6,
        total_borrow_assets=10_000 * 10**6,
        total_borrow_shares=10_000 * 10**6,  # 1:1 → 100 USDC debt
    )
    state = _spec_state(position_hex, market_hex)
    assert state is not None
    assert state.collateral_usd == Decimal("3500")  # 1 wstETH * $3500
    assert state.debt_usd == Decimal("100")  # 100 USDC * $1
    assert state.lltv == Decimal("0.86")
    assert state.health_factor == (Decimal("3500") * Decimal("0.86")) / Decimal("100")  # 30.1
    # Morpho carries the threshold as lltv, not bps, and has no e-mode concept.
    assert state.liquidation_threshold_bps is None
    assert state.e_mode_category is None


def test_zero_borrow_shares_yields_no_debt_sentinel_like_reader() -> None:
    position_hex = _position_hex(0, 0, 1 * 10**18)  # collateral, no borrow
    market_hex = _market_hex(
        total_supply_assets=20_000 * 10**6,
        total_supply_shares=20_000 * 10**6,
        total_borrow_assets=0,
        total_borrow_shares=0,
    )
    state = _spec_state(position_hex, market_hex)
    assert state is not None
    assert state.debt_usd == Decimal("0")
    assert state.health_factor == Decimal("999999")  # infinite-HF sentinel, capped
    assert state.collateral_usd == Decimal("3500")
    _assert_equivalent(position_hex, market_hex)


def test_nonzero_shares_but_zero_total_assets_gives_zero_debt() -> None:
    position_hex = _position_hex(0, 100 * 10**6, 1 * 10**18)
    market_hex = _market_hex(
        total_supply_assets=10_000 * 10**6,
        total_supply_shares=10_000 * 10**18,
        total_borrow_assets=0,  # edge: assets are 0
        total_borrow_shares=100 * 10**6,
    )
    state = _spec_state(position_hex, market_hex)
    assert state is not None
    assert state.debt_usd == Decimal("0")
    assert state.health_factor == Decimal("999999")
    _assert_equivalent(position_hex, market_hex)


# ---------------------------------------------------------------------------
# Empty ≠ Zero — fail-closed semantics
# ---------------------------------------------------------------------------


def test_missing_price_reduces_to_none() -> None:
    # A None injected price must fail closed (Empty ≠ Zero), never fabricate 0.
    position_hex = _position_hex(0, 100_000_000, 1 * 10**18)
    market_hex = _market_hex(20_000 * 10**6, 20_000 * 10**6, 10_000 * 10**6, 10_000 * 10**6)
    query = AccountStateQuery(
        chain=_CHAIN,
        wallet_address=_WALLET,
        position_manager_address=_ETHEREUM_MORPHO,
        market_id=_MARKET_ID,
        prices={_COLLATERAL: Decimal("3500")},  # loan price MISSING
        decimals={_COLLATERAL: _COLLATERAL_DECIMALS, _LOAN: _LOAN_DECIMALS},
        market_params={"lltv": _LLTV_RAW},
        collateral_token=_COLLATERAL,
        loan_token=_LOAN,
    )
    assert MORPHO_BLUE_ACCOUNT_STATE_READ.reduce_calls(query, [position_hex, market_hex]) is None


def test_missing_market_params_reduces_to_none() -> None:
    position_hex = _position_hex(0, 100_000_000, 1 * 10**18)
    market_hex = _market_hex(20_000 * 10**6, 20_000 * 10**6, 10_000 * 10**6, 10_000 * 10**6)
    query = AccountStateQuery(
        chain=_CHAIN,
        wallet_address=_WALLET,
        position_manager_address=_ETHEREUM_MORPHO,
        market_id=_MARKET_ID,
        prices=_PRICES,
        decimals={_COLLATERAL: _COLLATERAL_DECIMALS, _LOAN: _LOAN_DECIMALS},
        market_params={},  # no lltv
        collateral_token=_COLLATERAL,
        loan_token=_LOAN,
    )
    assert MORPHO_BLUE_ACCOUNT_STATE_READ.reduce_calls(query, [position_hex, market_hex]) is None


def test_missing_position_blob_reduces_to_none() -> None:
    market_hex = _market_hex(20_000 * 10**6, 20_000 * 10**6, 10_000 * 10**6, 10_000 * 10**6)
    assert _spec_state(None, market_hex) is None
    _assert_equivalent(None, market_hex)


def test_short_position_blob_reduces_to_none() -> None:
    short = "0x" + "00" * 32  # one word, < 3 words
    market_hex = _market_hex(20_000 * 10**6, 20_000 * 10**6, 10_000 * 10**6, 10_000 * 10**6)
    assert _spec_state(short, market_hex) is None
    _assert_equivalent(short, market_hex)


def test_short_market_blob_reduces_to_none() -> None:
    position_hex = _position_hex(0, 100_000_000, 1 * 10**18)
    five_words = "0x" + _word(10_000 * 10**6) * 5  # 5 words, < 6
    assert _spec_state(position_hex, five_words) is None
    _assert_equivalent(position_hex, five_words)


# ---------------------------------------------------------------------------
# build_calls — emits the reader's two reads against the resolved singleton
# ---------------------------------------------------------------------------


def test_build_calls_emits_position_then_market_against_singleton() -> None:
    calls = MORPHO_BLUE_ACCOUNT_STATE_READ.build_calls(_spec_query())
    assert len(calls) == 2
    assert all(isinstance(c, EthCall) for c in calls)
    # Both target the Morpho singleton; first is position(id, user), second market(id).
    assert calls[0].to == _ETHEREUM_MORPHO
    assert calls[1].to == _ETHEREUM_MORPHO
    assert calls[0].data.startswith(_MORPHO_POSITION_SELECTOR)
    assert calls[1].data.startswith(_MORPHO_MARKET_SELECTOR)
    # position calldata == selector + market_id(32B) + wallet(32B, padded, lower).
    market_id_hex = _MARKET_ID.lower().replace("0x", "").zfill(64)
    wallet_hex = _WALLET.lower().replace("0x", "").zfill(64)
    assert calls[0].data == _MORPHO_POSITION_SELECTOR + market_id_hex + wallet_hex
    # market calldata == selector + market_id(32B), no wallet arg.
    assert calls[1].data == _MORPHO_MARKET_SELECTOR + market_id_hex


def test_market_call_returns_none_reduces_to_none() -> None:
    # A successful position read but a FAILED second eth_call (market(id) → None)
    # is a distinct fail-closed branch from a malformed market blob — pin it.
    position_hex = _position_hex(0, 100_000_000, 1 * 10**18)
    assert _spec_state(position_hex, None) is None
    _assert_equivalent(position_hex, None)


def test_reader_returns_none_when_market_unknown_to_registry() -> None:
    # Sanity: the generic reader resolves the singleton via the registry; an
    # unsupported chain (no Morpho deployment) fails closed without touching the
    # gateway.
    gateway = MagicMock()
    state = read_lending_account_state(
        protocol="morpho_blue",
        chain="fantom",  # no Morpho deployment
        wallet_address=_WALLET,
        market_id=_MARKET_ID,
        gateway_client=gateway,
        price_oracle=_PRICES,
    )
    assert state is None
    gateway.eth_call.assert_not_called()


# ---------------------------------------------------------------------------
# Registry resolution — market_params() is now the LIVE valuation linchpin
# ---------------------------------------------------------------------------


def test_position_manager_address_resolves_morpho_singleton() -> None:
    addr = LendingReadRegistry.position_manager_address("morpho_blue", _CHAIN)
    assert addr == AddressRegistry.addresses_for("morpho_blue", _CHAIN)["morpho"]


def test_resolve_account_state_plan_binds_morpho_singleton_and_calls() -> None:
    # A placeholder target on the query must be overwritten by the registry, and
    # both planned calls target the resolved Morpho singleton.
    query = AccountStateQuery(
        chain=_CHAIN,
        wallet_address=_WALLET,
        position_manager_address="0xPLACEHOLDER",
        market_id=_MARKET_ID,
        prices=_PRICES,
        decimals={_COLLATERAL: _COLLATERAL_DECIMALS, _LOAN: _LOAN_DECIMALS},
        market_params={"lltv": _LLTV_RAW},
        collateral_token=_COLLATERAL,
        loan_token=_LOAN,
    )
    plan = LendingReadRegistry.resolve_account_state_plan("morpho_blue", query)
    assert isinstance(plan, AccountStatePlan)
    expected = AddressRegistry.addresses_for("morpho_blue", _CHAIN)["morpho"]
    assert plan.query.position_manager_address == expected
    assert len(plan.calls) == 2
    assert all(c.to == expected for c in plan.calls)
    assert plan.reduce is MORPHO_BLUE_ACCOUNT_STATE_READ.reduce_calls


def test_market_params_resolves_live_lltv_and_tokens() -> None:
    # Proves market_params() is live (not the dead accessor a prior reviewer
    # claimed): the generic reader reads lltv + the valuation token symbols from
    # it. The wstETH/USDC Ethereum market is 86 % LLTV.
    params = LendingReadRegistry.market_params("morpho_blue", _CHAIN, _MARKET_ID)
    assert params is not None
    assert params["lltv"] == _LLTV_RAW
    assert params["collateral_token"] == _COLLATERAL
    assert params["loan_token"] == _LOAN


def test_valuation_roles_names_both_legs_from_market_table() -> None:
    # The generic reader prices exactly the (collateral, loan) tokens the
    # registry names from the market table — not the intent.
    roles = LendingReadRegistry.valuation_roles("morpho_blue", _CHAIN, _MARKET_ID)
    assert roles == (("collateral_token", _COLLATERAL), ("loan_token", _LOAN))
