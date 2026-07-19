"""VIB-5795 / VIB-5896 — the generic fungible-ERC-20-LP teardown post-condition
and its registration as a framework default for every ``ProtocolKind.LP``
connector with ``fungible_lp=True`` (curve, fluid_dex_lp).

Covers the closure rule (LP-token ``balanceOf(wallet)`` within the wei dust
floor), the three-valued outcome (closed / residual / unmeasured), the bounded
read-retry, the address-resolution priority (``details['lp_token']`` >
``lp_token_address`` > address-shaped ``position_id``; burn-amount
position_id never used), and that the default resolves for the fungible-LP
slugs (curve was previously pinned at UNVERIFIED — the 20260718 quant-test
false-negative).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from almanak.connectors._strategy_base import vault_post_condition as vpc
from almanak.connectors._strategy_base.fungible_lp_post_condition import (
    _LP_TOKEN_DUST_WEI,
    fungible_lp_teardown_post_condition as lp_hook,
)
from almanak.connectors._strategy_base.teardown_post_condition import get_teardown_post_condition

# 3pool 3Crv LP token (mainnet) — any valid address works for the double.
_LP_TOKEN = "0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490"
_OWNER = "0x8a8678A18e60c7eDbacC6FCBd5A08D0A6ECb21e8"


class _FakeGateway:
    """Minimal gateway_client double: scripted balanceOf.

    ``balances`` may be a single value or a list consumed in call order (to
    script a transient None → value read-retry). ``"raise"`` raises.
    """

    def __init__(self, balances):
        self._balances = list(balances) if isinstance(balances, list) else [balances]
        self.balance_calls = 0
        self.last_token = None

    def query_erc20_balance(self, *, chain, token_address, wallet_address, block=None):
        self.last_token = token_address
        v = self._balances[min(self.balance_calls, len(self._balances) - 1)]
        self.balance_calls += 1
        if v == "raise":
            raise RuntimeError("gateway balanceOf blip")
        return v


def _position(details=None, position_id="curve-3pool-lp", chain="ethereum", protocol="curve"):
    return SimpleNamespace(
        protocol=protocol,
        position_id=position_id,
        chain=chain,
        details={"lp_token": _LP_TOKEN} if details is None else details,
    )


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    """Zero the shared read-retry backoff so retry tests don't sleep."""
    monkeypatch.setattr(vpc, "_READ_BACKOFF_S", 0.0)


def test_zero_balance_is_closed():
    gw = _FakeGateway(balances=0)
    r = lp_hook(_position(), _OWNER, gateway_client=gw)
    assert r.closed is True
    assert r.unmeasured is False
    assert gw.last_token == _LP_TOKEN


def test_dust_floor_boundary():
    # Exactly at the floor → closed; one wei above → measured residual.
    assert lp_hook(_position(), _OWNER, gateway_client=_FakeGateway(_LP_TOKEN_DUST_WEI)).closed is True
    r = lp_hook(_position(), _OWNER, gateway_client=_FakeGateway(_LP_TOKEN_DUST_WEI + 1))
    assert r.closed is False
    assert r.unmeasured is False


def test_positive_balance_is_measured_open():
    # The class of strand this hook exists to catch: LP tokens still held.
    gw = _FakeGateway(balances=288_540_000_000_000_000_000)  # ~288.54 3Crv
    r = lp_hook(_position(), _OWNER, gateway_client=gw)
    assert r.closed is False
    assert r.unmeasured is False
    assert r.residual["lp_token"] == _LP_TOKEN
    assert r.residual["balance"] == "288540000000000000000"


def test_missing_gateway_client_is_unmeasured():
    r = lp_hook(_position(), _OWNER, gateway_client=None)
    assert r.unmeasured is True
    assert r.closed is False
    assert r.error and "gateway_client" in r.error


def test_missing_chain_is_unmeasured():
    r = lp_hook(_position(chain=""), _OWNER, gateway_client=_FakeGateway(0))
    assert r.unmeasured is True
    assert r.error and "chain" in r.error


def test_no_resolvable_lp_token_is_unmeasured():
    # No lp_token detail and a non-address position_id → honest unmeasured.
    r = lp_hook(_position(details={}, position_id="curve-3pool-lp"), _OWNER, gateway_client=_FakeGateway(0))
    assert r.unmeasured is True
    assert "lp_token" in (r.error or "")


def test_burn_amount_position_id_never_used_as_address():
    # Curve overloads LPCloseIntent.position_id as the burn AMOUNT (VIB-4968);
    # an integer string must NOT be treated as a token address.
    r = lp_hook(
        _position(details={}, position_id="288540000000000000000"),
        _OWNER,
        gateway_client=_FakeGateway(0),
    )
    assert r.unmeasured is True


def test_address_shaped_position_id_fallback():
    gw = _FakeGateway(balances=0)
    r = lp_hook(_position(details={}, position_id=_LP_TOKEN), _OWNER, gateway_client=gw)
    assert r.closed is True
    assert gw.last_token == _LP_TOKEN


def test_lp_token_address_detail_key_resolves():
    gw = _FakeGateway(balances=0)
    r = lp_hook(_position(details={"lp_token_address": _LP_TOKEN}), _OWNER, gateway_client=gw)
    assert r.closed is True


def test_ambiguous_address_detail_key_is_not_used():
    # ``address`` / ``pool_address`` may hold the POOL contract (legacy Curve
    # pools: pool != LP token) — reading it could fabricate a residual (false
    # FAILED) or, worse, read a balance the wallet never holds → 0 → a FALSE
    # CHAIN_VERIFIED on an unclosed position. The narrow key policy must skip
    # both (unmeasured, never a wrong measurement). See the ``fungible_lp``
    # manifest-field invariant in ``_connector_descriptor.py``.
    for key in ("address", "pool_address"):
        r = lp_hook(_position(details={key: _LP_TOKEN}), _OWNER, gateway_client=_FakeGateway(5))
        assert r.unmeasured is True, f"details[{key!r}] must not be used as the LP token"


def test_balance_read_fault_is_unmeasured_not_residual():
    gw = _FakeGateway(balances=["raise", "raise", "raise"])
    r = lp_hook(_position(), _OWNER, gateway_client=gw)
    assert r.unmeasured is True
    assert r.closed is False
    assert not r.residual
    # The bounded retry budget must actually be exhausted before declaring
    # unmeasured, and the verdict must carry an explanatory error.
    from almanak.connectors._strategy_base.vault_post_condition import _READ_ATTEMPTS

    assert gw.balance_calls == _READ_ATTEMPTS
    assert r.error and "retry" in r.error


def test_balance_read_retry_recovers_from_transient_none():
    gw = _FakeGateway(balances=[None, 0])
    r = lp_hook(_position(), _OWNER, gateway_client=gw)
    assert r.closed is True
    assert gw.balance_calls == 2


def test_non_numeric_balance_is_unmeasured():
    gw = _FakeGateway(balances="not-a-number")
    r = lp_hook(_position(), _OWNER, gateway_client=gw)
    assert r.unmeasured is True


@pytest.mark.parametrize("slug", ["curve", "fluid_dex_lp"])
def test_default_registered_for_fungible_lp_slugs(slug):
    """The framework default resolves for fungible-LP connector slugs.

    Pre-fix, ``get_teardown_post_condition("curve")`` returned ``None`` and
    both verify lanes skipped the position → structurally UNVERIFIED.
    """
    import almanak.framework.teardown.post_conditions  # noqa: F401 — triggers registration

    hook = get_teardown_post_condition(slug)
    assert hook is lp_hook


def test_registration_is_capability_derived_not_a_name_list():
    """The covered set is DERIVED from the manifest capability, not hardcoded.

    Enumerates ``CONNECTOR_REGISTRY`` live: every ``ProtocolKind.LP`` connector
    with ``fungible_lp=True`` must resolve a teardown post-condition under
    EVERY slug it can emit (the fungible-LP default unless the connector owns
    a manifest hook, which wins). A future fungible-LP connector is therefore
    covered the day its manifest lands — no name list to update, and this test
    fails if registration ever regresses to a hardcoded protocol list that
    misses it.
    """
    from almanak.connectors._base.types import ProtocolKind
    from almanak.connectors._connector import CONNECTOR_REGISTRY
    from almanak.framework.teardown.post_conditions import _connector_teardown_slugs

    covered = 0
    for connector in CONNECTOR_REGISTRY.all():
        if connector.kind is not ProtocolKind.LP or not getattr(connector, "fungible_lp", False):
            continue
        for slug in _connector_teardown_slugs(connector):
            hook = get_teardown_post_condition(slug)
            assert hook is not None, f"fungible_lp connector {connector.name!r} slug {slug!r} has no hook"
            if connector.teardown_post_condition is None:
                assert hook is lp_hook, f"{slug!r} should resolve the fungible-LP default"
            covered += 1
    assert covered >= 2, "expected at least the curve + fluid_dex_lp slugs to be capability-covered"


def test_curve_pool_table_lp_tokens_are_erc20_addresses_not_pools():
    """Drift guard for the ``fungible_lp`` teardown invariant (pr-auditor #3329).

    The verifier reads ``balanceOf`` on the LP-token address a position carries;
    curve positions source that from ``CURVE_POOLS[chain][pool]['lp_token']``.
    Every entry must be a valid ERC-20 address, and for the legacy pools where
    pool != LP token (3pool/3Crv is the canonical case) the two must differ —
    a table edit that collapses them would make the verifier read the POOL
    contract and fabricate a false CHAIN_VERIFIED.
    """
    from almanak.connectors._strategy_base.vault_post_condition import _is_evm_address
    from almanak.connectors.curve.adapter import CURVE_POOLS

    checked = 0
    for chain, pools in CURVE_POOLS.items():
        for name, meta in pools.items():
            lp_token = meta.get("lp_token")
            assert _is_evm_address(str(lp_token)), f"{chain}/{name}: lp_token {lp_token!r} is not an EVM address"
            checked += 1
    assert checked > 0
    # The canonical legacy split that motivates the invariant.
    threepool = CURVE_POOLS["ethereum"]["3pool"]
    assert threepool["lp_token"].lower() != threepool["address"].lower()
    assert threepool["lp_token"].lower() == _CURVE_3CRV_LOWER


_CURVE_3CRV_LOWER = _LP_TOKEN.lower()


def test_v3_slugs_not_clobbered():
    """The fungible-LP default must not replace the V3-NPM default."""
    import almanak.framework.teardown.post_conditions  # noqa: F401

    hook = get_teardown_post_condition("uniswap_v3")
    assert hook is not None
    assert hook is not lp_hook
