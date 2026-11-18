"""Reviewed hook profiles: admission by code, exit over pin and the Roles PoolKey."""

import json

import pytest
from pydantic import ValidationError

from almanak.caip import AssetType, ChainId
from almanak.connectors.uniswap_v4 import hooks
from almanak.connectors.uniswap_v4.execution import require_hook_code
from almanak.connectors.uniswap_v4.hooks import (
    DYNAMIC_FEE,
    PROFILES,
    admit_key,
    registry,
    require_entry_code,
    require_reviewed_code,
)
from almanak.connectors.uniswap_v4.models import Range, Target
from almanak.connectors.uniswap_v4.permissions import MARKERS, swap_grant
from almanak.connectors.uniswap_v4.resolution import (
    HookAdmission,
    MarketSelection,
    admitted_hook,
    require_scope,
)
from almanak.connectors.uniswap_v4.state import Approval
from almanak.gateway.ethereum import DataError
from almanak.gateway.roles_conditions import flatten
from almanak.models import ZERO_ADDRESS
from tests.uniswap_hook_support import (
    FABLES_RAMP,
    PONS_HOOK,
    PRIORS,
    ROBINHOOD,
    STABLE_PAIR,
    USDG,
    Authority,
    key,
    pinned,
    pool,
    swap,
    target,
)

CHANGED = "0x" + "12" * 32


def changed(address, chain="eip155:4663"):
    return pinned(address, chain).model_copy(update={"code_hash": CHANGED})


def test_registry_covers_every_reviewed_instance_with_its_profile():
    counts = {}
    for (chain, address), entry in registry().items():
        profile = entry.reviewed
        assert int(address, 16) & hooks.PERMISSION_MASK == profile.flags
        counts[(chain, profile.name)] = counts.get((chain, profile.name), 0) + 1
    assert counts == {
        ("eip155:1", "uniswap-stable-pair"): 1,
        ("eip155:42161", "uniswap-stable-pair"): 1,
        ("eip155:4663", "fables-ramp"): 1,
        ("eip155:4663", "fables-ramp-eth"): 2,
        ("eip155:4663", "fables-rwa"): 7,
        ("eip155:4663", "pons-v2-meme"): 3,
    }
    proxies = [e for e in registry().values() if e.identity.implementation is not None]
    assert {e.profile for e in proxies} == {"uniswap-stable-pair"}
    assert all(e.identity.implementation_hash for e in proxies)


def test_registry_refuses_an_unknown_profile_when_it_loads():
    (entry,) = (e for e in registry().values() if e.identity.address == PONS_HOOK)
    raw = entry.model_dump(mode="json") | {"profile": "pons_v2_meme"}
    with pytest.raises(ValidationError, match="unknown profile"):
        hooks.ReviewedHook.model_validate(raw)


@pytest.mark.parametrize("name", ["fables", "stable_pair", "pons"])
def test_reviewed_keys_are_admitted_with_their_profile(name):
    chain, _, _, fee, _, address = key(name)
    entry = admit_key(chain.chain_id, fee, address)
    assert entry.identity.address == address
    assert pool(name).hooks == address


def test_unknown_hook_and_foreign_chain_are_refused():
    with pytest.raises(ValueError, match="no reviewed profile"):
        admit_key(ChainId("eip155:4663"), DYNAMIC_FEE, "0x" + "12" * 20)
    # A reviewed address is reviewed only on its own chain.
    with pytest.raises(ValueError, match="no reviewed profile"):
        admit_key(ChainId("eip155:8453"), DYNAMIC_FEE, FABLES_RAMP)
    with pytest.raises(ValidationError, match="no reviewed profile"):
        pool("fables", hooks="0x" + "12" * 20)


def test_permission_bits_must_match_the_reviewed_profile(monkeypatch):
    entry = registry()[("eip155:4663", FABLES_RAMP)]
    mislabelled = {
        **registry(),
        ("eip155:4663", FABLES_RAMP): entry.model_copy(update={"profile": "pons-v2-meme"}),
    }
    monkeypatch.setattr(hooks, "registry", lambda: mislabelled)
    with pytest.raises(ValueError, match="permission bits"):
        admit_key(ChainId("eip155:4663"), 0, FABLES_RAMP)


@pytest.mark.parametrize(
    ("name", "fee"), [("fables", 3000), ("stable_pair", 100), ("pons", DYNAMIC_FEE)]
)
def test_fee_mode_must_match_the_profile(name, fee):
    with pytest.raises(ValidationError, match="requires"):
        pool(name, fee=fee)


def test_hookless_keys_keep_static_fees_only():
    assert pool("hookless").hooks == ZERO_ADDRESS
    with pytest.raises(ValidationError, match="dynamic-fee flag"):
        pool("hookless", fee=DYNAMIC_FEE)
    with pytest.raises(ValidationError, match="at most 1000000"):
        pool("hookless", fee=1_000_001)


def selection(name, **overrides):
    chain, currency0, currency1, fee, spacing, address = key(name)
    raw = {
        "connector": "uniswap_v4",
        "chain": str(chain.chain_id),
        "currency0": f"{chain.chain_id}/erc20:{currency0}",
        "currency1": f"{chain.chain_id}/erc20:{currency1}",
        "fee": fee,
        "tick_spacing": spacing,
        "hooks": address,
        **overrides,
    }
    return MarketSelection.model_validate_json(json.dumps(raw))


class Reader:
    chain = ROBINHOOD

    def __init__(self, block):
        self.block = block


def test_resolution_refuses_unreviewed_keys_before_any_read():
    block = type("B", (), {"chain": ROBINHOOD})()
    require_scope(Reader(block), selection("fables"), block)
    for mutation in ({"hooks": "0x" + "12" * 20}, {"fee": 3000}):
        with pytest.raises(DataError) as error:
            require_scope(Reader(block), selection("fables", **mutation), block)
        assert error.value.kind == "unsupported_pool"


def test_admission_requires_the_pinned_code(monkeypatch):
    source = pool("fables")
    from almanak.connectors.uniswap_v4 import resolution
    from almanak.connectors.uniswap_v4.hook_terms import OverrideFee

    monkeypatch.setattr(resolution, "reviewed_terms", lambda *_: OverrideFee(fees=(8674, 3499)))
    monkeypatch.setattr(
        resolution, "AuthorityReader", lambda *_: Authority({FABLES_RAMP: pinned(FABLES_RAMP)})
    )
    admitted = admitted_hook(None, source, None)
    assert admitted == HookAdmission(
        profile="fables-ramp", identity=pinned(FABLES_RAMP), terms=OverrideFee(fees=(8674, 3499))
    )
    monkeypatch.setattr(
        resolution, "AuthorityReader", lambda *_: Authority({FABLES_RAMP: changed(FABLES_RAMP)})
    )
    with pytest.raises(DataError) as error:
        admitted_hook(None, source, None)
    assert error.value.kind == "unsupported_revision"
    assert admitted_hook(None, pool("hookless"), None) is None


def test_proxy_admission_pins_the_implementation():
    identity = pinned(STABLE_PAIR, "eip155:1")
    upgraded = identity.model_copy(update={"implementation_hash": CHANGED})
    source = pool("stable_pair")
    assert require_reviewed_code(Authority({STABLE_PAIR: identity}), source).profile == (
        "uniswap-stable-pair"
    )
    with pytest.raises(DataError, match="differs from its reviewed"):
        require_reviewed_code(Authority({STABLE_PAIR: upgraded}), source)


def test_exit_over_pin_refuses_entries_and_admits_exits():
    source = pool("stable_pair")
    upgraded = Authority(
        {
            STABLE_PAIR: pinned(STABLE_PAIR, "eip155:1").model_copy(
                update={"implementation": "0x" + "34" * 20, "implementation_hash": CHANGED}
            )
        }
    )
    with pytest.raises(DataError) as error:
        require_entry_code(upgraded, source, entering=True)
    assert error.value.kind == "hook_changed"
    assert "exits remain available" in str(error.value)
    assert require_entry_code(upgraded, source, entering=False) is False
    reviewed = Authority({STABLE_PAIR: pinned(STABLE_PAIR, "eip155:1")})
    assert require_entry_code(reviewed, source, entering=True) is True


def test_gateway_planning_applies_exit_over_pin_to_each_operation():
    authority = Authority({PONS_HOOK: changed(PONS_HOOK)})
    entry = swap("pons", USDG, 10**6, 1, funding=USDG)
    exit_leg = swap("pons", PRIORS, 10**18, 1, funding=USDG)
    with pytest.raises(DataError, match="new exposure is refused"):
        require_hook_code(None, entry, authority)
    require_hook_code(None, exit_leg, authority)
    for asset, amount in ((PRIORS, 10**18), (USDG, 0)):
        # Approving the counter currency for the exit and revoking either one are exits.
        approval = Approval(
            target=entry.target,
            kind="approve_erc20" if amount else "revoke_erc20",
            asset=AssetType(f"eip155:4663/erc20:{asset}"),
            spender=entry.target.contracts.permit2,
            amount=amount,
        )
        require_hook_code(None, approval, authority)
    funding = approval.model_copy(update={"kind": "approve_erc20", "amount": 10**6})
    with pytest.raises(DataError, match="new exposure is refused"):
        require_hook_code(None, funding, authority)


@pytest.mark.parametrize("name", ["fables", "stable_pair"])
def test_override_fee_hooks_admit_liquidity_allocations(name):
    spacing = key(name)[4]
    selected = target(name, range=Range(lower=-10 * spacing, upper=10 * spacing))
    assert selected.range is not None and selected.pool.hooks != ZERO_ADDRESS


def test_pons_pools_pay_no_lp_fee_and_stay_trading_only():
    selected = target("pons")
    with pytest.raises(ValidationError, match="no fee; this pool supports trading"):
        Target.model_validate(
            {**selected.model_dump(), "range": Range(lower=-2000, upper=2000).model_dump()}
        )


@pytest.mark.parametrize(
    "permission",
    [
        "before_remove_liquidity",
        "after_remove_liquidity",
        "after_add_liquidity_returns_delta",
        "after_remove_liquidity_returns_delta",
    ],
)
def test_a_profile_that_could_hold_a_position_cannot_admit_liquidity(permission):
    reviewed = hooks.PROFILES["uniswap-stable-pair"].model_dump()
    with pytest.raises(ValidationError, match="cannot admit LP"):
        hooks.HookProfile.model_validate(
            {**reviewed, "permissions": (*reviewed["permissions"], permission)}
        )
    hooks.HookProfile.model_validate(
        {
            **reviewed,
            "permissions": (*reviewed["permissions"], permission),
            "liquidity_refusal": "reviewed for trading only",
        }
    )


@pytest.mark.parametrize("direction", [True, False])
def test_roles_swap_grant_pins_the_full_pool_key_including_the_hook(direction):
    selected = target("pons", funding=USDG)
    grant = swap_grant(selected, direction)
    words = {
        int(c.value, 16)
        for c in flatten(grant.condition)
        if c.operator == 16 and len(c.value) == 66
    }
    assert int(PONS_HOOK, 16) in words
    assert int(USDG, 16) in words and int(PRIORS, 16) in words
    assert not any(marker in words for marker in MARKERS[:2])


def test_profiles_name_their_models():
    assert {p.name: p.model for p in PROFILES.values()} == {
        "fables-ramp": "override_fee",
        "fables-ramp-eth": "override_fee",
        "fables-rwa": "override_fee",
        "uniswap-stable-pair": "override_fee",
        "pons-v2-meme": "output_haircut",
    }


def test_history_reads_and_pins_the_hook_with_the_protocol(monkeypatch):
    from almanak.connectors.uniswap_v4 import history
    from almanak.connectors.uniswap_v4.deployments import historical_addresses, target_addresses

    selected = target("pons", funding=USDG)
    assert historical_addresses(selected) == target_addresses(selected) | {PONS_HOOK}
    assert historical_addresses(target("hookless")) == target_addresses(target("hookless"))
    table = {a: pinned(PONS_HOOK) for a in historical_addresses(selected)}
    monkeypatch.setattr(history, "verify_identities", lambda *_: None)
    monkeypatch.setattr(history, "AuthorityReader", lambda *_: Authority(table))
    assert pinned(PONS_HOOK) in history.contract_identities(None, selected, None)
    table[PONS_HOOK] = changed(PONS_HOOK)
    with pytest.raises(DataError) as error:
        history.contract_identities(None, selected, None)
    assert error.value.kind == "unsupported_revision"
