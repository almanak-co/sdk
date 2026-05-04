"""Unit coverage for the auto-derived negative-selector path.

Every active ``PermissionTestCase`` now gets a negative-authorisation test
for free — ``run_negative_authorisation_case`` falls back to introspecting
the generated manifest and picks a non-approve function-scoped target to
revoke. These tests cover that derivation logic in isolation (no Anvil,
no Zodiac, no manifest generator) so regressions are caught at PR time,
not the nightly.

The matching end-to-end pilot lives in
``tests/intents/arbitrum/test_zodiac_permission_correctness.py`` — it still
passes an explicit selector for belt-and-suspenders verification.

Plan doc: ``docs/internal/zodiac-permission-onchain-coverage-plan.md``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from unittest.mock import patch

import pytest

from almanak.framework.execution.signer.safe.constants import (
    MULTISEND_SELECTOR as _SAFE_MULTISEND_SELECTOR,
)
from almanak.framework.intents.compiler import (
    ERC20_APPROVE_SELECTOR as _ERC20_APPROVE_SELECTOR,
)
from tests.intents._permission_onchain_harness import (
    PermissionTestCase,
    _auto_derive_load_bearing_selector,
    run_negative_authorisation_case,
)

# -----------------------------------------------------------------------------
# Manifest fixtures
# -----------------------------------------------------------------------------
#
# These dicts mirror the shape of ``PermissionManifest.to_zodiac_targets()``:
# a list of ``{address, clearance, executionOptions, functions?}`` entries.
# ``clearance == 2`` is function-scoped; ``clearance == 1`` is wildcard.
# ``functions`` is a list of ``{selector, wildcarded}``.
#
# The real selectors used below are:
#   - ``0x095ea7b3`` — ERC-20 approve(address,uint256)
#   - ``0x04e45aaf`` — UniswapV3 SwapRouter02 exactInputSingle (load-bearing)
#   - ``0x617ba037`` — Aave V3 Pool.supply (load-bearing)
#
# The exact values don't matter for the derivation logic — only the shape.

_USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
_WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
_SWAP_ROUTER = "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45"
_AAVE_POOL = "0x794a61358d6845594f94dc1db02a252b5b4814ad"


def _approve_target(address: str) -> dict[str, Any]:
    return {
        "address": address,
        "clearance": 2,
        "executionOptions": 0,
        "functions": [{"selector": _ERC20_APPROVE_SELECTOR, "wildcarded": True}],
    }


def _function_target(address: str, selector: str) -> dict[str, Any]:
    return {
        "address": address,
        "clearance": 2,
        "executionOptions": 0,
        "functions": [{"selector": selector, "wildcarded": True}],
    }


def _wildcard_target(address: str) -> dict[str, Any]:
    return {
        "address": address,
        "clearance": 1,
        "executionOptions": 0,
    }


# =============================================================================
# _auto_derive_load_bearing_selector
# =============================================================================


def test_auto_derive_returns_none_for_approve_only_manifest() -> None:
    """Approve-only manifests have no load-bearing target to strip — return None.

    The caller pytest.skips in this case. Proving "revoking approve blocks the
    bundle" is not what the negative test is for — it'd pass for trivial
    reasons (approval blocked, not the protocol call).
    """
    targets = [_approve_target(_USDC), _approve_target(_WETH)]
    assert _auto_derive_load_bearing_selector(targets) is None


def test_auto_derive_returns_none_for_empty_targets() -> None:
    """Empty manifest — nothing to pick from, return None."""
    assert _auto_derive_load_bearing_selector([]) is None


def test_auto_derive_returns_none_for_wildcard_only_manifest() -> None:
    """Wildcard-scoped (clearance=1) entries have no selectors — not candidates.

    Revoking a wildcard removes the entire address, which is a different
    negative-test pattern (``revoke_target`` without a selector argument).
    This helper is specifically for the selector-narrowed path.
    """
    targets = [_wildcard_target(_SWAP_ROUTER)]
    assert _auto_derive_load_bearing_selector(targets) is None


def test_auto_derive_picks_single_non_approve_function_target() -> None:
    """One non-approve function-scoped target → that (address, selector) is picked.

    The canonical SWAP manifest shape: approve(USDC), approve(WETH),
    exactInputSingle(SwapRouter). Only the last is load-bearing. The tuple
    return shape means the caller gets the exact address to revoke without
    a second lookup.
    """
    exact_input_single = "0x04e45aaf"
    targets = [
        _approve_target(_USDC),
        _approve_target(_WETH),
        _function_target(_SWAP_ROUTER, exact_input_single),
    ]
    derived = _auto_derive_load_bearing_selector(targets)
    assert derived == (_SWAP_ROUTER.lower(), exact_input_single)


def test_auto_derive_picks_deterministically_by_lowest_address() -> None:
    """Multiple non-approve candidates → lowest ``(address, selector)`` tuple wins.

    Ordering must be stable across Python versions and dict-iteration orders
    so the negative test parametrize IDs don't flap between CI runs. The
    returned tuple carries the winning address directly so the caller can
    revoke it without another pass over ``targets``.
    """
    supply_selector = "0x617ba037"
    exact_input_single = "0x04e45aaf"
    # _AAVE_POOL starts with 0x794... which is higher than _SWAP_ROUTER (0x68b...).
    # Expect _SWAP_ROUTER's selector to win.
    assert _SWAP_ROUTER < _AAVE_POOL
    targets = [
        _function_target(_AAVE_POOL, supply_selector),
        _function_target(_SWAP_ROUTER, exact_input_single),
    ]
    derived = _auto_derive_load_bearing_selector(targets)
    assert derived == (_SWAP_ROUTER.lower(), exact_input_single)


def test_auto_derive_disambiguates_shared_selector_across_addresses() -> None:
    """Two targets sharing a selector at different addresses → returned tuple pins the winner.

    This is the concrete motivation for returning ``(address, selector)``
    rather than just ``selector``: previously a downstream lookup via
    ``_find_target_by_selector`` would be ambiguous when a selector appeared
    on multiple function-scoped targets (uncommon but possible — e.g. two
    router deployments at distinct addresses that expose the same core
    call). Returning the tuple removes the ambiguity entirely.
    """
    shared_selector = "0x04e45aaf"
    # Both addresses expose the same selector. Lexicographic tuple order
    # means the lower address wins deterministically.
    assert _SWAP_ROUTER < _AAVE_POOL
    targets = [
        _function_target(_AAVE_POOL, shared_selector),
        _function_target(_SWAP_ROUTER, shared_selector),
    ]
    derived = _auto_derive_load_bearing_selector(targets)
    assert derived == (_SWAP_ROUTER.lower(), shared_selector)


def test_auto_derive_skips_safe_multisend_selector() -> None:
    """Safe MultiSend is included on every manifest but is batching infra.

    A simple single-leg bundle (e.g. a bare SWAP on arbitrum) does not hit
    MultiSend. If auto-derivation picked the MultiSend target because it
    happened to sort lowest, revoking it would leave the bundle succeeding
    through the non-MultiSend path and the negative test would surface as
    "DID NOT RAISE" — a false negative masquerading as a pass.

    Observed in the real arbitrum manifest: MultiSend lives at
    ``0x38869bf...`` which sorts below SwapRouter02 at ``0x68b346...``,
    so the naive "lowest address" pick would have been wrong without this
    exclusion. Regression guard.
    """
    multisend_address = "0x38869bf66a61cf6bdb996a6ae40d5853fd43b526"
    exact_input_single = "0x04e45aaf"
    targets = [
        # MultiSend sorts lowest by address — if we didn't skip it, the
        # naive "lowest address wins" pick would return its selector.
        _function_target(multisend_address, _SAFE_MULTISEND_SELECTOR),
        _function_target(_SWAP_ROUTER, exact_input_single),
    ]
    assert multisend_address < _SWAP_ROUTER
    # MultiSend is excluded, so the SwapRouter's (address, selector) pair wins.
    derived = _auto_derive_load_bearing_selector(targets)
    assert derived == (_SWAP_ROUTER.lower(), exact_input_single)


def test_auto_derive_returns_none_for_multisend_only_manifest() -> None:
    """Manifest with only approve + MultiSend (no core call) → skip cleanly."""
    multisend_address = "0x38869bf66a61cf6bdb996a6ae40d5853fd43b526"
    targets = [
        _approve_target(_USDC),
        _function_target(multisend_address, _SAFE_MULTISEND_SELECTOR),
    ]
    assert _auto_derive_load_bearing_selector(targets) is None


def test_auto_derive_ignores_approve_even_when_alongside_real_call() -> None:
    """An approve target on the same address as a real call must not be picked.

    The returned tuple still names the correct address — the same one as
    the approve entry, since they share a target — paired with the
    non-approve selector.
    """
    exact_input_single = "0x04e45aaf"
    targets = [
        # Same address exposing both selectors — e.g. a router that also happens
        # to implement an approve-compatible signature. The derivation must
        # pick the non-approve selector regardless of ordering.
        {
            "address": _SWAP_ROUTER,
            "clearance": 2,
            "executionOptions": 0,
            "functions": [
                {"selector": _ERC20_APPROVE_SELECTOR, "wildcarded": True},
                {"selector": exact_input_single, "wildcarded": True},
            ],
        },
    ]
    derived = _auto_derive_load_bearing_selector(targets)
    assert derived == (_SWAP_ROUTER.lower(), exact_input_single)


def test_auto_derive_is_case_insensitive_on_approve_selector() -> None:
    """Selectors written in uppercase still match the approve sentinel.

    The manifest generator currently emits lowercase, but the harness is
    defensive — downstream selectors shouldn't become load-bearing just
    because somebody mixed-cased the hex.
    """
    # Uppercase approve — should still be excluded.
    targets = [
        {
            "address": _USDC,
            "clearance": 2,
            "executionOptions": 0,
            "functions": [
                {"selector": _ERC20_APPROVE_SELECTOR.upper(), "wildcarded": True}
            ],
        },
    ]
    assert _auto_derive_load_bearing_selector(targets) is None


def test_auto_derive_skips_missing_address_entries() -> None:
    """Malformed target lacking an address is skipped, not crashed on.

    Defensive: if the manifest generator ever emits a target without a
    resolvable address (should never happen on main, but cheap to guard),
    the derivation should degrade gracefully.
    """
    targets = [
        {
            "address": "",
            "clearance": 2,
            "executionOptions": 0,
            "functions": [{"selector": "0xdeadbeef", "wildcarded": True}],
        },
    ]
    assert _auto_derive_load_bearing_selector(targets) is None


# =============================================================================
# run_negative_authorisation_case — selector resolution order
# =============================================================================


class _FakeManifest:
    """Stand-in for ``PermissionManifest`` — only ``to_zodiac_targets`` is called."""

    def __init__(self, targets: list[dict[str, Any]]) -> None:
        self._targets = targets

    def to_zodiac_targets(self) -> list[dict[str, Any]]:
        return self._targets


@dataclass
class _HarnessObservation:
    """What ``run_negative_authorisation_case`` did, observed under patches.

    Exactly one of ``skip_message``, ``find_selector`` + ``revoked_address``,
    or ``revoked_address`` alone will be populated depending on which branch
    ran:

    - ``skip_message`` — auto-derivation returned ``None``, harness called
      ``pytest.skip``.
    - ``find_selector`` set → explicit path: ``_find_target_by_selector`` was
      called with this selector, then ``revoke_target`` with the resolved
      address.
    - ``find_selector`` is ``None`` and ``revoked_address`` is set →
      auto-derived path: the tuple return was used directly to call
      ``revoke_target``, with no intermediate ``_find_target_by_selector``
      lookup.
    """

    skip_message: str | None = None
    find_selector: str | None = None
    revoked_address: str | None = None


def _run_with_patched_harness(
    case: PermissionTestCase,
    *,
    targets: list[dict[str, Any]],
    load_bearing_selector: str | None,
) -> _HarnessObservation:
    """Drive ``run_negative_authorisation_case`` up to revoke and observe the path taken.

    Patches every side-effecting dependency so the call is a pure-Python
    dry run: no Anvil, no Web3, no compiler. The observation distinguishes
    explicit-selector flow (``_find_target_by_selector`` was called) from
    auto-derived flow (tuple return used directly, no lookup).

    Why this is an OK test shape: the selector-resolution branch is a
    handful of lines but it's the whole point of this PR. An integration
    test would hide regressions behind deployment flake; a unit test
    surfaces them as fast, deterministic failures.
    """
    observation = _HarnessObservation()

    def _fake_setup(case_arg: PermissionTestCase, **_kw: Any) -> tuple[str, str, bytes, list[dict]]:
        return ("0xsafe", "0xroles", b"role_key_padded_to_32_bytes____", targets)

    _LOAD_BEARING_LOOKUP_ADDR = "0xloadbearing"

    def _fake_find(targets_arg: list[dict], selector: str) -> dict:
        observation.find_selector = selector
        # Return a realistic-looking target so the harness can extract an
        # address to hand to revoke_target; the next patch short-circuits
        # immediately after.
        return {
            "address": _LOAD_BEARING_LOOKUP_ADDR,
            "clearance": 2,
            "executionOptions": 0,
        }

    def _fake_revoke(
        _web3: Any,
        _roles: Any,
        _safe: Any,
        _role_key: Any,
        target_address: str,
        **_kw: Any,
    ) -> None:
        observation.revoked_address = target_address
        # Short-circuit — the test only cares which target got revoked;
        # re-raising here is the cleanest way to stop execution without
        # affecting the branch we want to observe.
        raise RuntimeError("__stop_after_selector_resolution__")

    class _SkipSentinel(Exception):
        """Raised by the patched ``pytest.skip`` so the test can observe the skip.

        We can't re-use ``pytest.skip.Exception`` after ``pytest.skip`` itself
        has been monkey-patched — the patched object is a plain function and
        no longer carries the nested exception class. A dedicated sentinel is
        the cleanest signal.
        """

    def _fake_skip(message: str) -> None:
        observation.skip_message = message
        raise _SkipSentinel(message)

    with (
        patch(
            "tests.intents._permission_onchain_harness._setup_zodiac_and_apply_manifest",
            _fake_setup,
        ),
        patch(
            "tests.intents._permission_onchain_harness._find_target_by_selector",
            _fake_find,
        ),
        patch(
            "tests.intents._permission_onchain_harness.revoke_target",
            _fake_revoke,
        ),
        patch("tests.intents._permission_onchain_harness.pytest.skip", _fake_skip),
        patch(
            "tests.intents._permission_onchain_harness._prefund_for_negative",
            lambda *a, **kw: [],
        ),
    ):
        try:
            run_negative_authorisation_case(
                case,
                load_bearing_selector=load_bearing_selector,
                web3=None,  # type: ignore[arg-type]  # unused under patches
                anvil_rpc_url="http://localhost:8545",
                funded_wallet="0xwallet",
                test_private_key="0x" + "00" * 32,
                price_oracle=None,
            )
        except RuntimeError as exc:
            if str(exc) != "__stop_after_selector_resolution__":
                raise
        except _SkipSentinel:
            pass

    return observation


def _swap_case() -> PermissionTestCase:
    return PermissionTestCase(
        chain="arbitrum",
        protocol="uniswap_v3",
        intent_type="SWAP",
        config={"from_token": "USDC", "to_token": "WETH", "amount": "100"},
    )


def test_run_negative_prefers_explicit_kwarg_over_case_and_auto() -> None:
    """Explicit ``load_bearing_selector`` kwarg wins over every other source.

    This is the pilot path — the arbitrum test file still passes the selector
    explicitly for belt-and-suspenders verification. Explicit selectors go
    through ``_find_target_by_selector`` so a typo on a case file surfaces
    as an assertion error rather than silently revoking the wrong target.
    """
    targets = [_function_target(_SWAP_ROUTER, "0xAUTODERIVED000")]
    case = PermissionTestCase(
        chain="arbitrum",
        protocol="uniswap_v3",
        intent_type="SWAP",
        config={"from_token": "USDC", "to_token": "WETH", "amount": "100"},
        negative_selector="0xCASEDECLARED0",
    )
    obs = _run_with_patched_harness(
        case, targets=targets, load_bearing_selector="0xEXPLICITKWARG"
    )
    assert obs.skip_message is None
    assert obs.find_selector == "0xEXPLICITKWARG"
    # Explicit path routes through _find_target_by_selector — the address
    # the fake returned is what gets revoked.
    assert obs.revoked_address == "0xloadbearing"


def test_run_negative_prefers_case_negative_selector_over_auto() -> None:
    """Case-declared ``negative_selector`` wins when no explicit kwarg is passed."""
    targets = [_function_target(_SWAP_ROUTER, "0xAUTODERIVED000")]
    case = PermissionTestCase(
        chain="arbitrum",
        protocol="uniswap_v3",
        intent_type="SWAP",
        config={"from_token": "USDC", "to_token": "WETH", "amount": "100"},
        negative_selector="0xCASEDECLARED0",
    )
    obs = _run_with_patched_harness(
        case, targets=targets, load_bearing_selector=None
    )
    assert obs.skip_message is None
    assert obs.find_selector == "0xCASEDECLARED0"
    assert obs.revoked_address == "0xloadbearing"


def test_run_negative_auto_derives_when_neither_kwarg_nor_case_set() -> None:
    """No kwarg, no case declaration → harness auto-derives from manifest.

    The tuple return shape means the harness revokes the derived address
    directly — ``_find_target_by_selector`` is NOT called, which is the
    whole point of the tuple refactor (no second lookup, no ambiguity).
    """
    exact_input_single = "0x04e45aaf"
    targets = [
        _approve_target(_USDC),
        _approve_target(_WETH),
        _function_target(_SWAP_ROUTER, exact_input_single),
    ]
    case = _swap_case()
    obs = _run_with_patched_harness(
        case, targets=targets, load_bearing_selector=None
    )
    assert obs.skip_message is None
    # Auto-derived path must NOT go through _find_target_by_selector — the
    # tuple return is the lookup.
    assert obs.find_selector is None
    # The derived address itself is what gets revoked, not the lookup stub.
    assert obs.revoked_address == _SWAP_ROUTER.lower()


def test_run_negative_auto_derive_picks_derived_address_not_lookup() -> None:
    """When two targets share a selector, the derived address — not a lookup stub — is revoked.

    Direct regression test for the tuple refactor: previously the harness
    resolved the selector from auto-derivation then re-walked ``targets``
    via ``_find_target_by_selector``, which would have raised on the
    duplicate-selector case. Now the tuple return pins the exact address
    up front, so the ambiguity never arises.
    """
    shared_selector = "0x04e45aaf"
    # Two targets, same selector, different addresses. Lexicographic tuple
    # order picks _SWAP_ROUTER (lower address) deterministically.
    assert _SWAP_ROUTER < _AAVE_POOL
    targets = [
        _function_target(_AAVE_POOL, shared_selector),
        _function_target(_SWAP_ROUTER, shared_selector),
    ]
    case = _swap_case()
    obs = _run_with_patched_harness(
        case, targets=targets, load_bearing_selector=None
    )
    assert obs.skip_message is None
    assert obs.find_selector is None  # tuple return, no ambiguous lookup
    assert obs.revoked_address == _SWAP_ROUTER.lower()


def test_run_negative_skips_when_no_load_bearing_target() -> None:
    """Approve-only manifest → pytest.skip, not crash.

    This is the key contract: a case whose manifest happens to be
    approve-only (or wildcard-only) does not break the nightly — it skips
    with a clear message so the operator knows the test was a no-op.
    """
    targets = [_approve_target(_USDC), _approve_target(_WETH)]
    case = _swap_case()
    obs = _run_with_patched_harness(
        case, targets=targets, load_bearing_selector=None
    )
    assert obs.find_selector is None
    assert obs.revoked_address is None
    assert obs.skip_message is not None
    assert "No load-bearing" in obs.skip_message
    assert "uniswap_v3" in obs.skip_message
    assert "SWAP" in obs.skip_message
