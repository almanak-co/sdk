"""Reviewed Uniswap V4 hook profiles, admitted by code and never by strategy configuration.

A hooked PoolKey is supported only when its hook address is listed in
`artifacts/reviewed-hooks.json` for the pool's chain. Each entry pins the runtime
code, and for an ERC-1967 proxy its implementation, that was reviewed, and names a
profile. The profile fixes the permission bits the hook address must encode, whether
the PoolKey fee must carry the dynamic-fee flag, and the behaviour model the
connector applies to quotes, receipts and backtests:

- `override_fee`: `beforeSwap` returns only an LP fee override; the hook's own fee
  view reports it per direction at a block.
- `output_haircut`: `afterSwap` takes a fixed per-pool share of the unspecified
  (on exact input, the output) currency through `afterSwapReturnDelta`.

A profile admits liquidity allocations only when its review covered them. Such a hook
has no remove-liquidity callback and no liquidity return delta: callbacks are fixed by
the address bits, so no later code can block a withdrawal or move a position's tokens.
LP fees need no model, since the PoolManager's fee growth already carries the hook's fee.

Exit over pin: a hook whose code no longer matches its pin refuses new exposure,
but exits still execute through the pool with a fresh quote and the router's
strict minimum output, so a pin never traps a position.
"""

import json
from functools import cache
from pathlib import Path
from typing import Annotated, Literal

from pydantic import AfterValidator, model_validator

from almanak.caip import ChainId
from almanak.gateway.ethereum import DataError
from almanak.gateway.permission_models import ContractIdentity
from almanak.models import ZERO_ADDRESS, Address, Model, UInt

ARTIFACT = Path(__file__).with_name("artifacts") / "reviewed-hooks.json"
DYNAMIC_FEE = 0x800000
# v4-core Hooks: the low 14 address bits are the callback permissions.
PERMISSION_MASK = 0x3FFF
FLAGS = {
    "before_initialize": 1 << 13,
    "after_initialize": 1 << 12,
    "before_add_liquidity": 1 << 11,
    "after_add_liquidity": 1 << 10,
    "before_remove_liquidity": 1 << 9,
    "after_remove_liquidity": 1 << 8,
    "before_swap": 1 << 7,
    "after_swap": 1 << 6,
    "before_donate": 1 << 5,
    "after_donate": 1 << 4,
    "before_swap_returns_delta": 1 << 3,
    "after_swap_returns_delta": 1 << 2,
    "after_add_liquidity_returns_delta": 1 << 1,
    "after_remove_liquidity_returns_delta": 1 << 0,
}
Permission = Literal[
    "before_initialize",
    "after_initialize",
    "before_add_liquidity",
    "after_add_liquidity",
    "before_remove_liquidity",
    "after_remove_liquidity",
    "before_swap",
    "after_swap",
    "before_donate",
    "after_donate",
    "before_swap_returns_delta",
    "after_swap_returns_delta",
    "after_add_liquidity_returns_delta",
    "after_remove_liquidity_returns_delta",
]
Terms = Literal["fables_current_fee", "stable_pair_get_fee", "pons_launch"]
LIQUIDITY_BLOCKERS = frozenset(
    (
        "before_remove_liquidity",
        "after_remove_liquidity",
        "after_add_liquidity_returns_delta",
        "after_remove_liquidity_returns_delta",
    )
)


class HookProfile(Model):
    name: str
    model: Literal["override_fee", "output_haircut"]
    terms: Terms
    permissions: tuple[Permission, ...]
    dynamic_fee: bool
    # Why liquidity allocations are refused; None when the review admits them.
    liquidity_refusal: str | None = None

    @model_validator(mode="after")
    def liquidity_cannot_be_held(self):
        if self.liquidity_refusal is None and LIQUIDITY_BLOCKERS.intersection(self.permissions):
            raise ValueError("a hook with remove-liquidity callbacks or deltas cannot admit LP")
        return self

    @property
    def flags(self) -> int:
        return sum(FLAGS[name] for name in self.permissions)


FABLES: tuple[Permission, ...] = ("before_initialize", "before_swap")
PROFILES = {
    profile.name: profile
    for profile in (
        HookProfile(
            name=name,
            model="override_fee",
            terms="fables_current_fee",
            permissions=FABLES,
            dynamic_fee=True,
        )
        for name in ("fables-ramp", "fables-ramp-eth", "fables-rwa")
    )
}
PROFILES["uniswap-stable-pair"] = HookProfile(
    name="uniswap-stable-pair",
    model="override_fee",
    terms="stable_pair_get_fee",
    permissions=(
        "before_initialize",
        "after_initialize",
        "before_add_liquidity",
        "after_add_liquidity",
        "before_swap",
        "after_swap",
    ),
    dynamic_fee=True,
)
PROFILES["pons-v2-meme"] = HookProfile(
    name="pons-v2-meme",
    model="output_haircut",
    terms="pons_launch",
    permissions=("before_initialize", "after_swap", "after_swap_returns_delta"),
    dynamic_fee=False,
    # The launch pools carry a zero LP fee and the hook keeps the whole take.
    liquidity_refusal="Pons launch pools pay liquidity providers no fee",
)


def known_profile(name: str) -> str:
    if name not in PROFILES:
        raise ValueError(f"reviewed hook names unknown profile {name!r}")
    return name


class ReviewedHook(Model):
    chain: ChainId
    profile: Annotated[str, AfterValidator(known_profile)]
    reviewed_at_block: UInt
    identity: ContractIdentity

    @property
    def reviewed(self) -> HookProfile:
        return PROFILES[self.profile]


@cache
def registry() -> dict[tuple[str, str], ReviewedHook]:
    raw = json.loads(ARTIFACT.read_text())
    entries = tuple(ReviewedHook.model_validate_json(json.dumps(entry)) for entry in raw["hooks"])
    return {(str(e.chain), e.identity.address): e for e in entries}


def logical_chain(chain: ChainId) -> str:
    # Retained Ethereum checkpoints used the local transport ID as logical identity.
    return "eip155:1" if str(chain) == "eip155:31337" else str(chain)


def reviewed_hook(chain: ChainId, hooks: Address) -> ReviewedHook | None:
    """The reviewed entry of a hooked key; None for a hookless key."""
    if hooks == ZERO_ADDRESS:
        return None
    entry = registry().get((logical_chain(chain), hooks))
    if entry is None:
        raise ValueError(
            f"Uniswap V4 hook {hooks} has no reviewed profile on {chain}; "
            "hooked pool behavior is supported only for reviewed hooks"
        )
    return entry


def admit_key(chain: ChainId, fee: int, hooks: Address) -> ReviewedHook | None:
    """Static admission of a PoolKey: reviewed address, permission bits and fee mode."""
    entry = reviewed_hook(chain, hooks)
    if entry is None:
        if fee == DYNAMIC_FEE:
            raise ValueError("a hookless PoolKey cannot use the dynamic-fee flag")
        return None
    profile = entry.reviewed
    if int(hooks, 16) & PERMISSION_MASK != profile.flags:
        raise ValueError(f"hook {hooks} permission bits differ from its reviewed profile")
    if (fee == DYNAMIC_FEE) != profile.dynamic_fee:
        mode = "the dynamic-fee flag" if profile.dynamic_fee else "a static fee"
        raise ValueError(f"reviewed hook profile {profile.name} requires {mode}")
    return entry


def pool_hook(pool) -> ReviewedHook | None:
    return reviewed_hook(pool.chain.chain_id, pool.hooks)


def code_matches(authority, entry: ReviewedHook) -> bool:
    return authority.identity(entry.identity.address) == entry.identity


def require_reviewed_code(authority, pool) -> ReviewedHook | None:
    """Admission: a hooked pool's current code must equal its reviewed pin."""
    entry = pool_hook(pool)
    if entry is not None and not code_matches(authority, entry):
        raise DataError(
            "unsupported_revision",
            f"Uniswap V4 hook {pool.hooks} code differs from its reviewed {entry.profile} pin",
        )
    return entry


def require_entry_code(authority, pool, *, entering: bool) -> bool:
    """Exit over pin: changed hook code refuses new exposure and still admits exits.

    Returns whether the pin holds, so callers can make an admitted exit visible.
    """
    entry = pool_hook(pool)
    if entry is None or code_matches(authority, entry):
        return True
    if entering:
        raise DataError(
            "hook_changed",
            f"Uniswap V4 hook {pool.hooks} code differs from its reviewed {entry.profile} pin; "
            "new exposure is refused and exits remain available",
        )
    return False
