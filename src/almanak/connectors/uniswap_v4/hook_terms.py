"""Measured terms of a reviewed hook at one block, and the swap effect they imply.

Live and fork quotes come from the V4 Quoter, which executes the hook, so they
already carry its fee or take. These terms serve the offline economic model,
receipt reconciliation and observation evidence.
"""

from collections.abc import Callable
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.connectors.uniswap_v4.abi import POOL_KEY, pool_key
from almanak.connectors.uniswap_v4.hooks import code_matches, pool_hook
from almanak.connectors.uniswap_v4.pool import pool_id
from almanak.gateway.ethereum import DataError, throttled
from almanak.models import Model

BPS = 10_000
# PonsV2MemeHook.MAX_TOTAL_TRADE_FEE_BPS: registration refuses a larger total.
MAX_HAIRCUT_BPS = 2_000
MAX_LP_FEE = 1_000_000
# Provider failures say nothing about the hook; they fail the observation instead of
# leaving an entry's terms unmeasured. A null result from a view is the provider's,
# not the hook's. Reverts and undecodable views stay evidence; any other `rpc` error,
# such as a -32005 throttle, is the provider's.
PROVIDER_FAILURES = frozenset(("transport", "http", "malformed", "rate_limit", "unavailable"))
# PonsV2MemeHook.LaunchInfo, in declaration order.
LAUNCH = (
    "bool",
    "bool",
    "address",
    "address",
    "address",
    "address",
    "address",
    "uint16",
    "uint16",
    "uint16",
    "uint16",
    "uint16",
    "bool",
)
Fee = Annotated[int, Field(strict=True, ge=0, le=MAX_LP_FEE)]
Bps = Annotated[int, Field(strict=True, ge=0, le=MAX_HAIRCUT_BPS)]


class OverrideFee(Model):
    """The LP fee override, in pips, for zero-for-one and one-for-zero swaps."""

    kind: Literal["override_fee"] = "override_fee"
    fees: tuple[Fee, Fee]

    def lp_fee(self, zero_for_one: bool, stored: int) -> int:
        return self.fees[0 if zero_for_one else 1]

    def take(self, output: int) -> int:
        return 0


class OutputHaircut(Model):
    """A pool's hook fee and creator tax, each floored separately as the hook computes them."""

    kind: Literal["output_haircut"] = "output_haircut"
    hook_fee_bps: Bps
    creator_tax_bps: Bps

    @model_validator(mode="after")
    def capped(self):
        if self.hook_fee_bps + self.creator_tax_bps > MAX_HAIRCUT_BPS:
            raise ValueError("hook output haircut exceeds the reviewed 2000 bps total")
        return self

    def lp_fee(self, zero_for_one: bool, stored: int) -> int:
        return stored

    def take(self, output: int) -> int:
        return output * self.hook_fee_bps // BPS + output * self.creator_tax_bps // BPS


HookTerms = Annotated[OverrideFee | OutputHaircut, Field(discriminator="kind")]


def identifier(pool) -> bytes:
    return bytes.fromhex(pool_id(pool)[2:])


def fables_fees(reader, pool, block) -> OverrideFee:
    fees = tuple(
        reader.call(
            pool.hooks,
            "currentFee(bytes32,bool)",
            block,
            inputs=("bytes32", "bool"),
            args=(identifier(pool), direction),
            outputs=("uint24",),
        )[0]
        for direction in (True, False)
    )
    return OverrideFee(fees=fees)


def stable_pair_fees(reader, pool, block) -> OverrideFee:
    fees = reader.call(
        pool.hooks,
        f"getFee({POOL_KEY})",
        block,
        inputs=(POOL_KEY,),
        args=(pool_key(pool),),
        outputs=("uint24", "uint24"),
    )
    return OverrideFee(fees=tuple(fees))


def pons_haircut(reader, pool, block) -> OutputHaircut:
    launch = reader.call(
        pool.hooks,
        "launches(bytes32)",
        block,
        inputs=("bytes32",),
        args=(identifier(pool),),
        outputs=LAUNCH,
    )
    if not launch[0]:
        raise DataError("unsupported_pool", "Pons pool is not registered with its hook")
    return OutputHaircut(hook_fee_bps=launch[10], creator_tax_bps=launch[7])


READERS: dict[str, Callable[..., OverrideFee | OutputHaircut]] = {
    "fables_current_fee": fables_fees,
    "stable_pair_get_fee": stable_pair_fees,
    "pons_launch": pons_haircut,
}


def read_terms(reader, pool, block) -> OverrideFee | OutputHaircut | None:
    entry = pool_hook(pool)
    return None if entry is None else reviewed_terms(reader, pool, block, entry)


def reviewed_terms(reader, pool, block, entry) -> OverrideFee | OutputHaircut:
    try:
        return READERS[entry.reviewed.terms](reader, pool, block)
    except ValueError as error:
        raise DataError("unsupported_pool", f"hook terms are outside review: {error}") from None


def observed_terms(reader, pool, block, *, live=False) -> dict:
    """Snapshot evidence: terms, or the reason they are unmeasured. Exits never need them.

    Live snapshots read only an output haircut, the one term receipts reconcile; the
    Quoter already executes an override fee.
    """
    entry = pool_hook(pool)
    if entry is None or (live and entry.reviewed.model != "output_haircut"):
        return {}
    try:
        return {"hook_terms": reviewed_terms(reader, pool, block, entry)}
    except DataError as error:
        if provider_failure(error):
            raise
        return {"hook_error": f"hook terms unavailable: {error.kind}"}


def provider_failure(error: DataError) -> bool:
    """A failure a later attempt may pass; anything else the view did is hook evidence.

    Of the node's own refusals only a throttle (429, -32005) is the provider's. A revert,
    and also an invalid opcode or out-of-gas that geth-based nodes report as a bare
    -32000, is what changed hook code does deterministically: kept as evidence, an exit's
    snapshot and reconcile still complete, while `require_hook_terms` refuses new exposure.
    """
    if error.kind == "rpc":
        return throttled(error)
    return error.kind in PROVIDER_FAILURES


def describe(reader, pool, block) -> dict:
    """Read-only report of a hooked pool: its reviewed profile, pin status and terms."""
    from almanak.gateway.roles_storage import AuthorityReader

    entry = pool_hook(pool)
    if entry is None:
        return {}
    observed = observed_terms(reader, pool, block)
    terms = observed.get("hook_terms")
    return {
        "hook": {
            "profile": entry.profile,
            "model": entry.reviewed.model,
            "code_matches_pin": code_matches(AuthorityReader(reader, block), entry),
            "terms": None if terms is None else terms.model_dump(mode="json"),
            **({"error": observed["hook_error"]} if "hook_error" in observed else {}),
        }
    }
