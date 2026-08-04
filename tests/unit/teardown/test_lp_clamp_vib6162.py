"""VIB-6162 — a fungible-LP close is bounded to this deployment's own liquidity.

Each test names the failure it would catch. Several exist specifically because a
previous attempt at this ticket passed every test it had and was still inert in
production, so "the clamp works when it engages" is deliberately not the only property
asserted here — "the clamp refuses when it cannot engage" carries equal weight.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.connectors._connector_descriptor import FungibleLpCloseDecl, ImportRef
from almanak.connectors._strategy_base.fungible_lp_identity import canonical_pool_key
from almanak.framework.teardown.lp_clamp import (
    LpClampUnresolved,
    bound_close_amount,
    clamp_decl_for,
    read_outstanding_liquidity,
)

POOL = "0xcdac0d6c6c59727a65f871236188350531885c43"


class _StubStateManager:
    """Minimal async stand-in for the position-events read."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self.calls: list[dict] = []

    async def get_position_events_filtered(self, *, deployment_id: str, position_types: frozenset) -> list[dict]:
        self.calls.append({"deployment_id": deployment_id, "position_types": position_types})
        return self._rows


def _row(event_type: str, liquidity: object, *, position_id: str = POOL, row_id: int = 1) -> dict:
    return {"id": row_id, "event_type": event_type, "liquidity": liquidity, "position_id": position_id, "pool": None}


# ---------------------------------------------------------------------------
# Identity canonicalization — the property whose absence made the last fix inert
# ---------------------------------------------------------------------------


def test_token_order_does_not_change_the_key():
    """Catches: a clamp inert for half its callers because token order differs.

    The Aerodrome stable pool's own ``symbol()`` is ``sAMM-DAI/USDC`` while the shipped
    strategy config names the same pool ``USDC/DAI``. Both orderings are live in one
    venue, so an order-sensitive key silently fails to match stored history for one of
    them while looking healthy for the other.
    """
    assert canonical_pool_key("USDC/DAI/stable") == canonical_pool_key("DAI/USDC/stable")


def test_summary_lane_and_close_lane_ids_agree():
    """Catches: a limit keyed on the id the teardown SUMMARY reported.

    ``aerodrome_aave_carry_base`` reports ``aerodrome-lp-{pool}-{chain}`` and closes
    with a different string; ``lp_curve`` reports ``curve_3pool_{lp_token}`` and closes
    with the bare token. A raw comparison matches neither.
    """
    assert canonical_pool_key(f"aerodrome-lp-{POOL}-base") == canonical_pool_key(POOL)
    assert canonical_pool_key(f"curve_3pool_{POOL}") == canonical_pool_key(POOL)


def test_a_decimal_string_is_an_amount_not_an_identity():
    """Catches: a partial-withdrawal request being matched against history and widened.

    Curve accepts a decimal ``position_id`` as the amount to withdraw. Treating it as a
    name would let the clamp reinterpret "withdraw 1234.5" as "withdraw this
    deployment's outstanding", which is larger.
    """
    assert canonical_pool_key("1234.5") is None
    assert canonical_pool_key("0") is None


def test_empty_identity_is_unresolvable():
    assert canonical_pool_key(None) is None
    assert canonical_pool_key("") is None


# ---------------------------------------------------------------------------
# Manifest eligibility — the connector decides, not the framework
# ---------------------------------------------------------------------------


def test_aerodrome_is_clamped_and_curve_is_deliberately_not():
    """Curve's exclusion is a decision (VIB-6489), pinned so it cannot drift silently.

    Curve declares ``fungible_lp=True``, which auto-registers a teardown post-condition
    whose closure rule is ``balanceOf <= 10 wei``. A correct clamped close leaves a
    residual by design, so that post-condition would report it FAILED. The two must
    move together.
    """
    aero = clamp_decl_for("aerodrome")
    assert aero is not None and aero.clamp is True
    assert aero.units == "raw" and aero.decimals == 18

    curve = clamp_decl_for("curve")
    assert curve is not None and curve.clamp is False
    assert curve.units == "token"


def test_clamp_without_a_resolver_is_rejected_at_construction():
    """Catches the inert-guard shape at import time rather than in production.

    ``clamp=True`` with no identity resolver reads as "protected" and behaves as
    "unprotected" — precisely how the previous attempt shipped.
    """
    with pytest.raises(ValueError, match="requires an identity ImportRef"):
        FungibleLpCloseDecl(units="raw", decimals=18, clamp=True)


def test_raw_units_require_decimals():
    """Catches an unscaled raw value being compared against a token-unit balance."""
    with pytest.raises(ValueError, match="requires a non-negative int decimals"):
        FungibleLpCloseDecl(units="raw", clamp=False)
    with pytest.raises(ValueError, match="must not declare decimals"):
        FungibleLpCloseDecl(units="token", decimals=18, clamp=False)


def test_unknown_units_are_rejected():
    with pytest.raises(ValueError, match="units must be one of"):
        FungibleLpCloseDecl(units="wei", clamp=False)


# ---------------------------------------------------------------------------
# Outstanding is a running balance
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_outstanding_subtracts_prior_closes():
    """Catches: bounding by total minted after OPEN -> partial CLOSE -> OPEN.

    Summing opens without subtracting closes over-withdraws by exactly the amount
    already closed — and one-open/one-close fixtures cannot tell the two apart.
    """
    sm = _StubStateManager(
        [
            _row("OPEN", 10 * 10**18, row_id=1),
            _row("CLOSE", 4 * 10**18, row_id=2),
            _row("OPEN", 5 * 10**18, row_id=3),
        ]
    )
    got = await read_outstanding_liquidity(sm, "deployment:abc", protocol="aerodrome", position_id=POOL)
    assert got == Decimal(11)


@pytest.mark.asyncio
async def test_snapshot_rows_are_excluded():
    """Catches: SNAPSHOT rows inflating the bound above what was ever minted.

    SNAPSHOT carries CURRENT liquidity once per iteration, not a delta.
    """
    sm = _StubStateManager([_row("OPEN", 10 * 10**18, row_id=1), _row("SNAPSHOT", 10 * 10**18, row_id=2)])
    got = await read_outstanding_liquidity(sm, "deployment:abc", protocol="aerodrome", position_id=POOL)
    assert got == Decimal(10)


@pytest.mark.asyncio
async def test_the_read_is_scoped_to_this_deployment():
    """Catches: a sibling deployment's LP counted as ours and therefore burned."""
    sm = _StubStateManager([_row("OPEN", 10**18)])
    await read_outstanding_liquidity(sm, "deployment:abc", protocol="aerodrome", position_id=POOL)
    assert sm.calls[0]["deployment_id"] == "deployment:abc"


@pytest.mark.asyncio
async def test_other_pools_do_not_contribute():
    sm = _StubStateManager(
        [_row("OPEN", 10**18, position_id=POOL), _row("OPEN", 99 * 10**18, position_id="0x" + "ab" * 20)]
    )
    got = await read_outstanding_liquidity(sm, "deployment:abc", protocol="aerodrome", position_id=POOL)
    assert got == Decimal(1)


# ---------------------------------------------------------------------------
# Refusal — Empty is not Zero, and unresolved is not unlimited
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unresolvable_identifier_refuses():
    """The inertness gate. An id the connector cannot resolve must NOT close unbounded."""
    sm = _StubStateManager([_row("OPEN", 10**18)])
    with pytest.raises(LpClampUnresolved, match="cannot resolve a canonical pool identity"):
        await read_outstanding_liquidity(sm, "deployment:abc", protocol="aerodrome", position_id="1234.5")


@pytest.mark.asyncio
async def test_unmeasured_liquidity_refuses_rather_than_reading_as_zero():
    """Empty != Zero. An unrecorded mint must not silently become 'minted nothing'."""
    sm = _StubStateManager([_row("OPEN", None)])
    with pytest.raises(LpClampUnresolved, match="no measured"):
        await read_outstanding_liquidity(sm, "deployment:abc", protocol="aerodrome", position_id=POOL)

    sm_empty = _StubStateManager([_row("OPEN", "")])
    with pytest.raises(LpClampUnresolved, match="no measured"):
        await read_outstanding_liquidity(sm_empty, "deployment:abc", protocol="aerodrome", position_id=POOL)


@pytest.mark.asyncio
async def test_no_history_is_unmeasured_not_zero():
    sm = _StubStateManager([])
    with pytest.raises(LpClampUnresolved, match="unmeasured, not zero"):
        await read_outstanding_liquidity(sm, "deployment:abc", protocol="aerodrome", position_id=POOL)


@pytest.mark.asyncio
async def test_an_unclamped_venue_returns_none_and_reads_nothing():
    """Curve is not clamped, so no bound is produced — and the caller must not read
    ``None`` as permission to close unbounded."""
    sm = _StubStateManager([_row("OPEN", 10**18)])
    assert await read_outstanding_liquidity(sm, "deployment:abc", protocol="curve", position_id=POOL) is None
    assert sm.calls == []


# ---------------------------------------------------------------------------
# The bound itself — refusal, NOT min()
# ---------------------------------------------------------------------------


def test_bound_is_the_outstanding_amount_when_live_covers_it():
    assert bound_close_amount(Decimal(10), Decimal(15)) == Decimal(10)


def test_tracked_exceeding_live_refuses_instead_of_taking_the_minimum():
    """The counterexample that killed ``min(tracked, live)`` during design review.

    outstanding=100, foreign=50, 60 of the strategy's own LP transferred out. The wallet
    holds 40 own + 50 foreign = 90. ``min(100, 90) = 90`` burns all 50 foreign shares —
    the exact defect this module exists to remove. Refusal is the only safe contract.
    """
    with pytest.raises(LpClampUnresolved, match="exceeds live balance"):
        bound_close_amount(Decimal(100), Decimal(90), pool_key=POOL)


def test_unmeasured_inputs_refuse():
    with pytest.raises(LpClampUnresolved, match="outstanding is unmeasured"):
        bound_close_amount(None, Decimal(10))
    with pytest.raises(LpClampUnresolved, match="live balance is unmeasured"):
        bound_close_amount(Decimal(10), None)


def test_a_negative_fold_refuses():
    """Closes exceeding opens means the history is inconsistent, not that we may burn."""
    with pytest.raises(LpClampUnresolved, match="folded negative"):
        bound_close_amount(Decimal(-1), Decimal(10))


def test_a_measured_zero_is_allowed_and_withdraws_nothing():
    """Decimal('0') is a measured zero and must stay distinct from unmeasured None."""
    assert bound_close_amount(Decimal(0), Decimal(500000)) == Decimal(0)


# ---------------------------------------------------------------------------
# Resolver wiring
# ---------------------------------------------------------------------------


def test_the_declared_resolver_actually_loads():
    """Catches a manifest naming a resolver that does not exist — the declaration would
    otherwise look complete and fail only at teardown, on the money path."""
    decl = clamp_decl_for("aerodrome")
    assert isinstance(decl.identity, ImportRef)
    assert decl.identity.load() is canonical_pool_key


# ---------------------------------------------------------------------------
# Declaration scope — one connector, two LP models (critique r3)
# ---------------------------------------------------------------------------


def test_the_fungible_clamp_does_not_reach_slipstream():
    """The aerodrome connector owns slugs with DIFFERENT LP models.

    ``aerodrome`` is Solidly-fork fungible LP; its alias ``aerodrome_slipstream`` is
    NFT-based concentrated liquidity. Resolving this declaration through the whole alias
    namespace handed the fungible clamp to Slipstream, whose ``position_id`` is an NFT
    tokenId — a decimal string ``canonical_pool_key`` correctly refuses to read as a
    name — so every Slipstream close was REFUSED and every Slipstream position stranded
    on teardown. A V1-only liveness test cannot see it: ``aerodrome`` proceeds correctly
    while the alias refuses 100%. The discriminator has to name the alias.
    """
    from almanak.connectors._connector_descriptor import CONNECTOR_REGISTRY

    assert CONNECTOR_REGISTRY.fungible_lp_close_for("aerodrome_slipstream") is None
    # The hyphenated spelling normalises to the same slug and must not slip through.
    assert CONNECTOR_REGISTRY.fungible_lp_close_for("aerodrome-slipstream") is None


def test_the_fungible_clamp_still_reaches_aerodrome_v1():
    """The other half of the discriminator.

    Without this, deleting the declaration outright would satisfy the assertion above —
    a guard that reaches nothing passes every "does not reach X" test.
    """
    from almanak.connectors._connector_descriptor import CONNECTOR_REGISTRY

    decl = CONNECTOR_REGISTRY.fungible_lp_close_for("aerodrome")
    assert decl is not None
    assert decl.clamp is True


@pytest.mark.asyncio
async def test_a_slipstream_close_is_not_clamped_end_to_end():
    """Reach it through the real read path, not only the registry accessor.

    ``None`` means "this venue declares no clamp", which the caller passes through
    unbounded — the pre-existing behaviour — rather than refusing.
    """
    sm = _StubStateManager([_row("OPEN", "5", position_id="123456")])
    outstanding = await read_outstanding_liquidity(
        sm,
        "deployment:abc",
        protocol="aerodrome_slipstream",
        position_id="123456",
        pool="WETH/USDC/100",
    )
    assert outstanding is None


# ---------------------------------------------------------------------------
# FungibleLpCloseDecl validation — every rejection branch (critique r4 / CRAP gate)
# ---------------------------------------------------------------------------
#
# This declaration decides whether a venue is clamped, in what units, and by which
# identity resolver. A malformed value does not fail at USE -- it silently matches
# nothing, or scales by an unknown factor, and the venue reads as unclamped. So
# "declared clamped" must never be able to mean "unclamped", and every branch that
# enforces that is exercised here rather than trusted.
#
# The CRAP gate is what surfaced the gap: cc=20 at 62% coverage scored 42, because the
# validation branches added for the protocols scoping had no test driving them. Covering
# them takes the score to ~cc. That is the gate working as intended -- a guard nothing
# executes is a guard nobody has checked.


@pytest.mark.parametrize(
    ("kwargs", "needle"),
    [
        # units
        ({"units": "wei"}, "units must be one of"),
        ({"units": 18}, "units must be one of"),
        # units/decimals pairing -- `raw` is uninterpretable without a scale, and `token`
        # is already scaled, so carrying decimals there invites a second, contradictory one
        ({"units": "raw"}, "requires a non-negative int decimals"),
        ({"units": "raw", "decimals": -1}, "requires a non-negative int decimals"),
        ({"units": "raw", "decimals": True}, "requires a non-negative int decimals"),
        ({"units": "token", "decimals": 18}, "decimals"),
        # protocols scoping -- a bare string is the nasty one: it iterates per character
        # and matches no slug, so the venue silently reads as unclamped
        ({"units": "token", "protocols": "aerodrome"}, "must be a tuple of slugs"),
        ({"units": "token", "protocols": ["aerodrome"]}, "must be a tuple of slugs"),
        ({"units": "token", "protocols": ()}, "applies the declaration to no slug"),
        ({"units": "token", "protocols": ("",)}, "non-empty strings"),
        ({"units": "token", "protocols": ("aerodrome", 7)}, "non-empty strings"),
        # clamp / identity pairing -- a clamp that cannot resolve identity is a clamp
        # that silently does not engage, which is exactly how PR #3588 shipped inert
        ({"units": "token", "clamp": "yes"}, "clamp must be a bool"),
        ({"units": "token", "identity": "not_an_import_ref"}, "identity must be None or an ImportRef"),
        ({"units": "token", "clamp": True}, "requires an identity ImportRef"),
    ],
)
def test_malformed_declarations_are_rejected_at_construction(kwargs, needle):
    with pytest.raises(ValueError, match=needle):
        FungibleLpCloseDecl(**kwargs)


def test_well_formed_declarations_are_accepted():
    """The other half of the discriminator.

    Without this, a validator that rejected EVERYTHING would satisfy every assertion
    above -- the same fail-closed-guard-with-no-liveness-test shape that let an earlier
    round of this ticket refuse 100% of closes.
    """
    ref = ImportRef(module="almanak.connectors._strategy_base.fungible_lp_identity", attribute="canonical_pool_key")
    assert FungibleLpCloseDecl(units="token").units == "token"
    assert FungibleLpCloseDecl(units="raw", decimals=18).decimals == 18
    assert FungibleLpCloseDecl(units="raw", decimals=0).decimals == 0
    assert FungibleLpCloseDecl(units="token", clamp=True, identity=ref).clamp is True
    assert FungibleLpCloseDecl(units="token", protocols=("aerodrome", "velodrome")).protocols == (
        "aerodrome",
        "velodrome",
    )
