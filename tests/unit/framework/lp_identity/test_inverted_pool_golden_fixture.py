"""VIB-6104 — the inverted-pool golden fixture (Move C enforcement artifact).

Why this file exists
--------------------
The address-vs-label token-ordering phantom has now been fixed in **six** separate
consumers with the same offline realign primitive — VIB-5851 (LP cost basis),
VIB-5983 (``position_events``), VIB-5988 (TraderJoe ``PrimitiveMoneyLegs``),
VIB-6053 (``transaction_ledger`` money rows), VIB-6383 (Liquidity Book close),
VIB-6471 / VIB-6476 (presence-vs-success). Every one of those was found in
production, after the money had already been misreported: a ``$2.47`` position
booked at ``$322,107,799,472.28``, a ``$2.80`` close booked at
``$228,032,393,195.42``.

Six instances is not six bugs. It is one defect class with no enforcement
artifact, so instance #7 arrives with the next connector. VIB-6104 §Enforcement
names the artifact that stops it:

    Mandatory per-LP-connector **inverted-pool golden fixture** (label order ≠
    address order) asserting ledger + position_events + accounting_events all
    bind identically, plus a non-address-sorted control (Curve) that must NOT
    address-sort.

This is that fixture.

What it asserts
---------------
For each LP connector, on a pool whose **label order disagrees with its
on-chain address order**:

1. **Golden binding** — each amount slot is bound to the symbol that actually
   owns it. The expectation is declared per scenario from real mainnet
   addresses and real pool facts, never computed by the code under test.
2. **Lane agreement** — ``transaction_ledger``, ``accounting_events``, and
   ``position_events`` bind the pair identically. A reader is supposed to be
   able to reconcile these three surfaces; when they disagree, one of them is
   lying and nothing in CI notices.
3. **Anti-sort control** — for venues that are NOT address-sorted (TraderJoe
   Liquidity Book ``tokenX``/``tokenY``, Curve ``coins(i)``), the binding must
   differ from what an address sort would produce. This is the assertion that
   would have caught VIB-6383 before it shipped.
4. **Coverage** — every connector declaring an ``LP_*`` intent in its manifest
   must appear here, either with a scenario or with an explicit, reasoned
   exclusion. Connector #13 cannot be added without confronting this file.
5. **Liveness** — the mutation controls in ``TestFixtureLiveness`` prove the
   fixture actually fails when the defect is reintroduced. A golden fixture
   that cannot go red is decoration.

What it does NOT assert
-----------------------
This fixture pins the **binding decision** at each lane's decision point. It
does not re-test each connector's *receipt parsing* — that its parser stamps
``currency0``/``currency1`` from the right log — which is a per-connector
property covered by per-connector tests (e.g.
``test_lp_handler_lb_token_order_vib6383.py`` drives the real TraderJoe parser
over a real receipt). The two are complementary: that test proves the stamp is
correct, this one proves every consumer of the stamp agrees about what it means.

Nor does it assert value: no prices, no ``value_usd``, no cost basis. Binding is
upstream of all of them, and every phantom in the list above was a binding error
that valuation then amplified.

Provenance of the modelled shapes
---------------------------------
A fixture that guards a shape production never emits proves nothing. Every shape
below was checked against a census of the **real** ``transaction_ledger`` LP rows
in this repo's run DBs (2026-08-04, ~180 rows across uniswap_v3, uniswap_v4,
pancakeswap_v3, aerodrome, aerodrome_slipstream, traderjoe_v2, curve, camelot):

* **``stamp=BOTH``** — observed. uniswap_v3 (arbitrum, base) and uniswap_v4
  (base) from 2026-08-02 onward, i.e. once VIB-6053's stamping shipped.
* **``stamp=NONE``** — observed, and dominant historically: every run predating
  the stamp fix, including the traderjoe_v2 close of 2026-08-03 that IS the
  VIB-6383 row (``amount0_collected=228032393198215910``, ``currency0=null``).
* **``stamp=PARTIAL``** — **not** observed in any stored row, and modelled here
  anyway. It is not speculative: ``currencies_for_amounts`` documents that "a
  slot whose amount is ``None`` (unmeasured) or ``0`` gets ``None``", and
  single-sided closes with a genuinely zero slot ARE in the corpus — traderjoe_v2
  2026-08-02 (``amount1_collected=0``) and uniswap_v3 2026-06-16. Those same
  receipts, replayed through today's stamping code, yield exactly one currency.
  PARTIAL is absent from the stored rows only because stamping post-dates the
  runs that produced the zero slots, not because the shape is unreachable.

**The gap the census exposed**: every stamped row in it is on arbitrum or base,
where WETH/USDC is *not* inverted. The stamped-AND-inverted combination — the main
case below — had never executed in production. Inverted pools had run
(aerodrome/optimism, uniswap_v3 and pancakeswap_v3 on bsc), but only unstamped.

**Fork proof closing that gap** — ``tests/reports/vib6104_lp_identity_e2e_anvil_report.md``.
Two full managed-Anvil lifecycles (open → separate teardown signal → close), with
both pools' slot order re-read from live mainnet with ``cast call`` afterwards:

* ``traderjoe_lp`` / avalanche / WAVAX-USDT-20 — ``getTokenX()=0xB31f…(WAVAX)``,
  ``getTokenY()=0x9702…(USDT)``. All three lanes bound **(WAVAX, USDT)**: the
  higher address first, i.e. NOT the address sort. The VIB-6383 shape did not
  recur.
* ``uniswap_lp`` / **optimism** / WETH-USDC-500 — ``token0()=0x0b2C…(USDC)``,
  ``token1()=0x4200…(WETH)``. All three lanes bound **(USDC, WETH)** — address
  order, against a "WETH/USDC" label. Its exact values are the
  ``stamped-fork-verified`` scenario below. (optimism, not the demo's default
  arbitrum: on arbitrum the pair is not inverted, so that run would have proved
  nothing.)

Both runs stamped BOTH currencies on all four LP rows, with ``coin_symbols=null``
— the address pair was the sole identity carrier.

**What the fork run did NOT settle**: neither close was single-sided (both
positions were in range at close, all four legs non-zero), so ``stamp=PARTIAL``
remains reasoned-but-unobserved. It is modelled here on the documented
``currencies_for_amounts`` contract plus the real zero-slot closes above, and an
out-of-range close would be needed to observe it directly.

Two fidelity bugs this fixture had, that only real data exposed
---------------------------------------------------------------
Both were silent, both produced green tests, and neither was findable by
reasoning about the fixture in isolation. They are recorded because the same
mistakes are the easiest ones to make when extending it.

1. **Declared money legs were plain dicts.** Both consumers type-check and fail
   *silently* — ``_pair_tokens_from_declared_legs`` returns ``(None, None)``,
   ``_declared_money_legs`` returns ``None``. The TraderJoe scenarios were
   getting the right binding from the currency stamp alone while never entering
   the declared-legs branch they appeared to cover.
   ``test_declared_money_legs_are_live_not_inert`` now asserts the branch is
   reached.
2. **``PositionEvent`` amounts were ``Decimal``; production assigns ``str``**
   (``position_events.py:1191/1267/1456``, field typed ``str``, rows persist as
   text). The ladder's first guard is
   ``if not (event.amount0 and event.amount1): return`` — and a measured zero is
   **falsy** as ``Decimal(0)`` but **truthy** as ``"0"``. So a single-sided close
   early-returned in the fixture while production runs the whole ladder. That
   one type error had this fixture pinning a ``position_events`` "defect" that
   does not exist: with the correct type the lane binds the golden pair. The
   deviation table below is the corrected one.

Deviations are pinned, not hidden — and never become the expectation
--------------------------------------------------------------------
Four ``(scenario, lane)`` pairs bind something other than the golden answer.
Three are ``DEFECT`` (VIB-6542, VIB-6543); one is ``REPRESENTATIONAL`` — a
single-sided close legitimately writes one money leg and leaves the other slot
empty, which is Empty ≠ Zero working correctly, not a transposition. Filing that
one as a bug would send someone to "fix" correct behaviour, so it carries no
ticket.

**Each DEFECT row is asserted twice, and the pair of assertions is the point.**
Pinning alone would fairly be read as turning three accounting defects into
passing expectations — green CI, invariant unmet. So:

* ``TestGoldenBinding`` asserts the **pinned** value, which detects the defect
  changing shape. A *different* wrong answer is a new bug, not the old one, and
  an xfail cannot tell those apart because any failure satisfies it.
* ``TestDefectsAgainstTheInvariant`` asserts the **golden** value under
  ``xfail(strict=True)``. The invariant VIB-6104 requires is therefore stated in
  the suite rather than quietly suspended — and the moment a lane is fixed the
  test XPASSes, which under strict mode **fails CI** and forces the pin and the
  xfail to be deleted together. A fix cannot land while this file still calls it
  broken.

Neither assertion alone is sufficient; that is why both are there.
"""

from __future__ import annotations

import ast
import warnings
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors._strategy_base.primitive_money_leg import (
    MoneyLegRole,
    PrimitiveMoneyLeg,
    PrimitiveMoneyLegs,
)
from almanak.framework.accounting.category_handlers.lp_handler import (
    _resolve_lp_tokens,
    _v3_realign_token_pair,
    _v4_realign_token_pair,
)
from almanak.framework.accounting.measured import MeasuredMoney
from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData
from almanak.framework.observability.ledger import _extract_tokens_and_amounts
from almanak.framework.observability.position_events import (
    IntentEventContext,
    PositionEvent,
    _realign_event_lp_pair_if_needed,
)

# ── Real mainnet identities ──────────────────────────────────────────────────
# Using REAL addresses is load-bearing: the whole defect class only manifests
# when a pool's canonical slot order disagrees with the human label order, and
# that disagreement is a fact about these specific contracts. A fabricated
# address pair would let the fixture pass while proving nothing.
#
# Verified against the static token registry (``skip_gateway=True``) on
# 2026-08-04; the INVERTED column is ``int(addr0, 16) > int(addr1, 16)``.
WETH_ETHEREUM = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"  # 18 dec
USDC_ETHEREUM = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"  # 6 dec  -> sorts BELOW WETH
WAVAX_AVALANCHE = "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7"  # 18 dec
USDT_AVALANCHE = "0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7"  # 6 dec  -> sorts BELOW WAVAX
AERO_BASE = "0x940181a94a35a4569e4529a3cdfb74e38fd98631"  # 18 dec
USDC_BASE = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"  # 6 dec  -> sorts BELOW AERO
WBNB_BSC = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"  # 18 dec
USDT_BSC = "0x55d398326f99059ff775485246999027b3197955"  # 18 dec -> sorts BELOW WBNB

# A real V3 pool address. ``lp_handler`` refuses to emit an event without one,
# and a fee-tier descriptor tail is explicitly rejected (VIB-4274).
POOL_ADDRESS = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
WALLET = "0xD739d9ecF38190F1EbFa537D955229Da8872d6f5"

# ── Venue slot-ordering families ─────────────────────────────────────────────
# Which order a venue puts its coins in is a property OF THE VENUE, and getting
# it wrong in either direction is a money bug. Sorting an address-sorted venue
# is a no-op; sorting a pool-defined one transposes the row.
ADDRESS_SORTED = "address_sorted"  # V3 family, Solidly, V4 PoolKey
POOL_DEFINED = "pool_defined"  # TraderJoe LB tokenX/tokenY — fixed at creation
POOL_INDEX = "pool_index"  # Curve coins(i) — pool-index order

NOT_ADDRESS_SORTED = frozenset({POOL_DEFINED, POOL_INDEX})


@dataclass(frozen=True)
class Scenario:
    """One inverted-pool case: a pool whose label order ≠ its slot order."""

    name: str
    connector: str
    chain: str
    venue_order: str
    opening: bool
    # The pool LABEL, in the order a user writes it in strategy config.
    label0: str
    label1: str
    # Raw amounts in the VENUE's canonical slot order (what the parser decodes).
    raw0: int
    raw1: int
    # What the parser stamped alongside those amounts. ``None`` = not stamped.
    currency0: str | None
    currency1: str | None
    # GROUND TRUTH: which label symbol owns slot 0 / slot 1. Declared from the
    # real addresses above and the venue's real ordering rule — deliberately not
    # computed by any function under test.
    expect0: str
    expect1: str
    why: str
    coin_symbols: tuple[str, ...] | None = None
    money_legs: PrimitiveMoneyLegs | None = None
    tags: tuple[str, ...] = field(default=())

    @property
    def id(self) -> str:
        return self.name


# TraderJoe declares money legs on every LP receipt; omitting them from a TJ
# scenario would test a shape that connector never emits.
#
# These MUST be the real typed value objects, not plain dicts. Both consumers
# type-check before reading — ``_pair_tokens_from_declared_legs`` does
# ``isinstance(legs, PrimitiveMoneyLegs)`` and returns ``(None, None)`` otherwise,
# and the ledger's ``_extract_from_declared_legs`` reads ``legs.legs`` /
# ``leg.role is MoneyLegRole.OUTPUT``. An earlier revision of this corpus passed
# a bare list of dicts with ``"role": "OUTPUT"``; it type-checked FALSE, so the
# declared-legs branch silently no-opped and the TraderJoe scenarios were getting
# the right answer from the currency stamp alone while appearing to exercise a
# branch they never entered. The real anvil run (see the fork proof named in the
# module docstring) emits ``{"legs": [...], "_type": "PrimitiveMoneyLegs"}`` with
# lowercase ``"role": "output"`` — which is what deserialises back into these
# objects, and is what caught the discrepancy.
def _tj_legs(role: MoneyLegRole, wavax: str, usdt: str) -> PrimitiveMoneyLegs:
    return PrimitiveMoneyLegs(
        legs=(
            PrimitiveMoneyLeg(role=role, token="WAVAX", amount=MeasuredMoney.measured(Decimal(wavax))),
            PrimitiveMoneyLeg(role=role, token="USDT", amount=MeasuredMoney.measured(Decimal(usdt))),
        )
    )


# Amounts from the real VIB-6383 close (batch 20260803-0430-noneth8).
_TJ_CLOSE_LEGS = _tj_legs(MoneyLegRole.OUTPUT, "0.228032393198215910", "1.334492")

# A SINGLE-SIDED close declares ONE leg, not two. The real 2026-08-02 LB close
# with ``amount1_collected=0`` wrote ``token_in=USDT, token_out=""`` — one leg
# projected into one slot. Reusing the dual-leg object on a scenario whose
# ``raw1=0`` / ``currency1=None`` would be internally inconsistent: the legs
# would assert a second non-zero payout the LP data says never happened, and the
# ledger would take the declared-legs branch instead of the partial-stamp path
# the scenario exists to exercise.
_TJ_SINGLE_SIDED_LEGS = PrimitiveMoneyLegs(
    legs=(
        PrimitiveMoneyLeg(
            role=MoneyLegRole.OUTPUT,
            token="WAVAX",
            amount=MeasuredMoney.measured(Decimal("0.228032393198215910")),
        ),
    )
)

CORPUS: tuple[Scenario, ...] = (
    # ── V3 family: uniswap_v3 ────────────────────────────────────────────────
    # Ethereum WETH/USDC is THE canonical inverted pool: USDC 0xA0b8… sorts below
    # WETH 0xC02a…, so the pool's token0() is USDC while every strategy config in
    # the repo writes "WETH/USDC/500". This exact pair produced VIB-5851's ~$1bn
    # phantom cost basis on a ~$4 position.
    Scenario(
        name="uniswap_v3/ethereum/WETH-USDC/open/stamped",
        connector="uniswap_v3",
        chain="ethereum",
        venue_order=ADDRESS_SORTED,
        opening=True,
        label0="WETH",
        label1="USDC",
        raw0=100_000_000,  # 100 USDC  (slot 0 == the LOWER address == USDC)
        raw1=29_000_000_000_000_000,  # 0.029 WETH
        currency0=USDC_ETHEREUM,
        currency1=WETH_ETHEREUM,
        expect0="USDC",
        expect1="WETH",
        why="slot0 is the lower address (USDC); the label says WETH first",
    ),
    Scenario(
        name="uniswap_v3/ethereum/WETH-USDC/open/unstamped",
        connector="uniswap_v3",
        chain="ethereum",
        venue_order=ADDRESS_SORTED,
        opening=True,
        label0="WETH",
        label1="USDC",
        raw0=100_000_000,
        raw1=29_000_000_000_000_000,
        currency0=None,
        currency1=None,
        expect0="USDC",
        expect1="WETH",
        why=(
            "the residual surface ledger.py instruments with "
            "_record_lp_leg_identity_missing: amounts are still address-ordered, "
            "so the correct binding is unchanged even with no stamp"
        ),
        tags=("residual-unstamped",),
    ),
    Scenario(
        name="uniswap_v3/ethereum/WETH-USDC/close/partial-stamp",
        connector="uniswap_v3",
        chain="ethereum",
        venue_order=ADDRESS_SORTED,
        opening=False,
        label0="WETH",
        label1="USDC",
        raw0=100_000_000,  # USDC came back
        raw1=0,  # single-sided close: price fully out of range
        currency0=USDC_ETHEREUM,
        currency1=None,  # currencies_for_amounts() -> None for a zero slot
        expect0="USDC",
        expect1="WETH",
        why=(
            "a single-sided close is routine, not pathological: when price "
            "leaves the range all liquidity sits in one token, so slot1 moves "
            "nothing and carries no identity"
        ),
        tags=("partial-stamp",),
    ),
    # ── V3 family: the other three forks, same rule, different chains ────────
    Scenario(
        name="pancakeswap_v3/bsc/WBNB-USDT/open/stamped",
        connector="pancakeswap_v3",
        chain="bsc",
        venue_order=ADDRESS_SORTED,
        opening=True,
        label0="WBNB",
        label1="USDT",
        raw0=100_000_000_000_000_000_000,  # 100 USDT (18 dec on BSC)
        raw1=150_000_000_000_000_000,  # 0.15 WBNB
        currency0=USDT_BSC,
        currency1=WBNB_BSC,
        expect0="USDT",
        expect1="WBNB",
        why="USDT 0x55d3… sorts below WBNB 0xbb4c…; the label says WBNB first",
    ),
    Scenario(
        name="sushiswap_v3/ethereum/WETH-USDC/close/stamped",
        connector="sushiswap_v3",
        chain="ethereum",
        venue_order=ADDRESS_SORTED,
        opening=False,
        label0="WETH",
        label1="USDC",
        raw0=100_000_000,
        raw1=29_000_000_000_000_000,
        currency0=USDC_ETHEREUM,
        currency1=WETH_ETHEREUM,
        expect0="USDC",
        expect1="WETH",
        why="same inverted pair on the close lane, which VIB-6383 showed is gated differently",
    ),
    Scenario(
        name="aerodrome/base/AERO-USDC/open/stamped",
        connector="aerodrome",
        chain="base",
        venue_order=ADDRESS_SORTED,
        opening=True,
        label0="AERO",
        label1="USDC",
        raw0=50_000_000,  # 50 USDC
        raw1=40_000_000_000_000_000_000,  # 40 AERO
        currency0=USDC_BASE,
        currency1=AERO_BASE,
        expect0="USDC",
        expect1="AERO",
        why="USDC 0x8335… sorts below AERO 0x9401…; Slipstream follows the V3 rule",
    ),
    # ── V4 PoolKey — address-sorted by construction ──────────────────────────
    Scenario(
        name="uniswap_v4/ethereum/WETH-USDC/open/stamped",
        connector="uniswap_v4",
        chain="ethereum",
        venue_order=ADDRESS_SORTED,
        opening=True,
        label0="WETH",
        label1="USDC",
        raw0=100_000_000,
        raw1=29_000_000_000_000_000,
        currency0=USDC_ETHEREUM,
        currency1=WETH_ETHEREUM,
        expect0="USDC",
        expect1="WETH",
        why="V4 PoolKey requires currency0 < currency1, so slot order IS address order",
    ),
    # ── CONTROL: TraderJoe Liquidity Book — NOT address-sorted ───────────────
    # getTokenX()/getTokenY() are fixed at pool creation. On WAVAX/USDT/20 the
    # pool's tokenX is WAVAX even though USDT 0x9702… sorts BELOW WAVAX 0xB31f…,
    # so an address sort here TRANSPOSES the row. That is VIB-6383: a $2.80 close
    # booked as $228,032,393,195.42.
    Scenario(
        name="traderjoe_v2/avalanche/WAVAX-USDT/close/stamped",
        connector="traderjoe_v2",
        chain="avalanche",
        venue_order=POOL_DEFINED,
        opening=False,
        label0="WAVAX",
        label1="USDT",
        raw0=228_032_393_198_215_910,  # WAVAX — tokenX, i.e. slot 0
        raw1=1_334_492,  # USDT — tokenY
        currency0=WAVAX_AVALANCHE,
        currency1=USDT_AVALANCHE,
        expect0="WAVAX",
        expect1="USDT",
        why=(
            "tokenX is WAVAX despite USDT sorting lower — an address sort would "
            "give (USDT, WAVAX) and scale each leg by the other's decimals"
        ),
        money_legs=_TJ_CLOSE_LEGS,
        tags=("anti-sort-control",),
    ),
    Scenario(
        name="traderjoe_v2/avalanche/WAVAX-USDT/close/partial-stamp",
        connector="traderjoe_v2",
        chain="avalanche",
        venue_order=POOL_DEFINED,
        opening=False,
        label0="WAVAX",
        label1="USDT",
        raw0=228_032_393_198_215_910,
        raw1=0,
        currency0=WAVAX_AVALANCHE,
        currency1=None,  # _leg_currency_pair -> _addr(1) is None with one transfer
        expect0="WAVAX",
        expect1="USDT",
        why=(
            "a single-sided LB withdraw emits ONE Transfer, so the parser stamps "
            "(addr, None) — a partial stamp on a venue where sorting is wrong"
        ),
        money_legs=_TJ_SINGLE_SIDED_LEGS,
        tags=("anti-sort-control", "partial-stamp"),
    ),
    # ── FORK-VERIFIED: uniswap_v3 on optimism ────────────────────────────────
    # These exact values came off a real managed-Anvil run against an optimism
    # fork (chain_id 10, block 155134404) — see the fork proof named in the module
    # docstring. Pool 0x1fb3cf6e…db7b, verified post-run with `cast call`:
    # token0() = 0x0b2C…(USDC), token1() = 0x4200…(WETH), while the strategy's
    # pool label is "WETH/USDC/500". This is the stamped-AND-inverted combination
    # that had never executed in production before that run.
    Scenario(
        name="uniswap_v3/optimism/WETH-USDC/open/stamped-fork-verified",
        connector="uniswap_v3",
        chain="optimism",
        venue_order=ADDRESS_SORTED,
        opening=True,
        label0="WETH",
        label1="USDC",
        raw0=5_641_504_442,  # USDC, 6 dp   — slot 0, the lower address
        raw1=2_762_101_173_378_363_386,  # WETH, 18 dp
        currency0="0x0b2c639c533813f4aa9d7837caf62653d097ff85",
        currency1="0x4200000000000000000000000000000000000006",
        expect0="USDC",
        expect1="WETH",
        why=(
            "observed on a real optimism fork: token0() is USDC although the "
            "pool label leads with WETH; the decimals corroborate the binding "
            "(5.64e9 at 6dp = 5641 USDC, 2.76e18 at 18dp = 2.76 WETH)"
        ),
        tags=("fork-verified",),
    ),
    # ── CONTROL: Curve — pool-index order, N-coin ────────────────────────────
    # The Base ``weth_cbeth`` pool is the discriminating choice: its registry
    # ``coin_addresses`` are [WETH 0x4200…, cbETH 0x2Ae3…], so coins(0) is WETH
    # even though cbETH sorts BELOW it. Most curated Curve pools happen to be
    # address-ordered by coincidence, and one of those cannot tell a correct
    # binding from a wrongly-applied sort (see
    # ``test_control_binding_is_not_an_address_sort``, which enforces exactly that).
    Scenario(
        name="curve/base/WETH-cbETH/open/n-coin",
        connector="curve",
        chain="base",
        venue_order=POOL_INDEX,
        opening=True,
        label0="WETH",
        label1="cbETH",
        raw0=1_000_000_000_000_000_000,  # 1 WETH   — coins(0)
        raw1=900_000_000_000_000_000,  # 0.9 cbETH — coins(1)
        currency0=None,
        currency1=None,
        expect0="WETH",
        expect1="cbETH",
        why=(
            "coins(i) is pool-index order: coins(0) is WETH although cbETH "
            "0x2Ae3… sorts below WETH 0x4200…, so an address sort transposes it"
        ),
        coin_symbols=("WETH", "cbETH"),
        tags=("anti-sort-control",),
    ),
)

# ── Connectors that declare LP intents but emit no typed LP evidence ─────────
# Verified 2026-08-04: none of these constructs ``LPOpenData`` / ``LPCloseData``
# anywhere under its package, and none references ``primitive_money_legs``. Their
# LP rows are therefore built from intent attributes, which are in label order on
# BOTH halves — so the amount/symbol pair cannot be transposed by the ladder this
# fixture guards. They are excluded from the scenario corpus for that reason, not
# because they were skipped.
#
# This is not a clean bill of health. Building rows from intent attributes means
# those rows carry REQUESTED rather than MEASURED amounts, which is a different
# defect class (and a real one) — out of scope here, and noted so the absence of
# a scenario is not read as coverage.
NO_TYPED_LP_EVIDENCE: dict[str, str] = {
    "meteora": "no LPOpenData/LPCloseData and no declared money legs (Solana)",
    "orca": "no LPOpenData/LPCloseData and no declared money legs (Solana)",
    "raydium": "no LPOpenData/LPCloseData and no declared money legs (Solana)",
    "fluid_dex_lp": "no LPOpenData/LPCloseData and no declared money legs (EVM)",
}

# ── Time-bounded gaps ────────────────────────────────────────────────────────
# A connector that DOES emit the guarded surface but has no scenario yet is a
# hole in the "mandatory per-LP-connector" requirement, not an exemption. An
# open-ended entry here would let CI stay green forever while the connector goes
# unguarded — which is the same shape as omitting it.
#
# So each entry carries an EXPIRY. Past that date this gate fails, forcing the
# choice the entry deferred: write the scenario, or make a reasoned case that the
# class cannot reach this connector. Mirrors the repo's own
# ``scripts/ci/demo-quarantine.yml`` pattern (ticket + `until:` date).
#
# Pendle emits typed LP data AND declares money legs — the exact surface guarded
# here — so it is a genuine gap. Its LP pair is PT/SY rather than two spot
# tokens, and "inverted" is not yet defined for it: PT and SY are not
# interchangeable coins whose slot order a venue picks, so the address-sort
# question may not even apply. Settling that is a modelling question, not a
# transcription one, and getting it wrong would bake a false expectation into a
# fixture whose whole value is that its expectations are trustworthy.
NEEDS_SCENARIO: dict[str, tuple[str, str]] = {
    "pendle": (
        "2026-09-04",
        "PT/SY pair — 'inverted pool' is not yet defined for a PT/SY venue; "
        "needs the identity model settled before a scenario can state a "
        "trustworthy expectation. Tracked under VIB-6104.",
    ),
}

# ── Lanes that currently bind something OTHER than the golden pair ──────────
# Keyed ``(scenario name, lane)`` -> ``(observed pair, kind, ticket, why)``.
#
# NOT every entry is a defect, and not every entry carries a ticket. ``DEFECT``
# rows are real bugs and each names its Linear issue; the ``REPRESENTATIONAL``
# row is correct behaviour that merely differs between lanes, so it has no
# ticket (field is ``""``) — filing it would send someone to "fix" code that is
# already right.
#
# Pinning the OBSERVED value — rather than excluding the scenario — keeps this
# from degrading into a list of cases we stopped checking: each lane is still
# asserted, against what it actually does, so movement in either direction turns
# the suite red.
#
# Pinning alone would leave the INVARIANT unstated, though, which is why every
# ``DEFECT`` row is ALSO asserted against the golden pair under a strict xfail in
# ``TestDefectsAgainstTheInvariant``. See that class for why both are needed.
# A pinned deviation is one of two very different things, and conflating them is
# how a fixture turns real findings into noise (or noise into false alarms).
DEFECT = "defect"  # the lane is WRONG here; fixing it should delete the entry
REPRESENTATIONAL = "representational"  # a legitimate difference, not a misreport

LANE_DEVIATIONS: dict[tuple[str, str], tuple[tuple[str, str], str, str, str]] = {
    (
        "uniswap_v3/ethereum/WETH-USDC/open/unstamped",
        "transaction_ledger",
    ): (
        ("WETH", "USDC"),
        DEFECT,
        "VIB-6542",
        "the ledger keeps LABEL order on an unstamped LP_OPEN while both other "
        "lanes address-sort. `_extract_from_lp_open` got the stamp-based fix "
        "(VIB-6053) but never an address-sort fallback, so on this residual "
        "shape it is the ONLY lane still transposed — and it then scales each "
        "leg by the other token's decimals, the $26.5bn shape VIB-5851 booked.",
    ),
    (
        "uniswap_v3/ethereum/WETH-USDC/close/partial-stamp",
        "transaction_ledger",
    ): (
        ("USDC", "USDC"),
        DEFECT,
        "VIB-6543",
        "slot1 has no stamp, so the per-leg fallback takes `intent.token1` — "
        "which slot0 already claimed from a direct observation — and the row "
        "names one token twice. slot1 moved no money, so no value is misstated; "
        "the row is still not a true pair.",
    ),
    (
        "uniswap_v3/ethereum/WETH-USDC/close/partial-stamp",
        "accounting_events",
    ): (
        ("USDC", "USDC"),
        DEFECT,
        "VIB-6543",
        "inherited from the ledger row (`_resolve_lp_tokens` seeds from "
        "token_in/token_out), then left unchanged by both realigners.",
    ),
    (
        "curve/base/WETH-cbETH/open/n-coin",
        "accounting_events",
    ): (
        ("WETH", "CBETH"),
        REPRESENTATIONAL,
        "",
        "NOT a defect: the BINDING is identical, only the symbol's CASE differs. "
        "`_resolve_lp_tokens` uppercases both columns "
        "(`(ledger_row.get('token_in') or '').upper()`), so the accounting lane "
        "emits CBETH while the ledger and position_events preserve the "
        "registry's `cbETH`. Invisible for the all-caps symbols in every other "
        "scenario, which is why it took a mixed-case token to surface it. "
        "Recorded because a reader reconciling the three surfaces by symbol "
        "string — rather than by identity — would see a spurious mismatch here, "
        "and because this fixture only saw it once `bind_accounting` started "
        "seeding from the production `_resolve_lp_tokens` instead of the labels.",
    ),
    (
        "traderjoe_v2/avalanche/WAVAX-USDT/close/partial-stamp",
        "transaction_ledger",
    ): (
        ("WAVAX", ""),
        REPRESENTATIONAL,
        "",
        "NOT a defect, so it carries no ticket. A single-sided close declares "
        "ONE output leg and `_extract_from_declared_legs` projects it into one "
        "slot, leaving token_out empty. That is Empty != Zero working "
        "correctly: there was no second payout to name, and a fabricated "
        "'USDT 0' would be the wrong answer. The real 2026-08-02 LB close wrote "
        "this shape — one token, one empty slot (its live leg happened to be "
        "USDT; this scenario's is WAVAX, which is the same shape, not the same "
        "row). Pinned so the difference from the other two lanes is recorded "
        "rather than mistaken for a transposition.",
    ),
}

# DEFECT rows, as pytest params carrying a strict xfail against the GOLDEN pair.
#
# This is the answer to the obvious objection that pinning an observed-wrong
# value turns a defect into a passing expectation. Two assertions run per DEFECT
# row and they measure different things:
#
#   * ``TestGoldenBinding`` asserts the PINNED value — it detects the defect
#     CHANGING SHAPE (a different wrong answer is a new bug, not the old one).
#   * this strict xfail asserts the CORRECT value — so the invariant VIB-6104
#     actually requires is stated in the suite, and the moment someone fixes the
#     lane it XPASSes and **fails CI**, forcing the pin's removal.
#
# Neither alone is sufficient: pinning alone never states the invariant, and
# xfail alone cannot tell "still broken the same way" from "broken differently".
_DEFECT_PARAMS = [
    pytest.param(
        name,
        lane,
        marks=pytest.mark.xfail(
            strict=True,
            reason=f"{ticket} as of 2026-08-05 — {lane} does not bind the golden pair",
        ),
        id=f"{name}|{lane}",
    )
    for (name, lane), (_observed, kind, ticket, _why) in sorted(LANE_DEVIATIONS.items())
    if kind == DEFECT
]


def expected_binding(sc: Scenario, lane: str) -> tuple[str, str]:
    """The pair ``lane`` must produce for ``sc`` — golden, or a pinned deviation."""
    deviation = LANE_DEVIATIONS.get((sc.name, lane))
    return deviation[0] if deviation else (sc.expect0, sc.expect1)


# ── Lane drivers ─────────────────────────────────────────────────────────────
# Each driver calls the lane at ITS OWN binding decision point, composed exactly
# as production composes it.


def _build_lp_data(sc: Scenario):
    """The typed LP evidence the connector's parser would emit for ``sc``."""
    common = {
        "currency0": sc.currency0,
        "currency1": sc.currency1,
        "coin_symbols": list(sc.coin_symbols) if sc.coin_symbols else None,
        "pool_address": POOL_ADDRESS,
    }
    if sc.opening:
        return LPOpenData(position_id=0, amount0=sc.raw0, amount1=sc.raw1, **common)
    return LPCloseData(amount0_collected=sc.raw0, amount1_collected=sc.raw1, **common)


def _build_context(sc: Scenario):
    lp_data = _build_lp_data(sc)
    intent_type = "LP_OPEN" if sc.opening else "LP_CLOSE"
    extracted: dict = {("lp_open_data" if sc.opening else "lp_close_data"): lp_data}
    if sc.money_legs is not None:
        extracted["primitive_money_legs"] = sc.money_legs

    intent = SimpleNamespace(
        token0=sc.label0,
        token1=sc.label1,
        protocol=sc.connector,
        intent_type=SimpleNamespace(value=intent_type),
        amount0=None,
        amount1=None,
        pool=f"{sc.label0}/{sc.label1}/500",
    )
    result = SimpleNamespace(
        extracted_data=extracted,
        primitive_money_legs=extracted.get("primitive_money_legs"),
    )
    return lp_data, extracted, intent_type, intent, result


def bind_ledger(sc: Scenario) -> tuple[str, str]:
    """``transaction_ledger`` — ``_extract_tokens_and_amounts`` dispatch."""
    _lp, _extracted, _it, intent, result = _build_context(sc)
    token_in, token_out, *_rest = _extract_tokens_and_amounts(intent, result, sc.chain)
    return (token_in, token_out)


def bind_accounting(sc: Scenario) -> tuple[str, str]:
    """``accounting_events`` — the V4-then-V3 realign pair, as ``handle_lp`` composes it.

    ``handle_lp`` seeds ``(token0, token1)`` from the LEDGER row's
    ``token_in``/``token_out`` (``_resolve_lp_tokens``), so this lane consumes the
    previous one. That coupling is exactly why cross-lane agreement is worth
    asserting: a divergence means accounting re-derived what the ledger already
    decided, and reached a different answer.
    """
    lp_data, extracted, intent_type, _intent, _result = _build_context(sc)
    token_in, token_out = bind_ledger(sc)

    # Seed exactly as ``handle_lp`` does — via the production ``_resolve_lp_tokens``,
    # which falls back to the position-key descriptor when a close leaves the
    # ledger's token columns empty.
    #
    # An earlier revision fell back to ``sc.label0``/``sc.label1`` instead. That
    # produced the same answer for every scenario here, but only by coincidence:
    # these position keys are BUILT from the labels, so the two agree by
    # construction. A scenario whose key tail differed from its labels — a stale
    # or bridged pool label, which is exactly the case `place_token_pair_by_
    # observed_identity` fails closed on — would have diverged from production
    # silently. Calling the real function removes the coincidence.
    position_key = f"lp:{sc.connector}:{sc.chain}:{WALLET.lower()}:{sc.label0.lower()}/{sc.label1.lower()}/500"
    token0, token1 = _resolve_lp_tokens({"token_in": token_in, "token_out": token_out}, position_key)

    token0, token1, v4_realigned = _v4_realign_token_pair(lp_data, sc.chain, token0, token1)
    return _v3_realign_token_pair(
        lp_data=lp_data,
        intent_type_str=intent_type,
        extracted=extracted,
        chain=sc.chain,
        token0=token0,
        token1=token1,
        v4_realigned=v4_realigned,
    )


def bind_position_events(sc: Scenario) -> tuple[str, str]:
    """``position_events`` — ``_realign_event_lp_pair_if_needed``."""
    _lp, extracted, _it, intent, result = _build_context(sc)
    event = PositionEvent(
        id="pe-golden",
        deployment_id="deployment:aaaabbbbcccc",
        cycle_id="cycle-1",
        execution_mode="paper",
        position_id="pos-1",
        position_type="LP",
        event_type="OPEN" if sc.opening else "CLOSE",
        timestamp="2026-08-04T00:00:00+00:00",
        protocol=sc.connector,
        chain=sc.chain,
        token0=sc.label0,
        token1=sc.label1,
        # Production assigns ``event.amount0 = str(lp_data.amount0)`` — the RAW
        # value, stringified (position_events.py:1191/1267/1456; the field is
        # typed ``str`` and real rows persist as text). The type is load-bearing
        # here, not cosmetic: the ladder's first guard is
        # ``if not (event.amount0 and event.amount1): return``, and a measured
        # zero is FALSY as ``Decimal(0)`` but TRUTHY as ``"0"``. An earlier
        # revision passed Decimals, so a single-sided close early-returned in the
        # fixture while production runs the full ladder — which pinned a
        # position_events "defect" that was an artifact of the fixture's own type.
        amount0=str(sc.raw0),
        amount1=str(sc.raw1),
    )
    if sc.coin_symbols:
        event.coin_symbols = list(sc.coin_symbols)
    ctx = IntentEventContext(
        intent=intent,
        result=result,
        extracted=extracted,
        deployment_id="deployment:aaaabbbbcccc",
        chain=sc.chain,
        ledger_entry_id="le-golden",
    )
    _realign_event_lp_pair_if_needed(event, ctx, opening=sc.opening)
    return (event.token0, event.token1)


LANES = {
    "transaction_ledger": bind_ledger,
    "accounting_events": bind_accounting,
    "position_events": bind_position_events,
}

_DEVIATING_SCENARIOS = frozenset(name for name, _lane in LANE_DEVIATIONS)
# Scenarios on which all three lanes are expected to be correct AND to agree.
_AGREEING = tuple(sc for sc in CORPUS if sc.name not in _DEVIATING_SCENARIOS)
_CONTROLS = tuple(sc for sc in CORPUS if "anti-sort-control" in sc.tags)


@pytest.fixture(autouse=True)
def _silence_symbol_deprecation():
    """Silence ONLY the symbol-keyed resolution deprecation.

    The corpus keys tokens by symbol because the pool LABEL is what a strategy
    writes, and that is the input whose disagreement with address order this
    fixture exists to test. The registry deprecates symbol lookups, so the noise
    is expected here.

    Scoped to that one category rather than ``simplefilter("ignore")``: a blanket
    filter would also swallow a warning from the code under test — including the
    ``VIB-6053``/``VIB-6383`` "could not resolve / keeping label order" warnings
    that are this module's own early-warning signal.
    """
    from almanak.framework.data.tokens.deprecation import SymbolTokenResolutionWarning

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=SymbolTokenResolutionWarning)
        yield


class TestGoldenBinding:
    """Every lane must bind each amount slot to the symbol that owns it.

    Runs over the WHOLE corpus — including scenarios with known defects, which
    are asserted against their pinned observed value. Excluding them instead
    would leave the residual unstamped path (the only one where the address sort
    is still load-bearing) unasserted by anything.
    """

    @pytest.mark.parametrize("sc", CORPUS, ids=lambda s: s.id)
    @pytest.mark.parametrize("lane", sorted(LANES))
    def test_lane_binds_slots_to_their_true_owners(self, lane: str, sc: Scenario) -> None:
        got = LANES[lane](sc)
        expected = expected_binding(sc, lane)
        deviation = LANE_DEVIATIONS.get((sc.name, lane))
        if deviation is None:
            assert got == expected, (
                f"{lane} bound {got} on {sc.name}; the pool's slot order says "
                f"{expected}.\nWhy: {sc.why}\n"
                f"A transposed pair also scales each leg by the OTHER token's "
                f"decimals — that is how a $2.47 position books "
                f"$322,107,799,472.28."
            )
        else:
            _observed, kind, _ticket, why = deviation
            assert got == expected, (
                f"{lane} on {sc.name} was pinned at {expected} ({kind}) but "
                f"returned {got}.\nPinned because: {why}\n"
                f"If {got} == {(sc.expect0, sc.expect1)} this deviation is GONE — "
                f"delete its LANE_DEVIATIONS entry. Otherwise it changed shape "
                f"and needs re-analysis."
            )


class TestDefectsAgainstTheInvariant:
    """State the invariant VIB-6104 requires, for the lanes that violate it.

    Without this class the suite would only ever assert the *observed* value on a
    defective lane, and a reader could fairly say the fixture had turned three
    accounting defects into passing expectations — green CI, invariant unmet.

    Each test below asserts the CORRECT binding and is marked
    ``xfail(strict=True)``. That gives the property pinning cannot:

    * today it xfails, so CI is green without the suite ever *claiming* a
      transposed pair is right;
    * the moment someone fixes the lane it XPASSes, and strict mode turns that
      into a **failure** — which forces the pin and this xfail to be deleted
      together. A fix cannot land while the fixture still calls it broken.

    The paired ``TestGoldenBinding`` assertion on the same rows stays, because
    xfail alone cannot distinguish "still broken the same way" from "broken in a
    new way" — any failure satisfies it.
    """

    @pytest.mark.parametrize(("name", "lane"), _DEFECT_PARAMS)
    def test_defective_lane_should_bind_the_golden_pair(self, name: str, lane: str) -> None:
        sc = next(s for s in CORPUS if s.name == name)
        assert LANES[lane](sc) == (sc.expect0, sc.expect1)


class TestLaneAgreement:
    """The three write surfaces must tell one story about one transaction."""

    @pytest.mark.parametrize("sc", _AGREEING, ids=lambda s: s.id)
    def test_all_three_lanes_bind_identically(self, sc: Scenario) -> None:
        bindings = {lane: fn(sc) for lane, fn in LANES.items()}
        assert len(set(bindings.values())) == 1, (
            f"lane divergence on {sc.name}: {bindings}.\n"
            "A reader is supposed to reconcile transaction_ledger, "
            "accounting_events and position_events; when they disagree at least "
            "one is misreporting and nothing else in CI notices."
        )

    def test_scenarios_with_divergent_lanes_are_exactly_the_pinned_ones(self) -> None:
        """Red if a divergence was fixed (delete the entry) or a new one shipped."""
        observed = {sc.name for sc in CORPUS if len({fn(sc) for fn in LANES.values()}) > 1}
        assert observed == _DEVIATING_SCENARIOS, (
            f"pinned divergent scenarios {sorted(_DEVIATING_SCENARIOS)}, observed "
            f"{sorted(observed)}. See LANE_DEVIATIONS for each ticket."
        )

    def test_lane_deviations_are_current(self) -> None:
        """Table hygiene: every entry names a real scenario+lane, a known kind,
        and a pair that differs from golden.

        Scope, stated precisely because the earlier docstring overclaimed: this
        is a **static** consistency check on the table. It does not detect that a
        lane has been fixed — nothing here executes a lane.

        The live guards are elsewhere, and between them they close the hole:
        ``TestGoldenBinding`` fails if a pinned lane returns anything other than
        its pinned value (so a fix, or a differently-shaped regression, is
        caught), and ``TestDefectsAgainstTheInvariant`` XPASSes under
        ``strict=True`` the moment a DEFECT lane starts binding correctly.
        """
        by_name = {sc.name: sc for sc in CORPUS}
        for (name, lane), (observed, kind, ticket, _why) in sorted(LANE_DEVIATIONS.items()):
            assert name in by_name, f"LANE_DEVIATIONS names unknown scenario {name!r}"
            assert lane in LANES, f"LANE_DEVIATIONS names unknown lane {lane!r}"
            sc = by_name[name]
            assert kind in (DEFECT, REPRESENTATIONAL), f"LANE_DEVIATIONS[{name!r}, {lane!r}] has unknown kind {kind!r}"
            # A DEFECT with no ticket is an untracked bug wearing a pin, which is
            # how a "temporary" exemption becomes permanent. A REPRESENTATIONAL
            # row WITH one points a reader at a bug that does not exist.
            if kind == DEFECT:
                assert ticket.startswith("VIB-"), (
                    f"LANE_DEVIATIONS[{name!r}, {lane!r}] is a DEFECT but names no "
                    f"tracking ticket (got {ticket!r}); it would go untracked."
                )
            else:
                assert not ticket, (
                    f"LANE_DEVIATIONS[{name!r}, {lane!r}] is {kind} — correct "
                    f"behaviour — but cites ticket {ticket!r}, which would send "
                    f"someone to 'fix' code that is already right."
                )
            assert observed != (sc.expect0, sc.expect1), (
                f"LANE_DEVIATIONS[{name!r}, {lane!r}] pins {observed}, which EQUALS "
                f"the golden binding — that is not a deviation, so the entry "
                f"should be deleted rather than exempting the lane."
            )


class TestNonAddressSortedControls:
    """VIB-6383's assertion: a venue that is not address-sorted must not be sorted."""

    @pytest.mark.parametrize("sc", _CONTROLS, ids=lambda s: s.id)
    def test_control_binding_is_not_an_address_sort(self, sc: Scenario) -> None:
        from almanak.framework.data.tokens.pair_order import realign_token_pair_by_address

        assert sc.venue_order in NOT_ADDRESS_SORTED, "control must be a non-sorted venue"
        sorted_pair = realign_token_pair_by_address(sc.label0, sc.label1, sc.chain)
        assert sorted_pair != (sc.expect0, sc.expect1), (
            f"{sc.name} is not a valid control: the address sort {sorted_pair} "
            f"already equals the correct binding, so this scenario cannot "
            f"discriminate a wrongly-applied sort from a correct binding."
        )
        for lane, fn in LANES.items():
            assert fn(sc) != sorted_pair, (
                f"{lane} address-sorted a {sc.venue_order} venue on {sc.name} -> {sorted_pair}. {sc.why}"
            )


class TestProductionCallSites:
    """Guard the wiring this fixture deliberately does not execute.

    KNOWN LIMITATION, stated plainly (VIB-6544). The lane drivers call each
    lane's binding decision point directly: ``bind_accounting`` reproduces the
    ``_v4_realign_token_pair`` -> ``_v3_realign_token_pair`` composition rather
    than driving ``handle_lp``, and ``bind_position_events`` invokes
    ``_realign_event_lp_pair_if_needed`` rather than the event-application path.
    That was a deliberate scope choice — driving ``handle_lp`` end to end drags
    in pool-address resolution, prices and a basis store, none of which bear on
    *binding* — but it has a real cost: **deleting or reordering those calls at
    the production call site would leave every binding test green.**

    These tests close that specific hole cheaply, by asserting the call sites
    still exist and still run in the order the fixture assumes. They are a
    structural guard, not a substitute for end-to-end coverage; VIB-6544 tracks
    driving ``handle_lp`` properly.
    """

    @staticmethod
    def _call_lines(path: Path, func_name: str) -> list[int]:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        return sorted(
            node.lineno
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == func_name
        )

    @staticmethod
    def _call_keyword_literals(path: Path, func_name: str, keyword: str) -> set[object]:
        """Literal values passed as ``keyword`` at every call to ``func_name``.

        Read off the call nodes, not the file text. A substring search for
        ``"opening=True"`` is satisfied by a comment, a docstring, or an
        unrelated call — so it would report both lifecycle roles as covered
        while the realigner received neither. That is the same "a check that
        cannot discriminate" failure this whole fixture exists to catch, so it
        has no business living inside it.
        """
        tree = ast.parse(path.read_text(encoding="utf-8"))
        values: set[object] = set()
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == func_name):
                continue
            for kw in node.keywords:
                if kw.arg == keyword and isinstance(kw.value, ast.Constant):
                    values.add(kw.value.value)
        return values

    @property
    def _framework(self) -> Path:
        return Path(__file__).resolve().parents[4] / "almanak" / "framework"

    def test_accounting_still_runs_v4_realign_before_v3(self) -> None:
        """``bind_accounting`` composes V4-then-V3; the handler must still do so.

        Order is the property. ``_v3_realign_token_pair`` takes ``v4_realigned``
        and short-circuits on it, so running V3 first — or dropping V4 — changes
        which pair wins on a stamped pool without any binding test noticing.
        """
        handler = self._framework / "accounting" / "category_handlers" / "lp_handler.py"
        v4 = self._call_lines(handler, "_v4_realign_token_pair")
        v3 = self._call_lines(handler, "_v3_realign_token_pair")
        assert v4, "lp_handler no longer calls _v4_realign_token_pair — bind_accounting is stale"
        assert v3, "lp_handler no longer calls _v3_realign_token_pair — bind_accounting is stale"
        assert min(v4) < min(v3), (
            f"lp_handler calls _v3_realign_token_pair (line {min(v3)}) before "
            f"_v4_realign_token_pair (line {min(v4)}). bind_accounting composes them "
            f"the other way round, so this fixture no longer models the handler."
        )

    def test_position_events_still_realigns_on_both_lifecycle_lanes(self) -> None:
        """OPEN and CLOSE must both call the realigner.

        VIB-6383 was a lane asymmetry — a gate that fired on one lane and not the
        other — so "it is called somewhere" is not enough. ``opening`` selects
        INPUT vs OUTPUT legs, and reading the wrong role silently adopts the
        wrong pair.
        """
        module = self._framework / "observability" / "position_events.py"
        calls = self._call_lines(module, "_realign_event_lp_pair_if_needed")
        assert len(calls) >= 2, (
            f"expected the realigner on both the OPEN and CLOSE paths, found {len(calls)} call site(s) at {calls}"
        )
        roles = self._call_keyword_literals(module, "_realign_event_lp_pair_if_needed", "opening")
        assert roles == {True, False}, (
            f"the realigner must be called with `opening=True` on the OPEN path "
            f"and `opening=False` on the CLOSE path; the call sites pass "
            f"{sorted(roles, key=str) or 'nothing'}. A missing role means one "
            f"lane reads the wrong money legs (INPUT vs OUTPUT) and silently "
            f"adopts the wrong pair — and `opening` must stay explicit, since "
            f"re-deriving it from event_type mis-reads INCREASE/DECREASE."
        )


class TestConnectorCoverage:
    """Connector #13 cannot be added without confronting this file."""

    @staticmethod
    def _lp_connectors_from_manifests() -> set[str]:
        """Connectors declaring an ``LP_*`` intent, read from the manifest itself.

        Parsed with ``ast`` rather than imported, matching
        ``scripts/ci/check_connector_registry.py`` — importing every connector
        drags in each one's SDK just to read a tuple of enum names.
        """
        root = Path(__file__).resolve().parents[4] / "almanak" / "connectors"
        assert root.is_dir(), f"connectors dir not found at {root}"
        found: set[str] = set()
        for manifest in sorted(root.glob("*/connector.py")):
            tree = ast.parse(manifest.read_text(encoding="utf-8"))
            # Scan the WHOLE module for `IntentType.LP_*`, not just the immediate
            # `strategy_intents=` expression.
            #
            # The narrow form missed a legal declaration style. A connector that
            # writes
            #
            #     LP_INTENTS = (IntentType.LP_OPEN, IntentType.LP_CLOSE)
            #     CONNECTOR = ConnectorManifest(strategy_intents=LP_INTENTS)
            #
            # produces an `ast.Name` at the keyword, so walking only that
            # expression finds no `LP_` attribute and the connector lands with no
            # scenario and no exclusion — silently unguarded. That is precisely
            # the "connector #13 slips through" failure this gate exists to stop,
            # so the gate was defeated by the same class of omission it guards.
            #
            # A module-wide scan cannot be evaded by indirection, at the cost of
            # over-matching a file that merely NAMES an LP intent (e.g. only in
            # `intent_overrides`). That trade is deliberate: over-matching demands
            # a scenario or a reasoned exclusion, while under-matching ships an
            # unguarded connector. Pick the loud failure.
            for sub in ast.walk(tree):
                if isinstance(sub, ast.Attribute) and sub.attr.startswith("LP_"):
                    found.add(manifest.parent.name)
                    break
        return found

    def test_every_lp_connector_is_covered_or_explicitly_excluded(self) -> None:
        declared = self._lp_connectors_from_manifests()
        assert declared, "found no LP-declaring connectors — the manifest scan is broken"

        covered = {sc.connector for sc in CORPUS}
        accounted = covered | set(NO_TYPED_LP_EVIDENCE) | set(NEEDS_SCENARIO)
        missing = declared - accounted
        assert not missing, (
            f"LP connectors with no inverted-pool scenario and no recorded "
            f"reason: {sorted(missing)}.\nVIB-6104 §Enforcement requires a "
            f"per-LP-connector inverted-pool fixture. Add a Scenario to CORPUS, "
            f"or an entry to NO_TYPED_LP_EVIDENCE / NEEDS_SCENARIO stating why "
            f"the class cannot reach it."
        )

    def test_time_bounded_gaps_have_not_expired(self) -> None:
        """A deferred scenario is a deadline, not an exemption.

        Without this, ``NEEDS_SCENARIO`` would satisfy the very
        "mandatory per-LP-connector" requirement it is recording a failure to
        meet — CI green forever while the connector stays unguarded.
        """
        from datetime import date

        today = date.today()
        expired = [
            f"{connector} (expired {until}): {why}"
            for connector, (until, why) in sorted(NEEDS_SCENARIO.items())
            if today > date.fromisoformat(until)
        ]
        assert not expired, (
            "Time-bounded coverage gaps are past their expiry:\n  "
            + "\n  ".join(expired)
            + "\n\nResolve by adding a Scenario to CORPUS, or by moving the "
            "connector to NO_TYPED_LP_EVIDENCE with evidence that the class "
            "cannot reach it. Extending the date is a decision, not a default — "
            "record why."
        )

    def test_exclusion_lists_do_not_name_unknown_connectors(self) -> None:
        """A stale exclusion silently shrinks the required coverage set."""
        declared = self._lp_connectors_from_manifests()
        stale = (set(NO_TYPED_LP_EVIDENCE) | set(NEEDS_SCENARIO)) - declared
        assert not stale, f"exclusion entries for non-LP connectors: {sorted(stale)}"

    def test_no_connector_is_both_covered_and_excluded(self) -> None:
        overlap = {sc.connector for sc in CORPUS} & (set(NO_TYPED_LP_EVIDENCE) | set(NEEDS_SCENARIO))
        assert not overlap, f"connectors both covered and excluded: {sorted(overlap)}"


class TestCorpusIsDiscriminating:
    """A scenario that isn't actually inverted proves nothing."""

    @pytest.mark.parametrize("sc", [s for s in CORPUS if s.money_legs is not None], ids=lambda s: s.id)
    def test_declared_money_legs_are_live_not_inert(self, sc: Scenario) -> None:
        """A scenario's declared legs must actually reach the declared-legs branch.

        Both consumers type-check before reading, and BOTH fail silently: the
        ledger's ``_declared_money_legs`` returns ``None`` and
        ``_pair_tokens_from_declared_legs`` returns ``(None, None)``. A corpus
        that hands them the wrong type therefore still produces correct-looking
        bindings — from a *different* branch — while appearing to cover this one.
        That is exactly what happened here: an earlier revision passed plain
        dicts and the TraderJoe scenarios silently never entered the branch.

        Asserting the branch is entered is the only thing that distinguishes
        "covered" from "looks covered".
        """
        from almanak.framework.observability.ledger import _declared_money_legs
        from almanak.framework.observability.position_events import (
            _pair_tokens_from_declared_legs,
        )

        _lp, extracted, _it, _intent, result = _build_context(sc)

        assert _declared_money_legs(result) is not None, (
            f"{sc.name}: the ledger does not recognise these declared legs, so "
            f"_extract_from_declared_legs never runs. Legs must be a real "
            f"PrimitiveMoneyLegs, not a list/dict."
        )
        pair = _pair_tokens_from_declared_legs(extracted, opening=sc.opening)
        assert pair != (None, None), (
            f"{sc.name}: position_events read (None, None) from these declared "
            f"legs — the branch is INERT and this scenario is not testing it."
        )

    @pytest.mark.parametrize("sc", CORPUS, ids=lambda s: s.id)
    def test_the_two_candidate_orders_actually_differ(self, sc: Scenario) -> None:
        """Guard against a scenario quietly becoming a no-op.

        Every lane is choosing between exactly two candidate orders: the pool
        LABEL order, and what an ADDRESS SORT of those labels produces. When
        those two coincide the pool is not inverted, every lane passes without
        deciding anything, and the scenario cannot detect a transposition in
        either direction.

        Note this is NOT ``expect != label order``. For a non-address-sorted
        venue the correct binding IS label order — TraderJoe's WAVAX/USDT pool
        has tokenX == WAVAX == label0 — and its inversion is against the address
        sort, which is precisely the wrong answer the venue invites.
        """
        from almanak.framework.data.tokens.pair_order import realign_token_pair_by_address

        labels = (sc.label0, sc.label1)
        sorted_pair = realign_token_pair_by_address(sc.label0, sc.label1, sc.chain)
        assert sorted_pair != labels, (
            f"{sc.name} is not an inverted pool: an address sort of "
            f"{labels} on {sc.chain} returns it unchanged, so this scenario "
            f"cannot discriminate a correct binding from a sorted one."
        )

    @pytest.mark.parametrize("sc", CORPUS, ids=lambda s: s.id)
    def test_expectation_follows_the_declared_venue_rule(self, sc: Scenario) -> None:
        """The golden pair must be what the venue's OWN ordering rule dictates.

        This is what stops a scenario's expectation from being quietly tuned to
        whatever the code currently returns: the expectation is derivable from
        the venue family plus real addresses, and nothing else.
        """
        from almanak.framework.data.tokens.pair_order import realign_token_pair_by_address

        expected = (sc.expect0, sc.expect1)
        if sc.venue_order == ADDRESS_SORTED:
            assert expected == realign_token_pair_by_address(sc.label0, sc.label1, sc.chain), (
                "an address-sorted venue's slot order IS the address sort"
            )
        else:
            assert expected == (sc.label0, sc.label1), (
                f"a {sc.venue_order} venue's slot order is the pool's own, which this corpus states as the label order"
            )


class TestFixtureLiveness:
    """Prove this fixture fails when the defect is put back.

    A golden fixture nobody has seen go red is indistinguishable from one that
    cannot. Each control below reintroduces a specific historical defect and
    asserts the corpus detects it.
    """

    def test_removing_the_address_sort_reintroduces_vib5851(self, monkeypatch) -> None:
        """VIB-5851 — without the sort, unstamped V3 pairs keep label order.

        Neutralising ``realign_token_pair_by_address`` must make at least one
        corpus scenario stop matching its golden binding. If nothing moves, the
        corpus contains no scenario that depends on the sort.

        Measured over the WHOLE corpus, not just the agreeing subset. With
        identity stamped, the sort is no longer load-bearing on any agreeing
        scenario — positional placement decides those. The scenario that still
        needs it is the residual UNSTAMPED one, which is also a known-divergent
        row. That is Move C's thesis showing up as a test observation: once
        identity travels with the amount, the sort is vestigial everywhere it is
        still correct, and load-bearing only where identity is missing.
        """
        monkeypatch.setattr(
            "almanak.framework.data.tokens.pair_order.realign_token_pair_by_address",
            lambda token0, token1, chain: (token0, token1),
        )
        broken = [
            (sc.name, lane) for sc in CORPUS for lane, fn in LANES.items() if fn(sc) != expected_binding(sc, lane)
        ]
        assert broken, (
            "neutralising the address sort broke NOTHING — no scenario in the "
            "corpus actually exercises it, so this fixture could not have "
            "caught VIB-5851."
        )
        # State the REACH precisely, so this is not read as "all three lanes
        # detect it". `transaction_ledger` never calls either patched helper —
        # that is the VIB-6542 defect, not a gap in the control — so the sort is
        # reachable only from `accounting_events` and the `position_events`
        # fallback. Asserting a lane the mutation cannot reach would be a control
        # that passes for the wrong reason.
        reached = {lane for _name, lane in broken}
        assert reached <= {"accounting_events", "position_events"}, (
            f"the address sort was detected in {reached - {'accounting_events', 'position_events'}}, "
            f"which does not call it — the mutation is being detected for the wrong reason."
        )
        assert "accounting_events" in reached, (
            "accounting_events is the lane that reaches the address sort directly; "
            "if neutralising it no longer changes that lane, this control is inert."
        )

    def test_forcing_the_sort_onto_lb_reintroduces_vib6383(self, monkeypatch) -> None:
        """VIB-6383 — make positional placement fail, so the LB pair falls through
        to the address sort, and assert the controls catch it.

        ``place_token_pair_by_observed_identity`` returning ``None`` is its real
        fail-closed contract, so this mutation is a shape production can reach —
        not an invented one.

        Precisely which control this trips is worth stating, because the obvious
        reading is wrong. When BOTH currencies are stamped, placement failing
        does NOT reach the address sort: ``_v3_realign_token_pair`` has an
        explicit ``if currency0 and currency1: return token0, token1`` arm that
        keeps label order by design (the VIB-6383 fix). The sort is reachable
        only on a PARTIAL stamp, where that arm does not fire — which is why the
        single-sided LB scenario, not the two-sided one, is what goes red here.
        """
        monkeypatch.setattr(
            "almanak.framework.data.tokens.pair_order.place_token_pair_by_observed_identity",
            lambda *args, **kwargs: None,
        )
        pool_defined = [sc for sc in _CONTROLS if sc.venue_order == POOL_DEFINED]
        assert pool_defined, "no pool-defined control in the corpus"

        transposed = [
            (sc.name, lane) for sc in pool_defined for lane, fn in LANES.items() if fn(sc) != expected_binding(sc, lane)
        ]
        assert transposed, (
            "forcing positional placement to fail did NOT transpose any "
            "Liquidity Book binding — the anti-sort control is not wired to the "
            "code path VIB-6383 travelled."
        )

    def test_a_transposed_expectation_fails(self) -> None:
        """The assertions are sensitive to the thing they claim to measure.

        Swapping a scenario's golden pair must make it fail. Without this, a
        binding function that returned a constant could still satisfy the suite.
        """
        sc = next(s for s in _AGREEING if s.venue_order == ADDRESS_SORTED)
        flipped = (sc.expect1, sc.expect0)
        for lane, fn in LANES.items():
            assert fn(sc) != flipped, f"{lane} matched a deliberately transposed expectation"
