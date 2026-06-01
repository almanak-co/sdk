"""VIB-4275 — LP_CLOSE cross-contamination for co-pool CL positions (FIXED).

Confirmed on production deployment ``4d0fd01e-9ea4-4186-88d1-b6d196cbf888``
(arbitrum, WBTC/USDC 0.3%): two concurrent Uniswap-V3 NFTs in the SAME pool
(NFT 5503727 narrow ``[66360, 66540]`` + NFT 5503728 wide ``[65940, 66960]``)
closed in a teardown. Both ``LP_CLOSE`` accounting rows carried the WIDE leg's
tick range and an identical ``hodl_value_usd`` — so the narrow leg's
``realized_pnl_usd`` was sign-flipped (reported ``-0.205`` vs a true ``+0.0018``).

Root cause: ``position_key`` is POOL-LEVEL and identical for every position in a
pool, and the close-side resolver
(``AccountingProcessor._lookup_prior_lp_open``) returned the MOST-RECENT
``LP_OPEN`` by timestamp because it had no per-position discriminator to choose
between co-pool legs.

The fix (VIB-4275):

* The LP_OPEN accounting payload now carries ``position_id`` (the minted NFT
  token id) — ``lp_handler._resolve_lp_open_discriminator``.
* The LP_CLOSE / LP_COLLECT_FEES path threads the closing leg's discriminator
  (stamped onto ``LPCloseData.position_id`` from the close intent at
  ledger-build time) into ``_lookup_prior_lp_open(position_key, discriminator)``.
* The resolver filters same-``position_key`` candidate opens by the
  discriminator and returns the UNIQUE match. Zero or >1 match → ``None``. It
  **never** falls back to "latest open" — that is the bug being removed.

This suite drives the REAL production code paths
(``AccountingProcessor._lookup_prior_lp_open``, ``handle_lp``,
``_resolve_lp_close_discriminator``) and encodes the per-leg attribution
invariant. After the fix these assertions are GREEN.

Run:
    uv run pytest tests/unit/framework/accounting/test_lp_close_copool_attribution_vib4275.py -v
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.accounting.category_handlers.lp_handler import (
    _resolve_lp_close_discriminator,
    handle_lp,
)
from almanak.framework.accounting.processor import AccountingProcessor
from almanak.framework.execution.extracted_data import LPCloseData
from almanak.framework.observability.ledger import serialize_extracted_data

DEPLOYMENT_ID = "vib4275-deploy"
CHAIN = "arbitrum"
PROTOCOL = "uniswap_v3"
WALLET = "0xb5a91428000000000000000000000000000000aa"
POOL = "0x6985cb98ce393fce8d6272127f39013f61e36166"  # WBTC/USDC 0.3%

# Pool-level key — identical for BOTH legs (the bug surface).
POSITION_KEY = f"lp:{PROTOCOL}:{CHAIN}:{WALLET}:{POOL}"

# Mirror the production legs. Distinct ticks AND materially distinct open
# compositions so V_hodl/IL must differ between legs.
NARROW = {
    "token_id": 5503727,
    "tick_lower": 66360,
    "tick_upper": 66540,
    "amount0": Decimal("0.00000967"),  # WBTC (8dp human)
    "amount1": Decimal("0.999056"),  # USDC
}
WIDE = {
    "token_id": 5503728,
    "tick_lower": 65940,
    "tick_upper": 66960,
    "amount0": Decimal("0.00001234"),  # WBTC
    "amount1": Decimal("0.999538"),  # USDC
}

# Close-block prices (token symbol → USD). WBTC ~ $96,000.
PRICE_WBTC = Decimal("96000")
PRICE_USDC = Decimal("1")


def _v_hodl(leg: dict[str, Any]) -> Decimal:
    """The HODL anchor the handler SHOULD compute for ``leg``."""
    return leg["amount0"] * PRICE_WBTC + leg["amount1"] * PRICE_USDC


def _open_payload(leg: dict[str, Any], *, with_discriminator: bool = True) -> dict[str, Any]:
    """An ``accounting_events.payload_json`` dict as written for an LP_OPEN.

    Matches the keys ``lp_handler`` reads off the prior-open payload, plus the
    VIB-4275 ``position_id`` discriminator the resolver matches against.
    ``with_discriminator=False`` models a legacy (pre-VIB-4275) open row.
    """
    payload = {
        "event_type": "LP_OPEN",
        "protocol": PROTOCOL,
        "position_key": POSITION_KEY,
        "pool_address": POOL,
        "token0": "WBTC",
        "token1": "USDC",
        "amount0": str(leg["amount0"]),
        "amount1": str(leg["amount1"]),
        "tick_lower": leg["tick_lower"],
        "tick_upper": leg["tick_upper"],
        "cost_basis_usd": str(_v_hodl(leg)),  # opened in-range ⇒ basis ≈ V_hodl
        "position_hash": None,
    }
    if with_discriminator:
        payload["position_id"] = str(leg["token_id"])
    return payload


class _StubStore:
    """Minimal state-manager surface for ``_lookup_prior_lp_open``.

    Returns the LP_OPEN rows in ascending-timestamp order — exactly what
    ``SqliteBackend.get_accounting_events_sync`` returns (ORDER BY timestamp
    ASC, filtered only by deployment_id + position_key).
    """

    def __init__(self, open_rows: list[dict[str, Any]]) -> None:
        self._rows = open_rows

    def get_accounting_events_sync(
        self, deployment_id: str, position_key: str | None = None
    ) -> list[dict[str, Any]]:
        if deployment_id != DEPLOYMENT_ID:
            return []
        return [
            r
            for r in self._rows
            if position_key is None or r.get("position_key") == position_key
        ]


def _open_event_row(leg: dict[str, Any], *, with_discriminator: bool = True) -> dict[str, Any]:
    return {
        "event_type": "LP_OPEN",
        "position_key": POSITION_KEY,
        "payload_json": json.dumps(_open_payload(leg, with_discriminator=with_discriminator)),
    }


def _close_event_row() -> dict[str, Any]:
    """A minimal LP_CLOSE event row for the same pool key. The resolver counts
    these to derive ACTIVE (still-open) positions for the no-discriminator path,
    so an OPEN->CLOSE->OPEN lifecycle has exactly one active open, not two."""
    return {
        "event_type": "LP_CLOSE",
        "position_key": POSITION_KEY,
        "payload_json": json.dumps({"event_type": "LP_CLOSE", "position_key": POSITION_KEY}),
    }


def _processor(open_rows: list[dict[str, Any]]) -> AccountingProcessor:
    return AccountingProcessor(
        state_manager=_StubStore(open_rows), basis_store=None, deployment_id=DEPLOYMENT_ID
    )


# =============================================================================
# Layer 1 — the resolver attributes each leg to its OWN open via discriminator.
# =============================================================================


class TestPriorOpenLookupResolvesOwnLeg:
    """With the closing leg's discriminator, ``_lookup_prior_lp_open`` resolves
    the leg's OWN open even when both opens share one pool-level key."""

    def test_narrow_leg_resolves_to_narrow_open(self):
        proc = _processor([_open_event_row(NARROW), _open_event_row(WIDE)])
        resolved = proc._lookup_prior_lp_open(POSITION_KEY, str(NARROW["token_id"]))
        assert resolved is not None
        assert resolved["tick_lower"] == NARROW["tick_lower"]
        assert resolved["tick_upper"] == NARROW["tick_upper"]
        assert resolved["position_id"] == str(NARROW["token_id"])

    def test_wide_leg_resolves_to_wide_open(self):
        proc = _processor([_open_event_row(NARROW), _open_event_row(WIDE)])
        resolved = proc._lookup_prior_lp_open(POSITION_KEY, str(WIDE["token_id"]))
        assert resolved is not None
        assert resolved["tick_lower"] == WIDE["tick_lower"]
        assert resolved["tick_upper"] == WIDE["tick_upper"]
        assert resolved["position_id"] == str(WIDE["token_id"])

    def test_two_opens_no_discriminator_returns_none_not_latest(self):
        """THE non-negotiable invariant: ambiguous (N>1, no discriminator) ⇒
        None. NEVER the most-recent / sibling open."""
        proc = _processor([_open_event_row(NARROW), _open_event_row(WIDE)])
        resolved = proc._lookup_prior_lp_open(POSITION_KEY, None)
        assert resolved is None, (
            "VIB-4275: with two co-pool opens and no discriminator the resolver "
            "MUST fail closed to None — it must NEVER fall back to the latest "
            f"open (which would be the WIDE leg [{WIDE['tick_lower']}, "
            f"{WIDE['tick_upper']}])."
        )

    def test_discriminator_with_no_match_returns_none_not_latest(self):
        """A discriminator that matches NO open (data gap / pre-stamp open) ⇒
        None, never a guessed sibling."""
        proc = _processor([_open_event_row(NARROW), _open_event_row(WIDE)])
        resolved = proc._lookup_prior_lp_open(POSITION_KEY, "9999999")
        assert resolved is None

    def test_single_open_legacy_resolves_without_discriminator(self):
        """Exactly one open for the key ⇒ resolve it (legacy 1:1 preserved),
        with or without a discriminator."""
        proc = _processor([_open_event_row(NARROW, with_discriminator=False)])
        resolved = proc._lookup_prior_lp_open(POSITION_KEY, None)
        assert resolved is not None
        assert resolved["tick_lower"] == NARROW["tick_lower"]

    def test_single_open_with_discriminator_still_must_match(self):
        """One open, but a discriminator is supplied that does NOT match it ⇒
        None. We do not silently accept the lone open when the close names a
        different position (could be a different leg whose open is missing)."""
        proc = _processor([_open_event_row(NARROW)])
        resolved = proc._lookup_prior_lp_open(POSITION_KEY, str(WIDE["token_id"]))
        assert resolved is None

    def test_no_opens_returns_none(self):
        proc = _processor([])
        assert proc._lookup_prior_lp_open(POSITION_KEY, str(NARROW["token_id"])) is None


class TestMigrationWindowAndSequentialLifecycle:
    """Codex review on #2459: the discriminator-only resolver regressed two real
    production lifecycles an all-fresh fixture run could not surface — opens that
    predate this PR (no position_id stamped), and OPEN->CLOSE->OPEN reuse of one
    pool key. Both must resolve via the single ACTIVE open, not fail closed."""

    def test_prefix_open_without_id_resolves_via_active_fallback(self):
        """Finding 1 — migration window. An LP opened BEFORE this PR has no
        position_id on its open payload; its close now carries a discriminator,
        so id-matching finds nothing. The single active open must still resolve
        (degrade to active-open), not drop ticks/HODL/IL/PnL to None."""
        proc = _processor([_open_event_row(NARROW, with_discriminator=False)])
        resolved = proc._lookup_prior_lp_open(POSITION_KEY, str(NARROW["token_id"]))
        assert resolved is not None, (
            "migration window: a pre-fix open (no position_id) whose close now "
            "carries a discriminator must still attribute via the single active "
            "open — not fail closed."
        )
        assert resolved["tick_lower"] == NARROW["tick_lower"]

    def test_sequential_lifecycle_resolves_the_active_open(self):
        """Finding 2 — OPEN A -> CLOSE A -> OPEN B in one pool, resolving B's
        close. Two historical opens but exactly ONE active (B): must resolve B,
        not fail closed on raw historical open count."""
        rows = [
            _open_event_row(NARROW, with_discriminator=False),
            _close_event_row(),
            _open_event_row(WIDE, with_discriminator=False),
        ]
        resolved = _processor(rows)._lookup_prior_lp_open(POSITION_KEY, None)
        assert resolved is not None, (
            "OPEN->CLOSE->OPEN leaves one ACTIVE open; the resolver must resolve "
            "it, not fail closed because two historical opens exist."
        )
        assert resolved["tick_lower"] == WIDE["tick_lower"]

    def test_two_active_opens_no_id_still_fail_closed(self):
        """Invariant guard: two ACTIVE opens, neither carrying an id, no
        discriminator => STILL fail closed. active-open resolves only the
        unambiguous single-active case; it must never guess among co-pool legs."""
        rows = [
            _open_event_row(NARROW, with_discriminator=False),
            _open_event_row(WIDE, with_discriminator=False),
        ]
        assert _processor(rows)._lookup_prior_lp_open(POSITION_KEY, None) is None

    def test_concurrent_opens_ambiguous_close_fails_closed(self):
        """CodeRabbit (#2459): OPEN A -> OPEN B -> CLOSE B leaves A active, but a
        naive FIFO drop (opens[n_closes:]) would pick B. With two opens live when
        the close fires and no per-position id, which leg closed is unknowable —
        a later no-id close MUST fail closed, never inherit a sibling's open."""
        rows = [
            _open_event_row(NARROW, with_discriminator=False),
            _open_event_row(WIDE, with_discriminator=False),
            _close_event_row(),  # retires one leg; without an id we cannot tell which
        ]
        assert _processor(rows)._lookup_prior_lp_open(POSITION_KEY, None) is None, (
            "ambiguous close (>=2 active opens, no discriminator) must fail closed, "
            "not pick a FIFO/sibling survivor"
        )

    def test_degenerate_close_discriminator_normalized_to_no_id(self):
        """A close-side discriminator of 0 / "0" is normalized to "no discriminator"
        (matching the open-side rule), so a single legacy open still resolves via
        the active-open path rather than hitting a spurious zero-match None."""
        proc = _processor([_open_event_row(NARROW, with_discriminator=False)])
        for degenerate in (0, "0", "  0  "):
            assert proc._lookup_prior_lp_open(POSITION_KEY, degenerate) is not None, (
                f"degenerate close discriminator {degenerate!r} must normalize to "
                "no-discriminator and resolve the single active open"
            )


# =============================================================================
# Layer 2 — the money consequence through the real handler, with the resolver
# supplying the CORRECT per-leg open.
# =============================================================================


def _close_ledger_row(leg: dict[str, Any], tx_hash: str, *, stamp_discriminator: bool = True) -> dict[str, Any]:
    """LP_CLOSE ledger row. The runner stamps the close intent's ``position_id``
    onto ``LPCloseData.position_id`` (VIB-4275) — model that here."""
    lp_close = LPCloseData(
        amount0_collected=int(leg["amount0"] * Decimal(10**8)),  # WBTC raw
        amount1_collected=int(leg["amount1"] * Decimal(10**6)),  # USDC raw
        fees0=0,
        fees1=0,
        liquidity_removed=1,
        current_tick=None,  # no Swap event in burn receipt → tick comes from prior open
        pool_address=POOL,
        position_id=str(leg["token_id"]) if stamp_discriminator else None,
    )
    return {
        "id": f"le-{leg['token_id']}",
        "deployment_id": DEPLOYMENT_ID,
        "cycle_id": "teardown-td_1ddb6300a431",
        "intent_type": "LP_CLOSE",
        "protocol": PROTOCOL,
        "chain": CHAIN,
        "execution_mode": "live",
        "tx_hash": tx_hash,
        "token_in": "",
        "token_out": "",
        "token0": "WBTC",
        "token1": "USDC",
        "token0_decimals": 8,
        "token1_decimals": 6,
        "amount_in": "",
        "amount_out": "",
        "timestamp": "2026-05-25T11:43:26+00:00",
        "extracted_data_json": serialize_extracted_data({"lp_close_data": lp_close}),
        "price_inputs_json": json.dumps(
            {"WBTC": str(PRICE_WBTC), "USDC": str(PRICE_USDC)}
        ),
    }


def _outbox_row() -> dict[str, Any]:
    return {
        "outbox_id": "ob-1",
        "deployment_id": DEPLOYMENT_ID,
        "cycle_id": "teardown-td_1ddb6300a431",
        "position_key": POSITION_KEY,
        "wallet_address": WALLET,
    }


def _resolve_and_handle(leg: dict[str, Any], tx_hash: str, open_rows: list[dict[str, Any]]):
    """End-to-end-ish: extract the close discriminator off the ledger row,
    resolve the prior open through the real resolver, then run ``handle_lp`` —
    exactly the chain ``_dispatch_lp`` drives in production."""
    ledger = _close_ledger_row(leg, tx_hash)
    discriminator = _resolve_lp_close_discriminator(ledger)
    prior_open = _processor(open_rows)._lookup_prior_lp_open(POSITION_KEY, discriminator)
    return handle_lp(_outbox_row(), ledger, prior_open_payload=prior_open, basis_store=None)


class TestLpCloseAttribution:
    """Through ``handle_lp`` with the resolver wired in — each co-pool close
    attributes to its OWN open (the production contamination is gone)."""

    def test_close_discriminator_round_trips_off_ledger(self):
        ledger = _close_ledger_row(NARROW, tx_hash="0xc675060a")
        assert _resolve_lp_close_discriminator(ledger) == str(NARROW["token_id"])

    def test_narrow_leg_close_uses_its_own_open(self):
        open_rows = [_open_event_row(NARROW), _open_event_row(WIDE)]
        event = _resolve_and_handle(NARROW, "0xc675060a", open_rows)
        assert event is not None
        payload = json.loads(event.to_payload_json())

        assert payload["tick_lower"] == NARROW["tick_lower"], (
            f"narrow leg's LP_CLOSE must carry its OWN tick_lower "
            f"{NARROW['tick_lower']}, got {payload['tick_lower']}"
        )
        assert payload["tick_upper"] == NARROW["tick_upper"]

        assert payload["position_id"] == str(NARROW["token_id"]), (
            "VIB-4275: the LP_CLOSE payload must carry the closing NFT's OWN "
            "position_id so the audit row is joinable to position_events by id — "
            f"not left None and inferred from the tick range (expected "
            f"{NARROW['token_id']}, got {payload.get('position_id')!r})"
        )

        got_hodl = Decimal(str(payload["hodl_value_usd"]))
        assert got_hodl == _v_hodl(NARROW), (
            f"narrow leg's hodl_value_usd must be computed against its OWN open "
            f"amounts ({_v_hodl(NARROW)}), got {got_hodl}"
        )

    def test_wide_leg_close_uses_its_own_open(self):
        open_rows = [_open_event_row(NARROW), _open_event_row(WIDE)]
        event = _resolve_and_handle(WIDE, "0xec78a234", open_rows)
        assert event is not None
        payload = json.loads(event.to_payload_json())
        assert payload["tick_lower"] == WIDE["tick_lower"]
        assert payload["tick_upper"] == WIDE["tick_upper"]
        assert payload["position_id"] == str(WIDE["token_id"])
        assert Decimal(str(payload["hodl_value_usd"])) == _v_hodl(WIDE)

    def test_co_pool_legs_have_distinct_hodl(self):
        """INV-4 — two legs with different open compositions must NOT share an
        identical ``hodl_value_usd`` (production showed both at 1.953086820)."""
        open_rows = [_open_event_row(NARROW), _open_event_row(WIDE)]
        narrow_event = _resolve_and_handle(NARROW, "0xc675060a", open_rows)
        wide_event = _resolve_and_handle(WIDE, "0xec78a234", open_rows)
        assert narrow_event is not None and wide_event is not None
        narrow_hodl = json.loads(narrow_event.to_payload_json())["hodl_value_usd"]
        wide_hodl = json.loads(wide_event.to_payload_json())["hodl_value_usd"]
        assert narrow_hodl != wide_hodl

    def test_close_with_no_resolvable_open_fails_closed(self):
        """When the resolver returns None (no matching open), the attribution-
        dependent fields are None — NOT a sibling open's values."""
        # Only the WIDE open exists; the NARROW close names token 5503727,
        # which matches nothing ⇒ resolver returns None ⇒ no hodl/il/ticks.
        open_rows = [_open_event_row(WIDE)]
        event = _resolve_and_handle(NARROW, "0xc675060a", open_rows)
        assert event is not None
        payload = json.loads(event.to_payload_json())
        assert payload["hodl_value_usd"] is None, (
            "fail-closed: with no resolvable own-open the HODL anchor must be "
            "None, never the WIDE leg's value"
        )
        assert payload["il_usd"] is None
        # Tick backfill came from the prior open; with None it stays None
        # rather than inheriting the WIDE leg's bracket.
        assert payload["tick_lower"] is None
        assert payload["tick_upper"] is None
        # realized_pnl needs the prior open's cost basis ⇒ also None.
        assert payload["realized_pnl_usd"] is None


class TestLpCollectFeesAttribution:
    """LP_COLLECT_FEES uses the same resolver — attributes to the correct leg —
    but must NEVER compute IL on a fee collect (principal stays on-chain)."""

    def _collect_ledger(self, leg: dict[str, Any]) -> dict[str, Any]:
        lp_close = LPCloseData(
            amount0_collected=0,
            amount1_collected=0,
            fees0=int(Decimal("0.00000010") * Decimal(10**8)),
            fees1=int(Decimal("0.50") * Decimal(10**6)),
            liquidity_removed=0,
            current_tick=None,
            pool_address=POOL,
            position_id=str(leg["token_id"]),
        )
        return {
            "id": f"le-collect-{leg['token_id']}",
            "deployment_id": DEPLOYMENT_ID,
            "cycle_id": "cycle-1",
            "intent_type": "LP_COLLECT_FEES",
            "protocol": PROTOCOL,
            "chain": CHAIN,
            "execution_mode": "live",
            "tx_hash": f"0xfee{leg['token_id']}",
            "token_in": "",
            "token_out": "",
            "token0": "WBTC",
            "token1": "USDC",
            "token0_decimals": 8,
            "token1_decimals": 6,
            "amount_in": "",
            "amount_out": "",
            "timestamp": "2026-05-25T11:43:26+00:00",
            "extracted_data_json": serialize_extracted_data({"lp_close_data": lp_close}),
            "price_inputs_json": json.dumps({"WBTC": str(PRICE_WBTC), "USDC": str(PRICE_USDC)}),
        }

    def test_collect_attributes_to_correct_leg_and_no_il(self):
        open_rows = [_open_event_row(NARROW), _open_event_row(WIDE)]
        ledger = self._collect_ledger(NARROW)
        discriminator = _resolve_lp_close_discriminator(ledger)
        assert discriminator == str(NARROW["token_id"])
        prior_open = _processor(open_rows)._lookup_prior_lp_open(POSITION_KEY, discriminator)
        assert prior_open is not None
        assert prior_open["tick_lower"] == NARROW["tick_lower"]

        event = handle_lp(_outbox_row(), ledger, prior_open_payload=prior_open, basis_store=None)
        assert event is not None
        payload = json.loads(event.to_payload_json())
        # IL is NEVER computed on a fee collect (principal not unwound).
        assert payload["il_usd"] is None
        assert payload["hodl_value_usd"] is None
        # ...but the collected-fees row still carries its OWN position_id.
        assert payload["position_id"] == str(NARROW["token_id"])


class TestDegenerateDiscriminatorUniformlyAbsent:
    """gemini review (#2459): the degenerate 0 / "0" id is "no discriminator"
    (Empty != Zero) uniformly across stamp + open/close resolvers — a real
    minted NFT id is a positive integer. The close resolver must treat it as
    absent so resolution falls to the single-open legacy path rather than
    matching an open whose id-0 was already discarded."""

    @pytest.mark.parametrize("degenerate", [0, "0", "  0  ", "", None])
    def test_close_resolver_treats_degenerate_id_as_absent(self, degenerate: Any) -> None:
        lp_close = LPCloseData(
            amount0_collected=0,
            amount1_collected=0,
            fees0=0,
            fees1=0,
            liquidity_removed=0,
            current_tick=None,
            pool_address=POOL,
            position_id=degenerate,
        )
        ledger = {"extracted_data_json": serialize_extracted_data({"lp_close_data": lp_close})}
        assert _resolve_lp_close_discriminator(ledger) is None


class TestParseOpenPayloadFailClosed:
    """``_parse_open_payload`` decodes an LP-open candidate's stored
    ``payload_json``. A malformed payload is a data-integrity anomaly; it must
    fail closed (return ``None``, skipping the candidate) rather than raise on
    the money path or silently swallow an unexpected error class."""

    def test_valid_json_dict_string_parses(self):
        row = {"payload_json": json.dumps({"position_id": "5467895", "x": 1})}
        assert AccountingProcessor._parse_open_payload(row) == {"position_id": "5467895", "x": 1}

    def test_already_dict_payload_passes_through(self):
        row = {"payload_json": {"position_id": "5467895"}}
        assert AccountingProcessor._parse_open_payload(row) == {"position_id": "5467895"}

    def test_malformed_json_string_fails_closed_to_none(self):
        # JSONDecodeError (a ValueError) is caught and logged, not propagated.
        row = {"payload_json": "{not valid json"}
        assert AccountingProcessor._parse_open_payload(row) is None

    def test_non_dict_json_payload_is_none(self):
        # Valid JSON that isn't an object (e.g. a list or scalar) is not a payload.
        assert AccountingProcessor._parse_open_payload({"payload_json": "[1, 2, 3]"}) is None
        assert AccountingProcessor._parse_open_payload({"payload_json": "123"}) is None

    def test_missing_or_none_payload_is_none(self):
        assert AccountingProcessor._parse_open_payload({}) is None
        assert AccountingProcessor._parse_open_payload({"payload_json": None}) is None


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-v"]))
