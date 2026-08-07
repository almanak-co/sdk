"""One physical GMX perp position must not enumerate as two (VIB-6287).

Measured on Arbitrum mainnet across four runs: a GMX close that verifiably
flattened the position on-chain (``getAccountPositions -> []``) reported a
**FAILED** teardown, because the teardown set opened with 2 rows for 1 physical
position and the completeness check (VIB-5469 / ALM-2900) failed the row that
never received a closing intent.

Why this file exists ALONGSIDE the dedupe tests in ``test_registry_enumeration.py``:
those hand-author the ``details`` dict on **both** sides, so they assert that two
dicts the test itself wrote compare equal. They are green, and they encode only
the author's belief about the producers. A test that authors both operands
cannot catch a producer mismatch.

ADDRESS-FIRST UPDATE: the market axis is unified in address space now.
Strategies supply the market-token ADDRESS (there is no symbol→address table
left to resolve through — VIB-6155 is why there must not be), the runtime
registry writer persists ``intent.market`` verbatim, and the adapter's on-chain
discovery reports the chain's address. The polysemy VIB-6287 documented
survives only on the COLLATERAL axis (symbol vs address under one key name),
which still resolves through ``GMX_V2_TOKENS``.

So every ``details`` dict here is built by **running a real producer**. The only
things stubbed are the chain read and the strategy's phase state — never the
identity fields under test.

This is written as a PAIRING MATRIX rather than a single canary on purpose.
There is more than one producer on each side, they do not agree with each other,
and which pair is live depends on whether the perp cutover is active. Asserting
one pair would have hidden the others. Each case below records what the current
code actually does, so the pair that fails is named rather than inferred.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from almanak.connectors.gmx_v2.adapter import GMXv2Adapter, GMXv2Config, GMXv2Position
from almanak.connectors.gmx_v2.addresses import GMX_V2_TOKENS
from almanak.framework.teardown import TeardownPositionSummary
from almanak.framework.teardown.models import PositionType
from almanak.framework.teardown.registry_enumeration import (
    _position_info_from_perp_registry_row,
    reconcile_lp_with_registry,
)
from tests.unit.connectors.gmx_v2.market_fixtures import market_address

CHAIN = "arbitrum"
MARKET_SYMBOL = "ETH/USD"
MARKET_ADDRESS = market_address(CHAIN, MARKET_SYMBOL)
# The on-chain reader returns collateral as an ADDRESS: `_parse_position_dicts`
# passes `pos["collateral_token"]` straight through, and `_get_collateral_decimals`
# / `_get_position_key` both consume it as one. The fixture must match.
COLLATERAL_ADDRESS = GMX_V2_TOKENS[CHAIN]["USDC"]
WALLET = "0xafeB2f5c213b5e7F37c3Fc171dfCb6270d07e21a"
# The venue key from the mainnet runs, so the fixture is anchored to a real one.
VENUE_KEY = "0xbf58e0307a44a17ea51e30850651f5269c9fc0f306990576c015e9a88ac9bafa"


# --------------------------------------------------------------------------
# HOT side — the strategy's own enumeration
# --------------------------------------------------------------------------


def strategy_stub_row():
    """Built by the REAL ``AccountingQuantPerpStrategy.get_open_positions``.

    This is the strategy that ran on mainnet. Only the attributes the producer
    reads are supplied; the ``details`` dict is built by the strategy's code.
    ``deployment_id`` and ``chain`` are read-only properties on the strategy
    base, so their backing fields are set rather than shadowing the accessors.

    Address-first: ``market`` holds the market-token ADDRESS, because that is
    what a migrated strategy config supplies (the producer writes it through
    verbatim). The collateral stays the symbol the strategy configures — the
    collateral axis still resolves through ``GMX_V2_TOKENS``.
    """
    from strategies.accounting.perp.strategy import PHASE_OPEN, AccountingQuantPerpStrategy

    strat = AccountingQuantPerpStrategy.__new__(AccountingQuantPerpStrategy)
    strat._deployment_id = "deployment:86f4562d5b6c"
    strat._chain = CHAIN
    strat.protocol = "gmx_v2"
    strat.market = MARKET_ADDRESS
    strat.collateral_token = "USDC"
    strat.is_long = True
    strat.leverage = Decimal("2")
    strat._position_size_usd = Decimal("6.0")
    strat._phase = PHASE_OPEN

    summary = strat.get_open_positions()
    assert len(summary.positions) == 1, "fixture no longer drives the open-position branch"
    return summary.positions[0]


# --------------------------------------------------------------------------
# WARM side — three different producers, which is the heart of the problem
# --------------------------------------------------------------------------


def warm_row_from_runtime_write():
    """Registry row as the RUNTIME writer produces it.

    ``_maybe_save_ledger_with_registry_perp`` (``strategy_runner.py`` ~:5660)
    builds the payload; ``_position_info_from_perp_registry_row`` turns a stored
    row back into a ``PositionInfo``. Both are real. The payload mirrors that
    writer field-for-field, including ``market=intent.market`` verbatim — under
    the address-first contract that is the market-token ADDRESS the strategy
    supplied on the intent.
    """
    payload = {
        "protocol": "gmx_v2",
        "position_id": VENUE_KEY.lower(),
        "market": MARKET_ADDRESS,
        "collateral_token": "USDC",
        "direction": "long",
        "size_usd": "6.0",
        "source": "runtime",
    }
    row = _position_info_from_perp_registry_row({"chain": CHAIN, "primitive": "perp", "payload": payload})
    assert row is not None
    return row


def warm_row_from_settlement_reconciler():
    """**The pairing that actually failed on mainnet.** Payload is the observed row.

    Read verbatim from the Run 4 strategy DB (`position_registry`, one row,
    `primitive='perp'`, `deployment:86f4562d5b6c`), written by the perp
    settlement reconciler right after the keeper fill and well before teardown
    began. Only the fields `_position_info_from_perp_registry_row` consumes are
    kept; `opened_at_block` / `closed_tx` / `keeper_tx_hash` are omitted because
    that function never reads them.

    Note the payload's own ``"source": "settlement_reconciler"`` is WRITE
    provenance — who wrote the registry row. It is a different thing from the
    ``details["source"] = "position_registry"`` that
    ``_position_info_from_perp_registry_row`` stamps on the ``PositionInfo`` it
    builds, which is READ-path attribution. Same word, two meanings.

    This was the pairing that made precedence-ordering impossible as a fix: the
    registry side carries **addresses under the same key names** the stub used
    to fill with **symbols**, and there is no ``market_address`` key here at
    all. The resolution shipped is address-first: the strategy side now writes
    the address too, so the MARKET axis agrees byte-for-byte, while the
    COLLATERAL axis (address here, symbol on the stub) still resolves through
    the token table.
    """
    payload = {
        "protocol": "gmx_v2",
        "position_id": VENUE_KEY.lower(),
        "market": "0x70d95587d40a2caf56bd97485ab3eec10bee6336",
        "collateral_token": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        "direction": "long",
        "source": "settlement_reconciler",
    }
    # Anchor the literals: if the catalogue moves, this fixture must be re-derived
    # from a fresh run rather than silently drifting off the observed row.
    assert payload["market"] == MARKET_ADDRESS.lower()
    assert payload["collateral_token"] == COLLATERAL_ADDRESS.lower()
    row = _position_info_from_perp_registry_row({"chain": CHAIN, "primitive": "perp", "payload": payload})
    assert row is not None
    return row


def warm_row_from_backfill():
    """Registry row as the BACKFILL producer produces it.

    ``backfill.fold_position_events_for_perp`` hardcodes ``"market": None`` — its
    own docstring says market is not persisted as a column, so it stays None
    (Empty != Zero). ``_position_info_from_perp_registry_row`` skips None values,
    so the resulting ``details`` carries **no market key at all**.
    """
    payload = {
        "protocol": "gmx_v2",
        "position_id": VENUE_KEY.lower(),
        "market": None,
        "collateral_token": "USDC",
        "direction": "long",
        "size_usd": None,
        "source": "backfill",
    }
    row = _position_info_from_perp_registry_row({"chain": CHAIN, "primitive": "perp", "payload": payload})
    assert row is not None
    return row


def warm_row_from_adapter_discovery():
    """Position row as the gmx_v2 ADAPTER's on-chain discovery produces it.

    ``get_positions_as_teardown_summary`` is real production code; only
    ``get_positions_onchain`` (the chain read) is stubbed. This producer emits
    BOTH ``market`` (the catalog label when this process venue-verified the
    market; the raw address otherwise — the shape here, since nothing primed
    the catalog) and ``market_address`` (always the address, the identity
    axis).
    """
    adapter = GMXv2Adapter(GMXv2Config(chain=CHAIN, wallet_address=WALLET))
    onchain = GMXv2Position(
        position_key=VENUE_KEY,
        market=MARKET_ADDRESS,
        collateral_token=COLLATERAL_ADDRESS,
        size_in_usd=Decimal("6.0"),
        size_in_tokens=Decimal("0.003218709779491469"),
        collateral_amount=Decimal("2.9964"),
        entry_price=Decimal("1864.10"),
        is_long=True,
        leverage=Decimal("2"),
        last_updated=datetime.now(UTC),
    )
    adapter.get_positions_onchain = lambda **_kwargs: [onchain]  # type: ignore[method-assign]
    summary = adapter.get_positions_as_teardown_summary(deployment_id="deployment:86f4562d5b6c")
    assert len(summary.positions) == 1
    return summary.positions[0]


def _union_size(hot_row, warm_row) -> int:
    """Rows surviving the real additive-union reconcile — where ``_dedupe_key`` lives.

    ``_dedupe_key`` is a closure inside ``reconcile_lp_with_registry`` and cannot
    be imported. Driving the public function is deliberate: copying the key
    builder into the test would assert against a duplicate of the logic rather
    than the logic.
    """
    summary = TeardownPositionSummary(
        deployment_id="deployment:86f4562d5b6c",
        timestamp=datetime.now(UTC),
        positions=[hot_row],
    )
    reconciled = reconcile_lp_with_registry(
        strategy_summary=summary,
        registry_positions=[warm_row],
        registry_available=True,
        # Production (``resolve_open_positions_with_registry``) ALWAYS supplies
        # this — it closes over ``_teardown_wallet_for_chain``. Withholding it
        # here would test a configuration production never runs. Needed only by
        # the backfill pairing, whose registry row carries ``market: None`` and
        # so has no semantic identity at all: its only certain identity is its
        # venue key, which the strategy row can reach only by deriving it from
        # the account. ``TestVib6287TheMutationGate`` below proves this line is
        # not what greens the canary.
        wallet_for_chain=lambda _chain: WALLET,
    )
    return len(reconciled.positions)


class TestVib6287FixtureIsAGenuineDuplicate:
    """Guard the guard: if these fail, every assertion below is vacuous."""

    def test_all_producers_describe_one_physical_position(self):
        hot = strategy_stub_row()
        for warm in (warm_row_from_runtime_write(), warm_row_from_backfill(), warm_row_from_adapter_discovery()):
            assert hot.position_type is warm.position_type is PositionType.PERP
            assert str(hot.chain).lower() == str(warm.chain).lower() == CHAIN
            assert str(hot.protocol).lower() == str(warm.protocol).lower() == "gmx_v2"
            # Different ids for one position — the condition that makes dedupe
            # necessary in the first place.
            assert hot.position_id != warm.position_id


class TestVib6287ProducerPairingMatrix:
    """What each real HOT/WARM pair actually does today.

    Recorded per-pair so a fix cannot make one pair pass while another silently
    keeps duplicating — the failure mode that let #3511 land a PERP dedupe arm
    that measurably did not resolve the mainnet defect.
    """

    def test_runtime_write_pair_collapses(self):
        """The same address on both sides — this pair dedupes correctly."""
        assert _union_size(strategy_stub_row(), warm_row_from_runtime_write()) == 1

    def test_backfill_pair_must_collapse(self):
        """This pair collapses on this branch; it did NOT before the fix.

        Pre-fix diagnosis, kept because it is what the fix had to defeat: backfill
        omits ``market`` entirely, so the old arm's all-four-present guard failed
        and the row fell through to raw ``position_id``. Nothing about the pair is
        ambiguous — the producer hardcodes ``market: None``.

        The alias set resolves it, but NOT merely because the backfill row carries
        a venue key. That row's only certain identity IS that key (it has no
        semantic identity at all), and the strategy row has no key of its own — so
        ``{key}`` and ``{sem}`` would not intersect. The pair collapses because the
        strategy row DERIVES the same key from the account, which is why
        ``_union_size`` supplies ``wallet_for_chain`` exactly as production does.

        No xfail: this asserts the POST-fix behaviour and must stay green.
        ``TestVib6287TheMutationGate`` is what pins the pre-fix baseline, by
        disabling the hook and asserting the counts revert.
        """
        assert _union_size(strategy_stub_row(), warm_row_from_backfill()) == 1

    def test_adapter_discovery_pair_must_collapse(self):
        """This pair collapses on this branch; it did NOT before the fix.

        Pre-fix diagnosis, kept because it is why a narrower fix would not have
        worked: the old dedupe key PREFERRED ``market_address``, a field only
        the adapter carries, and the adapter ALSO writes an address into
        ``collateral_token`` where the stub writes ``"USDC"`` — two independent
        axes, so a market-only fix would have turned one axis green and left
        this red.

        Address-first, the MARKET axis agrees byte-for-byte (the stub writes
        the address the adapter discovers); the COLLATERAL axis still spans two
        value spaces and is resolved by the identity hook's token-table lookup.

        No xfail: this asserts the POST-fix behaviour and must stay green.
        """
        assert _union_size(strategy_stub_row(), warm_row_from_adapter_discovery()) == 1

    def test_the_mainnet_pair_must_collapse(self):
        """**The pairing measured failing on Arbitrum mainnet, four runs.**

        This is the case of record for VIB-6287. Registry payload read verbatim
        from the Run 4 DB; strategy row from the real producer. Both sides use
        the SAME key names (``market``, ``collateral_token``); pre-migration
        they held DIFFERENT value types — address vs symbol — on two
        independent axes, and no ordering of ``market`` / ``market_address``
        could fix it (the registry row has no ``market_address`` key at all).
        Address-first closes the market axis at the PRODUCER (the strategy
        writes the address); collateral still spans two value spaces and is
        resolved by the hook. ``direction`` agrees on both sides and was never
        the mismatching field.
        """
        assert _union_size(strategy_stub_row(), warm_row_from_settlement_reconciler()) == 1


class TestVib6287TheMutationGate:
    """Prove the fix — not the fixture — is what greens the matrix above.

    ``_union_size`` gained ``wallet_for_chain=lambda _chain: WALLET`` as part of
    this change. A canary that passes with its mechanism disabled is not a
    canary, and THIS FILE has already been vacuously green once (a
    ``monkeypatch(..., raising=False)`` against an attribute that did not
    exist). So: disable the venue hook, keep the wallet, and require the matrix
    to revert EXACTLY to its pre-fix counts.

    The hook is removed from the registry, and its prior existence is asserted
    first — a deletion that silently ignores a missing key would be the same
    vacuous-green trap in a new costume. Replacing it with an empty callable is
    not equivalent after VIB-6329: a registered hook is authoritative, so an
    empty emission deliberately falls to raw identity rather than the coarser
    pre-hook default.
    """

    @staticmethod
    def _disable_gmx_identity(monkeypatch):
        # Hook registration is an import-time side effect of the framework
        # dispatch module (``_register_manifest_perp_identities``). Importing it
        # here makes the precondition below deterministic: without this, a test
        # process that had not yet touched the teardown identity path would see
        # an EMPTY registry and this gate would fail for an ordering reason,
        # not a missing fix.
        import almanak.framework.teardown.perp_identity  # noqa: F401 — registration side effect
        from almanak.connectors._strategy_base import perp_identity as seam

        assert "gmx_v2" in seam._REGISTRY, (
            "the gmx_v2 identity hook is not registered — this gate would be vacuous, "
            "and a hook that never resolves is indistinguishable from no fix at all"
        )
        monkeypatch.delitem(seam._REGISTRY, "gmx_v2")

    def test_matrix_reverts_to_the_pre_fix_counts_when_the_hook_is_disabled(self, monkeypatch):
        """The measured pre-fix baseline: 1 / 2 / 2 / 2."""
        self._disable_gmx_identity(monkeypatch)

        # The runtime-write pair collapsed BEFORE the fix too (both sides carry
        # symbols), via the framework's per-type perp default — so it must stay
        # at 1 here. Any other value means the default arm was not preserved.
        assert _union_size(strategy_stub_row(), warm_row_from_runtime_write()) == 1
        # The three that VIB-6287 is about must all split again.
        assert _union_size(strategy_stub_row(), warm_row_from_backfill()) == 2
        assert _union_size(strategy_stub_row(), warm_row_from_adapter_discovery()) == 2
        assert _union_size(strategy_stub_row(), warm_row_from_settlement_reconciler()) == 2

    def test_the_wallet_alone_does_not_collapse_anything(self, monkeypatch):
        """The approved canary edit is inert without the hook.

        Threading ``WALLET`` is what lets the STRATEGY row reach venue-key space,
        but only the hook can turn it into an identity. With the hook disabled
        the wallet buys nothing — which is the whole claim this gate exists to
        substantiate.
        """
        self._disable_gmx_identity(monkeypatch)
        with_wallet = _union_size(strategy_stub_row(), warm_row_from_settlement_reconciler())

        summary = TeardownPositionSummary(
            deployment_id="deployment:86f4562d5b6c",
            timestamp=datetime.now(UTC),
            positions=[strategy_stub_row()],
        )
        without_wallet = len(
            reconcile_lp_with_registry(
                strategy_summary=summary,
                registry_positions=[warm_row_from_settlement_reconciler()],
                registry_available=True,
            ).positions
        )
        assert with_wallet == without_wallet == 2


class TestVib6287TheMechanism:
    """Pin the mechanism, so a fix that changes the count for another reason
    does not read as a fix for this."""

    def test_both_sides_agree_on_the_market_address(self):
        """SUCCESSOR of ``test_both_sides_already_agree_on_the_market_symbol``.

        Pre-migration the shared ``market`` key agreed in SYMBOL space
        (``"eth/usd"``) while the adapter's extra ``market_address`` split the
        old preference-ordered key. Address-first, the shared key agrees in
        ADDRESS space — the strategy writes the address the chain reports, so
        there is nothing left for a key-preference to split (VIB-6287).
        """
        hot = strategy_stub_row().details
        adapter = warm_row_from_adapter_discovery().details

        assert "market_address" not in hot, "strategy stub gained market_address — re-derive the root cause"
        assert adapter["market_address"].lower() == MARKET_ADDRESS.lower()
        # The field they SHARE agrees, and it agrees on the ADDRESS.
        assert str(hot["market"]).lower() == str(adapter["market"]).lower() == MARKET_ADDRESS.lower()

    def test_collateral_token_is_polysemous_across_producers(self):
        """The SECOND axis, independent of market — and the reason a
        market-only fix is not a fix.

        ``details["collateral_token"]`` holds a symbol from the strategy stub and
        an ADDRESS from the adapter (``pos.collateral_token`` is the raw on-chain
        reader field, consumed elsewhere as an address by
        ``_get_collateral_decimals`` and ``_get_position_key``). One key, two
        value-spaces, exactly like ``market``.
        """
        hot = strategy_stub_row().details
        adapter = warm_row_from_adapter_discovery().details

        assert str(hot["collateral_token"]).lower() == "usdc"
        adapter_collateral = str(adapter["collateral_token"]).lower()
        assert adapter_collateral != "usdc", (
            "the adapter now agrees with the stub on collateral_token — if this is a real fix, "
            "assert the agreement positively rather than deleting this test"
        )

    def test_market_key_orderings_agree_in_address_space(self):
        """SUCCESSOR of ``test_market_key_precedence_disagrees_with_the_valuation_lane``.

        Pre-migration the enumeration lane (``market_address`` first) split
        this pair while the valuation lane (``market`` first) agreed — the
        cross-lane divergence VIB-6287 documented. Address-first dissolves it
        at the producer: every market-shaped field on both rows holds the SAME
        address, so BOTH orderings agree and no key-preference choice can
        split the pair again. Pinned so a producer regressing to symbol-space
        market details re-opens this test, not a mainnet run.
        """
        hot = strategy_stub_row().details
        adapter = warm_row_from_adapter_discovery().details

        def valuation_order(d):
            return str(d.get("market") or d.get("market_address") or "").lower()

        def enumeration_order(d):
            return str(d.get("market_address") or d.get("market") or "").lower()

        assert valuation_order(hot) == valuation_order(adapter) == MARKET_ADDRESS.lower()
        assert enumeration_order(hot) == enumeration_order(adapter) == MARKET_ADDRESS.lower(), (
            "the enumeration lane's order must no longer split them — the VIB-6287 "
            "divergence is closed in address space"
        )
