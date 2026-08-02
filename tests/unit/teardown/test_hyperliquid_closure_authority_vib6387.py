"""Hyperliquid perp teardown gains a real closure authority (VIB-6387).

**The banked failure this file exists to make impossible.** Mainnet, 2026-08-01,
``deployment:919d5bab4916`` (``docs/internal/quant-user-runs/20260801-1620-hlperp``)
— a Hyperliquid demo opened a perp, held an hour, and tore down. Every position
closed::

    teardown_requests: status=failed
                       positions_total=1  positions_closed=1  positions_failed=0
    error_message: "Teardown closure could not be verified: no measured on-chain
                    evidence of closure. …"
    log: "🛑 TD-15 (VIB-6285): … UNMEASURED for protocol(s) hyperliquid —
          no Plan-A DIVERGED_CLOSED read and no TD-14 hook proof … (proved: none)"

Nothing was open and nothing failed; the teardown was reported FAILED anyway. The
consequence is not cosmetic — token consolidation is gated on
``teardown_result.success`` and ``mark_failed`` is terminal, and the VIB-5572
entry latch then bricks the strategy against re-entry on a provably-flat book.

**Why it was unmeasured, and why that was a wiring gap rather than a capability
gap.** ``get_teardown_post_condition("hyperliquid")`` returned ``None``, so
``plan_a_reconciliation._reconcile_one`` left its PERP branch at the
``supports_open_state_reconciliation`` check and returned ``UNVERIFIABLE``. But
HyperCore's ``0x0800`` position precompile was already being read in production by
``perps_read.py`` and ``fill_reconciliation.py`` — the evidence existed and simply
was not wired to the teardown seam. That is what makes a hook the right fix and
the VIB-6311 capability gate the wrong one *for this venue*: waiving the ratchet
would certify a perp teardown off zero evidence when real evidence was one wire
away. The capability gate remains correct for primitives that genuinely cannot
measure (polymarket PREDICTION, STAKE, CEX) and is untouched here.

**The trap that makes the empty-return test load-bearing.** ``sdk.decode_position``
documents a zero-length return as "no Core account / no position" and maps it to
``Position(szi=0, …)``. Fed straight to "szi == 0 ⇒ closed", a zero-byte blob
would fabricate a closure proof off no chain data — the exact false-green
``ClosureCheckResult.not_applicable`` was introduced to stop. Confirmed live on
2026-08-02 against pool wallet 18: a real flat account returns the **complete**
160-byte struct for a traded-then-closed asset *and* for one it never traded, so
treating a short return as unmeasured costs nothing and is never the healthy path.
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from eth_abi import encode as abi_encode

from almanak.connectors.hyperliquid.addresses import PRECOMPILE_POSITION
from almanak.connectors.hyperliquid.teardown_post_condition import (
    hyperliquid_teardown_post_condition as HOOK,
)
from almanak.framework.teardown.models import (
    ClosureVerification,
    PositionInfo,
    PositionType,
    VerificationStatus,
)
from almanak.framework.teardown.plan_a_reconciliation import (
    ReconciliationVerdict,
    reconcile_known_positions_against_chain,
)
from almanak.framework.teardown.post_conditions import get_teardown_post_condition
from almanak.framework.teardown.teardown_manager import TeardownManager

CHAIN = "hyperevm"
PROTOCOL = "hyperliquid"
# Pool wallet 18 — the wallet that produced the banked mainnet failure above.
WALLET = "0xad71aCaC64d15C1d46F97c85B4174927047bDbef"
# The 0x0800 precompile struct: (int64 szi, uint64 entryNtl, int64 isolatedRawUsd,
# uint32 leverage, bool isIsolated) — five ABI words, 160 bytes.
_STRUCT_TYPES = ["int64", "uint64", "int64", "uint32", "bool"]


def _blob(*, szi: int = 0, entry_ntl: int = 0, leverage: int = 20) -> str:
    """A well-formed 0x0800 return. ``szi=0`` is a MEASURED flat, not an absence."""
    return "0x" + abi_encode(_STRUCT_TYPES, [szi, entry_ntl, 0, leverage, False]).hex()


def _gateway(blob: str | Exception) -> MagicMock:
    gw = MagicMock()
    if isinstance(blob, Exception):
        gw.eth_call.side_effect = blob
    else:
        gw.eth_call.return_value = blob
    return gw


def _position(
    *,
    # The SHIPPED shape, not the convenient one. ``hyperliquid_trailing_perp``
    # writes ``details["market"] = self.market``, whose default is "ETH/USD"
    # (strategy.py:125, :555) — the suffix is stripped by ``normalize_symbol``
    # inside ``resolve_market``. Defaulting to a bare "ETH" here would let a
    # normalization regression re-break demo teardown while every test in this
    # file stayed green: the hook would still resolve "ETH", and nothing would
    # exercise the path production actually takes. Raised by the Grok review of
    # PR #3553.
    market: str = "ETH/USD",
    position_type: PositionType = PositionType.PERP,
    chain: str = CHAIN,
    protocol: str = PROTOCOL,
    **details,
) -> PositionInfo:
    if market:
        details = {"market": market, **details}
    return PositionInfo(
        position_type=position_type,
        position_id=f"hyperliquid-{market or 'unknown'}-long",
        chain=chain,
        protocol=protocol,
        value_usd=Decimal("0"),
        details=details,
    )


# ---------------------------------------------------------------------------
# The hook's three-valued closure rule
# ---------------------------------------------------------------------------


class TestHookClosureRule:
    def test_measured_flat_is_closed(self):
        """szi == 0 from a full struct is a MEASURED closure — the whole point."""
        result = HOOK(_position(), WALLET, gateway_client=_gateway(_blob(szi=0)))
        assert result.closed is True
        assert result.unmeasured is False
        assert result.not_applicable is False
        assert result.protocol == PROTOCOL

    def test_measured_residual_is_not_closed(self):
        """A live position is the residual risk this seam exists to catch."""
        result = HOOK(_position(), WALLET, gateway_client=_gateway(_blob(szi=4200, entry_ntl=11_000_000)))
        assert result.closed is False
        assert result.unmeasured is False, "a measured residual must FAIL, never read as unmeasured"
        assert result.residual["szi"] == 4200

    def test_a_short_position_is_also_a_residual(self):
        """Side is deliberately not compared: a flipped position is still open risk."""
        result = HOOK(_position(), WALLET, gateway_client=_gateway(_blob(szi=-4200)))
        assert result.closed is False
        assert result.unmeasured is False
        assert result.residual["is_long"] is False

    @pytest.mark.parametrize(
        "blob",
        ["0x", "0x00", "0x" + "00" * 159],
        ids=["empty", "one-byte", "159-bytes"],
    )
    def test_an_empty_or_short_return_is_unmeasured_never_closed(self, blob):
        """THE trap. ``decode_position`` maps an empty return to ``szi=0``.

        Without the length check the hook would answer ``closed=True`` off zero
        bytes of chain data, record a ``hook_proven_position_keys`` entry, and
        certify a teardown having measured nothing — strictly worse than the
        false FAILED this PR removes, because it fails OPEN.
        """
        result = HOOK(_position(), WALLET, gateway_client=_gateway(blob))
        assert result.unmeasured is True
        assert result.closed is False

    def test_a_read_fault_is_unmeasured_never_a_residual(self):
        """A gateway blip must not fabricate a residual and brick a healthy strategy."""
        result = HOOK(_position(), WALLET, gateway_client=_gateway(RuntimeError("gateway down")))
        assert result.unmeasured is True
        assert result.residual == {}, "Empty != Zero: an unread market is not a measured one"

    def test_an_undecodable_return_is_unmeasured(self):
        """A full-length blob that ``abi_decode`` rejects must be UNMEASURED.

        ``ff`` * 160 is 160 bytes, so it clears the length guard and genuinely
        reaches ``decode_position``, where ``eth_abi`` raises
        ``NonEmptyPaddingBytes`` (the int64/uint64 words carry dirty high bits).
        That is the decode-failure branch, and it is the one under test.

        The previous assertion was ``not (result.closed and result.unmeasured)``,
        which cannot fail: ``_result`` never sets both. It would have passed just
        as happily on a hook that returned ``closed=True`` off a garbage blob.
        Raised by the Grok review of PR #3553.
        """
        result = HOOK(_position(), WALLET, gateway_client=_gateway("0x" + "ff" * 160))
        assert result.unmeasured is True
        assert result.closed is False
        assert result.residual == {}, "Empty != Zero: an undecodable read is not a measured one"

    @pytest.mark.parametrize("payload", ["0xzz", "0xabc", object()])
    def test_a_malformed_payload_is_unmeasured_and_does_not_raise(self, payload):
        """The hook promises it NEVER raises — including on a payload it cannot coerce.

        ``bytes.fromhex`` rejects non-hex digits and odd-length strings; ``bytes()``
        rejects a bare object. Before this guard these escaped as a traceback from
        outside every ``try``. The outer teardown layers caught them and stayed
        fail-closed, so no false green was ever possible — but the honest
        UNVERIFIED diagnostic was lost, and a direct caller would eat the raise.
        Raised by the Codex and Grok reviews of PR #3553.
        """
        result = HOOK(_position(), WALLET, gateway_client=_gateway(payload))
        assert result.unmeasured is True
        assert result.closed is False

    @pytest.mark.parametrize("symbol", ["ETH", "ETH/USD", "ETH-USD"])
    def test_every_symbol_form_the_demo_may_emit_reaches_the_same_asset(self, symbol):
        """``normalize_symbol`` is load-bearing between the demo and this hook.

        The shipped demo emits "ETH/USD"; the seed universe is keyed on "ETH".
        If normalization ever stops stripping the quote suffix, teardown goes
        unmeasured for every hyperliquid position and the VIB-6387 false-FAILED
        comes straight back — so pin all three forms on the same asset index.
        """
        from almanak.connectors.hyperliquid.markets import resolve_market
        from almanak.connectors.hyperliquid.sdk import encode_position_query

        gw = _gateway(_blob(szi=0))
        result = HOOK(_position(market=symbol), WALLET, gateway_client=gw)
        assert result.closed is True
        assert result.unmeasured is False

        # The asset index is the whole point — assert the CALLDATA, not just that
        # some call happened. Asserting only ``to == PRECOMPILE_POSITION`` would
        # pass even if every symbol resolved to a different (or wrong) market,
        # which is precisely the regression this test claims to catch.
        expected = "0x" + encode_position_query(WALLET, resolve_market("ETH").asset_index).hex()
        assert gw.eth_call.call_args.kwargs["to"] == PRECOMPILE_POSITION
        assert gw.eth_call.call_args.kwargs["data"] == expected, f"'{symbol}' did not normalize onto ETH's asset index"


class TestHookRefusesToGuess:
    """Every path where the hook lacks an input must be UNMEASURED, never closed."""

    @pytest.mark.parametrize(
        ("kwargs", "why"),
        [
            ({"market": "NOTACOIN"}, "market outside the resolvable perp universe"),
            ({"market": ""}, "no details['market'] to derive an asset index from"),
            ({"chain": ""}, "no chain to read on"),
        ],
    )
    def test_missing_inputs_are_unmeasured(self, kwargs, why):
        result = HOOK(_position(**kwargs), WALLET, gateway_client=_gateway(_blob()))
        assert result.unmeasured is True, why
        assert result.closed is False

    def test_no_gateway_client_is_unmeasured(self):
        assert HOOK(_position(), WALLET, gateway_client=None).unmeasured is True

    def test_no_wallet_is_unmeasured(self):
        assert HOOK(_position(), "   ", gateway_client=_gateway(_blob())).unmeasured is True

    def test_it_never_guesses_the_market_from_the_position_id(self):
        """A guessed market reads the WRONG asset, whose measured zero would
        certify closure of a position nobody looked at."""
        gw = _gateway(_blob(szi=0))
        result = HOOK(_position(market=""), WALLET, gateway_client=gw)
        assert result.unmeasured is True
        assert gw.eth_call.call_count == 0, "no read may be issued without a resolved market"

    def test_a_pending_order_is_unmeasured_not_a_waiver(self):
        """This hook reads positions, not HyperCore's order queue.

        Unmeasured rather than ``not_applicable``: an unfilled CoreWriter order is
        inside hyperliquid's domain and still holds risk, so the honest answer is
        doubt. Consistent with NOT declaring ``handles_pending_orders``.
        """
        result = HOOK(_position(kind="pending_order"), WALLET, gateway_client=_gateway(_blob()))
        assert result.unmeasured is True
        assert result.not_applicable is False

    def test_a_non_perp_position_is_not_applicable(self):
        """Contributes neither proof nor doubt — a perp read has no opinion on a token row."""
        result = HOOK(_position(position_type=PositionType.TOKEN), WALLET, gateway_client=_gateway(_blob()))
        assert result.not_applicable is True
        assert result.closed is False
        assert result.unmeasured is False


class TestHookIsWiredWhereTheRatchetLooks:
    def test_the_registry_resolves_the_hook_for_the_protocol_positions_carry(self):
        """The demo and the registry perp arm both emit protocol="hyperliquid"."""
        hook = get_teardown_post_condition(PROTOCOL)
        assert hook is not None, "unregistered ⇒ Plan-A's PERP branch bails and the ratchet blocks again"
        assert hook is HOOK

    def test_it_declares_open_state_reconciliation(self):
        """Plan-A's PERP branch checks this with ``is not True`` — an identity
        check, so a truthy non-bool would silently fail it."""
        assert get_teardown_post_condition(PROTOCOL).supports_open_state_reconciliation is True

    def test_it_does_not_claim_pending_order_coverage(self):
        """Declaring it would make Plan-A DEFER a pending-order residual to a hook
        that never reads the order queue — a waiver we have not earned."""
        assert getattr(get_teardown_post_condition(PROTOCOL), "handles_pending_orders", False) is not True


# ---------------------------------------------------------------------------
# Plan-A: the branch that used to bail now resolves
# ---------------------------------------------------------------------------


def _summary(*positions: PositionInfo) -> SimpleNamespace:
    return SimpleNamespace(deployment_id="deployment:919d5bab4916", positions=list(positions))


@pytest.mark.asyncio
class TestPlanAPerpBranch:
    async def _verdict(self, blob, **pos_kwargs) -> ReconciliationVerdict:
        report = await reconcile_known_positions_against_chain(
            summary=_summary(_position(**pos_kwargs)),
            gateway_client=_gateway(blob),
            market=None,
            network=CHAIN,
            wallet_address=WALLET,
            phase="post",
        )
        return report.entries[0].verdict

    async def test_a_flat_perp_is_now_diverged_closed(self):
        """Before VIB-6387 this returned UNVERIFIABLE — the banked mainnet failure."""
        assert await self._verdict(_blob(szi=0)) is ReconciliationVerdict.DIVERGED_CLOSED

    async def test_a_live_perp_is_confirmed_open(self):
        assert await self._verdict(_blob(szi=4200)) is ReconciliationVerdict.CONFIRMED_OPEN

    async def test_an_unreadable_perp_stays_unverifiable(self):
        """Fail-safe: the hook's unmeasured answer must not become a closure."""
        assert await self._verdict(RuntimeError("boom")) is ReconciliationVerdict.UNVERIFIABLE

    async def test_an_empty_return_stays_unverifiable_through_plan_a(self):
        """The empty-return trap, asserted at the layer the ratchet actually reads."""
        assert await self._verdict("0x") is ReconciliationVerdict.UNVERIFIABLE


# ---------------------------------------------------------------------------
# The ratchet: the banked mainnet verdict flips, and only on real evidence
# ---------------------------------------------------------------------------


def _manager(monkeypatch, gateway: MagicMock) -> TeardownManager:
    """A manager whose POST-teardown Plan-A runs FOR REAL against ``gateway``.

    Deliberately not a stubbed ``ReconciliationReport``: stubbing the report would
    assert the ratchet's arithmetic while skipping the hook, the registration and
    the Plan-A branch — every part this change actually touches.
    """
    mgr = TeardownManager()
    monkeypatch.setattr(mgr, "_teardown_gateway_client", lambda: gateway)
    monkeypatch.setattr(mgr, "_fresh_post_execution_market", lambda strategy, market: None)
    return mgr


@pytest.mark.asyncio
class TestTheBankedMainnetFailureIsFixed:
    async def _verify(self, monkeypatch, gateway) -> ClosureVerification:
        mgr = _manager(monkeypatch, gateway)
        # Exactly the banked shape: 1 position, all closed by execution, nothing
        # hook-proven yet, CHAIN_VERIFIED pending the POST read.
        verification = ClosureVerification(
            all_closed=True,
            positions_total=1,
            positions_closed=1,
            has_position_breakdown=True,
            verification_status=VerificationStatus.CHAIN_VERIFIED,
        )
        return await mgr.verify_closure_against_chain(
            SimpleNamespace(
                deployment_id="deployment:919d5bab4916",
                _gateway_network=CHAIN,
                get_wallet_for_chain=lambda _c: WALLET,
            ),
            verification=verification,
            pre_execution_positions=_summary(_position()),
            market=None,
        )

    async def test_a_flat_hyperliquid_teardown_now_certifies(self, monkeypatch):
        """The 2026-08-01 run, re-run against this code: FAILED becomes certified."""
        out = await self._verify(monkeypatch, _gateway(_blob(szi=0)))
        assert out.protocols_to_prove == (PROTOCOL,)
        assert out.measured_closed_protocols == (PROTOCOL,), "hyperliquid must now be PROVEN, not waived"
        assert out.unproven_protocols == ()
        assert out.closure_unknown is False
        assert out.all_closed is True

    async def test_a_residual_still_fails_the_teardown(self, monkeypatch):
        """The fix must not buy certification by becoming blind to open positions."""
        out = await self._verify(monkeypatch, _gateway(_blob(szi=4200)))
        assert out.all_closed is False
        assert out.verification_status is VerificationStatus.FAILED

    async def test_an_unreadable_book_still_blocks(self, monkeypatch):
        """Negative control — this is the 2026-08-01 verdict, and it must survive.

        If a read fault also certified, the test above would pass for the wrong
        reason: certification would prove nothing about the chain.
        """
        out = await self._verify(monkeypatch, _gateway(RuntimeError("gateway down")))
        assert out.closure_unknown is True
        assert out.unproven_protocols == (PROTOCOL,)

    async def test_an_empty_return_still_blocks(self, monkeypatch):
        """The empty-return trap at the ratchet: a zero-byte read must NOT certify."""
        out = await self._verify(monkeypatch, _gateway("0x"))
        assert out.closure_unknown is True


def test_the_hook_is_the_only_thing_standing_between_us_and_the_banked_failure(monkeypatch):
    """Mutation-style guard: unregister the hook and the 2026-08-01 failure returns.

    This pins the CAUSAL claim the report makes — that the missing registration,
    not something incidental, is what produced ``UNMEASURED for protocol(s)
    hyperliquid``. Without it, a future refactor could satisfy the ratchet by some
    other route and this file would keep passing while the fix rotted.
    """
    import asyncio

    # ONE patch, at the definition site. ``plan_a_reconciliation`` imports the
    # symbol at FUNCTION scope (lines 619 and 642), so it resolves from
    # ``post_conditions`` on every call and this patch is the one that bites.
    # A second ``setattr`` on ``plan_a_reconciliation.get_teardown_post_condition``
    # was inert — the module has no such attribute, and ``raising=False`` meant
    # it silently created one that nothing reads. An inert patch in a
    # mutation-style guard is worse than none: it reads as belt-and-braces while
    # only one belt exists, so a future move of the import to module scope would
    # look covered. Raised by the Grok review of PR #3553.
    monkeypatch.setattr(
        "almanak.framework.teardown.post_conditions.get_teardown_post_condition",
        lambda _protocol: None,
    )

    async def _run():
        report = await reconcile_known_positions_against_chain(
            summary=_summary(_position()),
            gateway_client=_gateway(_blob(szi=0)),
            market=None,
            network=CHAIN,
            wallet_address=WALLET,
            phase="post",
        )
        return report.entries[0].verdict

    assert asyncio.run(_run()) is ReconciliationVerdict.UNVERIFIABLE, (
        "with no registered hook the flat perp must be UNVERIFIABLE — the banked failure"
    )


def test_the_shipped_demo_still_supplies_the_market_the_hook_reads():
    """The hook resolves ``details['market']``; the demo must keep emitting it.

    A silent rename there would make every hyperliquid closure unmeasured again —
    fail-safe, but it would quietly restore the false FAILED. Asserted against the
    shipped source so the coupling is loud.
    """
    import inspect

    from almanak.demo_strategies.hyperliquid_trailing_perp import strategy as demo

    source = inspect.getsource(demo)
    assert '"market": self.market' in source, (
        "the demo no longer puts 'market' in PositionInfo.details — the hook cannot resolve an asset index"
    )


def test_closure_verification_replace_keeps_the_derived_property_honest():
    """``closure_unknown`` is derived, never stored — so it cannot be faked."""
    base = ClosureVerification(all_closed=True, positions_total=1, positions_closed=1)
    proven = replace(base, protocols_to_prove=(PROTOCOL,), measured_closed_protocols=(PROTOCOL,))
    unproven = replace(base, protocols_to_prove=(PROTOCOL,), measured_closed_protocols=())
    assert proven.closure_unknown is False
    assert unproven.closure_unknown is True
