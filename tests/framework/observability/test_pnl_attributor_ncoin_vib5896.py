"""VIB-5896 — N-coin (>2) fungible pools fail closed on the 2-token IL rail.

A Curve 3pool/4pool (or Balancer weighted) position deposits N coins, but the
``position_events`` schema and ``entry_state`` sidecar are 2-slot — the stamped
``amount0``/``amount1`` are a SUBSET of the deposit. Pre-fix,
``compute_impermanent_loss`` happily computed a subset-HODL off those two legs
and emitted a wrong ``impermanent_loss_usd`` (2-of-3 coins ⇒ IL off by the
missing coin's full notional). The fix threads the pool-coin universe
(``coin_symbols``, VIB-5429) into ``entry_state`` via ``build_entry_state`` /
``stamp_entry_state_on_open`` and gates the IL math: >2 coins ⇒ ``None``
(Empty ≠ Zero — unmeasurable, never a 2-of-N approximation). Mirrors the
identical gate in ``lp_handler._compute_lp_impermanent_loss``
(``tests/unit/framework/accounting/test_lp_perp_vault_handlers.py::
TestLpImpermanentLoss::test_lp_close_ncoin_pool_nulls_il_and_hodl``).
"""

from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.observability.pnl_attributor import (
    build_entry_state,
    compute_impermanent_loss,
    stamp_entry_state_on_open,
)


def _entry_state_json(*, coin_symbols=None, **kv) -> str:
    entry = {
        "token0": kv.get("token0", "DAI"),
        "token1": kv.get("token1", "USDC"),
        "amount0": kv.get("amount0", "100"),
        "amount1": kv.get("amount1", "100"),
        "price0": kv.get("price0", "1"),
        "price1": kv.get("price1", "1"),
    }
    if coin_symbols is not None:
        entry["coin_symbols"] = coin_symbols
    return json.dumps({"entry_state": entry})


def _close_evt(value_usd: str = "300.046") -> dict:
    return {
        "value_usd": value_usd,
        "attribution_json": json.dumps({"current_prices": {"DAI": "1", "USDC": "1", "USDT": "1"}}),
    }


class TestNcoinIlGate:
    def test_three_coin_entry_state_returns_none(self) -> None:
        """3-coin universe ⇒ IL unmeasurable on the 2-slot rail ⇒ None.

        Mutation guard: without the gate, the 2-slot math computes
        V_hodl = 100 + 100 = 200 against V_lp = 300.046 ⇒ a phantom
        +100.046 — exactly the wrong number the quant-audit observed.
        """
        open_evt = {
            "attribution_json": _entry_state_json(coin_symbols=["DAI", "USDC", "USDT"]),
        }
        assert compute_impermanent_loss(open_evt, _close_evt()) is None

    def test_two_coin_symbols_does_not_trip_gate(self) -> None:
        """A 2-entry coin_symbols has a well-defined 2-token HODL — unchanged."""
        open_evt = {
            "attribution_json": _entry_state_json(coin_symbols=["DAI", "USDC"]),
        }
        # V_hodl = 200, V_lp = 300.046 ⇒ IL = +100.046 (legitimately measured
        # here: BOTH coins are in the 2-slot state, nothing was dropped).
        assert compute_impermanent_loss(open_evt, _close_evt()) == Decimal("100.046")

    def test_absent_coin_symbols_preserves_legacy_behaviour(self) -> None:
        """Concentrated-liquidity / legacy rows carry no coin_symbols — unchanged."""
        open_evt = {"attribution_json": _entry_state_json()}
        assert compute_impermanent_loss(open_evt, _close_evt()) == Decimal("100.046")


class TestNcoinThreadingEndToEnd:
    """The full discriminator pipeline, not just its endpoints (spec-critique fix).

    ``lp_open_data.coin_symbols → _apply_lp_open → PositionEvent.coin_symbols →
    stamp_entry_state_on_open → entry_state["coin_symbols"] → IL gate``. If ANY
    link silently drops the field, attribution falls back to the legacy 2-token
    path and the phantom IL returns while every endpoint-only test still passes
    — exactly the silent failure this test exists to catch.
    """

    @staticmethod
    def _curve_open_event_via_apply_lp_open():
        from almanak.framework.execution.extracted_data import LPOpenData
        from almanak.framework.observability.position_events import (
            IntentEventContext,
            PositionEvent,
            _apply_lp_open,
        )

        lp_open = LPOpenData(
            position_id=0,
            amount0=100 * 10**18,
            amount1=100 * 10**6,
            additional_amounts={2: 100 * 10**6},
            coin_symbols=["DAI", "USDC", "USDT"],
            pool_address="0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
        )
        event = PositionEvent(
            deployment_id="deployment:test",
            position_id="curve-3pool-lp",
            position_type="LP",
            event_type="OPEN",
            protocol="curve",
            chain="ethereum",
        )
        ctx = IntentEventContext(
            intent=SimpleNamespace(intent_type="LP_OPEN"),
            result=SimpleNamespace(),
            extracted={"lp_open_data": lp_open},
            deployment_id="deployment:test",
            chain="ethereum",
            ledger_entry_id="led-1",
        )
        _apply_lp_open(event, ctx)
        return event

    def test_apply_lp_open_stamps_coin_symbols_on_event(self) -> None:
        event = self._curve_open_event_via_apply_lp_open()
        assert event.coin_symbols == ["DAI", "USDC", "USDT"]

    @pytest.mark.asyncio
    async def test_stamp_threads_coin_symbols_into_persisted_entry_state(self) -> None:
        event = self._curve_open_event_via_apply_lp_open()

        captured: dict[str, str] = {}

        class _Store:
            async def get_latest_snapshot(self, deployment_id):
                return SimpleNamespace(token_prices={"DAI": "1", "USDC": "1", "USDT": "1"})

            async def update_position_attribution(self, event_id, attribution_json, version):
                captured["json"] = attribution_json

        await stamp_entry_state_on_open(_Store(), event)

        entry = json.loads(captured["json"])["entry_state"]
        assert entry["coin_symbols"] == ["DAI", "USDC", "USDT"]
        # ...and the CLOSE-time IL gate fires off exactly this persisted shape.
        open_evt = {"attribution_json": captured["json"]}
        assert compute_impermanent_loss(open_evt, _close_evt()) is None


class TestBuildEntryStateCoinSymbols:
    def test_coin_symbols_included_when_supplied(self) -> None:
        state = build_entry_state(
            token0="DAI",
            token1="USDC",
            amount0="100",
            amount1="100",
            coin_symbols=["DAI", "USDC", "USDT"],
        )
        assert state["coin_symbols"] == ["DAI", "USDC", "USDT"]

    def test_coin_symbols_key_absent_for_two_coin_venues(self) -> None:
        """Key absent (not null) when not supplied — no fabricated placeholder."""
        state = build_entry_state(token0="WETH", token1="USDC", amount0="1", amount1="2000")
        assert "coin_symbols" not in state
