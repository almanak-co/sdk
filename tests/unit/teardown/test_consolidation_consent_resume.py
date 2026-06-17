"""VIB-5174 — persist manual consolidation consent across a teardown crash-resume.

The swap-back clamp (ALM-2766) is disabled for an operator-initiated MANUAL
consolidation via ``consolidation_consent = not is_auto_mode``, passed only on
the INITIAL ``run_token_consolidation`` → ``_execute_intents`` call. Before this
fix, a crash mid-consolidation made ``resume()`` re-enter ``_execute_intents``
with the default ``consolidation_consent=False`` → the consented full-wallet
sweep re-clamped and under-swept on resume.

These tests prove consent now survives:

* the model-level ``config_json`` envelope helpers (encode/decode);
* the SQLite adapter save/get round-trip;
* the gateway-serialization JSON round-trip (hosted path);
* ``resume()`` re-reads the persisted flag and threads it into
  ``_execute_intents`` — True for a resumed MANUAL consolidation tail (no
  re-clamp), False for an AUTOMATIC teardown (clamp stays on).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.teardown.models import (
    TeardownMode,
    TeardownState,
    TeardownStatus,
    decode_consolidation_consent,
    encode_consolidation_consent,
)
from almanak.framework.teardown.serialization import (
    teardown_state_from_json,
    teardown_state_to_json,
)
from almanak.framework.teardown.state_manager import TeardownStateAdapter
from almanak.framework.teardown.teardown_manager import TeardownManager


def _make_state(
    *,
    consolidation_consent: bool,
    config_json: str = "{}",
    deployment_id: str = "deployment:abc123",
    teardown_id: str = "td_consent",
) -> TeardownState:
    now = datetime.now(UTC)
    return TeardownState(
        teardown_id=teardown_id,
        deployment_id=deployment_id,
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=2,
        completed_intents=1,
        current_intent_index=1,
        started_at=now,
        updated_at=now,
        completed_at=None,
        pending_intents_json='[{"intent_type": "SWAP"}]',
        intent_results=[],
        cancel_window_until=None,
        config_json=config_json,
        consolidation_consent=consolidation_consent,
    )


# ---------------------------------------------------------------------------
# 1. config_json envelope helpers
# ---------------------------------------------------------------------------


class TestConfigJsonEnvelope:
    def test_encode_then_decode_true(self) -> None:
        encoded = encode_consolidation_consent('{"target_token": "USDC"}', True)
        assert decode_consolidation_consent(encoded) is True

    def test_encode_then_decode_false(self) -> None:
        encoded = encode_consolidation_consent('{"target_token": "USDC"}', False)
        assert decode_consolidation_consent(encoded) is False

    def test_encode_preserves_existing_config_content(self) -> None:
        import json

        encoded = encode_consolidation_consent('{"target_token": "USDC", "x": 1}', True)
        data = json.loads(encoded)
        # Existing config keys are preserved alongside the reserved consent key.
        assert data["target_token"] == "USDC"
        assert data["x"] == 1
        assert data["__consolidation_consent__"] is True

    def test_encode_is_idempotent(self) -> None:
        once = encode_consolidation_consent('{"a": 1}', True)
        twice = encode_consolidation_consent(once, True)
        assert once == twice
        assert decode_consolidation_consent(twice) is True

    def test_encode_false_removes_reserved_key(self) -> None:
        import json

        with_consent = encode_consolidation_consent('{"a": 1}', True)
        without = encode_consolidation_consent(with_consent, False)
        data = json.loads(without)
        assert "__consolidation_consent__" not in data
        assert data["a"] == 1

    def test_decode_pre_feature_row_is_false(self) -> None:
        # A plain TeardownConfig snapshot (no reserved key) predates the feature.
        assert decode_consolidation_consent('{"target_token": "USDC"}') is False

    def test_decode_tolerates_empty_and_corrupt(self) -> None:
        assert decode_consolidation_consent("") is False
        assert decode_consolidation_consent("not json{{{") is False
        assert decode_consolidation_consent("[1, 2, 3]") is False  # non-object

    def test_encode_replaces_corrupt_snapshot(self) -> None:
        # A corrupt/non-object snapshot is replaced rather than crashing.
        encoded = encode_consolidation_consent("not json", True)
        assert decode_consolidation_consent(encoded) is True

    def test_decode_requires_literal_json_true(self) -> None:
        # Money-path hardening (CodeRabbit): the consent flag DISABLES the
        # ALM-2766 swap-back clamp, so only the literal JSON boolean ``true``
        # grants consent. A malformed / externally-written snapshot carrying a
        # truthy-but-not-true value must default to ``False`` (clamp stays on).
        for raw in (
            '{"__consolidation_consent__": "true"}',
            '{"__consolidation_consent__": "false"}',
            '{"__consolidation_consent__": 1}',
            '{"__consolidation_consent__": "1"}',
            '{"__consolidation_consent__": "yes"}',
            '{"__consolidation_consent__": [1]}',
        ):
            assert decode_consolidation_consent(raw) is False, raw
        # Only the exact boolean true grants it.
        assert decode_consolidation_consent('{"__consolidation_consent__": true}') is True


# ---------------------------------------------------------------------------
# 2. SQLite adapter round-trip
# ---------------------------------------------------------------------------


class TestSqliteConsentRoundTrip:
    @pytest.fixture
    def adapter(self, tmp_path: Path) -> TeardownStateAdapter:
        return TeardownStateAdapter(db_path=tmp_path / "state.db")

    @pytest.mark.asyncio
    async def test_consent_true_round_trips(self, adapter: TeardownStateAdapter) -> None:
        state = _make_state(consolidation_consent=True, config_json='{"target_token": "USDC"}')
        await adapter.save_teardown_state(state)
        loaded = await adapter.get_teardown_state(state.deployment_id)
        assert loaded is not None
        assert loaded.consolidation_consent is True

    @pytest.mark.asyncio
    async def test_consent_false_round_trips(self, adapter: TeardownStateAdapter) -> None:
        state = _make_state(consolidation_consent=False, config_json='{"target_token": "USDC"}')
        await adapter.save_teardown_state(state)
        loaded = await adapter.get_teardown_state(state.deployment_id)
        assert loaded is not None
        assert loaded.consolidation_consent is False

    @pytest.mark.asyncio
    async def test_default_state_defaults_false(self, adapter: TeardownStateAdapter) -> None:
        # A state constructed without the field defaults to False and round-trips.
        state = _make_state(consolidation_consent=False, config_json="{}")
        await adapter.save_teardown_state(state)
        loaded = await adapter.get_teardown_state(state.deployment_id)
        assert loaded is not None
        assert loaded.consolidation_consent is False

    @pytest.mark.asyncio
    async def test_pre_feature_row_decodes_false(self, adapter: TeardownStateAdapter) -> None:
        # Simulate a row written before VIB-5174: config_json holds a plain
        # config dict with no reserved consent key. It must read back False
        # (safe re-clamp direction), never crash.
        state = _make_state(consolidation_consent=False, config_json='{"target_token": "USDC"}')
        await adapter.save_teardown_state(state)
        loaded = await adapter.get_teardown_state(state.deployment_id)
        assert loaded is not None
        assert loaded.consolidation_consent is False


# ---------------------------------------------------------------------------
# 3. Gateway-serialization (hosted) round-trip
# ---------------------------------------------------------------------------


class TestSerializationConsentRoundTrip:
    def test_consent_true_survives_json_round_trip(self) -> None:
        state = _make_state(consolidation_consent=True, config_json='{"target_token": "USDC"}')
        rebuilt = teardown_state_from_json(teardown_state_to_json(state))
        assert rebuilt.consolidation_consent is True

    def test_consent_false_survives_json_round_trip(self) -> None:
        state = _make_state(consolidation_consent=False)
        rebuilt = teardown_state_from_json(teardown_state_to_json(state))
        assert rebuilt.consolidation_consent is False

    def test_consent_survives_consent_agnostic_pg_adapter(self) -> None:
        """Hosted twin: even if the external Postgres adapter never maps the new
        field, consent rides config_json (a mapped column) and is recovered by
        the framework-side decode at the final hop."""
        state = _make_state(consolidation_consent=True, config_json='{"target_token": "USDC"}')
        # Emulate the PG adapter reconstructing a state from columns WITHOUT the
        # consolidation_consent field — it still preserves config_json verbatim.
        wire = teardown_state_to_json(state)
        gateway_side = teardown_state_from_json(wire)
        pg_reconstructed = TeardownState(
            teardown_id=gateway_side.teardown_id,
            deployment_id=gateway_side.deployment_id,
            mode=gateway_side.mode,
            status=gateway_side.status,
            total_intents=gateway_side.total_intents,
            completed_intents=gateway_side.completed_intents,
            current_intent_index=gateway_side.current_intent_index,
            started_at=gateway_side.started_at,
            updated_at=gateway_side.updated_at,
            completed_at=gateway_side.completed_at,
            pending_intents_json=gateway_side.pending_intents_json,
            intent_results=gateway_side.intent_results,
            cancel_window_until=gateway_side.cancel_window_until,
            config_json=gateway_side.config_json,
            # consolidation_consent intentionally left at the default — the PG
            # adapter is consent-agnostic.
        )
        # On LOAD, the gateway re-serializes and the framework decodes consent
        # back out of the config_json column the PG adapter faithfully stored.
        recovered = teardown_state_from_json(teardown_state_to_json(pg_reconstructed))
        assert recovered.consolidation_consent is True


# ---------------------------------------------------------------------------
# 4. resume() threads the persisted consent into _execute_intents
# ---------------------------------------------------------------------------


def _make_strategy() -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = "deployment:abc123"
    strategy.get_open_positions.return_value = MagicMock()
    strategy.generate_teardown_intents.return_value = []
    return strategy


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("persisted_consent", "expected_threaded"),
    [
        pytest.param(True, True, id="manual-consolidation-resume-no-reclamp"),
        pytest.param(False, False, id="auto-teardown-resume-clamp-stays-on"),
    ],
)
async def test_resume_threads_persisted_consent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    persisted_consent: bool,
    expected_threaded: bool,
) -> None:
    adapter = TeardownStateAdapter(db_path=tmp_path / "state.db")
    state = _make_state(consolidation_consent=persisted_consent, config_json='{"target_token": "USDC"}')
    await adapter.save_teardown_state(state)

    manager = TeardownManager(state_manager=adapter)

    # Avoid real oracle warming — irrelevant to the consent wiring under test.
    monkeypatch.setattr(
        "almanak.framework.teardown.teardown_manager._warm_oracle_risk_first",
        lambda *a, **k: {},
    )

    captured: dict[str, object] = {}

    async def _fake_execute_intents(*_args, **kwargs):
        captured["consolidation_consent"] = kwargs.get("consolidation_consent")
        return MagicMock(success=True)

    manager._execute_intents = AsyncMock(side_effect=_fake_execute_intents)  # type: ignore[method-assign]

    result = await manager.resume("deployment:abc123", _make_strategy(), market=None)

    assert result is not None
    assert result.success is True
    # The persisted consent reaches _execute_intents verbatim: True for a
    # resumed MANUAL consolidation tail (clamp skipped → no under-sweep),
    # False for an AUTOMATIC teardown (clamp stays on).
    assert captured["consolidation_consent"] is expected_threaded


# ---------------------------------------------------------------------------
# 5. Stale-regeneration resume MUST drop stale consent (fail-safe re-clamp)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stale_regeneration_resume_drops_consent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale resume regenerates a fresh CLOSING plan — persisted MANUAL
    consolidation consent MUST NOT carry over to it.

    Regression for the multi-auditor finding: ``resume()``'s staleness branch
    regenerates ``pending_intents_json`` and resets the progress counters, but
    the regenerated plan is a brand-new closing plan, NOT the consolidation tail
    the operator consented to. If the stale ``consolidation_consent=True`` leaked
    into the regenerated ``_execute_intents`` call, every ``amount='all'``
    swap-back would run with the ALM-2766 clamp OFF and could sweep commingled
    wallet funds. The fix resets consent (field AND the ``config_json`` reserved
    key) on the regeneration path so the regenerated swap-backs re-clamp.
    """
    adapter = TeardownStateAdapter(db_path=tmp_path / "state.db")
    # Operator-granted consent on a MANUAL consolidation that then crashed.
    state = _make_state(consolidation_consent=True, config_json='{"target_token": "USDC"}')
    # Make the persisted state STALE (older than the 300s staleness threshold)
    # so resume() takes the regeneration branch.
    state.updated_at = datetime.now(UTC) - timedelta(seconds=600)
    await adapter.save_teardown_state(state)
    # Sanity: the consent rode into config_json on save (the sticky reserved key).
    persisted = await adapter.get_teardown_state("deployment:abc123")
    assert persisted is not None
    assert persisted.consolidation_consent is True
    assert decode_consolidation_consent(persisted.config_json) is True

    manager = TeardownManager(state_manager=adapter)

    monkeypatch.setattr(
        "almanak.framework.teardown.teardown_manager._warm_oracle_risk_first",
        lambda *a, **k: {},
    )

    # The regenerated CLOSING plan contains an ``amount='all'`` swap-back — the
    # exact intent the clamp protects. It must reach _execute_intents UNDER the
    # clamp (consolidation_consent=False), not consented.
    strategy = _make_strategy()
    strategy.generate_teardown_intents.return_value = [
        {"intent_type": "SWAP", "amount": "all", "chain": "arbitrum"},
    ]

    captured: dict[str, object] = {}

    async def _fake_execute_intents(*_args, **kwargs):
        captured["consolidation_consent"] = kwargs.get("consolidation_consent")
        return MagicMock(success=True)

    manager._execute_intents = AsyncMock(side_effect=_fake_execute_intents)  # type: ignore[method-assign]

    result = await manager.resume("deployment:abc123", strategy, market=None)

    assert result is not None
    # The regenerated plan was actually executed (not an early None return).
    assert "consolidation_consent" in captured
    # The critical assertion: stale consent did NOT leak into the regenerated
    # closing plan — the clamp stays ON.
    assert captured["consolidation_consent"] is False
