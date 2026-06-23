"""Helper-isolation tests for ``almanak.gateway.services._dashboard_helpers``.

These tests are Phase 5d of the dashboard_service refactor track:

- Phase 5b extracted ``build_registry_strategy_info``, ``enrich_strategy_info``,
  and ``build_strategy_summary_kwargs`` out of ``ListStrategies`` /
  ``GetStrategyDetails``.
- Phase 5c extracted ``build_chain_health``, ``build_position_proto``, and
  ``lookup_strategy_source`` from the same RPCs.

The tests here exercise each helper in isolation, using small inline
fixtures rather than a full ``DashboardServiceServicer`` instance. They
complement — and do not overlap with — the end-to-end characterization
tests in ``test_dashboard_service.py``.

Known issues documented by assertion (current behaviour is the contract;
the assertions will catch any accidental drift):

- **#1705** (fixed): ``enrich_strategy_info`` does not rebuild
  ``info["chains"]`` from any state field, so a registry-mismatched
  chain list survives enrichment. The #1705 bug was about
  ``dashboard_service.GetStrategyDetails`` coercing tuple chains to an
  empty list at the call site; that is fixed in
  ``dashboard_service.py``. The enrichment helper is unchanged.
- **#1706** (fixed): When ``preserve_status_precedence=True``,
  ``is_paused=True`` now takes precedence over ``is_running=True``.
  A state dict with both flags set yields ``PAUSED`` — the pause signal
  is the safer default because a paused strategy advertised as
  RUNNING would mislead operators into thinking funds are actively
  managed.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.portfolio.models import (
    PortfolioSnapshot,
    PositionValue,
    TokenBalance,
    ValueConfidence,
)
from almanak.framework.teardown.models import PositionType
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services._dashboard_helpers import (
    build_chain_health,
    build_position_proto,
    build_registry_strategy_info,
    build_state_only_strategy_info,
    build_strategy_summary_kwargs,
    enrich_strategy_info,
    lookup_strategy_source,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _make_inst(
    *,
    deployment_id: str = "test_strategy",
    strategy_name: str = "test_strategy",
    chain: str = "arbitrum",
    protocol: str = "Uniswap V3",
    wallet_address: str = "0x1234",
    last_heartbeat_at: datetime | None = None,
    chain_wallets: str | None = None,
) -> MagicMock:
    """Build a minimal mock registry instance for helpers.

    Helpers only read attributes, so a ``MagicMock`` with the right
    attribute set is sufficient.
    """
    inst = MagicMock()
    inst.deployment_id = deployment_id
    inst.strategy_name = strategy_name
    inst.chain = chain
    inst.protocol = protocol
    inst.wallet_address = wallet_address
    inst.last_heartbeat_at = last_heartbeat_at
    # chain_wallets is optional — only set when provided so we can exercise
    # both the hasattr-present and hasattr-absent branches via del().
    if chain_wallets is None:
        inst.chain_wallets = ""
    else:
        inst.chain_wallets = chain_wallets
    return inst


# ---------------------------------------------------------------------------
# build_registry_strategy_info
# ---------------------------------------------------------------------------


class TestBuildRegistryStrategyInfo:
    """Covers the 5b helper ``build_registry_strategy_info``."""

    def test_happy_path_single_chain(self):
        """A fully-populated instance produces the expected dict shape."""
        hb = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        inst = _make_inst(last_heartbeat_at=hb)

        info = build_registry_strategy_info(inst, effective_status="RUNNING")

        assert info["deployment_id"] == "test_strategy"
        # strategy_name is title-cased with underscores → spaces.
        assert info["name"] == "Test Strategy"
        assert info["status"] == "RUNNING"
        assert info["chain"] == "arbitrum"
        assert info["protocol"] == "Uniswap V3"
        assert info["total_value_usd"] == "0"
        assert info["pnl_24h_usd"] == "0"
        assert info["last_action_at"] == int(hb.timestamp())
        assert info["attention_required"] is False
        assert info["attention_reason"] == ""
        assert info["is_multi_chain"] is False
        assert info["chains"] == ["arbitrum"]
        assert info["consecutive_errors"] == 0
        assert info["last_iteration_at"] == 0
        assert info["pnl_since_deploy_usd"] == ""
        assert info["wallet_address"] == "0x1234"
        assert info["chain_wallets"] == {}

    def test_stale_status_flags_attention(self):
        """``STALE`` effective status raises attention flag with a reason."""
        inst = _make_inst(last_heartbeat_at=datetime(2024, 1, 1, tzinfo=UTC))

        info = build_registry_strategy_info(inst, effective_status="STALE")

        assert info["status"] == "STALE"
        assert info["attention_required"] is True
        assert info["attention_reason"] == "Heartbeat stale"

    def test_error_status_flags_attention_but_no_reason(self):
        """``ERROR`` sets attention_required but leaves reason empty.

        Only ``STALE`` populates ``attention_reason`` in this helper. The
        enrichment step may later overwrite the reason when state points
        at an iteration failure.
        """
        inst = _make_inst(last_heartbeat_at=datetime(2024, 1, 1, tzinfo=UTC))

        info = build_registry_strategy_info(inst, effective_status="ERROR")

        assert info["status"] == "ERROR"
        assert info["attention_required"] is True
        assert info["attention_reason"] == ""

    def test_multi_chain_splits_on_comma(self):
        """A comma-delimited ``chain`` value yields a multi-chain row."""
        inst = _make_inst(
            chain="arbitrum, base , optimism",
            last_heartbeat_at=datetime(2024, 1, 1, tzinfo=UTC),
        )

        info = build_registry_strategy_info(inst, effective_status="RUNNING")

        assert info["is_multi_chain"] is True
        assert info["chains"] == ["arbitrum", "base", "optimism"]

    def test_missing_heartbeat_yields_zero_timestamp(self):
        """A ``None`` heartbeat must coalesce to ``last_action_at == 0``."""
        inst = _make_inst(last_heartbeat_at=None)

        info = build_registry_strategy_info(inst, effective_status="RUNNING")

        assert info["last_action_at"] == 0

    def test_naive_heartbeat_is_treated_as_utc(self):
        """A tz-naive ``last_heartbeat_at`` is upgraded to UTC."""
        naive = datetime(2024, 1, 1, 12, 0, 0)
        aware = naive.replace(tzinfo=UTC)
        inst = _make_inst(last_heartbeat_at=naive)

        info = build_registry_strategy_info(inst, effective_status="RUNNING")

        assert info["last_action_at"] == int(aware.timestamp())

    def test_chain_wallets_valid_json_dict(self):
        """Valid JSON object populates ``chain_wallets`` as a dict."""
        inst = _make_inst(
            chain_wallets='{"arbitrum": "0xaaa", "base": "0xbbb"}',
            last_heartbeat_at=datetime(2024, 1, 1, tzinfo=UTC),
        )

        info = build_registry_strategy_info(inst, effective_status="RUNNING")

        assert info["chain_wallets"] == {"arbitrum": "0xaaa", "base": "0xbbb"}

    def test_chain_wallets_non_dict_json_is_discarded(self):
        """Valid JSON that parses to a non-dict (e.g. a list) is dropped.

        This is the "stricter-of-the-two" hardening called out in the
        docstring — superset-safe vs the pre-refactor ListStrategies
        path, and removes the proto-time crash that the pre-refactor
        GetStrategyDetails path hit.
        """
        inst = _make_inst(
            chain_wallets='["arbitrum", "base"]',
            last_heartbeat_at=datetime(2024, 1, 1, tzinfo=UTC),
        )

        info = build_registry_strategy_info(inst, effective_status="RUNNING")

        assert info["chain_wallets"] == {}

    def test_chain_wallets_malformed_json_is_swallowed(self):
        """Malformed JSON does not propagate — ``chain_wallets`` stays empty."""
        inst = _make_inst(
            chain_wallets="{not valid json",
            last_heartbeat_at=datetime(2024, 1, 1, tzinfo=UTC),
        )

        info = build_registry_strategy_info(inst, effective_status="RUNNING")

        assert info["chain_wallets"] == {}

    def test_overflowing_heartbeat_timestamp_coalesces_to_zero(self):
        """``last_heartbeat_at.timestamp()`` raising is swallowed, not propagated.

        The helper wraps the timestamp call in
        ``except (ValueError, OSError)`` to guard against datetime objects
        that cannot be converted (extreme dates, platform quirks). Pin
        the swallowing behaviour.
        """

        class BadDatetime:
            tzinfo = UTC

            def replace(self, **kwargs):
                return self

            def timestamp(self):
                raise ValueError("out of range")

        inst = _make_inst(last_heartbeat_at=BadDatetime())

        info = build_registry_strategy_info(inst, effective_status="RUNNING")

        assert info["last_action_at"] == 0

    def test_instance_without_chain_wallets_attr(self):
        """Registry rows that pre-date the ``chain_wallets`` column still work.

        The helper guards with ``hasattr(inst, "chain_wallets")``, so a
        genuine attribute-absence must not crash. ``MagicMock`` auto-creates
        any attribute, so we ``del`` it to simulate the pre-column schema.
        """
        inst = _make_inst(last_heartbeat_at=datetime(2024, 1, 1, tzinfo=UTC))
        del inst.chain_wallets

        info = build_registry_strategy_info(inst, effective_status="RUNNING")

        assert info["chain_wallets"] == {}


# ---------------------------------------------------------------------------
# enrich_strategy_info
# ---------------------------------------------------------------------------


class TestBuildStateOnlyStrategyInfo:
    """Covers the hosted decoupled-dashboard fallback ``build_state_only_strategy_info``.

    On a standalone dashboard pod the registry/filesystem cascade misses, so
    ``GetStrategyDetails`` reconstructs a minimal ``strategy_info`` from the
    shared Postgres state/snapshot. These tests pin that contract (ALM-2732).
    """

    def _snapshot(self, *, naive: bool = False, chain: str = "base") -> PortfolioSnapshot:
        ts = datetime(2026, 6, 3, 6, 32, 0, tzinfo=None if naive else UTC)
        return PortfolioSnapshot(
            timestamp=ts,
            deployment_id="dep-1",
            total_value_usd=Decimal("5.99"),
            available_cash_usd=Decimal("0.77"),
            value_confidence=ValueConfidence.HIGH,
            chain=chain,
            wallet_balances=[TokenBalance(symbol="WETH", balance=Decimal("0.0017"), value_usd=Decimal("5.22"))],
        )

    def test_returns_none_when_no_postgres_trace(self):
        # Neither state nor snapshot → genuinely unknown → caller should 404.
        assert build_state_only_strategy_info("dep-1", None, None) is None

    def test_snapshot_only_builds_info(self):
        info = build_state_only_strategy_info("dep-1", None, self._snapshot())
        assert info is not None
        assert info["deployment_id"] == "dep-1"
        # Timestamp threads through to last_action_at.
        assert info["last_action_at"] == int(datetime(2026, 6, 3, 6, 32, 0, tzinfo=UTC).timestamp())
        # Chain is taken from the snapshot (not hardcoded blank).
        assert info["chain"] == "base"
        assert info["chains"] == ["base"]
        assert info["is_multi_chain"] is False

    def test_chain_blank_when_snapshot_has_no_chain(self):
        info = build_state_only_strategy_info("dep-1", None, self._snapshot(chain=""))
        assert info["chain"] == ""
        assert info["chains"] == []

    def test_state_only_builds_info(self):
        # State present but no snapshot is still a valid trace.
        info = build_state_only_strategy_info("dep-1", {"is_running": True}, None)
        assert info is not None
        assert info["deployment_id"] == "dep-1"

    def test_naive_snapshot_timestamp_is_tz_normalized(self):
        # A tz-naive snapshot timestamp must not raise; it's treated as UTC.
        info = build_state_only_strategy_info("dep-1", None, self._snapshot(naive=True))
        assert info["last_action_at"] == int(datetime(2026, 6, 3, 6, 32, 0, tzinfo=UTC).timestamp())

    def test_output_is_accepted_by_summary_builder(self):
        # The synthesized dict must satisfy build_strategy_summary_kwargs'
        # exact keyset — otherwise GetStrategyDetails would KeyError downstream.
        info = build_state_only_strategy_info("dep-1", None, self._snapshot())
        kwargs = build_strategy_summary_kwargs(info)
        summary = gateway_pb2.StrategySummary(**kwargs)
        assert summary.deployment_id == "dep-1"
        # Metadata not recoverable from Postgres is left neutral; chain is
        # populated from the snapshot when present.
        assert summary.name == ""
        assert summary.chain == "base"


class TestEnrichStrategyInfo:
    """Covers the 5b helper ``enrich_strategy_info``."""

    def _base_info(self, status: str = "RUNNING") -> dict:
        return {
            "deployment_id": "test_strategy",
            "name": "Test Strategy",
            "status": status,
            "chain": "arbitrum",
            "protocol": "Uniswap V3",
            "total_value_usd": "0",
            "pnl_24h_usd": "0",
            "last_action_at": 0,
            "attention_required": False,
            "attention_reason": "",
            "is_multi_chain": False,
            "chains": ["arbitrum"],
            "consecutive_errors": 0,
            "last_iteration_at": 0,
            "pnl_since_deploy_usd": "",
        }

    def test_no_state_only_portfolio_fields_merged(self):
        """With ``state=None`` only ``total_value_usd`` / ``pnl_24h_usd`` change."""
        info = self._base_info()

        enrich_strategy_info(
            info,
            state=None,
            total_value="123.45",
            pnl="6.78",
            pnl_metrics=None,
            preserve_status_precedence=True,
        )

        assert info["total_value_usd"] == "123.45"
        assert info["pnl_24h_usd"] == "6.78"
        assert info["status"] == "RUNNING"  # unchanged — no state signal
        assert info["consecutive_errors"] == 0  # unchanged — no state
        assert info["pnl_since_deploy_usd"] == ""  # pnl_metrics is None

    def test_pnl_metrics_stringified_as_decimal(self):
        """``pnl_since_deploy_usd`` mirrors ``str(pnl_metrics)`` exactly."""
        info = self._base_info()

        enrich_strategy_info(
            info,
            state=None,
            total_value="100",
            pnl="0",
            pnl_metrics=Decimal("-12.34"),
            preserve_status_precedence=True,
        )

        assert info["pnl_since_deploy_usd"] == "-12.34"

    def test_precedence_on_paused_registry_survives_iteration_error(self):
        """Registry PAUSED must NOT be downgraded to ERROR by a stale iteration.

        This is the load-bearing invariant called out in the helper
        docstring — an operator pause wins over any iteration signal.
        """
        info = self._base_info(status="PAUSED")
        state = {
            "last_iteration": {"status": "EXECUTION_FAILED"},
            "is_running": False,
            "is_paused": True,
        }

        enrich_strategy_info(
            info,
            state=state,
            total_value="1",
            pnl="0",
            pnl_metrics=None,
            preserve_status_precedence=True,
        )

        assert info["status"] == "PAUSED"
        assert info["attention_required"] is False

    def test_precedence_on_iteration_error_sets_attention(self):
        """Iteration status ``EXECUTION_FAILED`` flips ``status`` to ERROR."""
        info = self._base_info(status="RUNNING")
        state = {
            "last_iteration": {"status": "EXECUTION_FAILED"},
        }

        enrich_strategy_info(
            info,
            state=state,
            total_value="1",
            pnl="0",
            pnl_metrics=None,
            preserve_status_precedence=True,
        )

        assert info["status"] == "ERROR"
        assert info["attention_required"] is True
        assert info["attention_reason"] == "Last iteration: EXECUTION_FAILED"

    def test_precedence_on_is_running_flag(self):
        """``is_running=True`` overrides a registry-reported non-running status."""
        info = self._base_info(status="STALE")
        state = {"is_running": True}

        enrich_strategy_info(
            info,
            state=state,
            total_value="1",
            pnl="0",
            pnl_metrics=None,
            preserve_status_precedence=True,
        )

        assert info["status"] == "RUNNING"

    def test_precedence_on_is_paused_flag(self):
        """``is_paused=True`` overrides a registry-reported status."""
        info = self._base_info(status="STALE")
        state = {"is_paused": True}

        enrich_strategy_info(
            info,
            state=state,
            total_value="1",
            pnl="0",
            pnl_metrics=None,
            preserve_status_precedence=True,
        )

        assert info["status"] == "PAUSED"

    def test_precedence_paused_wins_over_running_issue_1706(self):
        """#1706 fix — when both flags are set, ``is_paused`` wins.

        A strategy carrying both ``is_running=True`` and ``is_paused=True``
        is almost certainly mid-transition; treating it as PAUSED is the
        safer default — a paused strategy advertised as RUNNING would
        mislead operators into thinking funds are actively managed.
        """
        info = self._base_info(status="STALE")
        state = {"is_running": True, "is_paused": True}

        enrich_strategy_info(
            info,
            state=state,
            total_value="1",
            pnl="0",
            pnl_metrics=None,
            preserve_status_precedence=True,
        )

        # Fixed behaviour: paused takes precedence over running.
        assert info["status"] == "PAUSED"

    def test_precedence_off_preserves_registry_status(self):
        """``preserve_status_precedence=False`` skips status derivation entirely.

        This is the ``ListStrategies`` contract: the registry-computed
        status stays authoritative even when state reports running/paused.
        """
        info = self._base_info(status="STALE")
        state = {
            "is_running": True,
            "is_paused": False,
            "last_iteration": {"status": "EXECUTION_FAILED"},
            "updated_at": "2024-01-01T00:00:00+00:00",
            "consecutive_errors": 7,
        }

        enrich_strategy_info(
            info,
            state=state,
            total_value="1",
            pnl="0",
            pnl_metrics=None,
            preserve_status_precedence=False,
        )

        # Status + attention untouched.
        assert info["status"] == "STALE"
        assert info["attention_required"] is False
        # last_action_at also untouched — only consecutive_errors/last_iteration_at flow through.
        assert info["last_action_at"] == 0
        # consecutive_errors still flows through regardless of precedence flag.
        assert info["consecutive_errors"] == 7

    def test_updated_at_flows_into_last_action_at(self):
        """A parseable ``state[updated_at]`` is converted to a unix timestamp."""
        info = self._base_info(status="RUNNING")
        ts = "2024-06-01T12:00:00+00:00"
        state = {"updated_at": ts}

        enrich_strategy_info(
            info,
            state=state,
            total_value="1",
            pnl="0",
            pnl_metrics=None,
            preserve_status_precedence=True,
        )

        expected = int(datetime.fromisoformat(ts).timestamp())
        assert info["last_action_at"] == expected

    def test_malformed_updated_at_is_silently_ignored(self):
        """A garbage ``updated_at`` string must not raise."""
        info = self._base_info(status="RUNNING")
        state = {"updated_at": "not-a-timestamp"}

        enrich_strategy_info(
            info,
            state=state,
            total_value="1",
            pnl="0",
            pnl_metrics=None,
            preserve_status_precedence=True,
        )

        # Left at the pre-call value (0 from _base_info).
        assert info["last_action_at"] == 0

    def test_consecutive_errors_bad_type_coalesces_to_zero(self):
        """A non-int/non-str ``consecutive_errors`` is clamped to 0."""
        info = self._base_info()
        state = {"consecutive_errors": "oops"}

        enrich_strategy_info(
            info,
            state=state,
            total_value="1",
            pnl="0",
            pnl_metrics=None,
            preserve_status_precedence=True,
        )

        assert info["consecutive_errors"] == 0

    def test_last_iteration_timestamp_flows_through(self):
        """A valid ``last_iteration.timestamp`` populates ``last_iteration_at``."""
        info = self._base_info()
        iteration_ts = "2024-06-01T12:00:00+00:00"
        state = {"last_iteration": {"timestamp": iteration_ts}}

        enrich_strategy_info(
            info,
            state=state,
            total_value="1",
            pnl="0",
            pnl_metrics=None,
            preserve_status_precedence=True,
        )

        expected = int(datetime.fromisoformat(iteration_ts).timestamp())
        assert info["last_iteration_at"] == expected

    def test_malformed_last_iteration_timestamp_coalesces_to_zero(self):
        """A garbage ``last_iteration.timestamp`` string is swallowed."""
        info = self._base_info()
        state = {"last_iteration": {"timestamp": "not-iso"}}

        enrich_strategy_info(
            info,
            state=state,
            total_value="1",
            pnl="0",
            pnl_metrics=None,
            preserve_status_precedence=True,
        )

        assert info["last_iteration_at"] == 0

    def test_issue_1705_chains_not_rebuilt_from_state(self):
        """#1705 — enrichment does not touch ``chains``.

        ``chains`` is populated by ``build_registry_strategy_info`` and
        never rebuilt from state. This test pins that behaviour so a
        future refactor doesn't silently start overwriting.
        """
        info = self._base_info()
        info["chains"] = ["arbitrum", "base"]
        state = {
            "chain": "optimism",
            "chains": ["optimism", "polygon"],
            "is_running": True,
        }

        enrich_strategy_info(
            info,
            state=state,
            total_value="1",
            pnl="0",
            pnl_metrics=None,
            preserve_status_precedence=True,
        )

        # chains must be unchanged — helper never reads state['chain']/['chains'].
        assert info["chains"] == ["arbitrum", "base"]


# ---------------------------------------------------------------------------
# build_strategy_summary_kwargs
# ---------------------------------------------------------------------------


class TestBuildStrategySummaryKwargs:
    """Covers the 5b helper ``build_strategy_summary_kwargs``."""

    # Golden keyset — every key the helper MUST emit when all optional
    # fields are present. If you add/remove a proto field on
    # ``StrategySummary``, update this set *and* update the helper.
    GOLDEN_KEYSET_ALL_PRESENT = frozenset(
        {
            "deployment_id",
            "name",
            "status",
            "chain",
            "protocol",
            "total_value_usd",
            "pnl_24h_usd",
            "last_action_at",
            "attention_required",
            "attention_reason",
            "is_multi_chain",
            "chains",
            "consecutive_errors",
            "last_iteration_at",
            "pnl_since_deploy_usd",
            "execution_mode",
            "paper_metrics_json",
            "wallet_address",
            "chain_wallets",
        }
    )

    # Optional-field-absent keyset — what the helper produces when the
    # registry-only optionals are missing (e.g. filesystem/paper sources).
    GOLDEN_KEYSET_OPTIONALS_ABSENT = GOLDEN_KEYSET_ALL_PRESENT - {
        "wallet_address",
        "chain_wallets",
    }

    def _full_info(self) -> dict:
        return {
            "deployment_id": "s1",
            "name": "S1",
            "status": "RUNNING",
            "chain": "arbitrum",
            "protocol": "Uniswap V3",
            "total_value_usd": "100",
            "pnl_24h_usd": "1",
            "last_action_at": 123,
            "attention_required": False,
            "attention_reason": "",
            "is_multi_chain": False,
            "chains": ["arbitrum"],
            "consecutive_errors": 0,
            "last_iteration_at": 456,
            "pnl_since_deploy_usd": "9",
            "execution_mode": "live",
            "paper_metrics_json": "",
            "wallet_address": "0x1234",
            "chain_wallets": {"arbitrum": "0x1234"},
        }

    def test_golden_keyset_when_all_present(self):
        """All 19 kwargs are emitted for a registry-origin info dict."""
        info = self._full_info()

        kwargs = build_strategy_summary_kwargs(info)

        assert set(kwargs.keys()) == self.GOLDEN_KEYSET_ALL_PRESENT

    def test_golden_keyset_without_optionals(self):
        """Filesystem/paper info (no wallet fields) emits 17 kwargs."""
        info = self._full_info()
        del info["wallet_address"]
        del info["chain_wallets"]

        kwargs = build_strategy_summary_kwargs(info)

        assert set(kwargs.keys()) == self.GOLDEN_KEYSET_OPTIONALS_ABSENT

    def test_kwargs_are_strategysummary_constructible(self):
        """The returned kwargs must successfully construct a proto.

        This is the end-to-end guarantee — if the helper emits a key the
        proto doesn't accept (or misses a required key), this raises.
        """
        info = self._full_info()

        kwargs = build_strategy_summary_kwargs(info)
        summary = gateway_pb2.StrategySummary(**kwargs)

        assert summary.deployment_id == "s1"
        assert summary.wallet_address == "0x1234"
        assert dict(summary.chain_wallets) == {"arbitrum": "0x1234"}

    def test_defaults_for_missing_optional_scalar_keys(self):
        """Missing optional scalar keys coalesce to helper-defined defaults."""
        info = {
            "deployment_id": "s1",
            "name": "S1",
            "status": "RUNNING",
            "chain": "arbitrum",
            "protocol": "Uniswap V3",
            "total_value_usd": "0",
            "pnl_24h_usd": "0",
            "last_action_at": 0,
            "attention_required": False,
            "attention_reason": "",
            "is_multi_chain": False,
            "chains": ["arbitrum"],
            # consecutive_errors / last_iteration_at / pnl_since_deploy_usd /
            # execution_mode / paper_metrics_json all intentionally absent.
        }

        kwargs = build_strategy_summary_kwargs(info)

        assert kwargs["consecutive_errors"] == 0
        assert kwargs["last_iteration_at"] == 0
        assert kwargs["pnl_since_deploy_usd"] == ""
        assert kwargs["execution_mode"] == ""
        assert kwargs["paper_metrics_json"] == ""


# ---------------------------------------------------------------------------
# build_chain_health
# ---------------------------------------------------------------------------


class TestBuildChainHealth:
    """Covers the 5c helper ``build_chain_health``."""

    def test_empty_chains_yields_empty_map(self):
        """No chains → empty map, not a default entry."""
        assert build_chain_health([]) == {}

    def test_single_chain(self):
        """Single chain → single map entry with UNKNOWN status and now ts."""
        before = int(datetime.now(UTC).timestamp())
        result = build_chain_health(["arbitrum"])
        after = int(datetime.now(UTC).timestamp())

        assert set(result.keys()) == {"arbitrum"}
        entry = result["arbitrum"]
        assert isinstance(entry, gateway_pb2.ChainHealthInfo)
        assert entry.chain == "arbitrum"
        assert entry.status == "UNKNOWN"
        # last_updated is a wall-clock read; allow a 1s window.
        assert before <= entry.last_updated <= after

    def test_multiple_chains(self):
        """Each chain gets its own ChainHealthInfo."""
        chains = ["arbitrum", "base", "optimism"]

        result = build_chain_health(chains)

        assert set(result.keys()) == set(chains)
        for c in chains:
            assert result[c].chain == c
            assert result[c].status == "UNKNOWN"


# ---------------------------------------------------------------------------
# build_position_proto
# ---------------------------------------------------------------------------


class TestBuildPositionProto:
    """Covers the 5c helper ``build_position_proto``."""

    def _make_snapshot(self, balances: list[tuple[str, str, str]]) -> PortfolioSnapshot:
        return PortfolioSnapshot(
            timestamp=datetime.now(UTC),
            deployment_id="s1",
            total_value_usd=Decimal("0"),
            available_cash_usd=Decimal("0"),
            value_confidence=ValueConfidence.HIGH,
            wallet_balances=[TokenBalance(symbol=sym, balance=bal, value_usd=val) for sym, bal, val in balances],
        )

    def test_all_none_returns_empty_position(self):
        """With no state / cache / snapshot the proto is a blank default."""
        position = build_position_proto(state=None, cached_positions=None, snapshot=None)

        assert isinstance(position, gateway_pb2.PositionInfo)
        assert len(position.token_balances) == 0
        assert len(position.strategy_positions) == 0
        # Empty scalar strings are the proto default; not "None".
        assert position.health_factor == ""
        assert position.leverage == ""

    def test_snapshot_balances_take_precedence_over_state(self):
        """Snapshot balances are the primary source; state balances ignored."""
        snapshot = self._make_snapshot([("WETH", "1", "3000")])
        state = {
            "balances": {
                "USDC": {"balance": "5000", "value_usd": "5000"},
            },
        }

        position = build_position_proto(
            state=state,
            cached_positions=None,
            snapshot=snapshot,
        )

        symbols = [b.symbol for b in position.token_balances]
        # Only snapshot balances — state dict is skipped.
        assert symbols == ["WETH"]
        assert position.token_balances[0].balance == "1"
        assert position.token_balances[0].value_usd == "3000"

    def test_state_balances_used_when_snapshot_absent(self):
        """State-dict balances fill in when no snapshot is provided."""
        state = {
            "balances": {
                "USDC": {"balance": "100", "value_usd": "100"},
                "WETH": {"balance": "0.5", "value_usd": "1500"},
            },
        }

        position = build_position_proto(
            state=state,
            cached_positions=None,
            snapshot=None,
        )

        symbols = {b.symbol for b in position.token_balances}
        assert symbols == {"USDC", "WETH"}

    def test_state_balances_used_when_snapshot_is_empty(self):
        """An empty-balances snapshot falls back to the state dict.

        ``wallet_balances=[]`` is falsy, so the snapshot-branch flag stays
        ``False`` and the state branch fills in balances.
        """
        snapshot = self._make_snapshot([])  # empty wallet_balances
        state = {
            "balances": {
                "USDC": {"balance": "50", "value_usd": "50"},
            },
        }

        position = build_position_proto(
            state=state,
            cached_positions=None,
            snapshot=snapshot,
        )

        assert [b.symbol for b in position.token_balances] == ["USDC"]

    def test_state_balances_non_dict_entries_skipped(self):
        """Non-dict balance_data values are silently dropped."""
        state = {
            "balances": {
                "USDC": "not_a_dict",  # invalid shape
                "WETH": {"balance": "1", "value_usd": "3000"},
            },
        }

        position = build_position_proto(
            state=state,
            cached_positions=None,
            snapshot=None,
        )

        # Only the well-formed row made it in.
        assert [b.symbol for b in position.token_balances] == ["WETH"]

    def test_health_factor_and_leverage_from_state(self):
        """State scalars flow onto the proto as strings."""
        state = {
            "health_factor": 1.8,
            "leverage": Decimal("2.5"),
        }

        position = build_position_proto(
            state=state,
            cached_positions=None,
            snapshot=None,
        )

        assert position.health_factor == "1.8"
        assert position.leverage == "2.5"

    def test_missing_health_factor_and_leverage_stay_empty(self):
        """Absent keys coalesce to the proto-default empty string."""
        state = {"balances": {}}  # present but empty dict

        position = build_position_proto(
            state=state,
            cached_positions=None,
            snapshot=None,
        )

        # Missing → proto default (empty string), not "None".
        assert position.health_factor == ""
        assert position.leverage == ""

    def test_cached_positions_are_extended(self):
        """Cached StrategyPosition protos are appended verbatim."""
        cached = [
            gateway_pb2.StrategyPosition(
                position_type="SUPPLY",
                position_id="aave-usdc",
                chain="arbitrum",
                protocol="Aave",
                value_usd="1000",
            ),
            gateway_pb2.StrategyPosition(
                position_type="BORROW",
                position_id="aave-weth",
                chain="arbitrum",
                protocol="Aave",
                value_usd="500",
            ),
        ]

        position = build_position_proto(
            state=None,
            cached_positions=cached,
            snapshot=None,
        )

        assert len(position.strategy_positions) == 2
        assert position.strategy_positions[0].position_id == "aave-usdc"
        assert position.strategy_positions[1].position_id == "aave-weth"

    def test_snapshot_plus_cached_positions_combine(self):
        """Wallet balances from snapshot and strategy positions from cache coexist."""
        snapshot = self._make_snapshot([("WETH", "1", "3000")])
        cached = [
            gateway_pb2.StrategyPosition(
                position_type="LP",
                position_id="uni-weth-usdc",
                chain="arbitrum",
                protocol="Uniswap V3",
                value_usd="5000",
            )
        ]

        position = build_position_proto(
            state=None,
            cached_positions=cached,
            snapshot=snapshot,
        )

        assert [b.symbol for b in position.token_balances] == ["WETH"]
        assert [p.position_id for p in position.strategy_positions] == ["uni-weth-usdc"]


# ---------------------------------------------------------------------------
# build_position_proto — FIFO-derived PT inventory (VIB-5317)
# ---------------------------------------------------------------------------


class TestBuildPositionProtoPtInventory:
    """PT inventory rows from ``snapshot.positions`` reach ``strategy_positions``.

    Mirrors the exact ``PositionValue`` shapes the valuer emits
    (``portfolio_valuer.py::_classify_pt_inventory`` for measured rows and
    ``_pt_unmeasured_row`` for unmeasured) so the proto hop is exercised against
    the real contract, not an invented detail dict.
    """

    def _snapshot_with_positions(self, positions: list[PositionValue]) -> PortfolioSnapshot:
        return PortfolioSnapshot(
            timestamp=datetime.now(UTC),
            deployment_id="s1",
            total_value_usd=Decimal("0"),
            available_cash_usd=Decimal("0"),
            value_confidence=ValueConfidence.HIGH,
            positions=positions,
        )

    def _measured_pt(self) -> PositionValue:
        # Shape from _classify_pt_inventory's measured branch.
        return PositionValue(
            position_type=PositionType.TOKEN,
            protocol="pt",
            chain="arbitrum",
            value_usd=Decimal("1050.50"),
            label="PT inventory PT-wstETH-26DEC2024",
            tokens=["PT-wstETH-26DEC2024"],
            details={
                "asset": "PT-wstETH-26DEC2024",
                "source": "pt_inventory_lots",
                "classification": "deployed_inventory",
                "pt_symbol": "PT-wstETH-26DEC2024",
                "quantity": "10.5",
                "sy_cost": "9.8",
                "days_to_maturity": 42,
                "price_confidence": "HIGH",
                "price_source": "pendle",
            },
            cost_basis_usd=Decimal("1000.00"),
            unrealized_pnl_usd=Decimal("50.50"),
        )

    def _unmeasured_pt(self) -> PositionValue:
        # Shape from _pt_unmeasured_row (Empty ≠ Zero: value_usd is a placeholder
        # 0 PAIRED WITH mark_unmeasured=True — never a measured zero).
        return PositionValue(
            position_type=PositionType.TOKEN,
            protocol="pt",
            chain="arbitrum",
            value_usd=Decimal("0"),
            label="PT inventory PT-eETH-26JUN2025",
            tokens=["PT-eETH-26JUN2025"],
            details={
                "asset": "PT-eETH-26JUN2025",
                "source": "pt_inventory_lots",
                "classification": "deployed_inventory",
                "pt_symbol": "PT-eETH-26JUN2025",
                "quantity": "3.0",
                "sy_cost": "2.9",
                "valuation_status": "no_path",
                "mark_unmeasured": True,
                "cost_basis_unmeasured": True,
                "unrealized_pnl_unmeasured": True,
                "unavailable_reason": "price_unmeasured",
            },
            cost_basis_usd=Decimal("0"),
            unrealized_pnl_usd=Decimal("0"),
        )

    def test_measured_pt_row_emits_strategy_position(self):
        """A measured PT row surfaces qty / USD mark / PnL / days / confidence."""
        snapshot = self._snapshot_with_positions([self._measured_pt()])

        position = build_position_proto(state=None, cached_positions=None, snapshot=snapshot)

        assert len(position.strategy_positions) == 1
        sp = position.strategy_positions[0]
        assert sp.position_type == "TOKEN"
        assert sp.protocol == "pt"
        assert sp.chain == "arbitrum"
        assert sp.position_id == "PT-wstETH-26DEC2024"
        # Measured → USD mark + PnL are stamped.
        assert sp.value_usd == "1050.50"
        assert sp.unrealized_pnl_usd == "50.50"
        # PT-specific drill-down details ride in the map.
        assert sp.details["quantity"] == "10.5"
        assert sp.details["days_to_maturity"] == "42"
        assert sp.details["price_confidence"] == "HIGH"
        assert sp.details["sy_cost"] == "9.8"
        # Measured row carries no unmeasured marker.
        assert "mark_unmeasured" not in sp.details

    def test_unmeasured_pt_row_has_no_fabricated_usd(self):
        """Unmeasured PT: NO USD mark / PnL string (Empty ≠ Zero), badge UNAVAILABLE."""
        snapshot = self._snapshot_with_positions([self._unmeasured_pt()])

        position = build_position_proto(state=None, cached_positions=None, snapshot=snapshot)

        assert len(position.strategy_positions) == 1
        sp = position.strategy_positions[0]
        # CRITICAL: the placeholder Decimal("0") must NOT become a displayed "$0".
        # The proto-default empty string renders as "—" downstream.
        assert sp.value_usd == ""
        assert sp.unrealized_pnl_usd == ""
        # Qty + SY cost still surface so the operator sees the holding.
        assert sp.details["quantity"] == "3.0"
        assert sp.details["sy_cost"] == "2.9"
        # Confidence badge is UNAVAILABLE, keyed on the mark_unmeasured flag.
        assert sp.details["price_confidence"] == "UNAVAILABLE"
        assert sp.details["mark_unmeasured"] == "true"

    def test_pt_rows_precede_cached_positions(self):
        """PT inventory is inserted BEFORE heartbeat-cached positions."""
        snapshot = self._snapshot_with_positions([self._measured_pt()])
        cached = [
            gateway_pb2.StrategyPosition(
                position_type="LP",
                position_id="uni-weth-usdc",
                chain="arbitrum",
                protocol="Uniswap V3",
                value_usd="5000",
            )
        ]

        position = build_position_proto(state=None, cached_positions=cached, snapshot=snapshot)

        ids = [p.position_id for p in position.strategy_positions]
        assert ids == ["PT-wstETH-26DEC2024", "uni-weth-usdc"]

    def test_non_pt_snapshot_position_is_not_emitted(self):
        """A non-PT position row in the snapshot is NOT surfaced here.

        Wallet-balance + heartbeat surfaces own non-PT rows; this hop is
        PT-only. Also a regression guard: wallet_balances + cached positions
        keep working alongside a non-PT snapshot position.
        """
        non_pt = PositionValue(
            position_type=PositionType.TOKEN,
            protocol="uniswap_v3",
            chain="arbitrum",
            value_usd=Decimal("123"),
            label="WETH/USDC LP",
            tokens=["WETH", "USDC"],
            details={"source": "lp_position"},
        )
        snapshot = PortfolioSnapshot(
            timestamp=datetime.now(UTC),
            deployment_id="s1",
            total_value_usd=Decimal("0"),
            available_cash_usd=Decimal("0"),
            value_confidence=ValueConfidence.HIGH,
            wallet_balances=[TokenBalance(symbol="WETH", balance=Decimal("1"), value_usd=Decimal("3000"))],
            positions=[non_pt],
        )
        cached = [
            gateway_pb2.StrategyPosition(
                position_type="SUPPLY", position_id="aave-usdc", chain="arbitrum", protocol="Aave", value_usd="10"
            )
        ]

        position = build_position_proto(state=None, cached_positions=cached, snapshot=snapshot)

        # Only the cached position — the non-PT snapshot row is ignored.
        assert [p.position_id for p in position.strategy_positions] == ["aave-usdc"]
        # Wallet balances unaffected.
        assert [b.symbol for b in position.token_balances] == ["WETH"]

    def test_empty_positions_emit_nothing(self):
        """A snapshot with no PT positions adds no strategy_positions."""
        snapshot = self._snapshot_with_positions([])

        position = build_position_proto(state=None, cached_positions=None, snapshot=snapshot)

        assert len(position.strategy_positions) == 0

    def test_source_marker_propagates_to_proto(self):
        """VIB-5317: the ``source`` marker is carried into the proto details map.

        Downstream display filters (CLI ``_format_pt_inventory_detail_line``,
        dashboard ``_extract_pt_inventory``) detect a PT-inventory row by
        ``source``, so a ``protocol="pendle"`` reported PT (whose protocol is NOT
        ``pt``) only reaches the surface when the marker rides the proto.
        """
        snapshot = self._snapshot_with_positions([self._measured_pt()])

        position = build_position_proto(state=None, cached_positions=None, snapshot=snapshot)

        sp = position.strategy_positions[0]
        assert sp.details["source"] == "pt_inventory_lots"

    def _reported_pendle_pt(self) -> PositionValue:
        """A REPORTED PT enriched by ``_reprice_principal_token_enriched``.

        ``protocol="pendle"`` (NOT ``pt``) — the common ``get_open_positions``
        case the first VIB-5317 impl was inert for. The valuer now stamps the
        ``source`` marker + display fields, so it must surface here just like a
        FIFO row.
        """
        return PositionValue(
            position_type=PositionType.SUPPLY,
            protocol="pendle",
            chain="arbitrum",
            value_usd=Decimal("2000.00"),
            label="pendle supply",
            tokens=["PT-wstETH"],
            details={
                "source": "pt_inventory_lots",
                "classification": "deployed_inventory",
                "pt_symbol": "PT-wstETH",
                "quantity": "20.0",
                "days_to_maturity": 180,
                "price_confidence": "HIGH",
            },
            cost_basis_usd=Decimal("1900.00"),
            unrealized_pnl_usd=Decimal("100.00"),
        )

    def test_reported_pendle_pt_is_emitted(self):
        """A reported PT (protocol='pendle' + source marker) reaches the proto."""
        snapshot = self._snapshot_with_positions([self._reported_pendle_pt()])

        position = build_position_proto(state=None, cached_positions=None, snapshot=snapshot)

        assert len(position.strategy_positions) == 1
        sp = position.strategy_positions[0]
        assert sp.protocol == "pendle"
        assert sp.details["source"] == "pt_inventory_lots"
        assert sp.position_id == "PT-wstETH"
        assert sp.value_usd == "2000.00"
        assert sp.unrealized_pnl_usd == "100.00"
        assert sp.details["quantity"] == "20.0"
        assert sp.details["days_to_maturity"] == "180"
        assert sp.details["price_confidence"] == "HIGH"


# ---------------------------------------------------------------------------
# lookup_strategy_source
# ---------------------------------------------------------------------------


class TestLookupStrategySource:
    """Covers the 5c helper ``lookup_strategy_source``.

    The helper is pure aside from the callables passed in; we stub those
    with lambdas / MagicMock rather than patching gateway internals.
    """

    def test_registry_hit_wins_over_filesystem_and_paper(self):
        """A registry hit short-circuits both discovery callables."""
        inst = _make_inst(last_heartbeat_at=datetime(2024, 1, 1, tzinfo=UTC))
        registry = MagicMock()
        registry.get.return_value = inst
        registry_getter = MagicMock(return_value=registry)

        compute_effective_status = MagicMock(return_value="RUNNING")
        discover_filesystem = MagicMock(return_value=[{"deployment_id": "test_strategy"}])
        discover_paper_sessions = MagicMock(return_value=[])

        result = lookup_strategy_source(
            deployment_id="test_strategy",
            registry_getter=registry_getter,
            compute_effective_status=compute_effective_status,
            discover_filesystem=discover_filesystem,
            discover_paper_sessions=discover_paper_sessions,
        )

        assert result is not None
        assert result["deployment_id"] == "test_strategy"
        # Fallbacks NOT invoked — registry short-circuit.
        discover_filesystem.assert_not_called()
        discover_paper_sessions.assert_not_called()
        compute_effective_status.assert_called_once_with(inst)

    def test_filesystem_hit_when_registry_empty(self):
        """Registry miss (``get`` returns None) falls through to filesystem."""
        registry = MagicMock()
        registry.get.return_value = None
        registry_getter = MagicMock(return_value=registry)

        filesystem_info = {
            "deployment_id": "fs_strategy",
            "name": "Fs",
            "status": "RUNNING",
            "chain": "arbitrum",
            "protocol": "Uniswap V3",
        }
        discover_filesystem = MagicMock(return_value=[filesystem_info])
        discover_paper_sessions = MagicMock(return_value=[])

        result = lookup_strategy_source(
            deployment_id="fs_strategy",
            registry_getter=registry_getter,
            compute_effective_status=MagicMock(),
            discover_filesystem=discover_filesystem,
            discover_paper_sessions=discover_paper_sessions,
        )

        assert result is filesystem_info
        discover_paper_sessions.assert_not_called()

    def test_paper_hit_matches_on_resolved_id(self):
        """Paper cascade matches when ``s[deployment_id] == deployment_id``."""
        registry = MagicMock()
        registry.get.return_value = None
        registry_getter = MagicMock(return_value=registry)

        paper_info = {"deployment_id": "paper:agent-xyz", "name": "P"}
        discover_filesystem = MagicMock(return_value=[])
        discover_paper_sessions = MagicMock(return_value=[paper_info])

        result = lookup_strategy_source(
            deployment_id="paper:agent-xyz",
            registry_getter=registry_getter,
            compute_effective_status=MagicMock(),
            discover_filesystem=discover_filesystem,
            discover_paper_sessions=discover_paper_sessions,
        )

        assert result is paper_info

    def test_paper_miss_on_id_mismatch_no_translation(self):
        """A paper session is found ONLY by its exact id (blueprint 29 §4).

        VIB-4722 removed the ``resolve_deployment_id`` rewrite: there is no
        "original vs resolved" id distinction. A lookup with a different id
        than the stored session genuinely misses — it is not a silent
        translation hit.
        """
        registry = MagicMock()
        registry.get.return_value = None
        registry_getter = MagicMock(return_value=registry)

        paper_info = {"deployment_id": "paper:stored-id", "name": "P"}
        discover_filesystem = MagicMock(return_value=[])
        discover_paper_sessions = MagicMock(return_value=[paper_info])

        result = lookup_strategy_source(
            deployment_id="paper:different-id",  # not the stored id
            registry_getter=registry_getter,
            compute_effective_status=MagicMock(),
            discover_filesystem=discover_filesystem,
            discover_paper_sessions=discover_paper_sessions,
        )

        assert result is None

    def test_all_miss_returns_none(self):
        """Every source empty → helper returns ``None``."""
        registry = MagicMock()
        registry.get.return_value = None
        registry_getter = MagicMock(return_value=registry)

        discover_filesystem = MagicMock(return_value=[])
        discover_paper_sessions = MagicMock(return_value=[])

        result = lookup_strategy_source(
            deployment_id="missing",
            registry_getter=registry_getter,
            compute_effective_status=MagicMock(),
            discover_filesystem=discover_filesystem,
            discover_paper_sessions=discover_paper_sessions,
        )

        assert result is None

    def test_registry_exception_falls_through_to_filesystem(self):
        """A raising registry does not propagate — filesystem is still tried."""
        registry_getter = MagicMock(side_effect=RuntimeError("db down"))

        filesystem_info = {"deployment_id": "fs_strategy", "name": "Fs"}
        discover_filesystem = MagicMock(return_value=[filesystem_info])

        result = lookup_strategy_source(
            deployment_id="fs_strategy",
            registry_getter=registry_getter,
            compute_effective_status=MagicMock(),
            discover_filesystem=discover_filesystem,
            discover_paper_sessions=MagicMock(return_value=[]),
        )

        assert result is filesystem_info


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
