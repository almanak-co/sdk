"""Contracts for the subject data-check: derivation, negative controls, seal honesty.

The mutation cases below are the induced-fault negative controls for VIB-6820:
the collector records raw hop observations and the sealer re-derives the verdict
from those bytes, so mutating one recorded hop IS the induced fault at exactly
the boundary the verdict is computed from. A check that cannot fail proves
nothing; these prove the localization can.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = REPO_ROOT / "qa_lab"

TEST_SDK = {
    "commit": "a" * 40,
    "branch": "test",
    "dirty": False,
    "sdk_version": "0.0.0-test",
    "source": "executing-worktree",
}


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def dc():
    return _load("qa_data_check_under_test", "qa_data_check.py")


@pytest.fixture(scope="module")
def qa():
    return _load("qa_coverage_data_check_test", "qa_coverage.py")


def _agreeing_payload(now: str = "2026-09-01T21:00:00+00:00") -> dict:
    """A bundle shaped like a real capture where every hop agrees."""
    return {
        "schema_version": 1,
        "kind": "data-check",
        "subject": "ARB",
        "chain": "arbitrum",
        "quote": "USD",
        "started_at_utc": now,
        "completed_at_utc": now,
        "gateway": "127.0.0.1:50961",
        "reported_price": None,
        "resolution": {
            "symbol": "ARB",
            "address": "0x912ce59144191c1204e64559fe8253a0e49e6548",
            "chain": "arbitrum",
            "decimals": 18,
            "source": "static",
            "is_verified": True,
        },
        "bounds": {
            "outlier_deviation_threshold": 0.02,
            "source": "almanak.gateway.data.price.aggregator.DEFAULT_OUTLIER_DEVIATION_THRESHOLD",
        },
        "venues": {
            "observed_at": now,
            "aggregate": {"price": "0.1095", "source": "aggregated", "confidence": 0.9, "stale": False},
            "observations": {
                "onchain_chainlink": {"price": "0.1096", "timestamp": now, "confidence": 0.95, "stale": False},
                "binance": {"price": "0.1094", "timestamp": now, "confidence": 1.0, "stale": False},
                "coingecko": {"price": "0.1095", "timestamp": now, "confidence": 1.0, "stale": False},
            },
            "sources_ok": ["onchain_chainlink", "binance", "coingecko"],
            "sources_failed": {"gmx_ticker": "venue ticker only serves symbols with no on-chain identity"},
            "outliers": [],
        },
        "hops": {
            "venue_fanout": {"value": "0.1095", "observed_at": now},
            "gateway": {"value": "0.10953", "observed_at": now, "source": "aggregated"},
            "snapshot": {"value": "0.10953", "observed_at": now},
        },
    }


def test_agreeing_hops_derive_no_mismatch(dc) -> None:
    derived = dc.derive_data_check_verdict(_agreeing_payload())
    assert derived["verdict"] == "NO_MISMATCH_REPRODUCED"
    assert derived["mismatch_hop"] is None
    assert all(row["within_bound"] for row in derived["comparisons"])


def test_induced_gateway_snapshot_mismatch_localizes_to_snapshot(dc) -> None:
    payload = _agreeing_payload()
    # Induced fault: the strategy-facing read disagrees with the gateway it
    # was served from by far more than production's own 2% bound.
    payload["hops"]["snapshot"]["value"] = "0.1210"
    derived = dc.derive_data_check_verdict(payload)
    assert derived["verdict"] == "MISMATCH_LOCALIZED"
    assert derived["mismatch_hop"] == "snapshot"
    assert derived["reason_codes"] == ["snapshot_exceeds_production_bound"]


def test_induced_fanout_gateway_mismatch_localizes_to_gateway(dc) -> None:
    payload = _agreeing_payload()
    payload["hops"]["gateway"]["value"] = "0.1210"
    payload["hops"]["snapshot"]["value"] = "0.1210"
    derived = dc.derive_data_check_verdict(payload)
    assert derived["verdict"] == "MISMATCH_LOCALIZED"
    # The mismatch ENTERS at the gateway hop; snapshot faithfully relays it.
    assert derived["mismatch_hop"] == "gateway"


def test_unobserved_hop_is_unmeasured_never_a_verdict(dc) -> None:
    payload = _agreeing_payload()
    del payload["hops"]["gateway"]["value"]
    derived = dc.derive_data_check_verdict(payload)
    assert derived["verdict"] == "UNMEASURED"
    assert derived["unmeasured_hop"] == "gateway"
    assert derived["comparisons"] == []


def test_missing_production_bound_cannot_yield_a_verdict(dc) -> None:
    payload = _agreeing_payload()
    payload["bounds"] = {}
    derived = dc.derive_data_check_verdict(payload)
    assert derived["verdict"] == "UNMEASURED"
    assert derived["reason_codes"] == ["production_bound_absent"]


def test_reported_price_is_located_or_declared_unreproduced(dc) -> None:
    payload = _agreeing_payload()
    payload["reported_price"] = "0.1095"
    located = dc.classify_reported_price(payload)
    assert located["status"] == "REPRODUCED"
    assert "hop:venue_fanout" in located["detail"]

    payload["reported_price"] = "0.5"
    assert dc.classify_reported_price(payload)["status"] == "NOT_REPRODUCED"

    payload["reported_price"] = None
    assert dc.classify_reported_price(payload)["status"] == "UNMEASURED"


def _write_bundle(tmp_path: Path, payload: dict) -> Path:
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "data-check.json").write_text(json.dumps(payload, indent=2, sort_keys=True))
    return bundle


def test_seal_rederives_and_overrules_a_lying_producer_verdict(qa, dc, tmp_path: Path, monkeypatch) -> None:
    """Seal-time verdicts come from raw observations; a producer cannot vote."""
    from datetime import UTC, datetime

    payload = _agreeing_payload()
    payload["hops"]["snapshot"]["value"] = "0.1210"  # real mismatch...
    payload["producer_verdict"] = {"verdict": "NO_MISMATCH_REPRODUCED", "comparisons": []}  # ...producer lies
    bundle = _write_bundle(tmp_path, payload)
    store = tmp_path / "store"

    target = qa.seal_data_check_bundle(
        bundle=bundle,
        store=store,
        catalog_path=qa.DEFAULT_CATALOG,
        sdk_provenance=dict(TEST_SDK),
        now=datetime(2026, 9, 1, 21, 30, tzinfo=UTC),
    )
    manifest = json.loads((target / "manifest.json").read_text())
    assert manifest["verdict"] == "MISMATCH_LOCALIZED"
    assert manifest["mismatch_hop"] == "snapshot"
    assert manifest["producer_verdict_matches"] is False
    derived_file = json.loads((target / "derived-verdict.json").read_text())
    assert derived_file["derived"]["verdict"] == "MISMATCH_LOCALIZED"

    latest = json.loads((store / "index" / "data_check_latest.json").read_text())
    row = latest["data_check.ARB.arbitrum"]
    assert row["verdict"] == "MISMATCH_LOCALIZED"
    assert "MISMATCH LOCALIZED · snapshot" in row["verdict_label"]

    report = (target / "report.html").read_text()
    assert "MISMATCH LOCALIZED · snapshot" in report
    assert "EXCEEDS BOUND" in report

    page = (store / "lab" / "data.html").read_text()
    assert "Subject data checks" in page
    assert "MISMATCH LOCALIZED · snapshot" in page
    # The absence-on-its-face rule: reason codes must reach the board row.
    assert "snapshot_exceeds_production_bound" in page


def test_seal_refuses_stale_and_future_bundles(qa, tmp_path: Path) -> None:
    from datetime import UTC, datetime

    payload = _agreeing_payload(now="2026-09-01T21:00:00+00:00")
    bundle = _write_bundle(tmp_path, payload)
    with pytest.raises(ValueError, match="24-hour sealing window"):
        qa.seal_data_check_bundle(
            bundle=bundle,
            store=tmp_path / "store-a",
            catalog_path=qa.DEFAULT_CATALOG,
            sdk_provenance=dict(TEST_SDK),
            now=datetime(2026, 9, 3, 21, 30, tzinfo=UTC),
        )
    with pytest.raises(ValueError, match="in the future"):
        qa.seal_data_check_bundle(
            bundle=bundle,
            store=tmp_path / "store-b",
            catalog_path=qa.DEFAULT_CATALOG,
            sdk_provenance=dict(TEST_SDK),
            now=datetime(2026, 9, 1, 20, 0, tzinfo=UTC),
        )


def test_seal_refuses_unknown_chain_and_missing_subject(qa, tmp_path: Path) -> None:
    from datetime import UTC, datetime

    now = datetime(2026, 9, 1, 21, 30, tzinfo=UTC)
    bad_chain = _agreeing_payload()
    bad_chain["chain"] = "dogechain"
    (tmp_path / "a").mkdir()
    with pytest.raises(ValueError, match="Unsupported Data Tests chain"):
        qa.seal_data_check_bundle(
            bundle=_write_bundle(tmp_path / "a", bad_chain),
            store=tmp_path / "store-a",
            catalog_path=qa.DEFAULT_CATALOG,
            sdk_provenance=dict(TEST_SDK),
            now=now,
        )
    no_subject = _agreeing_payload()
    no_subject["subject"] = ""
    (tmp_path / "b").mkdir()
    with pytest.raises(ValueError, match="must name its subject"):
        qa.seal_data_check_bundle(
            bundle=_write_bundle(tmp_path / "b", no_subject),
            store=tmp_path / "store-b",
            catalog_path=qa.DEFAULT_CATALOG,
            sdk_provenance=dict(TEST_SDK),
            now=now,
        )


def test_reported_price_without_production_bound_is_unmeasured(dc) -> None:
    """Never locate a reported value against a QA-invented substitute bound."""
    payload = _agreeing_payload()
    payload["reported_price"] = "0.1095"
    payload["bounds"] = {}
    located = dc.classify_reported_price(payload)
    assert located["status"] == "UNMEASURED"
    assert "production bound absent" in located["detail"]


def test_venue_fanout_records_total_failure_as_evidence(dc) -> None:
    """An unsupported chain yields an errored, empty fan-out — never a crash."""
    import asyncio

    result = asyncio.run(dc._venue_fanout("ARB", "not-a-chain", "USD", None))
    assert result["error"]
    assert result["observations"] == {}
    assert result["sources_ok"] == []
    # The unobserved hop must derive UNMEASURED, not a verdict.
    payload = _agreeing_payload()
    payload["hops"]["venue_fanout"] = {"value": None, "observed_at": result["observed_at"]}
    derived = dc.derive_data_check_verdict(payload)
    assert derived["verdict"] == "UNMEASURED"
    assert derived["unmeasured_hop"] == "venue_fanout"


def test_last_details_evidence_keys_never_cross_the_grpc_boundary() -> None:
    """The enriched aggregator diagnostics are evidence-only: PriceResponse has
    no field for them, and the GetPrice mapping forwards only the three
    original keys explicitly."""
    from almanak.gateway.proto import gateway_pb2

    fields = set(gateway_pb2.PriceResponse.DESCRIPTOR.fields_by_name)
    assert "observations" not in fields
    assert "aggregate_price" not in fields
    assert {"sources_ok", "sources_failed", "outliers"} <= fields

    source = (REPO_ROOT / "almanak" / "gateway" / "services" / "market_service.py").read_text()
    assert 'details.get("observations"' not in source
    assert 'details.get("aggregate_price"' not in source


def test_seal_derives_from_staged_bytes_not_the_pre_copy_read(qa, tmp_path: Path, monkeypatch) -> None:
    """A bundle mutated between the pre-copy read and the copy must be sealed
    by what was actually copied — the staged bytes govern the verdict."""
    import shutil as _shutil
    from datetime import UTC, datetime

    clean = _agreeing_payload()
    bundle = _write_bundle(tmp_path, clean)

    mutated = _agreeing_payload()
    mutated["hops"]["snapshot"]["value"] = "0.1210"  # mismatch lands mid-copy

    real_copytree = _shutil.copytree

    def racing_copytree(src, dst, *args, **kwargs):
        result = real_copytree(src, dst, *args, **kwargs)
        staged = Path(dst) / "data-check.json"
        if staged.is_file():  # only the sealing copy of the bundle races
            staged.write_text(json.dumps(mutated, indent=2, sort_keys=True))
        return result

    monkeypatch.setattr(qa.shutil, "copytree", racing_copytree)
    target = qa.seal_data_check_bundle(
        bundle=bundle,
        store=tmp_path / "store",
        catalog_path=qa.DEFAULT_CATALOG,
        sdk_provenance=dict(TEST_SDK),
        now=datetime(2026, 9, 1, 21, 30, tzinfo=UTC),
    )
    manifest = json.loads((target / "manifest.json").read_text())
    assert manifest["verdict"] == "MISMATCH_LOCALIZED"
    assert manifest["mismatch_hop"] == "snapshot"
    sealed_payload = json.loads((target / "data-check.json").read_text())
    assert sealed_payload["hops"]["snapshot"]["value"] == "0.1210"


def test_no_mismatch_without_venue_observations_names_the_absence(dc) -> None:
    """The stablecoin peg fast-path answers before any venue is consulted; a
    hop-consistent run with zero observations must say so on its face."""
    payload = _agreeing_payload()
    payload["venues"]["observations"] = {}
    payload["venues"]["sources_failed"] = {}
    derived = dc.derive_data_check_verdict(payload)
    assert derived["verdict"] == "NO_MISMATCH_REPRODUCED"
    assert derived["reason_codes"] == ["venue_observations_absent"]


def test_data_health_degrades_on_stale_ohlcv_upstream(qa) -> None:
    """A populated but stale candle feed is DEGRADED, never healthy PASS."""
    summary = {
        "status": "pass",
        "metrics": {
            "providers": {
                "ohlcv_source": {"source": "binance", "candle_count": 120, "stale": True},
                "price_sources": {},
            }
        },
    }
    health, degradations = qa._data_health(summary)
    assert health == "DEGRADED"
    assert any("stale beyond production" in item for item in degradations)


def test_aggregator_evidence_capture_survives_a_malformed_timestamp() -> None:
    """Evidence capture must never fail serving: a provider result carrying a
    non-datetime timestamp still prices and is still recorded."""
    import asyncio
    from decimal import Decimal as _Decimal

    from almanak.framework.data.interfaces import PriceResult
    from almanak.gateway.data.price.aggregator import PriceAggregator

    class _FakeSource:
        source_name = "fake"

        def get_cache_stats(self):
            return {}

        async def get_price(self, token, quote="USD", resolved_token=None):
            return PriceResult(
                price=_Decimal("1.23"),
                source="fake",
                timestamp="not-a-datetime",  # type: ignore[arg-type]
                confidence=1.0,
                stale=False,
            )

        async def close(self):
            return None

    aggregator = PriceAggregator(sources=[_FakeSource()])
    result = asyncio.run(aggregator.get_aggregated_price("XYZ", "USD"))
    assert result.price == _Decimal("1.23")
    details = aggregator.get_last_details("XYZ")
    assert details["observations"]["fake"]["timestamp"] == "not-a-datetime"


def test_seal_validates_the_staged_bytes_not_just_their_verdict(qa, tmp_path: Path, monkeypatch) -> None:
    """Schema and recency must hold for the STAGED payload — validating one
    copy while sealing another lets the ledger contradict the sealed bytes."""
    import shutil as _shutil
    from datetime import UTC, datetime

    real_copytree = _shutil.copytree

    def _seal_with_staged_mutation(mutation: dict, store: Path):
        store.parent.mkdir(parents=True, exist_ok=True)
        clean_bundle = _write_bundle(store.parent, _agreeing_payload())
        mutated = _agreeing_payload()
        mutated.update(mutation)

        def racing_copytree(src, dst, *args, **kwargs):
            result = real_copytree(src, dst, *args, **kwargs)
            staged = Path(dst) / "data-check.json"
            if staged.is_file():
                staged.write_text(json.dumps(mutated, indent=2, sort_keys=True))
            return result

        monkeypatch.setattr(qa.shutil, "copytree", racing_copytree)
        try:
            return qa.seal_data_check_bundle(
                bundle=clean_bundle,
                store=store,
                catalog_path=qa.DEFAULT_CATALOG,
                sdk_provenance=dict(TEST_SDK),
                now=datetime(2026, 9, 1, 21, 30, tzinfo=UTC),
            )
        finally:
            monkeypatch.setattr(qa.shutil, "copytree", real_copytree)

    with pytest.raises(ValueError, match="schema_version"):
        _seal_with_staged_mutation({"schema_version": 99}, tmp_path / "a" / "store")
    with pytest.raises(ValueError, match="24-hour sealing window"):
        _seal_with_staged_mutation({"completed_at_utc": "2019-01-01T00:00:00+00:00"}, tmp_path / "b" / "store")


def test_all_failed_venues_are_named_differently_from_unconsulted_venues(dc) -> None:
    """The peg fast-path (nothing consulted) and the total-failure synthetic
    peg (everything consulted, everything failed) are different empties."""
    unconsulted = _agreeing_payload()
    unconsulted["venues"]["observations"] = {}
    unconsulted["venues"]["sources_failed"] = {}
    assert dc.derive_data_check_verdict(unconsulted)["reason_codes"] == ["venue_observations_absent"]

    all_failed = _agreeing_payload()
    all_failed["venues"]["observations"] = {}
    all_failed["venues"]["sources_failed"] = {"binance": "down", "coingecko": "down"}
    assert dc.derive_data_check_verdict(all_failed)["reason_codes"] == ["all_venue_observations_failed"]


def test_seal_refuses_a_secret_bearing_file_injected_during_the_copy(qa, tmp_path: Path, monkeypatch) -> None:
    """Security validation holds for the STAGED bytes: a .env appearing in the
    copy window must refuse to seal, never land in the immutable store."""
    import shutil as _shutil
    from datetime import UTC, datetime

    bundle = _write_bundle(tmp_path, _agreeing_payload())
    real_copytree = _shutil.copytree

    def racing_copytree(src, dst, *args, **kwargs):
        result = real_copytree(src, dst, *args, **kwargs)
        if (Path(dst) / "data-check.json").is_file():
            (Path(dst) / ".env").write_text("ALMANAK_PRIVATE_KEY=0xdeadbeef\n")
        return result

    monkeypatch.setattr(qa.shutil, "copytree", racing_copytree)
    with pytest.raises(ValueError, match="secret-bearing"):
        qa.seal_data_check_bundle(
            bundle=bundle,
            store=tmp_path / "store",
            catalog_path=qa.DEFAULT_CATALOG,
            sdk_provenance=dict(TEST_SDK),
            now=datetime(2026, 9, 1, 21, 30, tzinfo=UTC),
        )
    day_root = tmp_path / "store" / "data-checks"
    sealed = list(day_root.rglob(".env")) if day_root.exists() else []
    assert sealed == []


def test_malformed_sources_failed_wears_the_loud_name(dc) -> None:
    """A sources_failed that is not a mapping must read as all-failed, never
    as the benign never-consulted case."""
    payload = _agreeing_payload()
    payload["venues"]["observations"] = {}
    payload["venues"]["sources_failed"] = ["binance", "coingecko"]
    assert dc.derive_data_check_verdict(payload)["reason_codes"] == ["all_venue_observations_failed"]


def test_data_health_unfetched_stale_feed_degrades_exactly_once(qa) -> None:
    """The producer's exception path sets source=unknown AND stale=True; that
    is one condition and must appear as one degradation, not two."""
    summary = {
        "status": "pass",
        "metrics": {
            "providers": {
                "ohlcv_source": {"source": "unknown", "candle_count": 0, "stale": True},
                "price_sources": {},
            }
        },
    }
    health, degradations = qa._data_health(summary)
    assert health == "DEGRADED"
    assert len(degradations) == 1
    assert "unavailable" in degradations[0]


def test_aggregator_records_an_absent_timestamp_as_null_not_the_string_none() -> None:
    import asyncio
    from decimal import Decimal as _Decimal

    from almanak.framework.data.interfaces import PriceResult
    from almanak.gateway.data.price.aggregator import PriceAggregator

    class _NoTimestampSource:
        source_name = "fake"

        def get_cache_stats(self):
            return {}

        async def get_price(self, token, quote="USD", resolved_token=None):
            return PriceResult(
                price=_Decimal("1.23"),
                source="fake",
                timestamp=None,  # type: ignore[arg-type]
                confidence=1.0,
                stale=False,
            )

        async def close(self):
            return None

    aggregator = PriceAggregator(sources=[_NoTimestampSource()])
    asyncio.run(aggregator.get_aggregated_price("XYZ", "USD"))
    assert aggregator.get_last_details("XYZ")["observations"]["fake"]["timestamp"] is None


def test_seal_refuses_a_symlink_injected_during_the_copy(qa, tmp_path: Path, monkeypatch) -> None:
    """copytree must carry links AS links so the staged symlink refusal can
    fire — dereferencing would seal the TARGET's content as an owned file."""
    import shutil as _shutil
    from datetime import UTC, datetime

    secret = tmp_path / "outside-the-bundle.txt"
    secret.write_text("ALMANAK_PRIVATE_KEY=0xdeadbeef\n")
    bundle = _write_bundle(tmp_path, _agreeing_payload())
    real_copytree = _shutil.copytree

    def racing_copytree(src, dst, *args, **kwargs):
        result = real_copytree(src, dst, *args, **kwargs)
        if (Path(dst) / "data-check.json").is_file():
            (Path(dst) / "notes.txt").symlink_to(secret)
        return result

    monkeypatch.setattr(qa.shutil, "copytree", racing_copytree)
    with pytest.raises(ValueError, match="symlink"):
        qa.seal_data_check_bundle(
            bundle=bundle,
            store=tmp_path / "store",
            catalog_path=qa.DEFAULT_CATALOG,
            sdk_provenance=dict(TEST_SDK),
            now=datetime(2026, 9, 1, 21, 30, tzinfo=UTC),
        )
    day_root = tmp_path / "store" / "data-checks"
    assert (list(day_root.rglob("notes.txt")) if day_root.exists() else []) == []


def test_copytree_carries_source_symlinks_as_links_for_the_staged_check(qa, tmp_path: Path, monkeypatch) -> None:
    """A symlink already present at copy time must arrive in the staged copy
    as a link (and refuse), never as dereferenced target content."""
    from datetime import UTC, datetime

    secret = tmp_path / "outside.txt"
    secret.write_text("ALMANAK_PRIVATE_KEY=0xdeadbeef\n")
    bundle = _write_bundle(tmp_path, _agreeing_payload())
    (bundle / "notes.txt").symlink_to(secret)

    # Bypass the pre-copy fail-fast so the STAGED check is what must refuse —
    # this simulates the link appearing after the source validation ran.
    real_validate = qa._validate_bundle
    calls = {"n": 0}

    def staged_only_validate(path):
        calls["n"] += 1
        if calls["n"] == 1:
            return None  # pre-copy call skipped (the race window)
        return real_validate(path)

    monkeypatch.setattr(qa, "_validate_bundle", staged_only_validate)
    with pytest.raises(ValueError, match="symlink"):
        qa.seal_data_check_bundle(
            bundle=bundle,
            store=tmp_path / "store",
            catalog_path=qa.DEFAULT_CATALOG,
            sdk_provenance=dict(TEST_SDK),
            now=datetime(2026, 9, 1, 21, 30, tzinfo=UTC),
        )
    # The residue assertion is what isolates the STAGED check: the downstream
    # history guard also raises on symlinks, but only AFTER os.replace has
    # published the immutable directory — a refusal that leaves the link in
    # the store is no protection at all.
    day_root = tmp_path / "store" / "data-checks"
    assert (list(day_root.rglob("notes.txt")) if day_root.exists() else []) == []


def test_seal_rejects_a_run_id_with_path_components(qa, tmp_path: Path) -> None:
    """run_id feeds os.replace via day_dir / run_id; traversal must refuse."""
    from datetime import UTC, datetime

    for case, evil in enumerate(("../../../../escaped", "a/b", "..", "  ")):
        workdir = tmp_path / f"case-{case}"
        workdir.mkdir()
        bundle = _write_bundle(workdir, _agreeing_payload())
        with pytest.raises(ValueError, match="single non-empty path component"):
            qa.seal_data_check_bundle(
                bundle=bundle,
                store=tmp_path / "store",
                catalog_path=qa.DEFAULT_CATALOG,
                sdk_provenance=dict(TEST_SDK),
                run_id=evil,
                now=datetime(2026, 9, 1, 21, 30, tzinfo=UTC),
            )
    assert not (tmp_path / "escaped").exists()


def test_unreachable_gateway_still_writes_the_bundle_with_unobserved_hops(dc, tmp_path: Path, monkeypatch) -> None:
    """A gateway that never comes up is an UNOBSERVED hop, not a lost bundle."""
    collected = dc.collect_data_check_bundle(
        subject="ARB",
        chain="not-a-chain",  # venue fan-out fails fast with no network
        output=tmp_path / "dc",
        gateway_host="127.0.0.1",
        gateway_port=1,  # nothing listens here; connect/ready fails
        timeout=2.0,
    )
    payload = json.loads((collected / "data-check.json").read_text())
    assert "error" in payload["hops"]["gateway"]
    assert "error" in payload["hops"]["snapshot"]
    assert payload["producer_verdict"]["verdict"] == "UNMEASURED"
    assert payload["producer_verdict"]["unmeasured_hop"] == "venue_fanout"


def test_aggregator_all_sources_failed_records_null_aggregate_price() -> None:
    """Empty≠Zero: total failure records aggregate_price as null, never 0."""
    import asyncio

    from almanak.framework.data.interfaces import AllDataSourcesFailed
    from almanak.gateway.data.price.aggregator import PriceAggregator

    class _FailingSource:
        source_name = "fake"

        def get_cache_stats(self):
            return {}

        async def get_price(self, token, quote="USD", resolved_token=None):
            raise RuntimeError("venue down")

        async def close(self):
            return None

    aggregator = PriceAggregator(sources=[_FailingSource()])
    with pytest.raises(AllDataSourcesFailed):
        asyncio.run(aggregator.get_aggregated_price("XYZ", "USD"))
    details = aggregator.get_last_details("XYZ")
    assert details["aggregate_price"] is None
    assert details["observations"] == {}
    assert "fake" in details["sources_failed"]
