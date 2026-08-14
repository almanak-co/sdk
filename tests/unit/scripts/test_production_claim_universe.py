"""Contract tests for the sealed VIB-6647 production claim universe."""

from __future__ import annotations

import json
import socket
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from almanak.connectors._connector_descriptor import ImportRef
from almanak.core.capability_obligations import SupportClaim
from almanak.framework.capabilities.effective_matrix import (
    UniverseKind,
    build_effective_capability_matrix,
    render_markdown,
)
from scripts.ci.production_claim_universe import (
    DEFAULT_PRODUCTION_INVENTORY_METADATA,
    EXPECTED_DISTRIBUTION,
    ProductionClaimUniverse,
    _parse_claim_rows,
    build_production_core_execution_matrix,
    load_production_claim_universe,
)


def _metadata() -> dict[str, Any]:
    return json.loads(DEFAULT_PRODUCTION_INVENTORY_METADATA.read_text())


def _write_metadata(tmp_path: Path, payload: dict[str, Any]) -> Path:
    path = tmp_path / "inventory.json"
    path.write_text(json.dumps(payload))
    return path


def test_approved_inventory_pins_exact_counts_hashes_and_historical_delta() -> None:
    universe = load_production_claim_universe()
    metadata = _metadata()

    assert metadata["claims"]["sha256"] == "653f94027ae19eb9a43f29f381dbe420f3e102af1f393f6d04c20df006b5f71b"
    assert metadata["surfaceDifferences"]["sha256"] == (
        "c001be79568e57add8bb097f56098df3f31bf1cfe5a6238d629eb18df0e2fd17"
    )
    assert len({cell.protocol for cell in universe.cells}) == 12
    assert len(universe.protocol_chain_pairs) == 46
    assert len(universe.cells) == 174
    assert len(universe.historical_deployed_pairs) == 43
    assert len(universe.historical_deployed_cells) == 163
    assert universe.live_deployment_status == "not_queried"

    added = set(universe.cells) - set(universe.historical_deployed_cells)
    assert len(added) == 11
    assert {(cell.protocol, cell.chain) for cell in added} == {
        ("aave_v3", "avalanche"),
        ("gmx_v2", "avalanche"),
        ("uniswap_v3", "avalanche"),
    }


def test_approved_inventory_is_exact_canonical_core_execution_surface() -> None:
    universe = load_production_claim_universe()

    assert universe.cells == tuple(sorted(universe.cells, key=lambda item: item.sort_key()))
    assert len(universe.cells) == len(set(universe.cells))
    assert all(cell.claim is SupportClaim.CORE_EXECUTION for cell in universe.cells)
    assert all(cell.exact_target_feature is None for cell in universe.cells)
    actual = {
        protocol: (
            len({pair for pair in universe.protocol_chain_pairs if pair[0] == protocol}),
            sum(cell.protocol == protocol for cell in universe.cells),
        )
        for protocol in sorted({cell.protocol for cell in universe.cells})
    }
    assert actual == EXPECTED_DISTRIBUTION


def test_production_matrix_is_injected_byte_stable_and_separate_from_registered_surface() -> None:
    first = build_production_core_execution_matrix()
    second = build_production_core_execution_matrix()

    assert first.universe.kind is UniverseKind.INJECTED_CLAIM_CELLS
    assert first.universe.source_ref.startswith(
        "docs/internal/reports/vib-6647-production-claim-inventory/candidate-claims.csv#"
    )
    assert len(first.cells) == 174
    assert first.to_json() == second.to_json()
    assert render_markdown(first) == render_markdown(second)

    registered = build_effective_capability_matrix()
    assert registered.universe.kind is UniverseKind.REGISTERED_STRATEGY_SUPPORT
    assert len(registered.cells) == 2148


def test_universe_carrier_rejects_pair_metadata_inconsistent_with_cells() -> None:
    universe = load_production_claim_universe()

    with pytest.raises(ValueError, match="protocol_chain_pairs must exactly match"):
        ProductionClaimUniverse(
            inventory_id=universe.inventory_id,
            source_ref=universe.source_ref,
            cells=universe.cells,
            historical_deployed_cells=universe.historical_deployed_cells,
            protocol_chain_pairs=universe.protocol_chain_pairs[:-1] + (("uniswap_v4", "ethereum"),),
            historical_deployed_pairs=universe.historical_deployed_pairs,
            live_deployment_status=universe.live_deployment_status,
        )


def test_universe_carrier_rejects_a_different_historical_subset() -> None:
    universe = load_production_claim_universe()
    wrong_delta = {
        ("spark", "ethereum"),
        ("gmx_v2", "avalanche"),
        ("pancakeswap_v3", "bsc"),
    }
    historical_cells = tuple(cell for cell in universe.cells if (cell.protocol, cell.chain) not in wrong_delta)
    historical_pairs = tuple(pair for pair in universe.protocol_chain_pairs if pair not in wrong_delta)

    with pytest.raises(ValueError, match="historical production delta"):
        ProductionClaimUniverse(
            inventory_id=universe.inventory_id,
            source_ref=universe.source_ref,
            cells=universe.cells,
            historical_deployed_cells=historical_cells,
            protocol_chain_pairs=universe.protocol_chain_pairs,
            historical_deployed_pairs=historical_pairs,
            live_deployment_status=universe.live_deployment_status,
        )


def test_production_matrix_construction_does_not_open_network_or_load_providers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("production universe validation must remain offline")

    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(ImportRef, "load", forbidden)

    assert len(build_production_core_execution_matrix().cells) == 174


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        (lambda value: value.__setitem__("schemaVersion", True), "schemaVersion"),
        (lambda value: value.__setitem__("inventoryId", "replacement"), "inventoryId"),
        (lambda value: value["claims"].__setitem__("sha256", "0" * 64), "claims hash mismatch"),
        (
            lambda value: value["claims"].__setitem__("path", "docs/internal/ACCOUNTING-PLAN.md"),
            "claims hash mismatch",
        ),
        (lambda value: value["claims"].__setitem__("cellCount", 173), "claims.cellCount"),
        (
            lambda value: value["sourceSnapshots"][0].__setitem__("platformCommit", "0" * 40),
            "reviewed Git seed snapshots exactly",
        ),
        (
            lambda value: value["sdkSources"].__setitem__("runtimePackage", "2.25.2"),
            "reviewed catalogue and runtime revisions exactly",
        ),
        (lambda value: value["historical43ToApproved46Delta"].reverse(), "historical delta"),
        (
            lambda value: value["liveDeploymentVerification"].__setitem__("status", "verified"),
            "explicitly not_queried",
        ),
    ],
)
def test_malformed_or_drifted_metadata_fails_closed(
    tmp_path: Path,
    mutation: Callable[[dict[str, Any]], None],
    match: str,
) -> None:
    payload = _metadata()
    mutation(payload)

    with pytest.raises(ValueError, match=match):
        load_production_claim_universe(_write_metadata(tmp_path, payload))


def test_claim_csv_rejects_noncanonical_order_duplicate_and_unknown_intent() -> None:
    header = b"protocol,chain,primitive,intent,claim\n"
    row_a = b"test,ethereum,swap,SWAP,core_execution\n"
    row_b = b"test,arbitrum,swap,SWAP,core_execution\n"

    with pytest.raises(ValueError, match="canonical source order"):
        _parse_claim_rows(header + row_a + row_b)
    with pytest.raises(ValueError, match="duplicate"):
        _parse_claim_rows(header + row_a + row_a)
    with pytest.raises(ValueError, match="unknown intent"):
        _parse_claim_rows(header + b"test,ethereum,swap,NOPE,core_execution\n")
