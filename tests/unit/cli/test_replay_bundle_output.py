"""Golden-output contracts for replay bundle discovery and inspection."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime
from importlib import import_module
from pathlib import Path

import pytest

from almanak.framework.cli.replay import list_available_bundles, print_bundle_info
from almanak.framework.models.reproduction_bundle import (
    ActionBundle,
    ReproductionBundle,
    TransactionReceipt,
)

replay_module = import_module("almanak.framework.cli.replay")


def _bundle(*, receipt_status: int, receipt_reason: str | None) -> ReproductionBundle:
    return ReproductionBundle(
        bundle_id="bundle-20260801",
        deployment_id="deployment:abc123def456",
        failure_timestamp=datetime(2026, 8, 1, 12, 34, 56, tzinfo=UTC),
        block_number=123456,
        chain="arbitrum",
        persistent_state={"position": "open", "counter": 2},
        config={"slippage": 0.01, "mode": "dry-run"},
        action_bundle=ActionBundle(
            intent_type="SWAP",
            transactions=[{"to": "0x1"}, {"to": "0x2"}],
        ),
        transaction_hash="0xtransaction",
        receipt=TransactionReceipt(
            transaction_hash="0xtransaction",
            block_number=123456,
            block_hash="0xblock",
            status=receipt_status,
            gas_used=210000,
            effective_gas_price=1,
            revert_reason=receipt_reason,
        ),
        tenderly_trace_url="https://dashboard.tenderly.co/tx/arbitrum/0xtransaction",
        revert_reason="top-level revert",
        created_at=datetime(2026, 8, 1, 12, 35, 1, tzinfo=UTC),
    )


@pytest.mark.parametrize(
    ("receipt_status", "receipt_reason", "status_line", "reason_line"),
    [
        (0, "receipt revert", "  Status: Failed\n", "  Revert Reason: receipt revert\n"),
        (1, None, "  Status: Success\n", ""),
    ],
)
def test_print_bundle_info_full_golden_without_mutation(
    capsys: pytest.CaptureFixture[str],
    receipt_status: int,
    receipt_reason: str | None,
    status_line: str,
    reason_line: str,
) -> None:
    bundle = _bundle(receipt_status=receipt_status, receipt_reason=receipt_reason)
    before = deepcopy(bundle)
    rule = "=" * 70
    divider = "-" * 70

    print_bundle_info(bundle)

    assert capsys.readouterr().out == (
        f"\n{rule}\n"
        "BUNDLE INFORMATION (dry run)\n"
        f"{rule}\n"
        "Bundle ID: bundle-20260801\n"
        "Deployment ID: deployment:abc123def456\n"
        "Chain: arbitrum\n"
        "Block Number: 123456\n"
        "Failure Time: 2026-08-01 12:34:56+00:00\n"
        "Created At: 2026-08-01 12:35:01+00:00\n"
        f"{divider}\n"
        "Persistent State:\n"
        "  Keys: ['position', 'counter']\n"
        "  Size: 34 bytes\n"
        "\n"
        "Configuration:\n"
        "  slippage: 0.01\n"
        "  mode: dry-run\n"
        "\n"
        "Action Bundle:\n"
        "  Intent Type: SWAP\n"
        "  Transactions: 2\n"
        "\n"
        "Transaction Hash: 0xtransaction\n"
        "\n"
        "Receipt:\n"
        f"{status_line}"
        "  Gas Used: 210000\n"
        f"{reason_line}"
        "\n"
        "Tenderly Trace: https://dashboard.tenderly.co/tx/arbitrum/0xtransaction\n"
        "\n"
        "Revert Reason: top-level revert\n"
        "\n"
        "Replay Command:\n"
        "  almanak replay --bundle bundle-20260801 --chain arbitrum --block 123456 --verbose\n"
        f"{rule}\n"
    )
    assert bundle == before


def test_print_bundle_info_minimal_golden_preserves_mapping_order(
    capsys: pytest.CaptureFixture[str],
) -> None:
    bundle = ReproductionBundle(
        bundle_id="bundle-minimal",
        deployment_id="deployment:minimal",
        failure_timestamp=datetime(2026, 8, 2, 3, 4, 5, tzinfo=UTC),
        block_number=7,
        chain="base",
        persistent_state={"z-last": 2, "a-first": 1},
        config={"zeta": "last", "alpha": "first"},
        created_at=datetime(2026, 8, 2, 3, 5, 6, tzinfo=UTC),
    )
    rule = "=" * 70
    divider = "-" * 70

    print_bundle_info(bundle)

    assert capsys.readouterr().out == (
        f"\n{rule}\n"
        "BUNDLE INFORMATION (dry run)\n"
        f"{rule}\n"
        "Bundle ID: bundle-minimal\n"
        "Deployment ID: deployment:minimal\n"
        "Chain: base\n"
        "Block Number: 7\n"
        "Failure Time: 2026-08-02 03:04:05+00:00\n"
        "Created At: 2026-08-02 03:05:06+00:00\n"
        f"{divider}\n"
        "Persistent State:\n"
        "  Keys: ['z-last', 'a-first']\n"
        "  Size: 27 bytes\n"
        "\n"
        "Configuration:\n"
        "  zeta: last\n"
        "  alpha: first\n"
        "\n"
        "Replay Command:\n"
        "  almanak replay --bundle bundle-minimal --chain base --block 7 --verbose\n"
        f"{rule}\n"
    )


def test_list_available_bundles_empty_golden_includes_missing_paths(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    missing_first = tmp_path / "missing-first"
    empty = tmp_path / "empty"
    empty.mkdir()
    missing_last = tmp_path / "missing-last"
    search_paths = [missing_first, empty, missing_last]
    monkeypatch.setattr(replay_module, "DEFAULT_BUNDLE_PATHS", search_paths)

    list_available_bundles()

    assert capsys.readouterr().out == (
        "Searching for bundles...\n"
        "\n"
        "No bundles found.\n"
        "\n"
        "Bundles are stored in:\n"
        f"  - {missing_first}\n"
        f"  - {empty}\n"
        f"  - {missing_last}\n"
        "\n"
        "Bundles are automatically generated when failures occur.\n"
        "See: src/models/reproduction_bundle.py\n"
    )


def test_list_available_bundles_golden_filters_malformed_and_sorts_each_path(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    malformed = first / "0-malformed.json"
    non_object = first / "1-non-object.json"
    legacy = first / "a-legacy.json"
    canonical = first / "z-canonical.json"
    later = second / "a-later.json"
    ignored = first / "00-ignored.txt"
    malformed.write_text("{not-json", encoding="utf-8")
    non_object.write_text("[]", encoding="utf-8")
    legacy.write_text(json.dumps({"legacy": "legacy-strategy"}), encoding="utf-8")
    canonical.write_text(
        json.dumps(
            {
                "bundle_id": "canonical-bundle",
                "deployment_id": "deployment:canonical",
            }
        ),
        encoding="utf-8",
    )
    later.write_text(
        json.dumps({"bundle_id": "later-bundle", "deployment_id": "deployment:later"}),
        encoding="utf-8",
    )
    ignored.write_text(
        json.dumps({"bundle_id": "ignored", "deployment_id": "deployment:ignored"}),
        encoding="utf-8",
    )
    before = {path: path.read_bytes() for path in (malformed, non_object, legacy, canonical, later, ignored)}
    monkeypatch.setattr(replay_module, "DEFAULT_BUNDLE_PATHS", [first, second])

    list_available_bundles()

    assert capsys.readouterr().out == (
        "Searching for bundles...\n"
        "\n"
        "Found 3 bundle(s):\n"
        "\n"
        "  ID: a-legacy\n"
        "  Strategy: unknown\n"
        f"  Path: {legacy}\n"
        "\n"
        "  ID: canonical-bundle\n"
        "  Strategy: deployment:canonical\n"
        f"  Path: {canonical}\n"
        "\n"
        "  ID: later-bundle\n"
        "  Strategy: deployment:later\n"
        f"  Path: {later}\n"
        "\n"
    )
    assert {path: path.read_bytes() for path in before} == before
