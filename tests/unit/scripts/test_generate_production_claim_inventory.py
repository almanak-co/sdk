"""Fail-closed tests for the VIB-6647 inventory reproduction helper."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from scripts.qa.generate_production_claim_inventory import _load_production_pairs, main


def test_seed_parser_accepts_multiline_rows_and_rejects_unrecognized_rows(tmp_path: Path) -> None:
    seed = tmp_path / "seed.sql"
    seed.write_text(
        """INSERT INTO supported_protocol
        (key, name, category, chain_ids, defillama_slugs, is_onchain, requires_api_key, is_active)
        VALUES
        (
          'aave_v3', 'Aave V3', 'lending', ARRAY['1', '42161'],
          ARRAY['aave-v3'], true, false, true
        )
        ON CONFLICT (key) DO NOTHING;
        """
    )
    assert _load_production_pairs(seed, {"1": "ethereum", "42161": "arbitrum"}) == {
        ("aave_v3", "ethereum"),
        ("aave_v3", "arbitrum"),
    }

    seed.write_text(seed.read_text().replace("ON CONFLICT", "('broken'),\nON CONFLICT"))
    with pytest.raises(SystemExit, match="unrecognized supported_protocol seed row"):
        _load_production_pairs(seed, {"1": "ethereum", "42161": "arbitrum"})


@pytest.mark.parametrize(
    ("primary", "differences", "message"),
    [
        ("--check", "--differences-output", "--differences-output requires --output"),
        ("--output", "--differences-check", "--differences-check requires --check"),
    ],
)
def test_cli_rejects_mixed_primary_and_differences_modes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    primary: str,
    differences: str,
    message: str,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "generate_production_claim_inventory.py",
            "--platform-config",
            str(tmp_path / "platform.yml"),
            "--prod-seed",
            str(tmp_path / "seed.sql"),
            primary,
            str(tmp_path / "claims.csv"),
            differences,
            str(tmp_path / "differences.csv"),
        ],
    )

    with pytest.raises(SystemExit, match=message):
        main()
