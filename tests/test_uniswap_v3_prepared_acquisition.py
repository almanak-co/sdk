"""Captured historical acquisition replays through replacement read-only gateways."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.backtest.engine import Schedule
from almanak.backtest.gateway import GatewayConfig
from almanak.connectors.uniswap_v3.economic_book import PoolBook
from almanak.connectors.uniswap_v3.economic_config import DataPolicy
from almanak.connectors.uniswap_v3.economic_prepare import acquire, authorize_ranges, provision
from almanak.connectors.uniswap_v3.models import Parameters
from tests.exchange_scope import without_exchange_budgets
from tests.rebuilt_datasets import assert_rebuilt_dataset

ROOT = Path(__file__).parents[1]
ROWS = json.loads(
    (
        ROOT
        / "docs/internal/reports/evidence/2026-09-07-rollout/uniswap-v3-anchor"
        / "preparation-component.json"
    ).read_text()
)


@pytest.mark.parametrize("row", ROWS, ids=lambda row: row["network"])
def test_actual_acquisition_replays_exactly_without_provider_authority(tmp_path, row):
    config = Parameters.model_validate_json(json.dumps(without_exchange_budgets(row["config"])))
    gateway = GatewayConfig.model_validate_json(
        gzip.decompress((ROOT / row["files"]["history.json"]).read_bytes())
    )
    settings = SimpleNamespace(
        history=gateway.providers,
        data=DataPolicy.model_validate_json(json.dumps(row["data"])),
        schedule=Schedule.model_validate_json(json.dumps(row["schedule"])),
    )
    provision(tmp_path, config, settings)
    (tmp_path / "observations.sqlite").write_bytes(
        gzip.decompress((ROOT / row["files"]["observations.sqlite"]).read_bytes())
    )
    # No env file means neither gateway process receives any provider access.
    boundaries = authorize_ranges(tmp_path, config, settings, None)
    acquire(tmp_path, config, settings, None, boundaries)
    book = PoolBook(tmp_path, config, settings)
    try:
        expected = json.loads(gzip.decompress((ROOT / row["files"]["dataset.json"]).read_bytes()))
        assert_rebuilt_dataset(
            tmp_path,
            book.manifest.model_dump(mode="json"),
            expected,
            gzip.decompress((ROOT / row["files"]["data.sqlite"]).read_bytes()),
        )
        assert book.manifest.records == 3
        timeline = book.timelines[config.target("position")]
        assert tuple(r.available_at for r in timeline.rows) == tuple(
            range(settings.schedule.start, settings.schedule.deadline + 1, 60)
        )
    finally:
        book.close()
