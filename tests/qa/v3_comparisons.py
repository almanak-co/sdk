"""Fresh independent Quoter checks over the exact prepared pool states."""

import json

from almanak.backtest.cli import GatewayProcess
from almanak.backtest.history import HistoricalReader
from almanak.connectors.uniswap_v3.economic_binding import Manifest
from almanak.connectors.uniswap_v3.economic_book import PoolBook
from almanak.connectors.uniswap_v3.reader import quote
from almanak.connectors.uniswap_v3.swap_math import exact_input
from almanak.gateway.auth import read_token
from almanak.models import AssetAmount
from tests.exchange_scope import without_exchange_budgets
from tests.qa.inputs import write_json


def compare_pool(directory, env_file):
    manifest = Manifest.model_validate_json(
        json.dumps(
            without_exchange_budgets(json.loads((directory / "run/manifest.json").read_text()))
        )
    )
    dataset = manifest.dataset
    records: list[dict[str, object]] = []
    book = PoolBook(dataset, manifest.config, manifest.settings, manifest.dataset_sha256)
    try:
        compare_book(book, dataset, env_file, records)
        assert records and all(r["equal"] for r in records)
    finally:
        book.close()
        write_json(directory / "quoter-comparisons.json", records)


def compare_book(book, dataset, env_file, records):
    with GatewayProcess(dataset, env_file) as gateway:
        for target, timeline in book.timelines.items():
            with HistoricalReader(
                target.fund.chain, gateway.endpoint, read_token(dataset / "gateway-auth.token")
            ) as reader:
                rows = timeline.rows
                for index in sorted({0, len(rows) // 2, len(rows) - 1}):
                    records.extend(compare_row(reader, target, rows[index]))


def compare_row(reader, target, row):
    block = reader.pin_block(row.reference.number)
    assert block == row.reference
    records = []
    for asset, decimals in zip(
        target.assets,
        (row.payload.pool.currency0.decimals, row.payload.pool.currency1.decimals),
        strict=True,
    ):
        for numerator in (1, 10, 100):
            amount = AssetAmount(asset=asset, amount=max(1, 10**decimals * numerator // 1000))
            result = exact_input(row.payload, amount.amount, asset == target.assets[0])
            observed = quote(
                reader,
                target.pool,
                target.contracts,
                block,
                amount,
                target.assets[1 - target.assets.index(asset)],
            )
            records.append(
                {
                    "block": block.model_dump(mode="json"),
                    "input": amount.model_dump(mode="json"),
                    "modeled": result.output,
                    "quoter": observed[0],
                    "modeled_sqrt_price_x96": result.sqrt_price_x96,
                    "quoter_sqrt_price_x96": observed[1],
                    "crossed_ticks": result.crossed_ticks,
                    "equal": (result.output, result.sqrt_price_x96) == (observed[0], observed[1]),
                }
            )
    return records
