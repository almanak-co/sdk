"""Compare sampled Curve pool math with archival contract quotes at the same block."""

from itertools import permutations

from almanak.backtest.cli import GatewayProcess
from almanak.backtest.history import HistoricalReader
from almanak.connectors.curve.abi import layout, signature
from almanak.connectors.curve.economic_binding import Manifest
from almanak.connectors.curve.economic_book import PoolBook
from almanak.connectors.curve.economic_pool import CryptoState, swap
from almanak.connectors.curve.reader import quote
from almanak.gateway.auth import read_token
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount
from tests.qa.inputs import write_json


def compare_pool(directory, env_file):
    manifest = Manifest.model_validate_json((directory / "run/manifest.json").read_text())
    dataset = manifest.dataset
    records: list[dict[str, object]] = []
    book = PoolBook(dataset, manifest.config, manifest.settings, manifest.dataset_sha256)
    try:
        compare_book(book, dataset, env_file, records)
        assert records and all(row["equal"] for row in records)
    finally:
        book.close()
        write_json(directory / "curve-contract-comparisons.json", records)


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
    pool = row.payload.pool
    block = reader.pin_block(row.reference.number)
    assert block == row.reference
    state = (
        pool.math_state(timestamp=block.timestamp)
        if target.pool.family == "stableswap_ng"
        else CryptoState(pool, block.timestamp)
    )
    records = []
    # Every ordered pair of distinct assets, in the pool's own asset order.
    for (first, asset), (second, output) in permutations(enumerate(target.assets), 2):
        records.extend(
            compare_pair(
                reader, target, block, state, pool.balances[first], first, second, asset, output
            )
        )
    return records


def compare_pair(reader, target, block, state, balance, first, second, asset, output):
    records = []
    for divisor in (1_000_000, 100_000, 10_000):
        amount = AssetAmount(asset=asset, amount=max(1, balance // divisor))
        modeled = swap(state, first, second, amount.amount)
        observed = contract_quote(reader, target.pool, block, amount, output, modeled)
        records.append(
            {
                "family": target.pool.family,
                "pool": target.pool.model_dump(mode="json"),
                "block": block.model_dump(mode="json"),
                "input": amount.model_dump(mode="json"),
                "output_asset": str(output),
                "modeled": modeled,
                "contract": observed,
                "equal": modeled == observed,
                "comparison": "exact_sample_block_without_hypothetical_overlay",
            }
        )
    return records


def contract_quote(reader, source, block, amount, output, modeled):
    try:
        return quote(reader, source, block, amount, output)
    except DataError as error:
        if error.kind != "liquidity" or modeled != 0:
            raise
    # Dust can round to zero. Require the raw archival contract measurement;
    # the refusal alone is not evidence of a measured zero amount.
    index = layout(source).index
    types = (index, index, "uint256")
    return reader.call(
        source.pool,
        signature("get_dy", types),
        block,
        inputs=types,
        args=(source.coins.index(amount.asset), source.coins.index(output), amount.amount),
    )[0]
