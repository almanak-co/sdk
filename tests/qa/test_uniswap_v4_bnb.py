"""BNB V4 prepared revoke preserves retained portfolio ownership."""

import hashlib
import json
import shutil
from pathlib import Path

from almanak.backtest.engine import Journal
from almanak.caip import ChainId
from almanak.connectors.uniswap_v4.economic_binding import Manifest, UniswapEconomic
from almanak.connectors.uniswap_v4.economic_state import State
from almanak.connectors.uniswap_v4.swap_math import exact_input
from almanak.dex_intents import RevokeExchange
from almanak.models import AssetAmount, Block
from tests.qa.retained import current_value

ROOT = Path(__file__).parents[2]
# Re-pinned from the workstation-only fullscope packet to the fresh
# integrated-source matrix evidence at 0564e0ad: the c01 trading and c02 LP
# prepared backtest cases, both passed on the integrated source. Each replay
# root holds run/manifest.json, run/state.sqlite and dataset/.
TRADING_REPLAY = (
    ROOT / "docs/internal/reports/evidence/"
    "bnb-v4-integrated-c01-backtest-pinned-uniswap_v4_bnb_trading-economic-0564e0ad-v1-bench-0564e0ad/"
    "public/cases/1086"
)
LP_REPLAY = (
    ROOT / "docs/internal/reports/evidence/"
    "bnb-v4-integrated-c02-backtest-pinned-uniswap_v4_bnb_lp-economic-0564e0ad-v1-bench-0564e0ad/"
    "public/cases/1077"
)
REPLAY = TRADING_REPLAY
REPLAYS = {
    "uniswap_v4_bnb_trading": TRADING_REPLAY,
    "uniswap_v4_bnb_lp": LP_REPLAY,
}
QUOTER_SHA256 = "a8601f8001c1681da1b95b5990dca7e6fb4148e0183a5127619882a8c38a1643"


def replay_manifest(replay: Path) -> Manifest:
    raw = current_value(json.loads((replay / "run/manifest.json").read_text()))
    raw["dataset"] = str(replay / "dataset")
    return Manifest.model_validate_json(json.dumps(raw))


def test_prepared_revoke(tmp_path):
    manifest = replay_manifest(REPLAY)
    state_path = tmp_path / "state.sqlite"
    shutil.copy2(REPLAY / "run/state.sqlite", state_path)
    state = Journal(state_path, State).load()
    prior = state.completed[0].pending
    intent = RevokeExchange[type(prior.intent.target)](target=prior.intent.target)
    pending = prior.model_copy(update={"intent": intent})
    book = UniswapEconomic.open_book(manifest)
    try:
        model = UniswapEconomic.driver(manifest.config, manifest.settings, manifest.strategy, book)
        change = model.execute(manifest.initial, pending)
    finally:
        book.close()
    assert intent.target.fund.chain.chain_id == ChainId("eip155:56")
    assert change.result.delta0 == change.result.delta1 == 0
    assert change.result.fill is None
    assert manifest.initial.portfolio.apply(change.effects) == manifest.initial.portfolio


def test_retained_quoter_comparisons_match_current_pool_math():
    for reference in ("uniswap_v4_bnb_lp", "uniswap_v4_bnb_trading"):
        replay = REPLAYS[reference]
        quoter = replay / "quoter-comparisons.json"
        assert hashlib.sha256(quoter.read_bytes()).hexdigest() == QUOTER_SHA256
        rows = json.loads(quoter.read_text())
        assert len(rows) == 18
        manifest = replay_manifest(replay)
        target = manifest.config.target("position")
        book = UniswapEconomic.open_book(manifest)
        try:
            for row in rows:
                block = Block.model_validate_json(json.dumps(row["block"]))
                record = book.at(target, block.timestamp)
                amount = AssetAmount.model_validate_json(json.dumps(row["input"]))
                calculated = exact_input(
                    record.payload,
                    amount.amount,
                    amount.asset == target.assets[0],
                )
                assert record.reference == block
                assert calculated.output == row["modeled"] == row["quoter"]
                assert list(calculated.crossed_ticks) == row["crossed_ticks"]
                assert row["equal"] is True
        finally:
            book.close()
