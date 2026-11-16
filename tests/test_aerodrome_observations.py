import gzip
import json
from types import SimpleNamespace

import pytest

from almanak.connectors.aerodrome.models import Parameters
from almanak.connectors.aerodrome.observations import Snapshot, snapshot
from almanak.connectors.aerodrome.ownership import at_indices
from almanak.connectors.aerodrome.positions import Position
from almanak.models import AssetAmount
from tests.exchange_scope import without_exchange_budgets
from tests.test_aerodrome_reader import EVIDENCE, Replay

CAPTURE = json.loads(gzip.decompress((EVIDENCE / "snapshot.json.gz").read_bytes()))


@pytest.mark.parametrize("index", [0, 1])
def test_sdk_snapshots_reproduce_scoped_fund_pool_and_payer_inputs(index):
    row = CAPTURE["rows"][index]
    config = Parameters.model_validate_json(json.dumps(without_exchange_budgets(row["config"])))
    with Replay(index) as reader:
        reader.records.update(
            {
                json.dumps([r["method"], r["params"]], sort_keys=True): r["result"]
                for r in CAPTURE["requests"]
            }
        )
        block = reader.pin_block(CAPTURE["block"]["number"])
        backend = SimpleNamespace(config=config, reader=reader)
        observed = snapshot(backend, "pool", block)
        captured = Snapshot.model_validate_json(json.dumps(row["snapshot"]))
        # The original capture calculated current claims. Durable ownership now
        # retains the actual contract checkpoint so later accrual rounds once.
        assert at_indices(observed.position, observed.pool) == captured.position
        assert observed.model_copy(update={"position": captured.position}) == captured
        # address(1) holds the pool's permanently locked initial shares. Reading
        # that balance does not assign it to the new strategy's ownership.
        assert observed.position is not None and observed.position.shares.amount == 1000
        assert observed.owned.position is None and not observed.needs_cleanup
        assert observed.payer == config.accounts.binding("fund").profile.submitter
        assert observed.effective_at.timestamp() == block.timestamp
        retained = snapshot(backend, "pool", block, observed)
        assert retained == observed
        unquoted = snapshot(backend, "pool", block, observed.owned, quotes=False)
        assert unquoted.entry_quote is None and unquoted.exit_quote is None


def test_last_share_exit_retains_unclaimed_fund_fee_obligations():
    observed = Snapshot.model_validate_json(json.dumps(CAPTURE["rows"][0]["snapshot"]))
    target = observed.target
    position = Position(
        shares=AssetAmount(asset=target.pool.shares_asset, amount=0),
        fees0=AssetAmount(asset=target.assets[0], amount=123),
        fees1=AssetAmount(asset=target.assets[1], amount=0),
        index0=observed.pool.index0,
        index1=observed.pool.index1,
    )
    assert not position.empty
    owned = observed.owned.model_copy(update={"position": position})
    assert type(owned).model_validate_json(owned.model_dump_json()) == owned
    assert position.model_copy(
        update={"fees0": AssetAmount(asset=target.assets[0], amount=0)}
    ).empty
    foreign = position.model_copy(update={"shares": AssetAmount(asset=target.assets[0], amount=1)})
    with pytest.raises(ValueError, match="configured pool"):
        foreign.validate_target(target)
