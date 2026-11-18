"""Hyperliquid phase 4 on a HyperEVM mainnet fork: resting and protective orders.

The HyperEVM side is authentic fork evidence, as in phase 3: the Safe, Roles, the
generated custody grants and every custody call run against mainnet contract code at the
reference anchor. Trading goes through the gateway path itself, with a D9 agent key the
gateway holds. Every HyperCore effect, every scripted
price and the venue's signature check come from the synthesized model (blueprint 30,
section 9) under register `hypercore-model:phase4-1`, so nothing here is evidence of
HyperCore's behaviour, Fork Qualification or promotion evidence.
"""

import json
from pathlib import Path

import pytest

from almanak.connectors.hyperliquid.custody import Custody
from almanak.connectors.hyperliquid.fork_model import REGISTER
from almanak.connectors.hyperliquid.fork_service import ModelService
from almanak.connectors.hyperliquid.trading_runtime import Trader
from almanak.gateway.signers import VerifiedSigner
from tests.fork.test_hypercore_custody import ANCHOR, Backend, clock, custody_fork
from tests.fork.test_hypercore_trading import ForkRun
from tests.hypercore_resting_support import open_store, resting_lifecycle
from tests.hypercore_trading_support import LocalSigner, ModelVenue, seed_model

pytestmark = pytest.mark.fork
ROOT = Path(__file__).resolve().parents[2]
__all__ = ["custody_fork"]


def test_resting_lifecycle_against_the_model(custody_fork, tmp_path):
    fixture = custody_fork
    meta = seed_model(fixture.model)
    with ModelService(fixture.model) as service:
        venue = ModelVenue(service.url, lambda: clock(fixture.node))
        backend = Backend(fixture, venue.info)
        start = clock(fixture.node) - 1000
        store = open_store(
            tmp_path / "hypercore.db",
            "hypercore-fork-resting",
            Custody(safe=fixture.safe),
            meta,
            start,
        )
        signer = VerifiedSigner(LocalSigner(fixture.keys), LocalSigner.address)
        report = resting_lifecycle(ForkRun(fixture, backend, store), Trader(store, venue, signer))
        venue.close()
    report.update(
        register=REGISTER,
        anchor={"block": ANCHOR[0], "hash": ANCHOR[1]},
        seed=fixture.model.doc["seed"]["source"],
        injected=fixture.model.doc["injected"],
        fired=fixture.model.doc["fired"],
        model=fixture.model.doc,
    )
    path = ROOT / "artifacts/hyperliquid-resting-fork/report.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=1, default=str) + "\n")
