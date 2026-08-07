"""The PERPS scaffold must emit teardown code that reads the VENUE.

`almanak strat new --template perps` is the highest-leverage copy of this
pattern: every strategy AlmanakCode scaffolds inherits it. Before ALM-3109 the
emitted `get_open_positions()` synthesised a position from
`self._position_state == PerpsState.OPEN` and valued it at the *requested*
`position_size_usd`, so a keeper fill that reverted, was cancelled, or was lost
to a crash produced a teardown that either closed nothing or reported a phantom.

These tests scaffold a real strategy through the CLI, import the emitted module,
and drive its teardown methods — the source is not merely grepped, it is run.
"""

import ast
import importlib.util
import json
import sys
import tempfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from almanak.connectors._strategy_base.perps_read_base import (
    PerpsPositionOnChain,
    PerpsReadResult,
)
from almanak.connectors.gmx_v2 import market_catalog
from almanak.framework.cli.new_strategy import new_strategy
from tests.unit.connectors.gmx_v2.market_fixtures import prime_catalog

ETH_USD_MARKET = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
USDC_ARBITRUM = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"


@pytest.fixture(autouse=True)
def _verified_markets():
    """Prime the venue-verified market catalog (address-first migration).

    The emitted strategy probes the venue by its ``perp_market`` label while the
    fake venue rows are keyed by the audited ETH/USD market-token address, so
    the probe's match (and its notional pricing) resolves through the
    connector's process catalog. tests/unit/cli has no catalog-clear conftest —
    clear on teardown so no verified row leaks to other test files.
    """
    # Clear FIRST: a record leaked by an earlier test in the same worker must
    # not survive underneath the primed snapshot (review pin).
    market_catalog.clear()
    prime_catalog()
    try:
        yield
    finally:
        market_catalog.clear()


@pytest.fixture(scope="module")
def emitted():
    """Scaffold a perps strategy via the CLI and import the emitted module."""
    with tempfile.TemporaryDirectory() as tmpdir:
        target = Path(tmpdir) / "emitted_perps"
        result = CliRunner().invoke(
            new_strategy,
            [
                "--template", "perps",
                "--name", "emitted_perps",
                "--chain", "arbitrum",
                "--output-dir", str(target),
            ],
            env={"CI": ""},
        )
        assert result.exit_code == 0, f"scaffold failed: {result.output}"

        spec = importlib.util.spec_from_file_location(
            "emitted_perps_strategy", target / "strategy.py"
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules["emitted_perps_strategy"] = module
        spec.loader.exec_module(module)
        config = json.loads((target / "config.json").read_text(encoding="utf-8"))
        try:
            yield module, config
        finally:
            sys.modules.pop("emitted_perps_strategy", None)


def _strategy(emitted):
    """Construct the emitted strategy exactly as its own scaffolded tests do."""
    module, config = emitted
    strat = module.EmittedPerpsStrategy(
        config=config,
        chain=config.get("chain", "arbitrum"),
        wallet_address="0x" + "1" * 40,
    )
    assert strat.protocol == "gmx_v2"
    assert strat.perp_market == "ETH/USD"
    return module, strat


def _venue(strat, *, positions, ok=True, price="3000"):
    snapshot = MagicMock()
    snapshot.perp_positions.return_value = PerpsReadResult(positions=tuple(positions), ok=ok)
    snapshot.price.return_value = Decimal(price)
    strat.create_market_snapshot = lambda: snapshot
    return snapshot


def _position(*, is_long=True):
    return PerpsPositionOnChain(
        account="0x" + "1" * 40,
        market=ETH_USD_MARKET,
        collateral_token=USDC_ARBITRUM,
        size_in_usd=100 * 10**30,
        size_in_tokens=10**17,  # 0.1 ETH
        collateral_amount=50 * 10**6,
        is_long=is_long,
        borrowing_factor=0,
        funding_fee_amount_per_size=0,
        increased_at_time=0,
        decreased_at_time=0,
    )


class TestEmittedTeardownReadsTheVenue:
    def test_venue_position_the_cache_missed_is_reported_and_closed(self, emitted):
        from almanak.framework.teardown import TeardownMode

        module, strat = _strategy(emitted)
        strat._position_state = module.PerpsState.IDLE  # the cache missed the fill
        strat._is_long = True
        _venue(strat, positions=[_position(is_long=False)])

        summary = strat.get_open_positions()
        assert len(summary.positions) == 1
        row = summary.positions[0]
        # The SIDE comes from the venue, not from _is_long.
        assert row.details["is_long"] is False
        assert row.details["position_source"] == "venue"
        # A real notional: 0.1 ETH @ $3000. Zero would be dropped as dust.
        assert row.value_usd == Decimal("300.0")

        intents = strat.generate_teardown_intents(TeardownMode.SOFT)
        assert len(intents) == 1
        assert intents[0].is_long is False
        # VIB-5950 / VIB-6160: never a cached notional.
        assert intents[0].size_usd is None

    def test_measured_flat_venue_overrides_a_stale_open_cache(self, emitted):
        from almanak.framework.teardown import TeardownMode

        module, strat = _strategy(emitted)
        strat._position_state = module.PerpsState.OPEN
        _venue(strat, positions=[])

        assert strat.get_open_positions().positions == []
        assert strat.generate_teardown_intents(TeardownMode.SOFT) == []

    def test_unavailable_read_is_not_reported_as_flat(self, emitted):
        from almanak.framework.teardown import TeardownMode

        module, strat = _strategy(emitted)
        strat._position_state = module.PerpsState.OPEN
        _venue(strat, positions=[], ok=False)

        summary = strat.get_open_positions()
        assert len(summary.positions) == 1
        row = summary.positions[0]
        assert row.details["position_source"] == "strategy_cache_unverified"
        assert row.details["value_usd_unknown"] is True
        assert row.details["valuation_status"] == "no_path"
        assert row.value_usd > Decimal("0.01")
        assert len(strat.generate_teardown_intents(TeardownMode.SOFT)) == 1

    def test_teardown_posture_flag_is_not_asserted(self, emitted):
        """The emitted strategy keeps a cached fallback, so it is NOT chain-derived.

        Setting ``teardown_state_derived_from_chain = True`` would assert the open
        set is re-derived PURELY from chain and would silence both the CI lint and
        the boot warning that require the state to be persisted.
        """
        module, strat = _strategy(emitted)

        assert strat.teardown_state_derived_from_chain is False

        # AST, not a substring: the emitted header comment mentions the flag as an
        # option, so only a real class-body ASSIGNMENT counts.
        source = Path(module.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)
        assigned = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Assign)
            for target in node.targets
            if isinstance(target, ast.Name) and target.id == "teardown_state_derived_from_chain"
        ]
        assert assigned == []

        # ...and the persistence it therefore still depends on is emitted.
        assert "def get_persistent_state" in source
        assert "def load_persistent_state" in source
