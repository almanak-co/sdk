"""Authentic report fixture executes the reusable Arbitrum GM liquidity reference."""

from tests import gmx_liquidity_path
from tests.fork.test_gmx_liquidity_recovery import source_for
from tests.gmx_liquidity_path import assert_closed as assert_closed
from tests.gmx_liquidity_path import assert_holding as assert_holding
from tests.gmx_liquidity_path import entered as entered

crash_operation = "gmx_create_deposit"
crash_stage = "broadcast"


def drive_external(node, journal, running):
    gmx_liquidity_path.drive_external(
        node,
        journal,
        running,
        "arbitrum",
        source_for=lambda request: source_for(request, "arbitrum"),
    )
