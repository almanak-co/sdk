"""Authentic report fixture executes the reusable Arbitrum swap reference."""

from tests import gmx_swap_path
from tests.gmx_swap_path import assert_closed as assert_closed
from tests.gmx_swap_path import assert_holding as assert_holding
from tests.gmx_swap_path import entered as entered

crash_operation = "gmx_create_swap"
crash_stage = "broadcast"


def drive_external(node, journal, running):
    gmx_swap_path.drive_external(node, journal, running, "arbitrum")
