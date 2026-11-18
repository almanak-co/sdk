"""Independent collateral and size changes use authentic, eligible keeper reports."""

from tests import gmx_collateral_path
from tests.gmx_collateral_path import assert_closed as assert_closed
from tests.gmx_collateral_path import assert_holding as assert_holding
from tests.gmx_collateral_path import entered as entered


def drive_external(node, journal, running):
    gmx_collateral_path.drive_external(node, journal, running, "avalanche")
