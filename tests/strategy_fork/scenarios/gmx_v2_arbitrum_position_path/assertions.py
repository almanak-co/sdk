"""The reusable position path settles each request through real keeper execution."""

from tests import gmx_position_path
from tests.gmx_position_path import assert_closed as assert_closed
from tests.gmx_position_path import assert_holding as assert_holding
from tests.gmx_position_path import entered as entered


def drive_external(node, journal, running):
    gmx_position_path.drive_external(node, journal, running, source_for)


def source_for(request):
    return gmx_position_path.source_for(request, "arbitrum")
