"""Authentic report fixture for the configured reusable GMX reference."""

import gzip
import json

from tests import gmx_fork
from tests.gmx_fork import assert_closed as assert_closed
from tests.gmx_fork import assert_holding as assert_holding
from tests.gmx_fork import entered as entered
from tests.retained_fixtures import retained


def drive_external(node, journal, running):
    gmx_fork.drive_external(node, journal, running, source_for)


def source_for(request):
    path = retained("2026-09-08-gmx-request-fixtures/arbitrum-eoa-open-close-cancel.json.gz")
    proof = json.loads(gzip.decompress(path.read_bytes()))
    kind = request.events[0].event().value("uint256", "orderType")
    assert kind in (2, 4)
    return proof["actions"][2 if kind == 2 else 4]
