"""`almanak ax` reads on public mainnet endpoints, as a user without a provider key.

Opt-in: `pytest tests/live --live-public -p no:xdist`. Run it serially; parallel
workers share each endpoint's rate limit. A case the endpoint did not answer is
skipped as blocked, which is not a pass. `python -m scripts.live_qa reads` runs the
same cases and writes the CSV and Markdown report.
"""

import pytest

from scripts.live_qa import reads
from scripts.live_qa.read_cases import CASES
from tests.live.conftest import ENDPOINTS, served

pytestmark = pytest.mark.live_public

PLAN = reads.planned(CASES, ENDPOINTS, served)


@pytest.mark.parametrize(
    ("case", "endpoint"), PLAN, ids=[f"{endpoint}:{case.id}" for case, endpoint in PLAN]
)
def test_read_on_public_endpoint(case, endpoint, live_session):
    result = live_session.one(case, endpoint)
    if result.status == "blocked":
        pytest.skip(f"blocked after {result.attempts} attempts: {result.detail}")
    assert result.status == "pass", f"{result.known or 'unexpected'}: {result.detail}"
