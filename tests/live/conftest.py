"""Live public-endpoint reads run only when `--live-public` is given.

The default suite and hosted CI never open a network, so without the flag this
directory is not collected at all: nothing here shows up as a skip. pytest does
not ask before collecting a file named on its command line, so the session
fixture checks the flag as well, and such a test is skipped without it.
"""

import pytest

from almanak.gateway.providers import PUBLIC_RPC
from scripts.live_qa import cli, reads
from scripts.live_qa.rpc import Rpc, endpoint_url


def pytest_ignore_collect(collection_path, config):
    return None if config.getoption("--live-public") else True


def require_opt_in(config) -> None:
    if not config.getoption("--live-public"):
        pytest.skip("live public-endpoint reads need --live-public")


@pytest.fixture(scope="session")
def live_session(request, tmp_path_factory):
    require_opt_in(request.config)
    workdir = tmp_path_factory.mktemp("live-public")
    return reads.Session(
        run=cli.runner(cli.default_command(), workdir),
        rpc=lambda endpoint, chain: Rpc(endpoint_url(endpoint, chain)),
        workdir=workdir,
    )


def served(endpoint: str, chain: str) -> bool:
    return endpoint_url(endpoint, chain) is not None


ENDPOINTS = sorted(PUBLIC_RPC)
