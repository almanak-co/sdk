"""Live public-endpoint reads are collected only when `--live-public` is given.

The default suite and hosted CI never open a network, so without the flag this
directory is not collected at all: nothing here shows up as a skip.
"""

import pytest

from almanak.gateway.providers import PUBLIC_RPC
from scripts.live_qa import cli, reads
from scripts.live_qa.rpc import Rpc, endpoint_url


def pytest_ignore_collect(collection_path, config):
    return None if config.getoption("--live-public") else True


@pytest.fixture(scope="session")
def live_session(tmp_path_factory):
    workdir = tmp_path_factory.mktemp("live-public")
    return reads.Session(
        run=cli.runner(cli.default_command(), workdir),
        rpc=lambda endpoint, chain: Rpc(endpoint_url(endpoint, chain)),
        workdir=workdir,
    )


def served(endpoint: str, chain: str) -> bool:
    return endpoint_url(endpoint, chain) is not None


ENDPOINTS = sorted(PUBLIC_RPC)
