"""Fresh QA keeps its resolved origin and explicitly selected authentic publication source."""

from contextlib import contextmanager
from unittest.mock import patch

from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.target_routing import allocation_connector
from almanak.gateway.ethereum import EthereumReader
from almanak.gateway.providers import provider_endpoint
from tests import gmx_fork
from tests.fork.test_gmx_fresh_source import assert_payloads, fresh_source


def fresh_market(author):
    allocations = tuple(
        allocation
        for allocation in author.allocations
        if allocation_connector(allocation) == ConnectorId("gmx_v2")
    )
    if len(author.forks) != 1 or len(allocations) != 1:
        raise ValueError("fresh GMX fixture requires one explicitly routed reference market")
    return allocations[0].market


@contextmanager
def fresh_keeper(case, config, env_file, output):
    """The authentic publication source is internal; the fork configuration is public."""
    from tests.qa.worker import author_fork_configuration

    author = author_fork_configuration(case, config, env_file)
    market = fresh_market(author)
    fork = author.forks[0]
    original = gmx_fork.drive_external
    with EthereumReader(fork.chain, provider_endpoint(fork.provider, env_file)) as reader:
        source = fresh_source(reader, market, fork.block, output)

        def drive(node, journal, running, pinned_source, *, block_interval=1, timeout=180):
            def selected(request):
                now = int(node.rpc("eth_getBlockByNumber", ["latest", False])["timestamp"], 16)
                return source.acquire(request, not_before=now)

            return original(node, journal, running, selected, block_interval=1, timeout=timeout)

        with patch.object(gmx_fork, "drive_external", drive):
            yield
        assert_payloads(source, fork.block)
        if not source.selected:
            raise AssertionError("fresh reference produced no post-origin keeper proof")


def execute(case, source, config, env_file, temporary, artifact):
    """The keeper source is internal; the run itself takes the lane of its root format."""
    from tests.qa.catalog import converted
    from tests.qa.worker import lending_eoa_case
    from tests.strategy_fork.harness import run_case
    from tests.strategy_fork.supervised import run_supervised_case

    with fresh_keeper(case, config, env_file, artifact.parent):
        if case.profile == "eoa":
            lending_eoa_case(case, source, config, env_file, temporary, artifact)
        elif converted(case):
            # The case operator file carries this run's fresh anchors, not the example's.
            run_supervised_case(
                source,
                config.parent / "operator.yaml",
                env_file,
                temporary,
                artifact,
                case.operator_mode,
            )
        else:
            run_case(source, env_file, {}, temporary, artifact, case.operator_mode)
