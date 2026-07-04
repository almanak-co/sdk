"""Root pytest configuration for all tests.

This file is automatically loaded by pytest and provides shared configuration
and plugins for the entire test suite.
"""

import importlib.util

# Load gateway fixtures for integration tests
# This makes gateway_server, gateway_client, and gateway_web3_* fixtures
# available to all test files that need them.
#
# The stripped strategy container image (deploy/docker/Dockerfile.strategy)
# deletes almanak/gateway/server.py, which tests.conftest_gateway imports at
# module level — registering the plugin there would abort collection of every
# test, including the network-isolation suite that image exists to run
# (deploy/docker/docker-compose.test.yml). find_spec resolves without
# executing the module, so a normal checkout always registers the plugin.
pytest_plugins = ["tests.conftest_gateway"] if importlib.util.find_spec("almanak.gateway.server") is not None else []
