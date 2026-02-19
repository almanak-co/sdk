"""Root pytest configuration for all tests.

This file is automatically loaded by pytest and provides shared configuration
and plugins for the entire test suite.
"""

# Load gateway fixtures for integration tests
# This makes gateway_server, gateway_client, and gateway_web3_* fixtures
# available to all test files that need them
pytest_plugins = ["tests.conftest_gateway"]
