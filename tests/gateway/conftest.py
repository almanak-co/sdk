"""Pytest configuration for gateway tests."""

import pytest


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "docker: marks tests as requiring Docker (deselect with '-m \"not docker\"')",
    )
