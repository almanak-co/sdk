from __future__ import annotations

from types import SimpleNamespace

import pytest

from almanak.framework.runner.runner_gateway import _strategy_display_name


@pytest.mark.parametrize(
    ("strategy", "expected"),
    [
        (
            SimpleNamespace(
                strategy_display_name="display",
                config=SimpleNamespace(strategy_display_name="config-display"),
                STRATEGY_NAME="canonical",
                STRATEGY_METADATA=SimpleNamespace(name="metadata-name"),
            ),
            "display",
        ),
        (
            SimpleNamespace(
                config=SimpleNamespace(strategy_display_name="config-display"),
                STRATEGY_NAME="canonical",
                STRATEGY_METADATA=SimpleNamespace(name="metadata-name"),
            ),
            "config-display",
        ),
        (
            SimpleNamespace(
                config=SimpleNamespace(strategy_display_name=""),
                STRATEGY_NAME="canonical",
                STRATEGY_METADATA=SimpleNamespace(name="metadata-name"),
            ),
            "canonical",
        ),
        (
            SimpleNamespace(
                config=SimpleNamespace(strategy_display_name=""),
                STRATEGY_NAME="",
                STRATEGY_METADATA=SimpleNamespace(name="metadata-name"),
            ),
            "metadata-name",
        ),
        (
            SimpleNamespace(
                config=SimpleNamespace(strategy_display_name=""),
                STRATEGY_NAME="",
                STRATEGY_METADATA={"canonical_name": "metadata-canonical"},
            ),
            "metadata-canonical",
        ),
    ],
)
def test_strategy_display_name_prefers_human_readable_metadata(strategy, expected) -> None:
    assert _strategy_display_name(strategy) == expected


def test_strategy_display_name_falls_back_to_class_name() -> None:
    class DemoStrategy:
        pass

    assert _strategy_display_name(DemoStrategy()) == "DemoStrategy"
