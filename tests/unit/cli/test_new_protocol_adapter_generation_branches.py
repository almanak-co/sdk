"""Branch coverage for the connector adapter scaffold generator."""

from __future__ import annotations

import ast
from dataclasses import replace

from almanak.framework.cli.new_protocol import (
    PROTOCOL_TYPE_CONFIGS,
    ProtocolType,
    generate_adapter_file,
)


def test_adapter_config_parameter_types_are_rendered_exactly(monkeypatch) -> None:
    config = replace(
        PROTOCOL_TYPE_CONFIGS[ProtocolType.YIELD],
        example_config_params={
            "enabled": "false",
            "ratio": "1.25",
            "mode": "balanced",
        },
    )
    monkeypatch.setitem(PROTOCOL_TYPE_CONFIGS, ProtocolType.YIELD, config)

    generated = generate_adapter_file("sample_yield", ProtocolType.YIELD, ["arbitrum"])

    ast.parse(generated)
    assert "    enabled: bool = False" in generated
    assert "    ratio: float = 1.25" in generated
    assert '    mode: str = "balanced"' in generated


def test_adapter_without_config_parameters_emits_pass(monkeypatch) -> None:
    config = replace(
        PROTOCOL_TYPE_CONFIGS[ProtocolType.DEX],
        example_config_params={},
    )
    monkeypatch.setitem(PROTOCOL_TYPE_CONFIGS, ProtocolType.DEX, config)

    generated = generate_adapter_file("sample_dex", ProtocolType.DEX, ["base"])

    ast.parse(generated)
    assert "    wallet_address: str\n    pass\n\n    def __post_init__" in generated
