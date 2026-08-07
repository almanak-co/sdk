"""CLI rendering of PermissionGenerationError (ALM-3175).

The permissions CLI must surface a fail-closed token-extraction error as a
clean ClickException (exit code 1, original message, no traceback) instead
of a raw stack trace.
"""

from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from almanak.framework.cli.permissions import permissions
from almanak.framework.permissions.generator import PermissionGenerationError


def test_cli_renders_permission_generation_error_without_traceback(tmp_path):
    (tmp_path / "strategy.py").write_text("# stub strategy file\n")

    meta = MagicMock()
    meta.name = "spcxb_test"
    meta.supported_protocols = ["pancakeswap_v3"]
    swap = MagicMock()
    swap.value = "SWAP"
    meta.intent_types = [swap]
    meta.supported_chains = ["bsc"]
    meta.default_chain = "bsc"
    stub_cls = type("StubStrategy", (), {"STRATEGY_METADATA": meta})

    err = PermissionGenerationError("Cannot resolve config token(s) SPCXB on bsc for the ERC-20 approve permission.")
    with (
        patch(
            "almanak.framework.cli.permissions.load_strategy_from_file",
            return_value=(stub_cls, None),
        ),
        patch(
            "almanak.framework.permissions.generator.discover_teardown_protocols",
            return_value=(set(), []),
        ),
        patch(
            "almanak.framework.permissions.generator.generate_manifest",
            side_effect=err,
        ),
    ):
        result = CliRunner().invoke(permissions, ["-d", str(tmp_path)])

    assert result.exit_code == 1
    assert "Cannot resolve config token(s) SPCXB" in result.output
    assert "Traceback" not in result.output
