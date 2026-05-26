"""Tests to verify protobuf generated files are up-to-date."""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path

import pytest


@dataclass(frozen=True)
class ProtoSpec:
    """One ``.proto`` source + the generated artefacts it produces.

    VIB-4813 introduced ``polymarket.proto`` as a connector-owned
    second source file (Phase 5 of VIB-4808 — the gateway proto layer
    no longer names individual connectors). Both protos compile under
    the same ``package = almanak.gateway.proto`` declaration so the
    wire-level service names are byte-identical.
    """

    proto_dir: Path
    proto_file: str
    generated_files: tuple[str, ...]
    # Import-fix the Makefile applies post-protoc so the generated grpc
    # module uses absolute imports (grpc_tools defaults to relative).
    import_fix_old: str
    import_fix_new: str


PROTO_SPECS: tuple[ProtoSpec, ...] = (
    ProtoSpec(
        proto_dir=Path("almanak/gateway/proto"),
        proto_file="gateway.proto",
        generated_files=("gateway_pb2.py", "gateway_pb2_grpc.py"),
        import_fix_old="import gateway_pb2 as gateway__pb2",
        import_fix_new="from almanak.gateway.proto import gateway_pb2 as gateway__pb2",
    ),
    ProtoSpec(
        proto_dir=Path("almanak/connectors/polymarket/proto"),
        proto_file="polymarket.proto",
        generated_files=("polymarket_pb2.py", "polymarket_pb2_grpc.py"),
        import_fix_old="import polymarket_pb2 as polymarket__pb2",
        import_fix_new=(
            "from almanak.connectors.polymarket.proto import polymarket_pb2 "
            "as polymarket__pb2"
        ),
    ),
)


class TestProtoFilesUpToDate:
    """Verify committed proto files match freshly generated ones."""

    @pytest.fixture
    def repo_root(self) -> Path:
        return Path(__file__).parent.parent.parent

    @pytest.mark.parametrize("spec", PROTO_SPECS, ids=lambda s: s.proto_file)
    def test_proto_files_are_up_to_date(self, repo_root: Path, spec: ProtoSpec) -> None:
        """Verify committed proto files match freshly generated ones."""
        from grpc_tools import protoc

        proto_source = repo_root / spec.proto_dir / spec.proto_file

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir)

            # Generate fresh proto files
            result = protoc.main([
                "protoc",
                f"-I{proto_source.parent}",
                f"--python_out={output_dir}",
                f"--grpc_python_out={output_dir}",
                str(proto_source),
            ])
            assert result == 0, f"protoc failed for {spec.proto_file} (rc={result})"

            # Apply import fix to grpc file
            grpc_basename = spec.proto_file.replace(".proto", "_pb2_grpc.py")
            grpc_file = output_dir / grpc_basename
            content = grpc_file.read_text()
            grpc_file.write_text(content.replace(spec.import_fix_old, spec.import_fix_new))

            # Compare each generated file
            mismatches = []
            for filename in spec.generated_files:
                generated = (output_dir / filename).read_text()
                committed = (repo_root / spec.proto_dir / filename).read_text()
                if generated != committed:
                    mismatches.append(filename)

            if mismatches:
                pytest.fail(
                    f"Proto files are out of date for {spec.proto_file}: "
                    f"{', '.join(mismatches)}\n"
                    f"Run 'make proto' to regenerate them."
                )
