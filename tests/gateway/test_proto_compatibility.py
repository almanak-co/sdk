"""Tests to verify protobuf generated files are up-to-date."""

import tempfile
from pathlib import Path

import pytest


class TestProtoFilesUpToDate:
    """Verify committed proto files match freshly generated ones."""

    PROTO_DIR = Path("almanak/gateway/proto")
    PROTO_FILE = "gateway.proto"
    GENERATED_FILES = ["gateway_pb2.py", "gateway_pb2_grpc.py"]

    # Import fix for grpc_tools relative imports
    IMPORT_FIX_OLD = "import gateway_pb2 as gateway__pb2"
    IMPORT_FIX_NEW = "from almanak.gateway.proto import gateway_pb2 as gateway__pb2"

    @pytest.fixture
    def repo_root(self) -> Path:
        return Path(__file__).parent.parent.parent

    def test_proto_files_are_up_to_date(self, repo_root: Path) -> None:
        """Verify committed proto files match freshly generated ones."""
        from grpc_tools import protoc

        proto_source = repo_root / self.PROTO_DIR / self.PROTO_FILE

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
            assert result == 0, f"protoc failed with return code {result}"

            # Apply import fix to grpc file
            grpc_file = output_dir / "gateway_pb2_grpc.py"
            content = grpc_file.read_text()
            grpc_file.write_text(content.replace(self.IMPORT_FIX_OLD, self.IMPORT_FIX_NEW))

            # Compare each generated file
            mismatches = []
            for filename in self.GENERATED_FILES:
                generated = (output_dir / filename).read_text()
                committed = (repo_root / self.PROTO_DIR / filename).read_text()
                if generated != committed:
                    mismatches.append(filename)

            if mismatches:
                pytest.fail(
                    f"Proto files are out of date: {', '.join(mismatches)}\n"
                    f"Run 'make proto' to regenerate them."
                )
