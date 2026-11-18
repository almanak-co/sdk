"""One read-only gateway process for commands that need current facts without a workspace."""

import json
import secrets
import time
from pathlib import Path

from almanak.configuration.resolution import ReadContext
from almanak.connectors.support import RunMode
from almanak.gateway.bootstrap import ReadBinding
from almanak.gateway.client import GatewayClient
from almanak.gateway.ethereum import DataError
from almanak.gateway.fork_workspace import write_private
from almanak.sandbox import readable_paths, validate_key_path
from almanak.supervisor_process import (
    SERVICE_READY_SECONDS,
    loopback_listener,
    running,
    spawn,
    stop,
)


def wait_read_gateway(client, context, process):
    deadline = time.monotonic() + SERVICE_READY_SECONDS
    while time.monotonic() < deadline:
        try:
            binding = ReadBinding.model_validate_json(
                json.dumps(client.request("resolution-context", context_digest=context.digest))
            )
            binding.require(context)
            return
        except DataError as error:
            if error.kind != "gateway_unavailable":
                raise
            if not running(process):
                raise RuntimeError(
                    "read gateway exited during start; inspect logs/read-gateway.log"
                ) from None
            time.sleep(0.1)
    raise RuntimeError(
        f"read gateway did not answer within {SERVICE_READY_SECONDS} seconds; "
        "inspect logs/read-gateway.log"
    )


class ReadGateway:
    """Spawn the read bootstrap service in a private directory and stop it on exit.

    The process reads the operator env file itself; the calling command never
    loads provider credentials, and the retained context carries none.
    """

    def __init__(
        self,
        directory: Path,
        gateway_config: Path,
        context: ReadContext,
        env_file: Path,
        mode: RunMode,
    ):
        self.directory = Path(directory).resolve()
        self.gateway_config = Path(gateway_config).resolve()
        self.env_file = Path(env_file).resolve()
        self.context, self.mode = context, mode
        self.process = None
        self.client: GatewayClient | None = None

    def __enter__(self) -> GatewayClient:
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        (self.directory / "logs").mkdir(mode=0o700, exist_ok=True)
        context_path = self.directory / "read-context.json"
        token_path = self.directory / "gateway-auth.token"
        for path in (context_path, token_path, self.gateway_config, self.env_file):
            validate_key_path(path, readable_paths())
        if context_path.exists():
            context_path.unlink()
        write_private(context_path, self.context.model_dump_json())
        if not token_path.exists():
            write_private(token_path, secrets.token_hex(32))
        arguments = [
            "service",
            "gateway",
            "--gateway-config",
            str(self.gateway_config),
            "--read-context",
            str(context_path),
            f"--{self.mode.value}",
            "--env-file",
            str(self.env_file),
            "--auth-token-file",
            str(token_path),
        ]
        with loopback_listener() as listener:
            port = listener.getsockname()[1]
            arguments.extend(["--listen-fd", str(listener.fileno())])
            self.process = spawn(self.directory, "read-gateway", arguments, listener=listener)
        self.client = GatewayClient(f"http://127.0.0.1:{port}", token_path)
        try:
            wait_read_gateway(self.client, self.context, self.process)
        except BaseException:
            self.__exit__(None, None, None)
            raise
        return self.client

    def __exit__(self, *_):
        if self.client is not None:
            self.client.close()
            self.client = None
        if self.process is not None:
            process, self.process = self.process, None
            stop(process)
