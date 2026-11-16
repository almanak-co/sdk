"""A supervised gateway serves on the loopback socket its supervisor reserved.

Selecting a free port, releasing it and letting the child bind it later allowed
a concurrent process to take the port first. The readiness probe then reached a
gateway holding a different token and failed with "authentication failed".
"""

import os
import secrets
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import httpx
import pytest

from almanak.cli_arguments import build_parser
from almanak.gateway.service import http_server, inherited_listener, listen_address
from almanak.models import Model
from almanak.supervisor_process import loopback_listener


def test_reserved_port_cannot_be_bound_by_another_server():
    with loopback_listener() as listener:
        host, port = listener.getsockname()
        assert host == "127.0.0.1" and port > 0
        # ThreadingHTTPServer sets SO_REUSEADDR, as every gateway server does.
        with pytest.raises(OSError):
            ThreadingHTTPServer(("127.0.0.1", port), BaseHTTPRequestHandler)


def test_gateway_serves_on_the_inherited_socket(tmp_path):
    token = tmp_path / "gateway.token"
    token.write_text(secrets.token_hex(32))
    token.chmod(0o600)
    service = SimpleNamespace(request_type=Model, dispatch=lambda method, request: {"ok": True})
    with loopback_listener() as listener:
        port = listener.getsockname()[1]
        # The child receives the same descriptor; a duplicate stands in for it here.
        adopted = inherited_listener(os.dup(listener.fileno()))
    assert not adopted.get_inheritable()
    server = http_server(service, token, adopted)
    serving = threading.Thread(target=server.serve_forever)
    serving.start()
    try:
        assert server.server_port == port
        response = httpx.post(
            f"http://127.0.0.1:{port}/state",
            headers={"Authorization": "Bearer " + token.read_text()},
            json={},
            timeout=5,
        )
        assert response.json() == {"result": {"ok": True}}
    finally:
        server.shutdown()
        serving.join(timeout=5)
        server.server_close()


@pytest.mark.parametrize(
    ("kind", "address"),
    [(socket.SOCK_STREAM, "0.0.0.0"), (socket.SOCK_DGRAM, "127.0.0.1")],
)
def test_inherited_listener_refuses_non_loopback_or_non_tcp_sockets(kind, address):
    sock = socket.socket(socket.AF_INET, kind)
    sock.bind((address, 0))
    fd = sock.detach()
    with pytest.raises(ValueError, match="loopback TCP socket"):
        inherited_listener(fd)


def test_gateway_cli_takes_exactly_one_listen_source():
    base = ["service", "gateway", "--gateway-config", "c", "--fork", "--auth-token-file", "t"]
    parser = build_parser()
    args = parser.parse_args([*base, "--listen-fd", "7"])
    assert (args.port, args.listen_fd) == (None, 7)
    assert listen_address(parser.parse_args([*base, "--port", "8545"])) == 8545
    for extra in ([], ["--port", "8545", "--listen-fd", "7"]):
        with pytest.raises(SystemExit):
            parser.parse_args([*base, *extra])
