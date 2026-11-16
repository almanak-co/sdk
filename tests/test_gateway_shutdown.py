"""Gateway shutdown drains admitted work before releasing shared native resources."""

import secrets
import threading
from types import SimpleNamespace

import httpx

from almanak.gateway.service import http_server
from almanak.models import Model


def test_server_close_waits_for_admitted_request(tmp_path):
    token = tmp_path / "gateway.token"
    token.write_text(secrets.token_hex(32))
    token.chmod(0o600)
    entered, release, closing, closed = (threading.Event() for _ in range(4))

    def dispatch(method, request):
        entered.set()
        assert release.wait(5)
        return {"completed": True}

    server = http_server(SimpleNamespace(request_type=Model, dispatch=dispatch), token, 0)
    serving = threading.Thread(target=server.serve_forever)
    serving.start()
    responses = []

    def request():
        responses.append(
            httpx.post(
                f"http://127.0.0.1:{server.server_port}/state",
                headers={"Authorization": "Bearer " + token.read_text()},
                json={},
                timeout=5,
            )
        )

    def close():
        closing.set()
        server.server_close()
        closed.set()

    caller, closer = threading.Thread(target=request), threading.Thread(target=close)
    try:
        caller.start()
        assert entered.wait(5)
        server.shutdown()
        serving.join(timeout=5)
        closer.start()
        assert closing.wait(5)
        assert not closed.wait(0.1)
    finally:
        release.set()
        caller.join(timeout=5)
        if closer.ident is not None:
            closer.join(timeout=5)
        server.shutdown()
        serving.join(timeout=5)
        server.server_close()
    assert closed.is_set()
    assert responses[0].status_code == 200
    assert responses[0].json() == {"result": {"completed": True}}
