"""The HyperCore model's loopback `/info` and `/exchange` endpoints (blueprint 30, 9.1).

In a fork workspace the gateway's HyperCore source is this service and nothing else; it
answers in the API's request and response shapes from the model's state, so its answers
pass the same closed shapes as mainnet's. An `/exchange` answer the model withholds is a
504 with no body, which the gateway can only read as an unknown outcome. It binds
loopback only.

The workspace process (`fork_venue.py`) adds two hooks: `synced` applies the fork blocks
mined since the last answer and holds them through every `/info` and `/exchange` answer,
so no keeper pass applies a block in between and the model's clock follows the fork head
the gateway's clock reads, and `control` is the keeper's and the test driver's face
(`POST /control`). `GET /ready` answers once the service serves.
"""

import json
import threading
from contextlib import nullcontext
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from pydantic import TypeAdapter, ValidationError

from almanak.connectors.hyperliquid.fork_model import HyperCoreModel, NotModeled
from almanak.gateway.hypercore_info import WIRE_NAMES, InfoQuery

QUERY: TypeAdapter = TypeAdapter(InfoQuery)
FIELD_NAMES = {wire: name for name, wire in WIRE_NAMES.items()}


def query_from(wire: dict):
    return QUERY.validate_python({FIELD_NAMES.get(k, k): v for k, v in wire.items()})


def handler_for(model: HyperCoreModel, synced=nullcontext, control=None):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/ready":
                return self.reply(404, {"error": "not found"})
            return self.reply(200, {"register": model.doc["register"]})

        def do_POST(self):
            if self.path == "/control" and control is not None:
                return self.control()
            if self.path not in ("/exchange", "/info"):
                return self.reply(404, {"error": "not found"})
            with synced():
                if self.path == "/exchange":
                    return self.exchange()
                return self.info()

        def body(self):
            length = int(self.headers.get("Content-Length", "0"))
            return json.loads(self.rfile.read(length))

        def info(self):
            try:
                query = query_from(self.body())
                return self.reply(200, model.answer(query))
            except (ValueError, ValidationError):
                return self.reply(422, {"error": "Failed to deserialize the JSON body"})
            except NotModeled as missing:
                return self.reply(422, {"error": f"{missing} is not modeled"})

        def exchange(self):
            try:
                answer = model.exchange(self.body())
            except (ValueError, KeyError, TypeError):
                return self.reply(422, {"error": "Failed to deserialize the JSON body"})
            if answer is None:
                self.send_response(504)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return None
            return self.reply(200, answer)

        def control(self):
            assert control is not None
            try:
                return self.reply(200, control(self.body()))
            except (ValueError, KeyError, TypeError) as refused:
                return self.reply(422, {"error": f"{type(refused).__name__}: {refused}"})

        def reply(self, status: int, payload) -> None:
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_) -> None:
            return

    return Handler


class ModelService:
    def __init__(self, model: HyperCoreModel):
        self.model = model

    def __enter__(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler_for(self.model))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        return self

    def __exit__(self, *_) -> None:
        self.server.shutdown()
        self.server.server_close()
