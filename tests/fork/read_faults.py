"""A loopback RPC proxy that makes selected observation reads unavailable."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
from eth_utils import keccak


class ReadFaultProxy:
    def __init__(self, endpoint, signatures):
        self.client = httpx.Client(timeout=60)
        self.endpoint = endpoint
        self.selectors = {"0x" + keccak(text=s)[:4].hex() for s in signatures}
        self.refused = 0

    def reply(self, request):
        method, params = request["method"], request["params"]
        if method == "eth_call" and params[0].get("data", "")[:10] in self.selectors:
            self.refused += 1
            return {
                "jsonrpc": "2.0",
                "id": request["id"],
                "error": {"code": -32000, "message": "fixture observation outage"},
            }
        return self.client.post(self.endpoint, json=request).json()

    def __enter__(self):
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                body = json.dumps(owner.reply(request)).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        return self

    def __exit__(self, *_):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.client.close()


class HealthShockProxy(ReadFaultProxy):
    """Inject a labeled low-health observation while retaining real execution on Anvil."""

    def __init__(self, endpoint, health_factor):
        super().__init__(endpoint, ())
        self.health_factor = health_factor
        self.changed = 0
        self.health_selector = "0x" + keccak(text="getUserAccountData(address)")[:4].hex()

    def reply(self, request):
        from eth_abi import decode, encode

        response = super().reply(request)
        if request["method"] != "eth_call":
            return response
        if request["params"][0].get("data", "")[:10] != self.health_selector:
            return response
        values = list(decode(("uint256",) * 7, bytes.fromhex(response["result"][2:])))
        values[2] = self.health_factor
        self.changed += 1
        return {**response, "result": "0x" + encode(("uint256",) * 7, values).hex()}
