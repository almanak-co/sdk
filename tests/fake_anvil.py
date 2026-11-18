"""A loopback JSON-RPC process standing in for an identified Anvil node.

It serves the metadata, snapshot and funding methods that supervisor recovery
uses, records every request, and never forks or executes a chain. It runs as a
child process, so each handler stays trivial: coverage never observes it.
"""

import argparse
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer


def methods(args):
    metadata = {
        "clientVersion": "anvil/test",
        "chainId": 31337,
        "instanceId": args.instance,
        "forkedNetwork": {
            "chainId": 1,
            "forkBlockNumber": args.block_number,
            "forkBlockHash": args.block_hash,
        },
    }
    live, balances, counter = set(), {}, [0]

    def snapshot(_):
        counter[0] += 1
        live.add(hex(counter[0]))
        return hex(counter[0])

    def revert(params):
        if params[0] not in live:
            return False
        live.discard(params[0])
        return True

    def set_balance(params):
        balances[params[0].lower()] = params[1]
        return True

    return {
        "anvil_metadata": lambda _: metadata,
        "eth_chainId": lambda _: hex(31337),
        "evm_snapshot": snapshot,
        "evm_revert": revert,
        "anvil_setBalance": set_balance,
        "eth_getBalance": lambda params: balances.get(params[0].lower(), "0x0"),
        "eth_getCode": lambda _: "0x",
    }


def reply(handlers, body):
    method, params = body["method"], body.get("params", [])
    if method not in handlers:
        return {"jsonrpc": "2.0", "id": body["id"], "error": {"code": -32601, "message": method}}
    return {"jsonrpc": "2.0", "id": body["id"], "result": handlers[method](params)}


def handler(handlers, log):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            with open(log, "a") as stream:
                stream.write(json.dumps([body["method"], body.get("params", [])]) + "\n")
            data = json.dumps(reply(handlers, body)).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    return Handler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--log", required=True)
    parser.add_argument("--instance", required=True)
    parser.add_argument("--block-number", type=int, required=True)
    parser.add_argument("--block-hash", required=True)
    args = parser.parse_args()
    server = HTTPServer(("127.0.0.1", args.port), handler(methods(args), args.log))
    sys.stdout.write(str(server.server_port) + "\n")
    sys.stdout.flush()
    server.serve_forever()


if __name__ == "__main__":
    main()
