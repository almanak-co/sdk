"""Privileged fork-fixture setup. No upstream write RPC is forwarded."""

import json
import os
import secrets
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
from eth_account import Account as Signer
from eth_utils import to_checksum_address

from almanak.caip import ChainId
from almanak.chains import evm_network
from almanak.connectors.composition import connector_for
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import chain_number, erc20_address
from almanak.gateway.fork_config import (
    ForkEoaFund,
    ForkSafeFund,
    ForkTarget,
    provider_endpoint,
)
from almanak.gateway.rate_limits import ProviderLimit
from almanak.models import (
    Account,
    AccountBinding,
    Block,
    Chain,
    Eoa,
)

READ_METHODS = {
    "eth_chainId",
    "eth_blockNumber",
    "eth_getBlockByNumber",
    "eth_getBlockByHash",
    "eth_getBalance",
    "eth_getCode",
    "eth_getStorageAt",
    "eth_getTransactionCount",
    "eth_getTransactionByHash",
    "eth_getTransactionReceipt",
    "eth_getProof",
    "eth_getLogs",
    "eth_call",
    "eth_gasPrice",
    "eth_feeHistory",
    "net_version",
}


class Rpc:
    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.signing_chain = ChainId("eip155:31337")

    def __call__(self, method, params=(), *, timeout=30):
        response = httpx.post(
            self.endpoint,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": list(params),
            },
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
        if "error" in payload:
            raise RuntimeError(f"fork RPC failed: {method}: {payload['error']}")
        return payload["result"]

    def send(self, signer, to, data="0x", value=0):
        tx = {"from": signer.address, "to": to, "data": data, "value": hex(value)}
        gas = int(self("eth_estimateGas", [tx]), 16)
        signed = signer.sign_transaction(
            {
                "chainId": chain_number(self.signing_chain),
                "nonce": int(self("eth_getTransactionCount", [signer.address, "pending"]), 16),
                "to": to,
                "data": data,
                "value": value,
                "gas": gas,
                "gasPrice": int(self("eth_gasPrice"), 16),
            }
        )
        tx_hash = self("eth_sendRawTransaction", ["0x" + signed.raw_transaction.hex()])
        return self.wait_receipt(tx_hash)

    def wait_receipt(self, tx_hash, *, timeout=30):
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"fork fixture receipt remains unknown: {tx_hash}")
            receipt = self("eth_getTransactionReceipt", [tx_hash], timeout=remaining)
            if receipt is not None:
                if receipt["status"] != "0x1":
                    raise RuntimeError(f"fork fixture transaction reverted: {tx_hash}: {receipt}")
                return receipt
            remaining = deadline - time.monotonic()
            time.sleep(max(0, min(0.05, remaining)))


def fee_arguments(reader, origin: Block) -> list[str]:
    """Price fork gas as the forked chain does, not at Anvil's default suggestion.

    Anvil adds a minimum suggested tip of 1 gwei to `eth_gasPrice`, and fixture
    transactions paying it raise the fee-history reward of later blocks, so a
    policy calibrated on Arbitrum's ~0.01 gwei market was refused on its fork. The
    node starts at the fork block's own base fee without that tip. Chains whose fee
    history reports no usable tips (BNB) quote the node's gas price instead, so their
    fork starts at the upstream market quote.
    """
    header = reader._rpc("eth_getBlockByNumber", [hex(origin.number), False])
    base_fee = int(header.get("baseFeePerGas") or "0x0", 16)
    if not evm_network(origin.chain.chain_id).fee_history_reports_tips:
        base_fee = max(base_fee, int(reader._rpc("eth_gasPrice", []), 16))
    return ["--base-fee", str(base_fee), "--no-priority-fee"]


class Fork:
    def __init__(
        self,
        env_file: Path,
        origin: Block,
        *,
        port: int | None = None,
        target: ForkTarget | None = None,
    ):
        self.env_file = env_file
        self.origin = origin
        self.port = port
        self.target = target or legacy_target(origin)
        if self.target.chain != origin.chain:
            raise ValueError("fork target and resolved origin have different chains")

    def __enter__(self):
        self.cleanup = ExitStack()
        try:
            return self._start()
        except BaseException:
            self.cleanup.close()
            raise

    def _start(self):
        executable = shutil.which("anvil")
        if not executable:
            raise RuntimeError("Anvil is required for the fork proof")
        endpoint = provider_endpoint(self.target.provider, self.env_file)
        from almanak.gateway.ethereum import EthereumReader

        with EthereumReader(self.origin.chain, endpoint) as reader:
            if reader.pin_block(self.origin.number) != self.origin:
                raise ValueError("provider block differs from the resolved origin")
            fees = fee_arguments(reader, self.origin)
        self.start_proxy(endpoint)
        self.temporary = self.cleanup.enter_context(
            tempfile.TemporaryDirectory(prefix="sdk-v3-fork-")
        )
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", self.port or 0))
            port = listener.getsockname()[1]
        self.rpc = Rpc(f"http://127.0.0.1:{port}")
        self.process = subprocess.Popen(
            [
                executable,
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--accounts",
                "0",
                "--chain-id",
                "31337",
                "--fork-url",
                f"http://127.0.0.1:{self.proxy.server_port}",
                "--fork-block-number",
                str(self.origin.number),
                "--retries",
                "1",
                "--network",
                evm_network(self.origin.chain.chain_id).family,
                *fees,
                "--silent",
            ],
            cwd=self.temporary,
            env={"PATH": os.environ["PATH"]},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        self.cleanup.callback(self._stop_node)
        return self._wait_ready()

    def start_proxy(self, endpoint, port=0):
        self.upstream = self.cleanup.enter_context(httpx.Client(timeout=25))
        handler = fork_read_handler(endpoint, self.upstream, ProviderLimit("rpc"))

        self.proxy = ThreadingHTTPServer(("127.0.0.1", port), handler)
        self.cleanup.callback(self.proxy.server_close)
        self.thread = threading.Thread(target=self.proxy.serve_forever, daemon=True)
        self.thread.start()
        self.cleanup.callback(self.proxy.shutdown)

    def _wait_ready(self):
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                break
            try:
                self.metadata = self.rpc("anvil_metadata")
                self._validate_origin()
            except (httpx.HTTPError, RuntimeError):
                time.sleep(0.1)
                continue
            # Once, outside the retry: a failed seal is permanent and must keep its reason.
            self._seal_local_head()
            return self
        raise RuntimeError("fork did not become ready")

    def _seal_local_head(self):
        # Anvil executes calls at the forked block with that block's upstream header.
        # Arbitrum-family headers (Arbitrum One, Robinhood Chain) carry no Cancun blob
        # fields, so every eth_call there fails with "Excess blob gas not set", and the
        # upstream fee history reports the upstream market (a zero BNB base fee), not
        # this node's base fee that policy-free preparation prices from. One empty local
        # block gives later reads a local header; the origin is unchanged.
        self.rpc("evm_mine")
        head = self.rpc("eth_getBlockByNumber", ["latest", False])
        if head.get("excessBlobGas") is None:
            raise RuntimeError("fork head still lacks Cancun blob fields")

    def _validate_origin(self):
        actual = self.rpc("anvil_metadata")
        network = actual.get("forkedNetwork") or {}
        observed = (
            actual.get("instanceId"),
            actual.get("chainId"),
            network.get("forkBlockNumber"),
            network.get("forkBlockHash"),
        )
        expected = (
            self.metadata["instanceId"],
            chain_number(self.rpc.signing_chain),
            self.origin.number,
            self.origin.hash,
        )
        if observed != expected:
            raise ValueError("Anvil origin differs from the explicit fork configuration")

    def __exit__(self, *_):
        self.cleanup.close()

    def _stop_node(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)

    def funded_signer(
        self, token, amount, native_balance, account_index=None, *, plain_account=False
    ):
        if account_index is None:
            signer = Signer.create()
        else:
            signer = fixture_signer(account_index)
        if plain_account:
            from almanak.gateway.accounts import require_plain_eoa

            require_plain_eoa(self.rpc, signer.address)
        self.rpc("anvil_setBalance", [signer.address, hex(native_balance)])
        if int(self.rpc("eth_getBalance", [signer.address, "latest"]), 16) != native_balance:
            raise ValueError("native fork funding was not applied")
        if amount:
            self.wrap_native(signer, token, amount)
        return signer

    def wrap_native(self, signer, token, amount):
        data = "0x70a08231" + signer.address[2:].lower().rjust(64, "0")
        before = int(self.rpc("eth_call", [{"to": token, "data": data}, "latest"]), 16)
        self.rpc.send(signer, token, "0xd0e30db0", amount)
        after = int(self.rpc("eth_call", [{"to": token, "data": data}, "latest"]), 16)
        if after - before != amount:
            raise ValueError("wrapped-native funding did not produce the declared token amount")

    def prepare(self, config, directory: Path):
        if config.origin != self.origin:
            raise ValueError("workspace configuration has a different fork origin")
        directory.mkdir(mode=0o700, parents=True, exist_ok=False)
        chain = Chain(
            chain_id=ChainId("eip155:31337"), environment="anvil:" + self.metadata["instanceId"]
        )
        if any(isinstance(f, ForkSafeFund) for f in config.funds):
            from almanak.gateway.safe_funding import prepare_safe_accounts

            if not all(isinstance(f, ForkSafeFund) for f in config.funds):
                raise ValueError("fork fixtures require one explicit account profile family")
            bindings, keys, evidence = prepare_safe_accounts(self, config, chain)
            (directory / "fixture-installation.json").write_text(
                json.dumps(evidence, indent=2) + "\n"
            )
        else:
            bindings, keys = self.prepare_eoa_accounts(config, chain)
        deployment = connector_for(config).fork.prepare(
            config,
            bindings,
            chain,
            self.metadata["instanceId"],
            self.origin,
        )
        (directory / "deployment.json").write_text(deployment.model_dump_json(indent=2) + "\n")
        descriptor = os.open(
            directory / "fork-keys.json", os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600
        )
        with os.fdopen(descriptor, "w") as handle:
            json.dump(keys, handle)
        descriptor = os.open(
            directory / "gateway-auth.token", os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600
        )
        with os.fdopen(descriptor, "w") as handle:
            handle.write(secrets.token_hex(32) + "\n")
        (directory / "fork.json").write_text(
            json.dumps({"endpoint": self.rpc.endpoint, "instance": self.metadata["instanceId"]})
            + "\n"
        )
        return deployment

    def prepare_eoa_accounts(self, config, chain):
        bindings, keys = [], {}
        self.funding_evidence = []
        for fund in config.funds:
            if isinstance(fund, ForkEoaFund):
                binding, key = self.prepare_explicit_eoa(fund, chain)
                bindings.append(binding)
                keys[fund.account] = key
                continue
            asset = connector_for(config).fork.funding_asset(config, fund.account)
            signer = self.funded_signer(
                to_checksum_address(erc20_address(asset, self.origin.chain.chain_id)),
                fund.token_balance,
                fund.native_balance.amount,
                fund.foundry_account_index,
                plain_account=True,
            )
            bindings.append(
                AccountBinding(
                    name=fund.account,
                    profile=Eoa(kind="eoa", fund=Account(chain=chain, address=signer.address)),
                )
            )
            keys[fund.account] = signer.key.hex()
        return bindings, keys

    def prepare_explicit_eoa(self, fund, chain):
        from almanak.gateway.safe_funding import fund_asset, native_wrapping_balance

        wrapping = native_wrapping_balance(fund)
        signer = self.funded_signer(
            "",
            0,
            fund.native_balance.amount + wrapping,
            fund.foundry_account_index,
            plain_account=True,
        )
        for asset in fund.assets:
            self.funding_evidence.append(fund_asset(self, signer, signer.address.lower(), asset))
        binding = AccountBinding(
            name=fund.account,
            profile=Eoa(kind="eoa", fund=Account(chain=chain, address=signer.address)),
        )
        return binding, signer.key.hex()


def fixture_signer(account_index):
    if type(account_index) is not int or not 0 <= account_index < 2**31:
        raise ValueError("fixture signer requires a supported explicit Foundry account index")
    Signer.enable_unaudited_hdwallet_features()
    return Signer.from_mnemonic(
        "test test test test test test test test test test test junk",
        account_path=f"m/44'/60'/0'/0/{account_index}",
    )


def redact_rpc_errors(payload):
    items = payload if isinstance(payload, list) else [payload]
    for item in items:
        if "error" in item:
            item["error"] = {"code": -32000, "message": "upstream read failed"}


def legacy_target(origin):
    from almanak.gateway.fork_config import AlchemyProvider

    if str(origin.chain.chain_id) != "eip155:1" or origin.chain.environment != "mainnet":
        raise ValueError("non-Ethereum forks require an explicit provider target")
    return ForkTarget(
        chain=origin.chain,
        provider=AlchemyProvider(
            kind="alchemy", network="eth-mainnet", api_key_env="ALCHEMY_API_KEY"
        ),
        block=origin,
    )


def fork_read_handler(endpoint, upstream, provider_limit):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            batch = body if isinstance(body, list) else [body]
            if any(item.get("method") not in READ_METHODS for item in batch):
                self.send_error(403)
                return
            try:
                provider_limit.admit(len(batch))
                response = upstream.post(endpoint, json=body)
                response.raise_for_status()
                payload = response.json()
                redact_rpc_errors(payload)
                data = json.dumps(payload).encode()
            except DataError as error:
                self.send_error(429 if error.kind == "rate_limit" else 503, error.kind)
                return
            except (httpx.HTTPError, ValueError):
                self.send_error(502, "upstream read failed")
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    return Handler
