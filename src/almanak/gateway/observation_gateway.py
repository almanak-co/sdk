"""Read-only observation gateway. Provider credentials stay in this process.

A session that must not hold a provider key runs its `almanak ax` reads
through this sidecar instead of a local env file. It serves three operations over
authenticated loopback HTTP: `read`, the whitelisted JSON-RPC reads; `lookup`,
the closed set of CoinGecko on-chain and Morpho API discovery lookups; and
`info`, the whitelisted HyperCore queries of Hyperliquid's `/info` API, which
needs no key but meters through the same process. Nothing here signs or submits.
"""

import json
import threading
from pathlib import Path
from typing import Any, Literal

import httpx
from pydantic import JsonValue, RootModel, ValidationError

from almanak.caip import ChainId
from almanak.gateway.auth import loopback_endpoint, read_token
from almanak.gateway.ethereum import DataError, EthereumReader, Reverted
from almanak.gateway.hypercore_info import HyperCoreInfo, InfoRequest
from almanak.gateway.onchain_lookup import LookupRequest, VendorLookups
from almanak.gateway.rate_limits import VARIABLES, configured_limit
from almanak.gateway.read_endpoints import verified_reader
from almanak.gateway.service import http_server
from almanak.models import Chain, Model

# The read-only JSON-RPC vocabulary, the same set the historical gateway admits.
# Every ax observation path issues only these; nothing here changes chain state.
ReadMethod = Literal[
    "eth_chainId",
    "eth_getBlockByNumber",
    "eth_call",
    "eth_getCode",
    "eth_getStorageAt",
    "eth_getBalance",
    "eth_getLogs",
    "eth_getTransactionByHash",
    "eth_getTransactionReceipt",
]


class ObservationRead(Model):
    chain: ChainId
    method: ReadMethod
    params: tuple[JsonValue, ...]


class ObservationResult(Model):
    request: ObservationRead
    # A null result is legitimate for some reads (an unknown block or receipt);
    # the client applies its own allow_none policy, so the gateway passes it through.
    result: JsonValue


class GatewayRequest(RootModel[ObservationRead | LookupRequest | InfoRequest]):
    """One request body; its fields decide which operation it can serve."""


class ObservationGateway:
    """Serves whitelisted reads and lookups; the provider credentials stay here."""

    request_type = GatewayRequest

    def __init__(self, env_file: Path):
        self.env_file = env_file
        self.readers: dict[ChainId, EthereumReader] = {}
        # Each vendor's client, and the CoinGecko key, are opened on first use, so a
        # gateway without the key still serves reads and Morpho lookups.
        self.lookups = VendorLookups(env_file)
        self.hypercore: HyperCoreInfo | None = None
        self.lock = threading.Lock()

    def reader(self, chain: ChainId) -> EthereumReader:
        # The first request per chain verifies the endpoint identity under the lock,
        # so concurrent requests never open two transports for one chain.
        with self.lock:
            reader = self.readers.get(chain)
            if reader is None:
                reader = self.readers[chain] = verified_reader(chain, self.env_file)
            return reader

    def info(self) -> HyperCoreInfo:
        with self.lock:
            if self.hypercore is None:
                self.hypercore = HyperCoreInfo()
            return self.hypercore

    def dispatch(self, method: str, body: GatewayRequest) -> dict[str, Any]:
        request = body.root
        if method == "read" and isinstance(request, ObservationRead):
            # A block tag passes through unchanged: the client pins its observation block
            # behind it (`EthereumReader.pin_block`), and this reader proves every pinned
            # block it reads at, so a read the provider runs elsewhere is refused here.
            reader = self.reader(request.chain)
            result = reader._rpc(request.method, list(request.params), allow_none=True)
            return {"request": request.model_dump(mode="json"), "result": result}
        if method == "lookup" and isinstance(request, LookupRequest):
            return self.lookups.fetch(request).model_dump(mode="json")
        if method == "info" and isinstance(request, InfoRequest):
            return self.info().fetch(request).model_dump(mode="json")
        raise ValueError("unsupported observation gateway operation")

    def close(self) -> None:
        for reader in self.readers.values():
            reader.__exit__()
        self.lookups.close()
        if self.hypercore is not None:
            self.hypercore.close()


class GatewayReader(EthereumReader):
    """Reads through an observation gateway. No provider credential is present here."""

    def __init__(self, chain: Chain, endpoint: str, token: str):
        # The gateway meters provider calls; this side only forwards them.
        super().__init__(chain, loopback_endpoint(endpoint), local_transport=True, observation=True)
        # The gateway's own provider hop has a 15 s deadline; waiting for its answer
        # must outlast that, or a slow provider surfaces here as an unreachable gateway.
        self._client.timeout = httpx.Timeout(15.0, read=60.0)
        self.token = token

    def _rpc(self, method: str, params: list[Any], *, allow_none: bool = False) -> Any:
        try:
            request = ObservationRead.model_validate(
                {"chain": self.chain.chain_id, "method": method, "params": tuple(params)}
            )
        except ValidationError:
            raise DataError("unsupported", f"{method}: not an observation gateway read") from None
        observed = self.send(request)
        if observed.request != request:
            raise DataError("source_mismatch", f"{method}: gateway answered a different read")
        if observed.result is None and not allow_none:
            raise DataError("unavailable", f"{method}: requested data is unavailable")
        return observed.result

    def send(self, request: ObservationRead) -> ObservationResult:
        method = request.method
        try:
            response = self._client.post(
                self._endpoint + "/read",
                content=request.model_dump_json(),
                headers={
                    "Authorization": "Bearer " + self.token,
                    "Content-Type": "application/json",
                },
            )
            payload = response.json()
        except (httpx.HTTPError, ValueError):
            raise DataError(
                "gateway_unavailable", f"{method}: observation gateway request failed"
            ) from None
        if not isinstance(payload, dict):
            raise DataError("malformed", f"{method}: invalid gateway response")
        if response.status_code != 200:
            # The gateway already sanitized the failure and named the method.
            kind = payload.get("error", "gateway_error")
            message = payload.get("message", f"{method}: observation gateway refused the read")
            if payload.get("reverted") is True:
                # A contract revert stays a revert on this side. Its data stays with the
                # gateway: the flag alone tells a revert from a refused read.
                raise Reverted(str(message), None, refusal_code(payload))
            raise DataError(str(kind), str(message), code=refusal_code(payload))
        try:
            # JSON mode keeps strict semantics: JSON arrays validate as typed tuples.
            return ObservationResult.model_validate_json(json.dumps(payload["result"]))
        except (KeyError, TypeError, ValidationError):
            raise DataError("malformed", f"{method}: invalid gateway response") from None


def refusal_code(payload: dict) -> int | None:
    # The provider's JSON-RPC code, which the gateway passes on beside its refusal.
    code = payload.get("code")
    if code is None:
        return None
    if type(code) is not int:
        raise DataError("malformed", "invalid gateway refusal code")
    return code


def gateway_reader(chain: ChainId, endpoint: str, token_file: Path) -> EthereumReader:
    """Open a gateway-backed reader and verify the gateway routed the requested chain."""
    reader = GatewayReader(
        Chain(chain_id=chain, environment="mainnet"), endpoint, read_token(token_file)
    )
    try:
        reader.verify_chain()
    except Exception:
        reader.__exit__()
        raise
    return reader


def command(args) -> int:
    # Port 0 binds an ephemeral port that gateway_ready reports; anything outside
    # the TCP range would otherwise fail inside the server as an OverflowError.
    if not 0 <= args.port <= 65535:
        raise ValueError("read gateway port must be between 0 and 65535")
    # Transports open on first use; an invalid provider cap must stop the gateway now.
    for category in VARIABLES:
        configured_limit(category)
    gateway = ObservationGateway(args.env_file)
    server = http_server(
        gateway,
        args.auth_token_file,
        args.port,
        request_type=GatewayRequest,
        fields={"read", "lookup", "info"},
    )
    try:
        print(
            json.dumps(
                {"event": "gateway_ready", "port": server.server_port, "authority": "read_only"}
            ),
            flush=True,
        )
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        gateway.close()
    return 0
