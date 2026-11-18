"""The `hypercore-model` fork workspace process (blueprint 30, section 9.1).

A Hyperliquid deployment's fork workspace runs the synthesized HyperCore beside its
`eip155:999` fork node (`gateway/fork_venues.py`): one process with two faces over one
durable state document, so a gateway, keeper or process restart resumes the same
HyperCore state.

- The loopback `/info` and `/exchange` service (`fork_service.py`) is the fork gateway's
  HyperCore venue and nothing else is. Before each answer it applies the fork blocks
  mined since the last one, so every custody action applies with its block and the
  model's clock follows the fork head the gateway's clock reads.
- The keeper pays what HyperCore owes back to HyperEVM, as mainnet's system transaction
  does in the next block (`fork_model.Keeper`). It runs every second, and whenever the run's
  fork keeper (`ModelKeeper`, the connector's `Requests.fork_keeper`) settles a request.

The workspace creates the process's state once, before the fork gateway starts:

- the model at the fork head, seeded from mainnet `/info` (section 9.2): `metaAndAssetCtxs`
  and the configured market's `l2Book`, read from the mainnet source the read gateway holds
  and retained with their read times, the anchor's time and the gap between them. `/info`
  has no as-of query, so the gap is a retained fact; the seed's market must still be the
  one resolution pinned. Fork accounts start with no HyperCore state: a fresh Safe has none.
- the deployment's agent key file, empty and owner-only, which the gateway's
  `LocalAgentKeys` fills with each D9 key (section 5.4);
- the Safe owners' HyperCore HYPE for withdrawals' system transactions (section 7), as the
  fork tests provide it: the owner sends it from the Safe to the HYPE system address once
  the first deposit has activated the account, since the model, like HyperCore, credits
  no HYPE to an account that does not exist (A7). The fixture funds the Safe with it on
  HyperEVM, and the keeper sends it as the owners from the first block that shows the
  account activated, at an owner nonce it retains first. After a restart the transaction at
  that nonce decides: the transfer is recorded, never sent twice, and a nonce another owner
  transaction took (permission installation also sends as the owners) is sent again.

The process serves only beside the identified Anvil fork it was created for. Its control
face (`POST /control`) is the keeper's and a test driver's: it advances the keeper,
returns the state as evidence, and injects section 9.6's faults. Like the fork node's own
cheat methods, it is reachable on loopback only.
"""

import json
import threading
from contextlib import contextmanager
from pathlib import Path

import httpx
from eth_account import Account

from almanak.connectors.hyperliquid.corewriter import HYPE_SYSTEM
from almanak.connectors.hyperliquid.fork import VENUE
from almanak.connectors.hyperliquid.fork_model import (
    RECEIVED,
    HyperCoreModel,
    Keeper,
    received,
)
from almanak.connectors.hyperliquid.fork_service import handler_for
from almanak.gateway.auth import loopback_endpoint
from almanak.gateway.ethereum import DataError
from almanak.gateway.hypercore_info import API, HyperCoreInfo, InfoRequest, L2Book, MetaAndAssetCtxs

PROCESS = "hypercore-model"
# 0.01 HYPE, as the fork tests send: five hundred withdrawals at 0.00002 HYPE each (A7).
HYPERCORE_HYPE = 10**16
KEEPER_SECONDS = 1.0
FIXTURE = "hypercore_hype"
# The model's fault injection (section 9.6) a test driver reaches, with its arguments.
FAULTS = {
    "withhold": ("action",),
    "lose": ("action",),
    "refuse": ("action",),
    "drop": ("action", "user"),
    "grief": ("address",),
    "move_mark": ("coin", "price", "reason"),
    "place_outside_order": ("user", "coin", "is_buy", "price", "size"),
}


def mainnet_info() -> HyperCoreInfo:
    """The seed's source: Hyperliquid's mainnet `/info`, the read gateway's own source."""
    return HyperCoreInfo()


def start(deployment, host) -> dict:
    """Create the venue's state once, start or reattach its process, and name both in
    the fork route of the deployment's chain."""
    (allocation,) = deployment.allocations
    node = host.node(allocation.chain)
    spec = prepare(host.directory / VENUE, deployment, node)
    endpoint = host.launch(PROCESS, spec)
    keys = json.loads(spec.read_text())["agent_keys"]
    return {allocation.chain: {VENUE: {"model": endpoint, "agent_keys": keys}}}


def prepare(directory: Path, deployment, node) -> Path:
    """The process's spec, its state written before it: once, never repeated after an
    interruption, as fixture funding is not."""
    from almanak.gateway.fork_workspace import write_private

    spec = directory / "spec.json"
    if spec.exists():
        return spec
    if directory.exists():
        raise RuntimeError("HyperCore model creation was interrupted; retain its evidence")
    (allocation,) = deployment.allocations
    safe = deployment.accounts.binding(allocation.account).profile.fund.address
    # The reads first: a failed one creates nothing, so the workspace can try again.
    info = mainnet_info()
    try:
        answers = seed_answers(info, allocation.market)
    finally:
        info.close()
    directory.mkdir(mode=0o700, parents=True)
    state = directory / "hypercore-model.json"
    model = HyperCoreModel(state, int(node.rpc("eth_blockNumber"), 16))
    seed(model, allocation.market, answers, node.origin)
    fund_safe(node, model, safe)
    keys = directory / "agent-keys.json"
    write_private(keys, "{}")
    values = {
        "connector": "hyperliquid",
        "venue": PROCESS,
        "rpc": node.rpc.endpoint,
        "instance": node.metadata["instanceId"],
        "origin": node.origin.model_dump(mode="json"),
        "state": str(state),
        "agent_keys": str(keys),
        "owner_key": node.fixture_owners[allocation.account].key.hex(),
    }
    write_private(spec, json.dumps(values, indent=2))
    return spec


def seed_answers(info, market) -> tuple:
    """The seed's `metaAndAssetCtxs` and the market's `l2Book`, the market checked to be
    the one resolution pinned."""
    contexts = info.fetch(InfoRequest(info=MetaAndAssetCtxs()))
    book = info.fetch(InfoRequest(info=L2Book(coin=market.coin)))
    require_market(contexts.result, market)
    return contexts, book


def seed(model: HyperCoreModel, market, answers, origin) -> None:
    """Seed the model's markets from mainnet answers, retained with their own source."""
    contexts, book = answers
    source = {
        "api": API,
        "metaAndAssetCtxs": contexts.received_at,
        f"l2Book.{market.coin}": book.received_at,
        "anchor": {"number": origin.number, "hash": origin.hash, "timestamp": origin.timestamp},
        "gap_seconds": min(contexts.received_at, book.received_at) // 1000 - origin.timestamp,
    }
    model.seed(contexts.result, {market.coin: book.result}, source)


def require_market(meta_and_contexts, market) -> None:
    universe = meta_and_contexts[0]["universe"]
    found = universe[market.asset] if market.asset < len(universe) else {}
    seeded = (found.get("name"), found.get("szDecimals"), found.get("maxLeverage"))
    if seeded != (market.coin, market.size_decimals, market.max_leverage):
        raise DataError("source_mismatch", "the HyperCore seed's market differs from resolution's")


def fund_safe(node, model: HyperCoreModel, safe: str) -> None:
    """The fixture's HyperCore HYPE, held by the Safe on HyperEVM until activation."""
    held = int(node.rpc("eth_getBalance", [safe, "latest"]), 16)
    node.rpc("anvil_setBalance", [safe, hex(held + HYPERCORE_HYPE)])
    with model.lock:
        fixture = {"user": safe, "amount": HYPERCORE_HYPE, "status": "waiting"}
        model.doc.setdefault("fixture", {})[FIXTURE] = fixture
        model.save()


def owner_transaction(rpc, owner, safe, to, data, value):
    from almanak.gateway.safe_fixture import owner_transaction as send

    return send(rpc, owner, safe, to, data, value)


class ModelProcess:
    """The process's model and keeper behind one lock: an answer, a keeper pass and an
    injected fault each see every block applied before them, once."""

    def __init__(self, model: HyperCoreModel, keeper: Keeper, owner):
        self.model, self.keeper, self.owner = model, keeper, owner
        self.lock = threading.Lock()

    @contextmanager
    def synced(self):
        """The fork's blocks applied and held: what runs inside sees exactly those."""
        with self.lock:
            self.keeper.apply()
            yield

    def advance(self) -> list[dict]:
        """Apply the fork's blocks, pay what HyperCore owes and send the owners' HYPE once
        the account exists; rows for blocks without inputs are left out."""
        with self.lock:
            rows = self.keeper.advance()
            funded = self.fund_hype()
            if funded is not None:
                rows += [funded, *self.keeper.apply()]
        return [row for row in rows if row.get("inputs", 1)]

    def fund_hype(self) -> dict | None:
        fixture = self.model.doc.get("fixture", {}).get(FIXTURE)
        if fixture is None or fixture["status"] == "sent":
            return None
        if fixture["user"] not in self.model.doc["accounts"]:
            return None
        if fixture["status"] == "sending" and self.nonce() > fixture["nonce"]:
            # Mined before a restart: sent only if the retained nonce carried the transfer.
            found = self.sent_at(fixture)
            if found is not None:
                return self.record(fixture, status="sent", transaction=found[0], block=found[1])
        # The head before the nonce: a transaction at that nonce is mined after the head.
        head = int(self.keeper.rpc("eth_blockNumber"), 16)
        self.record(fixture, status="sending", after=head, nonce=self.nonce())
        receipt = owner_transaction(
            self.keeper.rpc, self.owner, fixture["user"], HYPE_SYSTEM, "0x", fixture["amount"]
        )
        block = int(receipt["blockNumber"], 16)
        return self.record(
            fixture, status="sent", transaction=receipt["transactionHash"], block=block
        )

    def sent_at(self, fixture: dict) -> tuple[str, int] | None:
        """The executed transfer mined at the fixture's retained owner nonce, or None when
        another owner transaction took that nonce."""
        rpc, owner = self.keeper.rpc, (self.owner.address.lower(), fixture["nonce"])
        for number in range(fixture["after"] + 1, int(rpc("eth_blockNumber"), 16) + 1):
            for tx in rpc("eth_getBlockByNumber", [hex(number), True])["transactions"]:
                if (tx["from"].lower(), int(tx["nonce"], 16)) == owner:
                    return (tx["hash"], number) if self.transferred(tx["hash"], fixture) else None
        return None

    def transferred(self, digest: str, fixture: dict) -> bool:
        """Whether the transaction executed the fixture's HYPE transfer: the HYPE system
        address's `Received` for the Safe and amount, which the model credits."""
        receipt = self.keeper.rpc("eth_getTransactionReceipt", [digest])
        wanted = (fixture["user"], fixture["amount"])
        logs = receipt["logs"] if receipt["status"] == "0x1" else []
        return any(
            (log["address"].lower(), log["topics"][:1]) == (HYPE_SYSTEM, [RECEIVED])
            and received(log) == wanted
            for log in logs
        )

    def nonce(self) -> int:
        return int(self.keeper.rpc("eth_getTransactionCount", [self.owner.address, "latest"]), 16)

    def record(self, fixture: dict, **updates) -> dict:
        with self.model.lock:
            fixture.update(updates)
            self.model.save()
        return {"fixture": FIXTURE, **fixture}

    def control(self, body: dict) -> dict:
        operation = body["op"]
        if operation == "advance":
            return {"rows": self.advance()}
        if operation == "state":
            with self.model.lock:
                return {"model": json.loads(json.dumps(self.model.doc))}
        fields = FAULTS[operation]
        if set(body) != {"op", *fields}:
            raise ValueError(f"{operation} takes exactly {', '.join(fields)}")
        with self.synced():
            result = getattr(self.model, operation)(**{f: body[f] for f in fields})
        return {"result": result}

    def keep(self, stopped: threading.Event) -> None:
        while not stopped.wait(KEEPER_SECONDS):
            try:
                self.advance()
            except Exception as failure:  # noqa: BLE001 - a failed pass is retried next second
                event = {"event": "hypercore_keeper_failed", "error": str(failure)}
                print(json.dumps(event), flush=True)


def require_fork(rpc, spec: dict) -> None:
    metadata = rpc("anvil_metadata")
    network = metadata.get("forkedNetwork") or {}
    origin = spec["origin"]
    observed = (
        metadata.get("instanceId"),
        network.get("forkBlockNumber"),
        network.get("forkBlockHash"),
    )
    if observed != (spec["instance"], origin["number"], origin["hash"]):
        raise RuntimeError("the HyperCore model serves only beside its identified Anvil fork")


def open_process(spec: dict, rpc=None) -> ModelProcess:
    from almanak.gateway.fork_fixture import Rpc

    rpc = rpc or Rpc(spec["rpc"])
    require_fork(rpc, spec)
    state = Path(spec["state"])
    if not state.is_file():
        raise RuntimeError("the HyperCore model's state is created by its fork workspace")
    model = HyperCoreModel(state, 0)
    return ModelProcess(model, Keeper(rpc, model), Account.from_key(spec["owner_key"]))


def serve(spec: dict, listen) -> int:
    """The `hypercore-model` process: its keeper in the background, the service in front."""
    from almanak.gateway.service import loopback_server

    process = open_process(spec)
    server = loopback_server(listen, handler_for(process.model, process.synced, process.control))
    stopped = threading.Event()
    threading.Thread(target=process.keep, args=(stopped,), daemon=True).start()
    try:
        server.serve_forever()
    finally:
        stopped.set()
        server.server_close()
    return 0


class ModelKeeper:
    """The run's fork keeper: the `hypercore-model` process's keeper face (section 9.1).

    It settles nothing a GMX keeper would: HyperCore applies each custody action with
    its block, and the process pays what HyperCore owes on its own. Settling advances the
    process at once and returns what it did as the run's evidence; fork time never moves.
    """

    def __init__(self, endpoint: str):
        self.endpoint = loopback_endpoint(endpoint)

    @classmethod
    def open(cls, forks, output):
        return cls(forks.venue(PROCESS)["endpoint"])

    def control(self, body: dict) -> dict:
        response = httpx.post(self.endpoint + "/control", json=body, timeout=60)
        if response.status_code != 200:
            raise RuntimeError(f"the HyperCore model refused {body['op']}: {response.text}")
        return response.json()

    def align(self, state, seconds: int) -> int:
        return seconds

    def settle(self, state) -> list[dict]:
        rows = self.control({"op": "advance"})["rows"]
        return [{**row, "advanced_seconds": 0} for row in rows]
