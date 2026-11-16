"""A synthesized HyperCore for HyperEVM mainnet forks (blueprint 30, section 9).

Anvil runs no HyperCore: CoreWriter only emits its log. This model applies a fork's
HyperEVM to HyperCore inputs (CoreDepositWallet credits, HYPE sent to its system
address, CoreWriter `RawAction` logs) to its own state, answers the custody `/info`
queries from it in the API's shapes, and pays HyperCore to HyperEVM credits back onto
the fork as the mainnet system transaction does: the USDC system address calls
`CoreDepositWallet.transfer(safe, amount)`.

Nothing here is evidence of HyperCore's behaviour. Each rule follows a named
assumption of the register retained in the model's state, mostly settled by the
mainnet canary, and every answer carries the register version as its source. The
model never runs outside an identified Anvil fork workspace. Custody is CoreWriter
actions 7, 9, 13 and 16; trading, `/exchange` and the trading `/info` answers are in
`fork_trading.py` (phases 3 and 4).
"""

import json
import os
import threading
from decimal import Decimal
from pathlib import Path

from eth_abi import decode
from eth_utils import keccak

from almanak.connectors.hyperliquid.assets import CORE_DEPOSIT_WALLET
from almanak.connectors.hyperliquid.corewriter import (
    ADD_API_WALLET,
    CORE_WRITER,
    DISABLED,
    HYPE_SYSTEM,
    HYPE_TOKEN,
    SEND_ASSET,
    SET_ABSTRACTION,
    SPOT_DEX,
    USD_CLASS_TRANSFER,
    USDC_SYSTEM,
    USDC_TOKEN,
    decoded,
)
from almanak.connectors.hyperliquid.custody_evidence import RAW_ACTION, TRANSFER, topic
from almanak.connectors.hyperliquid.fork_trading import NotModeled, TradingModel
from almanak.gateway.safe_fixture import calldata
from almanak.models import ZERO_ADDRESS

REGISTER = "hypercore-model:phase4-1"
ASSUMPTIONS = {
    "A2": "Action 9 from a contract account registers the agent; userRole answers agent "
    "with the account (confirmed by the canary).",
    "A3": "valid_until in the name sets the expiry extraAgents reports as validUntil "
    "(confirmed by the canary); an expired agent is no longer listed or recognised.",
    "A7": "The first USDC credit activates the account and pays 1 USDC from it; USDC "
    "returns through CoreDepositWallet in the next block for 0.00002 HYPE on HyperCore "
    "(confirmed by the canary). A first credit of HYPE does not activate an account.",
    "A8": "Custody actions apply with their block (as in the canary).",
    "A9": "Action 9 applies only to an address HyperCore holds no account for; an applied "
    "approval under an existing agent's name, or a second empty-name one, replaces that "
    "agent; extraAgents lists named agents only; a second action 9 within seconds of the "
    "account's previous one is dropped (confirmed by the canary). The model drops an "
    "action 9 sent less than 30 seconds after the account's previous one, between the "
    "canary's dropped 2 seconds and applied 35 seconds, and a fourth named agent.",
    "M1": "Any action the model does not implement, or that its rules refuse, is dropped "
    "with no output, as on HyperCore: an amount above the balance, a perp-to-spot class "
    "transfer above the withdrawable amount an open position leaves, an action 13 that "
    "is not spot USDC to its system address or whose amount has no 6-decimal USDC "
    "value.",
    "A4": "An /exchange action past its expiresAfter is refused synchronously (confirmed by "
    "the canary); an order with no record is unknownOid, also after expiry; a noop uses "
    "its nonce, so the same nonce is refused afterwards.",
    "A5": "A repeated cloid is accepted; orderStatus by cloid answers the newest order "
    "with it (confirmed by the canary).",
    "A6": "A new account reads cross at the lower of 20x and the market's maximum "
    "(20x on BTC and ETH in the canary; the lower bound is unobserved).",
    "M2": "Trading is the model's own: an Ioc order walks the book to its limit and the "
    "rest is cancelled, an Ioc order that reaches no level is refused with no order "
    "record, the strategy's fills do not consume the book, taker fee 0.045% and maker fee "
    "0.015% rounded to the micro-USDC, no address limit; funding at each fork-time hour "
    "boundary is the seeded hourly rate times the notional at the oracle; below half the "
    "initial margin at maximum leverage the cross positions, or one isolated position, "
    "close at mark in liquidation fills paying the taker fee, with one liquidation ledger "
    "entry, and their open orders are cancelled (liquidatedCanceled).",
    "A10": "An agent's updateLeverage with isCross false sets isolated margin while flat, "
    "and its updateIsolatedMargin moves margin between the cross balance and the isolated "
    "position, answers default and leaves no record in any /info query, ledger included "
    "(T6 and T10 open); account value is unchanged by it.",
    "A11": "An agent's limit trigger order (isMarket false) is accepted and answers "
    "resting with its oid; it triggers when the mark reaches its price (below for a stop "
    "that sells or a take-profit that buys, above otherwise), keeps its oid and cloid "
    "through its fills, and is listed by openOrders; an agent's scheduleCancel answers "
    "default, is refused less than 5 seconds ahead, cancels every open order of the "
    "account, trigger orders included, at its time, and has no /info read (T9 open).",
    "M3": "Resting orders are the model's own: the book is the seeded one moved by the "
    "scripted mark's difference from the seeded mark; a Gtc order fills what crosses and "
    "rests the rest, an Alo that would cross is refused, a resting limit fills whole at "
    "its limit as maker once a scripted mark reaches it, a triggered order walks the book "
    "as taker to its limit and rests any rest; a reduce-only order trades at most the "
    "opposite position and is cancelled (reduceOnlyCanceled) when nothing is left to "
    "reduce, a resting order the margin no longer covers is cancelled (marginCanceled) "
    "when it would fill; a scheduled cancel fires at the first fork block past its time; "
    "an isolated position holds notional over leverage at each entry plus what "
    "updateIsolatedMargin adds, keeps at least its initial margin when margin is removed, "
    "and isolated leverage changes only while flat.",
}
CORE_DECIMALS = {USDC_TOKEN: 8, HYPE_TOKEN: 8}
TOKEN_NAMES = {USDC_TOKEN: "USDC", HYPE_TOKEN: "HYPE"}
ACTIVATION_FEE = 10**8  # 1 USDC in HyperCore units
SYSTEM_FEE = 2000  # 0.00002 HYPE in HyperCore units, per withdrawal (canary)
HYPE_EVM_PER_CORE = 10**10
MAXIMUM_EXPIRY_MS = 180 * 24 * 3600 * 1000
MAXIMUM_NAMED_AGENTS = 3
APPROVAL_SPACING_MS = 30 * 1000
RECEIVED = "0x" + keccak(text="Received(address,uint256)").hex()


def text(amount: int, decimals: int) -> str:
    """An amount as the API writes it: a plain decimal with at least one fraction digit."""
    value = format(Decimal(amount).scaleb(-decimals).normalize(), "f")
    return value if "." in value else value + ".0"


class Dropped(Exception):
    """The model refuses the action: it leaves no trace, as on HyperCore."""


class HyperCoreModel(TradingModel):
    """The model's state, one JSON document rewritten atomically after each block."""

    def __init__(self, path: Path, start_block: int):
        self.path = path
        self.lock = threading.RLock()
        if path.exists():
            self.doc = json.loads(path.read_text())
            if self.doc["register"] != REGISTER:
                raise ValueError("the model state belongs to another assumption register")
        else:
            self.doc = {
                "register": REGISTER,
                "assumptions": ASSUMPTIONS,
                "block": start_block,
                "time": 0,
                "nonce": 0,
                "hashes": 0,
                "accounts": {},
                "agents": {},
                "seen": [],
                "ledger": {},
                "faults": [],
                "injected": [],
                "deliveries": [],
                # Each account's last action 9, applied or dropped, in milliseconds.
                "approved_at": {},
                **self.trading_state(),
            }

    def save(self) -> None:
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.doc, indent=1, sort_keys=True))
        os.replace(temporary, self.path)

    def now(self) -> int:
        return self.doc["time"]

    # Fault injection (section 9.6). Each injected fault is retained in the state.

    def drop(self, action: int, user: str) -> None:
        with self.lock:
            self.doc["faults"].append({"kind": "drop", "action": action, "user": user.lower()})

    def grief(self, address: str) -> None:
        """Before the next block's actions, an outsider makes `address` a HyperCore user."""
        with self.lock:
            self.doc["faults"].append({"kind": "grief", "address": address.lower()})

    # Applying a fork block.

    def apply(self, number: int, timestamp: int, logs: list[dict]) -> list[dict]:
        """Apply one block: credits, then CoreWriter actions. Returns the HyperEVM credits
        HyperCore owes, to pay in a following block."""
        with self.lock:
            if number <= self.doc["block"]:
                raise ValueError("a fork block is applied once, in order")
            self.elapse(timestamp * 1000)
            self.griefs(number)
            for log in logs:
                self.credit(log, number)
            owed = []
            for log in logs:
                owed.extend(self.action(log, number))
            # Each credit HyperCore owes is retained under its own ID before it is paid.
            owed = [{"id": f"{number}:{i}", **d} for i, d in enumerate(owed)]
            self.doc["block"] = number
            self.doc["deliveries"].extend(owed)
            self.save()
            return owed

    def griefs(self, number: int) -> None:
        for fault in [f for f in self.doc["faults"] if f["kind"] == "grief"]:
            self.doc["faults"].remove(fault)
            address = fault["address"]
            self.account(address, number)
            self.remember(address)
            self.doc["injected"].append({**fault, "block": number})

    def credit(self, log: dict, number: int) -> None:
        source, topics = log["address"].lower(), log["topics"]
        if source == CORE_DEPOSIT_WALLET and topics[0::2] == [TRANSFER, topic(USDC_SYSTEM)]:
            recipient = "0x" + topics[1][-40:]
            self.credit_usdc(recipient, int(log["data"], 16) * 100, number)
        elif source == HYPE_SYSTEM and topics[:1] == [RECEIVED]:
            user, amount = received(log)
            self.credit_hype(user, amount // HYPE_EVM_PER_CORE, number)

    def credit_usdc(self, recipient: str, wei: int, number: int) -> None:
        new = recipient not in self.doc["accounts"]
        if new and wei < ACTIVATION_FEE:
            return
        fee = ACTIVATION_FEE if new else 0
        account = self.account(recipient, number)
        self.remember(recipient)
        account["spot"][str(USDC_TOKEN)] = account["spot"].get(str(USDC_TOKEN), 0) + wei - fee
        digest = self.hash()
        if new:
            activation = {"amount": "1.0", "token": "USDC", "type": "accountActivationGas"}
            self.write(recipient, digest, activation)
        entry = spot_transfer(USDC_SYSTEM, recipient, USDC_TOKEN, wei, fee, self.next_nonce())
        self.write(recipient, digest, entry)

    def credit_hype(self, user: str, wei: int, number: int) -> None:
        account = self.doc["accounts"].get(user)
        if account is None or wei == 0:
            return
        account["spot"][str(HYPE_TOKEN)] = account["spot"].get(str(HYPE_TOKEN), 0) + wei
        entry = spot_transfer(HYPE_SYSTEM, user, HYPE_TOKEN, wei, 0, self.next_nonce())
        self.write(user, self.hash(), entry)

    def action(self, log: dict, number: int) -> list[dict]:
        if log["address"].lower() != CORE_WRITER or log["topics"][:1] != [RAW_ACTION]:
            return []
        user = "0x" + log["topics"][1][-40:]
        (raw,) = decode(("bytes",), bytes.fromhex(log["data"][2:]))
        parsed = decoded(raw)
        try:
            if parsed is None or self.faulted(parsed[0], user, number):
                raise Dropped
            account = self.doc["accounts"].get(user)
            # An account HyperCore did not hold before this block cannot act in it.
            if account is None or account["created"] >= number:
                raise Dropped
            return HANDLERS[parsed[0]](self, user, account, parsed[1])
        except Dropped:
            return []

    def faulted(self, action: int, user: str, number: int) -> bool:
        for fault in self.doc["faults"]:
            if fault["kind"] == "drop" and (fault["action"], fault["user"]) == (action, user):
                self.doc["faults"].remove(fault)
                self.doc["injected"].append({**fault, "block": number})
                return True
        return False

    def class_transfer(self, user, account, fields) -> list[dict]:
        usdc, to_perp = fields
        spot = account["spot"].get(str(USDC_TOKEN), 0)
        # Perp USDC an open position needs as margin cannot leave for spot.
        available = spot // 100 if to_perp else self.withdrawable_micro(user)
        if not 0 < usdc <= available:
            raise Dropped
        sign = 1 if to_perp else -1
        account["spot"][str(USDC_TOKEN)] = spot - sign * usdc * 100
        account["perp"] += sign * usdc
        delta = {"toPerp": to_perp, "type": "accountClassTransfer", "usdc": text(usdc, 6)}
        self.write(user, self.hash(), delta)
        return []

    def approve_agent(self, user, account, fields) -> list[dict]:
        agent, label = fields
        agent = agent.lower()
        last = self.doc["approved_at"].get(user)
        self.doc["approved_at"][user] = self.now()
        if agent in self.doc["seen"] or (
            last is not None and self.now() - last < APPROVAL_SPACING_MS
        ):
            raise Dropped
        name, valid_until = parsed_label(label)
        if valid_until is not None and valid_until > self.now() + MAXIMUM_EXPIRY_MS:
            raise Dropped
        mine = {a: v for a, v in self.doc["agents"].items() if v["user"] == user}
        replaced = [a for a, v in mine.items() if v["name"] == name]
        named = [v for v in mine.values() if v["name"]]
        if name and not replaced and len(named) >= MAXIMUM_NAMED_AGENTS:
            raise Dropped
        for old in replaced:
            del self.doc["agents"][old]
        self.doc["agents"][agent] = {"user": user, "name": name, "valid_until": valid_until}
        self.remember(agent)
        return []

    def send_asset(self, user, account, fields) -> list[dict]:
        destination, sub_account, source, target, token, wei = fields
        spot = account["spot"].get(str(USDC_TOKEN), 0)
        hype = account["spot"].get(str(HYPE_TOKEN), 0)
        if (destination.lower(), sub_account.lower(), source, target, token) != (
            USDC_SYSTEM,
            ZERO_ADDRESS,
            SPOT_DEX,
            SPOT_DEX,
            USDC_TOKEN,
        ) or not (0 < wei <= spot and wei % 100 == 0 and hype >= SYSTEM_FEE):
            raise Dropped
        account["spot"][str(USDC_TOKEN)] = spot - wei
        account["spot"][str(HYPE_TOKEN)] = hype - SYSTEM_FEE
        self.write(user, self.hash(), send_entry(user, wei, self.next_nonce()))
        return [{"user": user, "amount": wei // 100}]

    def set_abstraction(self, user, account, fields) -> list[dict]:
        target, mode = fields
        if target.lower() != user or mode != DISABLED:
            raise Dropped
        account["abstraction"] = "disabled"
        return []

    # State helpers.

    def account(self, address: str, number: int) -> dict:
        return self.doc["accounts"].setdefault(
            address, {"created": number, "spot": {}, "perp": 0, "abstraction": "default"}
        )

    def remember(self, address: str) -> None:
        if address not in self.doc["seen"]:
            self.doc["seen"].append(address)

    def next_nonce(self) -> int:
        self.doc["nonce"] += 1
        return self.doc["nonce"]

    def hash(self) -> str:
        """A synthesized HyperCore transaction hash, unique within the run."""
        self.doc["hashes"] += 1
        return "0x" + keccak(text=f"{REGISTER}:{self.doc['hashes']}").hex()

    def write(self, user: str, digest: str, delta: dict) -> None:
        entry = {"delta": delta, "hash": digest, "time": self.now()}
        self.doc["ledger"].setdefault(user, []).append(entry)

    def delivery(self, identity: str) -> dict:
        return next(d for d in self.doc["deliveries"] if d["id"] == identity)

    def sending(self, identity: str, head: int, nonce: int) -> None:
        """Durably note, before a delivery is sent, the fork head and the system address's
        nonce it is sent at. Its transaction is in a later block or still pending, and no
        second transaction at that nonce can execute beside it."""
        with self.lock:
            self.delivery(identity).update(after=head, nonce=nonce)
            self.save()

    def delivered(self, identity: str, transaction: str, block: int) -> None:
        with self.lock:
            delivery = self.delivery(identity)
            self.doc["deliveries"].remove(delivery)
            paid = {**delivery, "transaction": transaction, "block": block}
            self.doc.setdefault("paid", []).append(paid)
            self.save()

    # `/info` answers, in the shapes `hypercore_shapes.py` closes.

    def answer(self, query) -> object:
        with self.lock:
            handler = ANSWERS.get(query.type)
            if handler is None:
                raise NotModeled(query.type)
            return handler(self, query)

    def role(self, query):
        agent = self.live_agent(query.user)
        if agent is not None:
            return {"role": "agent", "data": {"user": agent["user"]}}
        return {"role": "user" if query.user in self.doc["accounts"] else "missing"}

    def live_agent(self, address: str) -> dict | None:
        agent = self.doc["agents"].get(address)
        expiry = None if agent is None else agent["valid_until"]
        if agent is None or (expiry is not None and expiry <= self.now()):
            return None
        return agent

    def abstraction(self, query):
        return self.doc["accounts"].get(query.user, {}).get("abstraction", "default")

    def extra_agents(self, query):
        return [
            {"name": v["name"], "address": a, "validUntil": v["valid_until"]}
            for a, v in self.doc["agents"].items()
            if v["user"] == query.user and v["name"] and self.live_agent(a) is not None
        ]

    def spot(self, query):
        held = self.doc["accounts"].get(query.user, {}).get("spot", {})
        return {
            "balances": [
                {
                    "coin": TOKEN_NAMES[int(token)],
                    "token": int(token),
                    "total": text(wei, CORE_DECIMALS[int(token)]),
                    "hold": "0.0",
                    "entryNtl": "0.0",
                }
                for token, wei in sorted(held.items(), key=lambda item: int(item[0]))
                if wei
            ]
        }

    def ledger(self, query):
        entries = self.doc["ledger"].get(query.user, [])
        return [e for e in entries if query.start_time <= e["time"] <= query.end_time]


HANDLERS = {
    USD_CLASS_TRANSFER: HyperCoreModel.class_transfer,
    ADD_API_WALLET: HyperCoreModel.approve_agent,
    SEND_ASSET: HyperCoreModel.send_asset,
    SET_ABSTRACTION: HyperCoreModel.set_abstraction,
}
ANSWERS = {
    "userRole": HyperCoreModel.role,
    "userAbstraction": HyperCoreModel.abstraction,
    "extraAgents": HyperCoreModel.extra_agents,
    "spotClearinghouseState": HyperCoreModel.spot,
    "clearinghouseState": HyperCoreModel.clearinghouse,
    "userNonFundingLedgerUpdates": HyperCoreModel.ledger,
    "activeAssetData": HyperCoreModel.active_asset,
    "openOrders": HyperCoreModel.open_orders,
    "orderStatus": HyperCoreModel.order_status,
    "userFillsByTime": HyperCoreModel.user_fills,
    "userFunding": HyperCoreModel.user_funding,
    "l2Book": HyperCoreModel.book,
    "metaAndAssetCtxs": HyperCoreModel.meta_and_contexts,
    "meta": HyperCoreModel.meta,
}


def received(log: dict) -> tuple[str, int]:
    """The HYPE system contract's `Received(user, amount)`, with or without an index."""
    data = bytes.fromhex(log["data"][2:])
    if len(log["topics"]) > 1:
        return "0x" + log["topics"][1][-40:], int.from_bytes(data[:32], "big")
    user, amount = decode(("address", "uint256"), data)
    return user.lower(), amount


def parsed_label(label: str) -> tuple[str, int | None]:
    name, separator, suffix = label.partition(" valid_until ")
    if separator and suffix.isdigit():
        return name, int(suffix)
    return label, None


def spot_transfer(source, destination, token: int, wei: int, fee: int, nonce: int) -> dict:
    decimals = CORE_DECIMALS[token]
    return {
        "amount": text(wei, decimals),
        "destination": destination,
        "fee": text(fee, decimals),
        "feeToken": TOKEN_NAMES[token] if fee else "",
        "nativeTokenFee": "0.0",
        "nonce": nonce,
        "token": TOKEN_NAMES[token],
        "type": "spotTransfer",
        # A synthesized model has no HYPE price, so a HYPE credit writes no USDC value.
        "usdcValue": text(wei, decimals) if token == USDC_TOKEN else "0.0",
        "user": source,
    }


def send_entry(user: str, wei: int, nonce: int) -> dict:
    return {
        "amount": text(wei, 8),
        "destination": USDC_SYSTEM,
        "destinationDex": "spot",
        "fee": "0.0",
        "feeToken": "",
        "nativeTokenFee": text(SYSTEM_FEE, 8),
        "nonce": nonce,
        "sourceDex": "spot",
        "token": "USDC",
        "type": "send",
        "usdcValue": text(wei, 8),
        "user": user,
    }


def payment(delivery: dict) -> str:
    values = (delivery["user"], delivery["amount"])
    return calldata("transfer(address,uint256)", ("address", "uint256"), values)


class Keeper:
    """Tails the fork's blocks into the model and pays what HyperCore owes back to it."""

    INPUTS = [CORE_DEPOSIT_WALLET, HYPE_SYSTEM, CORE_WRITER]

    def __init__(self, rpc, model: HyperCoreModel):
        self.rpc = rpc
        self.model = model

    @classmethod
    def open(cls, forks, output: Path):
        from almanak.chains import HYPEREVM

        rpc = forks.rpc(HYPEREVM.chain)
        output.mkdir(parents=True, exist_ok=True)
        head = int(rpc("eth_blockNumber"), 16)
        return cls(rpc, HyperCoreModel(output / "hypercore-model.json", head))

    def advance(self) -> list[dict]:
        """Apply every block up to the head, then pay the credits owed so far."""
        rows = self.apply()
        for delivery in list(self.model.doc["deliveries"]):
            rows.append(self.pay(delivery))
        return rows

    def apply(self) -> list[dict]:
        """Apply every block up to the head, in order; it only reads the fork."""
        rows = []
        head = int(self.rpc("eth_blockNumber"), 16)
        for number in range(self.model.doc["block"] + 1, head + 1):
            block = self.rpc("eth_getBlockByNumber", [hex(number), False])
            query = {"address": self.INPUTS, "fromBlock": hex(number), "toBlock": hex(number)}
            logs = sorted(self.rpc("eth_getLogs", [query]), key=lambda e: int(e["logIndex"], 16))
            self.model.apply(number, int(block["timestamp"], 16), logs)
            rows.append({"block": number, "inputs": len(logs)})
        return rows

    def pay(self, delivery: dict) -> dict:
        """Pay a credit HyperCore owes once, also across a keeper restart.

        The payment is sent at a nonce of the system address retained beforehand. After a
        restart, a payment at that nonce that is mined is recorded, one still pending is
        waited for, and only an unused nonce is sent again, at the same nonce.
        """
        if "nonce" in delivery:
            state = self.at_nonce(delivery)
            if state == "pending":
                return {"pending": delivery}
            if state is not None:
                self.model.delivered(delivery["id"], *state)
                return {"paid": delivery, "transaction": state[0], "block": state[1]}
        else:
            nonce = int(self.rpc("eth_getTransactionCount", [USDC_SYSTEM, "pending"]), 16)
            head = int(self.rpc("eth_blockNumber"), 16)
            self.model.sending(delivery["id"], head, nonce)
            delivery = self.model.delivery(delivery["id"])
        digest = self.send(delivery)
        found = digest, int(self.rpc.wait_receipt(digest)["blockNumber"], 16)
        self.model.delivered(delivery["id"], *found)
        return {"paid": delivery, "transaction": found[0], "block": found[1]}

    def at_nonce(self, delivery: dict):
        """The mined payment at the delivery's nonce, "pending" while one waits at it, or
        None when the nonce is still unused."""
        nonce = delivery["nonce"]
        if int(self.rpc("eth_getTransactionCount", [USDC_SYSTEM, "latest"]), 16) > nonce:
            return self.mined(delivery)
        pending = int(self.rpc("eth_getTransactionCount", [USDC_SYSTEM, "pending"]), 16)
        return "pending" if pending > nonce else None

    def mined(self, delivery: dict) -> tuple[str, int]:
        """The transaction mined at the delivery's nonce, which must be its executed
        payment: the keeper alone sends from the system address on a fork."""
        head = int(self.rpc("eth_blockNumber"), 16)
        for number in range(delivery["after"] + 1, head + 1):
            block = self.rpc("eth_getBlockByNumber", [hex(number), True])
            for tx in block["transactions"]:
                if (tx["from"].lower(), int(tx["nonce"], 16)) != (USDC_SYSTEM, delivery["nonce"]):
                    continue
                if tx["input"] != payment(delivery) or not self.executed(tx["hash"]):
                    raise RuntimeError(f"the system address's nonce paid no delivery {delivery}")
                return tx["hash"], number
        raise RuntimeError(f"no mined transaction at the nonce of delivery {delivery}")

    def executed(self, digest: str) -> bool:
        receipt = self.rpc("eth_getTransactionReceipt", [digest])
        return receipt is not None and receipt["status"] == "0x1"

    def send(self, delivery: dict) -> str:
        """The system transaction mainnet sends: the system address calls
        `CoreDepositWallet.transfer(safe, amount)`. A fork charges it gas, so the system
        address is lent the fee and its balance restored."""
        native = self.rpc("eth_getBalance", [USDC_SYSTEM, "latest"])
        self.rpc("anvil_impersonateAccount", [USDC_SYSTEM])
        self.rpc("anvil_setBalance", [USDC_SYSTEM, hex(int(native, 16) + 10**18)])
        try:
            tx = {
                "from": USDC_SYSTEM,
                "to": CORE_DEPOSIT_WALLET,
                "data": payment(delivery),
                "gasPrice": self.rpc("eth_gasPrice"),
                "nonce": hex(delivery["nonce"]),
            }
            return self.rpc("eth_sendTransaction", [tx])
        finally:
            self.rpc("anvil_stopImpersonatingAccount", [USDC_SYSTEM])
            self.rpc("anvil_setBalance", [USDC_SYSTEM, native])

    def align(self, state, seconds: int) -> int:
        return seconds

    def settle(self, state) -> list[dict]:
        return [{**row, "advanced_seconds": 0} for row in self.advance()]
