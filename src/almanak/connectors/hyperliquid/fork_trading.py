"""The synthesized HyperCore's trading side (blueprint 30, sections 9.3 to 9.6).

`/exchange` checks an agent's signature with the model's own verifier, its expiry and
its nonce before it applies anything, and answers in the venue's synchronous shapes.
Markets come from a mainnet `/info` seed; prices stay at the seed unless the test driver
scripts a change, which is retained with its reason. The rules are the model's own,
stated in its assumption register, never claimed as HyperCore's:

- an `Ioc` order walks the book up to its limit and fills what it reaches, the rest
  cancelled; the strategy's fills do not consume the book, and the book is the seeded
  one moved by the scripted mark's difference from the seeded mark; taker fee 0.045%;
- a `Gtc` order fills what crosses the book as an `Ioc` order would and rests the
  rest; an `Alo` order that would cross is refused; a resting order fills whole at its
  limit, maker fee 0.015%, once a scripted mark reaches it;
- a limit trigger order rests until the mark reaches its trigger, then walks the book
  as a taker up to its limit and rests any remainder; it keeps its oid and cloid;
- a reduce-only order trades at most the opposite position, and one with nothing left
  to reduce is cancelled when it would trade; a resting order the margin no longer
  covers is cancelled when it would fill;
- a scheduled cancel fires at its time, at the first fork block past it, and cancels
  every open order of the account, trigger orders included;
- an order that would leave the cross account below initial margin (notional over
  leverage) is refused, and a new account reads cross at the lower of 20x and the
  market's maximum (A6); an isolated position holds its own margin, notional over
  leverage at each entry plus what `updateIsolatedMargin` adds or removes;
- at each fork-time hour boundary each position pays the seeded hourly rate times its
  notional at the oracle price;
- below maintenance margin (half the initial margin at maximum leverage) the cross
  positions, or one isolated position, close at mark in liquidation fills, a
  `liquidation` ledger entry is written and their open orders are cancelled.

Fault injection (section 9.6) applies an action and withholds its answer, loses an
action before it applies, refuses one whole, places an order outside the strategy's
attempts, and moves the mark. Each injected fault is retained in the model's state.
"""

import threading
from decimal import ROUND_DOWN, ROUND_HALF_EVEN, Decimal

from almanak.connectors.hyperliquid.fork_verifier import signer_of

ZERO_HASH = "0x" + "00" * 32
HOUR_MS = 3600 * 1000
TAKER_FEE = Decimal("0.00045")
MAKER_FEE = Decimal("0.00015")
USDC = Decimal("0.000001")
DEFAULT_LEVERAGE = 20
NONCE_SET = 100
NONCE_PAST_MS = 2 * 24 * 3600 * 1000
NONCE_FUTURE_MS = 24 * 3600 * 1000
EXPIRED = "Action already expired"
NO_MATCH = "Order could not immediately match against any resting orders."
NEVER_PLACED = "Order was never placed, already canceled, or filled."
REDUCE_ONLY = "Reduce only order would increase position."
# The order states in which the model still holds an order.
LIVE = ("open", "triggered")
# HyperCore refuses a scheduled cancel less than 5 seconds ahead (API documentation).
SCHEDULE_LEAD_MS = 5_000


class NotModeled(Exception):
    """A query the model does not answer, such as a market read before any seed."""


def dtext(value: Decimal) -> str:
    """A decimal as the API writes it: plain digits with at least one fraction digit."""
    written = format(value.normalize(), "f")
    if written in ("-0", "-0.0"):
        written = "0"
    return written if "." in written else written + ".0"


def usdc(value: Decimal) -> Decimal:
    return value.quantize(USDC, ROUND_HALF_EVEN)


def micro(value: Decimal) -> int:
    return int(usdc(value) / USDC)


def err(message: str) -> dict:
    return {"status": "err", "response": message}


def ok() -> dict:
    return {"status": "ok", "response": {"type": "default"}}


class TradingModel:
    """Trading state and answers mixed into `HyperCoreModel`; it shares its document."""

    doc: dict
    lock: threading.RLock

    def trading_state(self) -> dict:
        return {
            "seed": None,
            "prices": {},
            "marks": [],
            "positions": {},
            "leverage": {},
            "orders": {},
            "cloids": {},
            "resting": {},
            "fills": {},
            "funding": {},
            "nonces": {},
            # Each account's markets set isolated, its scheduled cancels and the cancels
            # that fired.
            "modes": {},
            "schedules": {},
            "fired": [],
            "next_oid": 1000,
            "next_tid": 1,
            "exchange_faults": [],
            # Every `/exchange` post's nonce and action type, in arrival order: the
            # evidence of how often a request was posted, refused ones included.
            "posts": [],
        }

    # Hooks the custody model provides.

    def now(self) -> int:
        raise NotImplementedError

    def save(self) -> None:
        raise NotImplementedError

    def hash(self) -> str:
        raise NotImplementedError

    def write(self, user: str, digest: str, delta: dict) -> None:
        raise NotImplementedError

    def live_agent(self, address: str) -> dict | None:
        raise NotImplementedError

    # The seed (section 9.2): mainnet answers, retained with their own source.

    def seed(self, meta_and_contexts: list, books: dict, source: dict) -> None:
        with self.lock:
            self.doc["seed"] = {"meta": meta_and_contexts, "books": books, "source": source}
            self.save()

    def seeded(self) -> dict:
        if self.doc["seed"] is None:
            raise NotModeled("markets before a seed")
        return self.doc["seed"]

    def universe(self) -> list:
        return self.seeded()["meta"][0]["universe"]

    def market(self, asset: int) -> tuple[str, dict, dict]:
        universe = self.universe()
        if not 0 <= asset < len(universe):
            raise KeyError(asset)
        return universe[asset]["name"], universe[asset], self.doc["seed"]["meta"][1][asset]

    def asset_of(self, coin: str) -> int:
        names = [asset["name"] for asset in self.universe()]
        return names.index(coin)

    def price(self, coin: str, kind: str) -> Decimal:
        scripted = self.doc["prices"].get(coin)
        if scripted is not None:
            return Decimal(scripted)
        _, _, context = self.market(self.asset_of(coin))
        return Decimal(context["markPx" if kind == "mark" else "oraclePx"])

    def move_mark(self, coin: str, price: str, reason: str) -> None:
        """A scripted mark and oracle change: synthesized market data, never an observation."""
        with self.lock:
            self.tick()
            self.doc["prices"][coin] = price
            self.doc["marks"].append(
                {"coin": coin, "price": price, "reason": reason, "time": self.now()}
            )
            self.doc["injected"].append({"kind": "mark", "coin": coin, "price": price})
            self.cross(coin)
            self.liquidate_underwater()
            self.save()

    # Fault injection.

    def withhold(self, action: str) -> None:
        """Apply the next `/exchange` action of this type and withhold its answer."""
        with self.lock:
            self.doc["exchange_faults"].append({"kind": "withhold", "action": action})

    def lose(self, action: str) -> None:
        """Drop the next `/exchange` action of this type before it applies, unanswered."""
        with self.lock:
            self.doc["exchange_faults"].append({"kind": "lose", "action": action})

    def refuse(self, action: str) -> None:
        """Refuse the next `/exchange` action of this type whole, applying nothing."""
        with self.lock:
            self.doc["exchange_faults"].append({"kind": "refuse", "action": action})

    def place_outside_order(self, user: str, coin: str, is_buy: bool, price: str, size: str):
        """A resting order the strategy never placed, as one of the Safe owners might."""
        with self.lock:
            self.tick()
            oid = self.next_id("next_oid")
            order = self.order_record(coin, is_buy, price, size, size, oid, None, "Gtc", False)
            self.keep_order(user, oid, order, "open")
            self.doc["injected"].append({"kind": "outside_order", "user": user, "oid": oid})
            self.save()
            return oid

    def exchange_fault(self, kind: str) -> str | None:
        for fault in self.doc["exchange_faults"]:
            if fault["action"] == kind:
                self.doc["exchange_faults"].remove(fault)
                self.doc["injected"].append({**fault, "time": self.now()})
                return fault["kind"]
        return None

    # Time. The clock follows fork blocks and moves one millisecond for each event between
    # them, so an effect is never written at the time of an answer that preceded it: a
    # range reconciled up to an answer's time stays complete.

    def tick(self) -> int:
        self.doc["time"] += 1
        return self.doc["time"]

    def elapse(self, time: int) -> None:
        """Advance to a block's time, paying funding at each hour boundary crossed."""
        before = self.doc["time"]
        if before:
            for boundary in range(before // HOUR_MS * HOUR_MS + HOUR_MS, time + 1, HOUR_MS):
                self.doc["time"] = boundary
                self.pay_funding(boundary)
                self.liquidate_underwater()
        self.doc["time"] = max(self.doc["time"] + 1, time) if before else time
        self.fire_schedules()

    def pay_funding(self, boundary: int) -> None:
        for user, held in self.doc["positions"].items():
            for coin, position in held.items():
                _, _, context = self.market(self.asset_of(coin))
                rate = Decimal(context["funding"])
                szi = Decimal(position["szi"])
                payment = usdc(-szi * self.price(coin, "oracle") * rate)
                self.credit_perp(user, payment)
                position["funding"] = dtext(Decimal(position["funding"]) - payment)
                delta = {
                    "coin": coin,
                    "fundingRate": dtext(rate),
                    "nSamples": None,
                    "szi": dtext(szi),
                    "type": "funding",
                    "usdc": dtext(payment),
                }
                entry = {"delta": delta, "hash": ZERO_HASH, "time": boundary}
                self.doc["funding"].setdefault(user, []).append(entry)

    # `/exchange`.

    def exchange(self, body: dict) -> dict | None:
        """The venue's answer, or None when the answer is withheld or the action lost."""
        with self.lock:
            answer, fault = self.process(body)
            self.save()
            return None if fault is not None else answer

    def process(self, body: dict) -> tuple[dict, str | None]:
        action, nonce = body["action"], body["nonce"]
        self.doc["posts"].append({"nonce": nonce, "type": action["type"]})
        expires = body.get("expiresAfter")
        if body.get("vaultAddress") is not None:
            return err("Vault actions are not modeled"), None
        if expires is not None and self.now() > expires:
            return err(EXPIRED), None
        signer = signer_of(action, nonce, expires, body["signature"])
        agent = self.live_agent(signer)
        if agent is None:
            return err(f"User or API Wallet {signer} does not exist."), None
        if not self.fresh_nonce(signer, nonce):
            return err(f"Invalid nonce: {nonce}"), None
        fault = self.exchange_fault(action["type"])
        if fault == "lose":
            return {}, fault
        if fault == "refuse":
            return err(f"{action['type']} refused by an injected fault"), None
        self.tick()
        self.use_nonce(signer, nonce)
        handler = EXCHANGE.get(action["type"])
        answer = handler(self, agent["user"], action) if handler else err("Action not modeled")
        return answer, fault

    def fresh_nonce(self, signer: str, nonce: int) -> bool:
        used = self.doc["nonces"].get(signer, [])
        window = self.now() - NONCE_PAST_MS < nonce < self.now() + NONCE_FUTURE_MS
        full = len(used) >= NONCE_SET
        return window and nonce not in used and not (full and nonce <= min(used))

    def use_nonce(self, signer: str, nonce: int) -> None:
        used = sorted([*self.doc["nonces"].get(signer, []), nonce])
        self.doc["nonces"][signer] = used[-NONCE_SET:]

    def update_leverage(self, user: str, action: dict) -> dict:
        coin, meta, _ = self.market(action["asset"])
        if not 1 <= action["leverage"] <= meta["maxLeverage"]:
            return err("Invalid leverage value")
        mode = "cross" if action["isCross"] is True else "isolated"
        if self.szi(user, coin) != 0 and "isolated" in (mode, self.mode_of(user, coin)):
            return err("Isolated leverage changes with an open position are not modeled")
        previous = self.leverage_of(user, coin)
        self.doc["leverage"].setdefault(user, {})[coin] = action["leverage"]
        if self.account_value(user) < self.initial_margin(user):
            self.doc["leverage"][user][coin] = previous
            return err("Insufficient margin to change leverage")
        modes = self.doc["modes"].setdefault(user, {})
        if mode == "isolated":
            modes[coin] = mode
        else:
            modes.pop(coin, None)
        return ok()

    def update_isolated_margin(self, user: str, action: dict) -> dict:
        coin, _, _ = self.market(action["asset"])
        position = self.doc["positions"].get(user, {}).get(coin)
        if position is None or self.mode_of(user, coin) != "isolated":
            return err("No isolated position to change the margin of")
        amount = Decimal(action["ntli"]) * USDC
        margin = Decimal(position["margin"]) + amount
        if amount > 0 and self.cross_free(user) < amount:
            return err("Insufficient margin to add to the isolated position")
        if amount < 0 and margin < self.notional(user, coin) / self.leverage_of(user, coin):
            return err("Insufficient margin to remove from the isolated position")
        position["margin"] = dtext(margin)
        return ok()

    def schedule_cancel(self, user: str, action: dict) -> dict:
        time = action.get("time")
        if time is None:
            self.doc["schedules"].pop(user, None)
            return ok()
        if time < self.now() + SCHEDULE_LEAD_MS:
            return err("Scheduled cancel time is less than 5 seconds ahead")
        self.doc["schedules"][user] = time
        return ok()

    def fire_schedules(self) -> None:
        """Cancel every open order of each account whose scheduled cancel has come."""
        for user, time in sorted(self.doc["schedules"].items()):
            if time <= self.now():
                cancelled = list(self.doc["resting"].get(user, []))
                for oid in cancelled:
                    self.close_order(oid, "scheduledCancel", time)
                del self.doc["schedules"][user]
                self.doc["fired"].append({"user": user, "time": time, "oids": cancelled})

    def cancel(self, user: str, action: dict) -> dict:
        return self.cancels([self.cancel_one(user, c["a"], c["o"]) for c in action["cancels"]])

    def cancel_by_cloid(self, user: str, action: dict) -> dict:
        cloids = self.doc["cloids"].get(user, {})
        statuses = [
            self.cancel_one(user, c["asset"], cloids.get(c["cloid"])) for c in action["cancels"]
        ]
        return self.cancels(statuses)

    def cancels(self, statuses: list) -> dict:
        return {"status": "ok", "response": {"type": "cancel", "data": {"statuses": statuses}}}

    def cancel_one(self, user: str, asset: int, oid: int | None):
        entry = self.doc["orders"].get(str(oid)) if oid is not None else None
        if (
            entry is None
            or entry["user"] != user
            or entry["status"] not in LIVE
            or self.asset_of(entry["record"]["coin"]) != asset
        ):
            return {"error": NEVER_PLACED}
        self.close_order(entry["record"]["oid"], "canceled")
        return "success"

    def noop(self, user: str, action: dict) -> dict:
        return ok()

    def order(self, user: str, action: dict) -> dict:
        statuses = [self.place(user, order) for order in action["orders"]]
        return {"status": "ok", "response": {"type": "order", "data": {"statuses": statuses}}}

    def place(self, user: str, order: dict) -> dict:
        try:
            coin, _, _ = self.market(order["a"])
        except KeyError:
            return {"error": f"Invalid asset: {order['a']}"}
        kind = order["t"]
        if "trigger" in kind:
            return self.place_trigger(user, coin, order)
        tif = kind.get("limit", {}).get("tif")
        if tif == "Ioc":
            return self.place_ioc(user, coin, order)
        if tif in ("Gtc", "Alo"):
            return self.place_resting(user, coin, order, tif)
        return {"error": "Order type not modeled"}

    def place_ioc(self, user: str, coin: str, order: dict) -> dict:
        is_buy = order["b"]
        size = self.reducible(user, coin, is_buy, Decimal(order["s"]), order["r"])
        if size is None:
            return {"error": f"{REDUCE_ONLY} asset={order['a']}"}
        levels = self.walk(coin, is_buy, Decimal(order["p"]), size)
        if not levels:
            return {"error": f"{NO_MATCH} asset={order['a']}"}
        if not order["r"] and not self.margin_allows(user, coin, is_buy, levels):
            return {"error": f"Insufficient margin to place order. asset={order['a']}"}
        return self.fill(user, coin, order, levels)

    def place_resting(self, user: str, coin: str, order: dict, tif: str) -> dict:
        """A Gtc order fills what crosses and rests the rest; an Alo that would cross is
        refused."""
        is_buy, limit, original = order["b"], Decimal(order["p"]), Decimal(order["s"])
        size = self.reducible(user, coin, is_buy, original, order["r"])
        if size is None:
            return {"error": f"{REDUCE_ONLY} asset={order['a']}"}
        levels = self.walk(coin, is_buy, limit, size)
        if tif == "Alo" and levels:
            bid, ask = (side[0]["px"] for side in self.levels(coin))
            return {
                "error": "Post only order would have immediately matched, "
                f"bbo was {bid}@{ask}. asset={order['a']}"
            }
        resting = size - sum((take for _, take in levels), Decimal(0))
        if not order["r"] and not self.margin_allows(
            user, coin, is_buy, [*levels, (limit, resting)]
        ):
            return {"error": f"Insufficient margin to place order. asset={order['a']}"}
        oid, cloid = self.next_id("next_oid"), order.get("c")
        self.execute(user, coin, oid, cloid, is_buy, levels, TAKER_FEE)
        record = self.order_record(
            coin, is_buy, order["p"], dtext(resting), dtext(original), oid, cloid, tif, order["r"]
        )
        self.keep_order(user, oid, record, "open" if resting > 0 else "filled")
        if resting == 0:
            return {"filled": self.filled(levels, oid, cloid)}
        return {"resting": {"oid": oid, **({"cloid": cloid} if cloid else {})}}

    def place_trigger(self, user: str, coin: str, order: dict) -> dict:
        """A reduce-only limit TP or SL, resting until the mark reaches its trigger."""
        trigger = order["t"]["trigger"]
        if trigger["isMarket"] is not False:
            return {"error": "Market trigger orders are not modeled"}
        is_buy = order["b"]
        if not order["r"] or self.reducible(user, coin, is_buy, Decimal(order["s"]), True) is None:
            return {"error": f"{REDUCE_ONLY} asset={order['a']}"}
        if fires(is_buy, trigger["tpsl"], Decimal(trigger["triggerPx"]), self.price(coin, "mark")):
            return {"error": f"Invalid TP/SL price. asset={order['a']}"}
        oid, cloid = self.next_id("next_oid"), order.get("c")
        record = self.order_record(
            coin, is_buy, order["p"], order["s"], order["s"], oid, cloid, None, True, trigger
        )
        self.keep_order(user, oid, record, "open")
        return {"resting": {"oid": oid, **({"cloid": cloid} if cloid else {})}}

    def reducible(self, user: str, coin: str, is_buy: bool, size: Decimal, reduce_only: bool):
        """The size an order may trade: a reduce-only one at most the opposite position,
        None when it would only increase it."""
        if not reduce_only:
            return size
        held = self.szi(user, coin)
        if held == 0 or is_buy != (held < 0):
            return None
        return min(size, abs(held))

    def keep_order(self, user: str, oid: int, record: dict, status: str) -> None:
        self.doc["orders"][str(oid)] = {
            "user": user,
            "record": record,
            "status": status,
            "status_time": self.now(),
        }
        if record["cloid"] is not None:
            self.doc["cloids"].setdefault(user, {})[record["cloid"]] = oid
        if status in LIVE:
            self.doc["resting"].setdefault(user, []).append(oid)

    def close_order(self, oid: int, status: str, time: int | None = None) -> None:
        entry = self.doc["orders"][str(oid)]
        entry["status"], entry["status_time"] = status, self.now() if time is None else time
        self.doc["resting"][entry["user"]].remove(oid)

    def cross(self, coin: str) -> None:
        """Fill or trigger the resting orders a scripted mark reaches, oldest first."""
        mark = self.price(coin, "mark")
        live = sorted(
            oid
            for oids in self.doc["resting"].values()
            for oid in oids
            if self.doc["orders"][str(oid)]["record"]["coin"] == coin
        )
        for oid in live:
            entry = self.doc["orders"][str(oid)]
            record = entry["record"]
            is_buy, limit = record["side"] == "B", Decimal(record["limitPx"])
            if record["isTrigger"] and entry["status"] == "open":
                tpsl = "sl" if record["orderType"].startswith("Stop") else "tp"
                if fires(is_buy, tpsl, Decimal(record["triggerPx"]), mark):
                    entry["status"], entry["status_time"] = "triggered", self.now()
                    self.fill_resting(oid, self.walk(coin, is_buy, limit, Decimal(record["sz"])))
            elif (mark <= limit) if is_buy else (mark >= limit):
                self.fill_resting(oid, [(limit, Decimal(record["sz"]))], MAKER_FEE)

    def fill_resting(self, oid: int, levels: list, fee: Decimal = TAKER_FEE) -> None:
        entry = self.doc["orders"][str(oid)]
        record, user = entry["record"], entry["user"]
        coin, is_buy, remaining = record["coin"], record["side"] == "B", Decimal(record["sz"])
        size = self.reducible(user, coin, is_buy, remaining, record["reduceOnly"])
        if size is None:
            return self.close_order(oid, "reduceOnlyCanceled")
        levels = clipped(levels, size)
        if not levels:
            return None
        if not record["reduceOnly"] and not self.margin_allows(user, coin, is_buy, levels):
            return self.close_order(oid, "marginCanceled")
        self.execute(user, coin, oid, record["cloid"], is_buy, levels, fee)
        left = remaining - sum((take for _, take in levels), Decimal(0))
        record["sz"] = dtext(left)
        if left == 0:
            self.close_order(oid, "filled")
        return None

    def walk(self, coin: str, is_buy: bool, limit: Decimal, size: Decimal) -> list:
        """The book levels an order reaches, as (price, size), without consuming them."""
        bids, asks = self.levels(coin)
        taken, remaining = [], size
        for level in asks if is_buy else bids:
            price = Decimal(level["px"])
            if remaining <= 0 or (price > limit if is_buy else price < limit):
                break
            take = min(remaining, Decimal(level["sz"]))
            taken.append((price, take))
            remaining -= take
        return taken

    def margin_allows(self, user: str, coin: str, is_buy: bool, levels: list) -> bool:
        """Whether the cross account still covers initial margin after these fills."""
        sign = 1 if is_buy else -1
        size = sum((take for _, take in levels), Decimal(0))
        fee = sum((usdc(price * take * TAKER_FEE) for price, take in levels), Decimal(0))
        start = self.szi(user, coin)
        after = start + sign * size
        per_unit = self.price(coin, "mark") / self.leverage_of(user, coin)
        if self.mode_of(user, coin) == "isolated":
            added = max(Decimal(0), abs(after) - abs(start)) * per_unit
        else:
            added = (abs(after) - abs(start)) * per_unit
        return self.cross_free(user) - fee >= added

    def fill(self, user: str, coin: str, order: dict, levels: list) -> dict:
        oid, cloid = self.next_id("next_oid"), order.get("c")
        self.execute(user, coin, oid, cloid, order["b"], levels, TAKER_FEE)
        filled = sum((take for _, take in levels), Decimal(0))
        size = Decimal(order["s"])
        record = self.order_record(
            coin,
            order["b"],
            order["p"],
            dtext(size - filled),
            dtext(size),
            oid,
            cloid,
            "Ioc",
            order["r"],
        )
        self.keep_order(user, oid, record, "filled" if filled == size else "canceled")
        return {"filled": self.filled(levels, oid, cloid)}

    def filled(self, levels: list, oid: int, cloid: str | None) -> dict:
        filled = sum((take for _, take in levels), Decimal(0))
        average = sum((price * take for price, take in levels), Decimal(0)) / filled
        done = {"totalSz": dtext(filled), "avgPx": dtext(average), "oid": oid}
        return {**done, **({"cloid": cloid} if cloid else {})}

    def execute(self, user, coin, oid, cloid, is_buy: bool, levels: list, fee: Decimal) -> None:
        if not levels:
            return
        digest = self.hash()
        sign = 1 if is_buy else -1
        for price, take in levels:
            self.trade(user, coin, sign * take, price, digest, oid, cloid, None, fee)

    def trade(self, user, coin, signed, price, digest, oid, cloid, liquidation, fee=TAKER_FEE):
        """Move a position by `signed` at `price`, write its fill and settle its cash."""
        held = self.doc["positions"].setdefault(user, {})
        position = held.get(coin, {"szi": "0", "entry": "0", "funding": "0", "margin": "0"})
        start, entry = Decimal(position["szi"]), Decimal(position["entry"])
        closing = start != 0 and (start > 0) != (signed > 0)
        closed = min(abs(signed), abs(start)) if closing else Decimal(0)
        pnl = usdc(closed * (price - entry) * (1 if start > 0 else -1))
        paid = usdc(abs(signed) * price * fee)
        margin = self.margin_after(user, coin, position, signed, price, closed)
        after = start + signed
        if after == 0:
            held.pop(coin, None)
        else:
            if not closing:
                entry = (abs(start) * entry + abs(signed) * price) / abs(after)
            elif (after > 0) != (start > 0):
                entry = price
            held[coin] = {
                "szi": dtext(after),
                "entry": dtext(entry),
                "funding": position["funding"],
                "margin": dtext(margin),
            }
        self.credit_perp(user, pnl - paid)
        side = "B" if signed > 0 else "A"
        crossed = fee == TAKER_FEE
        self.write_fill(
            user, coin, side, price, abs(signed), start, pnl, paid, digest, oid, cloid, liquidation
        )
        self.doc["fills"][user][-1]["crossed"] = crossed

    def margin_after(self, user, coin, position: dict, signed, price, closed) -> Decimal:
        """An isolated position's own margin after a trade: notional over leverage added
        on what opens, released in proportion to what closes."""
        if self.mode_of(user, coin) != "isolated":
            return Decimal(0)
        margin, start = Decimal(position.get("margin", "0")), abs(Decimal(position["szi"]))
        if closed:
            margin -= margin * closed / start
        opened = abs(signed) - closed
        return margin + opened * price / self.leverage_of(user, coin)

    def write_fill(
        self, user, coin, side, price, size, start, pnl, fee, digest, oid, cloid, liquidation
    ):
        long = start > 0 or (start == 0 and side == "B")
        closing = start != 0 and (side == "A") == (start > 0)
        if liquidation is not None:
            mode = "Isolated" if self.mode_of(user, coin) == "isolated" else "Cross"
            direction = f"Liquidated {mode} " + ("Long" if long else "Short")
        else:
            direction = ("Close " if closing else "Open ") + ("Long" if long else "Short")
        fill = {
            "coin": coin,
            "px": dtext(price),
            "sz": dtext(size),
            "side": side,
            "time": self.now(),
            "startPosition": dtext(start),
            "dir": direction,
            "closedPnl": dtext(pnl),
            "hash": digest,
            "oid": oid,
            "crossed": True,
            "fee": dtext(fee),
            "tid": self.next_id("next_tid"),
            "feeToken": "USDC",
            "twapId": None,
        }
        if cloid is not None:
            fill["cloid"] = cloid
        if liquidation is not None:
            fill["liquidation"] = liquidation
        self.doc["fills"].setdefault(user, []).append(fill)

    def liquidate_underwater(self) -> None:
        for user in list(self.doc["positions"]):
            held = self.doc["positions"][user]
            cross = [c for c in held if self.mode_of(user, c) == "cross"]
            if cross and self.cross_equity(user) < self.maintenance_margin(user):
                self.liquidate(user, cross, "Cross")
            for coin in [c for c in held if c not in cross]:
                if self.isolated_equity(user, coin) < self.maintenance_of(user, coin):
                    self.liquidate(user, [coin], "Isolated")

    def liquidate(self, user: str, coins: list, kind: str) -> None:
        value = self.account_value(user)
        held = {coin: dict(self.doc["positions"][user][coin]) for coin in coins}
        digest = self.hash()
        notional = Decimal(0)
        for coin, position in held.items():
            mark, szi = self.price(coin, "mark"), Decimal(position["szi"])
            notional += abs(szi) * mark
            detail = {"liquidatedUser": user, "markPx": dtext(mark), "method": "market"}
            self.trade(user, coin, -szi, mark, digest, self.next_id("next_oid"), None, detail)
        delta = {
            "accountValue": dtext(value),
            "leverageType": kind,
            "liquidatedNtlPos": dtext(notional),
            "liquidatedPositions": [
                {"coin": coin, "szi": position["szi"]} for coin, position in held.items()
            ],
            "type": "liquidation",
        }
        self.write(user, digest, delta)
        for oid in list(self.doc["resting"].get(user, [])):
            if self.doc["orders"][str(oid)]["record"]["coin"] in held:
                self.close_order(oid, "liquidatedCanceled")
        self.doc["injected"].append({"kind": "liquidation", "user": user, "time": self.now()})

    # Account figures.

    def szi(self, user: str, coin: str) -> Decimal:
        return Decimal(self.doc["positions"].get(user, {}).get(coin, {}).get("szi", "0"))

    def leverage_of(self, user: str, coin: str) -> int:
        _, meta, _ = self.market(self.asset_of(coin))
        default = min(DEFAULT_LEVERAGE, meta["maxLeverage"])
        return self.doc["leverage"].get(user, {}).get(coin, default)

    def cash(self, user: str) -> Decimal:
        return Decimal(self.doc["accounts"].get(user, {}).get("perp", 0)) * USDC

    def credit_perp(self, user: str, amount: Decimal) -> None:
        self.doc["accounts"][user]["perp"] += micro(amount)

    def unrealized(self, user: str, coin: str) -> Decimal:
        position = self.doc["positions"][user][coin]
        return Decimal(position["szi"]) * (self.price(coin, "mark") - Decimal(position["entry"]))

    def account_value(self, user: str) -> Decimal:
        held = self.doc["positions"].get(user, {})
        return self.cash(user) + sum((self.unrealized(user, c) for c in held), Decimal(0))

    def mode_of(self, user: str, coin: str) -> str:
        return self.doc["modes"].get(user, {}).get(coin, "cross")

    def notional(self, user: str, coin: str) -> Decimal:
        return abs(self.szi(user, coin)) * self.price(coin, "mark")

    def margin_of(self, user: str, coin: str) -> Decimal:
        """The initial margin a position holds: its own margin when isolated."""
        if self.mode_of(user, coin) == "isolated":
            return Decimal(self.doc["positions"][user][coin]["margin"])
        return self.notional(user, coin) / self.leverage_of(user, coin)

    def initial_margin(self, user: str) -> Decimal:
        held = self.doc["positions"].get(user, {})
        return sum((self.margin_of(user, c) for c in held), Decimal(0))

    def isolated(self, user: str) -> list:
        held = self.doc["positions"].get(user, {})
        return [c for c in held if self.mode_of(user, c) == "isolated"]

    def cross_equity(self, user: str) -> Decimal:
        """Account value outside the isolated positions, which hold their own margin."""
        return self.account_value(user) - sum(
            (self.isolated_equity(user, c) for c in self.isolated(user)), Decimal(0)
        )

    def cross_free(self, user: str) -> Decimal:
        """Cross account value above the initial margin every position holds."""
        isolated = self.isolated(user)
        held = self.doc["positions"].get(user, {})
        cross = sum((self.margin_of(user, c) for c in held if c not in isolated), Decimal(0))
        return self.cross_equity(user) - cross

    def isolated_equity(self, user: str, coin: str) -> Decimal:
        return Decimal(self.doc["positions"][user][coin]["margin"]) + self.unrealized(user, coin)

    def maintenance_of(self, user: str, coin: str) -> Decimal:
        _, meta, _ = self.market(self.asset_of(coin))
        return self.notional(user, coin) / (2 * meta["maxLeverage"])

    def withdrawable(self, user: str) -> Decimal:
        """Perp USDC that may leave the account: account value less the larger of initial
        margin and a tenth of position value (section 9.4)."""
        if user not in self.doc["accounts"]:
            return Decimal(0)
        held = self.doc["positions"].get(user, {})
        notional = sum((abs(self.szi(user, c)) * self.price(c, "mark") for c in held), Decimal(0))
        kept = max(self.initial_margin(user), notional / 10)
        return max(Decimal(0), self.account_value(user) - kept)

    def withdrawable_micro(self, user: str) -> int:
        return int((self.withdrawable(user) / USDC).to_integral_value(ROUND_DOWN))

    def maintenance_margin(self, user: str) -> Decimal:
        """The cross positions' maintenance margin."""
        isolated = self.isolated(user)
        held = self.doc["positions"].get(user, {})
        return sum((self.maintenance_of(user, c) for c in held if c not in isolated), Decimal(0))

    def next_id(self, counter: str) -> int:
        self.doc[counter] += 1
        return self.doc[counter]

    def order_record(
        self, coin, is_buy, price, size, original, oid, cloid, tif, reduce_only, trigger=None
    ):
        record = {
            "coin": coin,
            "side": "B" if is_buy else "A",
            "limitPx": dtext(Decimal(price)),
            "sz": size,
            "oid": oid,
            "timestamp": self.now(),
            "triggerCondition": "N/A",
            "isTrigger": False,
            "triggerPx": "0.0",
            "children": [],
            "isPositionTpsl": False,
            "reduceOnly": reduce_only,
            "orderType": "Limit",
            "origSz": original,
            "tif": tif,
            "cloid": cloid,
        }
        if trigger is not None:
            price = dtext(Decimal(trigger["triggerPx"]))
            below = (trigger["tpsl"] == "sl") != is_buy
            record |= {
                "triggerCondition": f"Price {'below' if below else 'above'} {price}",
                "isTrigger": True,
                "triggerPx": price,
                "orderType": "Stop Limit" if trigger["tpsl"] == "sl" else "Take Profit Limit",
            }
        return record

    # `/info` answers of the trading state.

    def clearinghouse(self, query) -> dict:
        user = query.user
        held = self.doc["positions"].get(user, {})
        positions = [self.position_answer(user, coin) for coin in sorted(held)]
        value = self.account_value(user) if user in self.doc["accounts"] else Decimal(0)
        notional = sum((Decimal(p["position"]["positionValue"]) for p in positions), Decimal(0))
        margin = sum((Decimal(p["position"]["marginUsed"]) for p in positions), Decimal(0))
        exposure = sum(
            (
                Decimal(p["position"]["szi"]) * self.price(p["position"]["coin"], "mark")
                for p in positions
            ),
            Decimal(0),
        )
        summary = {
            "accountValue": dtext(value),
            "totalNtlPos": dtext(notional),
            "totalRawUsd": dtext(value - exposure),
            "totalMarginUsed": dtext(margin),
        }
        return {
            "marginSummary": summary,
            "crossMarginSummary": summary,
            "crossMaintenanceMarginUsed": dtext(self.maintenance_margin(user)),
            "withdrawable": dtext(self.withdrawable(user)),
            "assetPositions": positions,
            "time": self.now(),
        }

    def position_answer(self, user: str, coin: str) -> dict:
        position = self.doc["positions"][user][coin]
        _, meta, _ = self.market(self.asset_of(coin))
        szi, leverage = Decimal(position["szi"]), self.leverage_of(user, coin)
        value = abs(szi) * self.price(coin, "mark")
        margin = self.margin_of(user, coin)
        pnl = self.unrealized(user, coin)
        funded = dtext(Decimal(position["funding"]))
        mode = {"type": "cross", "value": leverage}
        if self.mode_of(user, coin) == "isolated":
            raw = margin - szi * Decimal(position["entry"])
            mode = {"type": "isolated", "value": leverage, "rawUsd": dtext(raw)}
        return {
            "type": "oneWay",
            "position": {
                "coin": coin,
                "szi": dtext(szi),
                "leverage": mode,
                "entryPx": dtext(Decimal(position["entry"])),
                "positionValue": dtext(value),
                "unrealizedPnl": dtext(pnl),
                "returnOnEquity": dtext((pnl / margin).quantize(Decimal("1E-10"), ROUND_DOWN)),
                "liquidationPx": None,
                "marginUsed": dtext(margin),
                "maxLeverage": meta["maxLeverage"],
                "cumFunding": {"allTime": funded, "sinceOpen": funded, "sinceChange": funded},
            },
        }

    def active_asset(self, query) -> dict:
        coin, user = query.coin, query.user
        _, meta, _ = self.market(self.asset_of(coin))
        mark = self.price(coin, "mark")
        free = (
            max(Decimal(0), self.cross_free(user)) if (user in self.doc["accounts"]) else Decimal(0)
        )
        size = (free * self.leverage_of(user, coin) / mark).quantize(
            Decimal(1).scaleb(-meta["szDecimals"]), ROUND_DOWN
        )
        leverage = {"type": "cross", "value": self.leverage_of(user, coin)}
        if self.mode_of(user, coin) == "isolated":
            raw = "0.0"
            if coin in self.doc["positions"].get(user, {}):
                raw = self.position_answer(user, coin)["position"]["leverage"]["rawUsd"]
            leverage = {"type": "isolated", "value": self.leverage_of(user, coin), "rawUsd": raw}
        return {
            "user": user,
            "coin": coin,
            "leverage": leverage,
            "maxTradeSzs": [dtext(size), dtext(size)],
            "availableToTrade": [dtext(free), dtext(free)],
            "markPx": dtext(mark),
        }

    def open_orders(self, query) -> list:
        answers = []
        for oid in self.doc["resting"].get(query.user, []):
            record = self.doc["orders"][str(oid)]["record"]
            answer = {
                k: record[k]
                for k in ("coin", "side", "limitPx", "sz", "oid", "timestamp", "origSz")
            }
            if record["cloid"] is not None:
                answer["cloid"] = record["cloid"]
            answers.append(answer)
        return answers

    def order_status(self, query) -> dict:
        oid = query.oid
        if isinstance(oid, str):
            oid = self.doc["cloids"].get(query.user, {}).get(oid)
        found = self.doc["orders"].get(str(oid)) if oid is not None else None
        if found is None or found["user"] != query.user:
            return {"status": "unknownOid"}
        record = found["record"]
        return {
            "status": "order",
            "order": {
                "order": record,
                "status": found["status"],
                "statusTimestamp": found.get("status_time", record["timestamp"]),
            },
        }

    def ranged(self, kind: str, query) -> list:
        entries = self.doc[kind].get(query.user, [])
        return [e for e in entries if query.start_time <= e["time"] <= query.end_time]

    def user_fills(self, query) -> list:
        return self.ranged("fills", query)

    def user_funding(self, query) -> list:
        return self.ranged("funding", query)

    def levels(self, coin: str) -> list:
        """The seeded book moved by the scripted mark's difference from the seeded mark."""
        seeded = self.seeded()["books"][coin]["levels"]
        _, _, context = self.market(self.asset_of(coin))
        shift = self.price(coin, "mark") - Decimal(context["markPx"])
        if shift == 0:
            return seeded
        return [
            [{**level, "px": dtext(Decimal(level["px"]) + shift)} for level in side]
            for side in seeded
        ]

    def book(self, query) -> dict:
        return {"coin": query.coin, "time": self.now(), "levels": self.levels(query.coin)}

    def meta_and_contexts(self, query) -> list:
        meta, contexts = self.seeded()["meta"]
        scripted = []
        for asset, context in zip(meta["universe"], contexts, strict=True):
            price = self.doc["prices"].get(asset["name"])
            scripted.append(
                context if price is None else {**context, "markPx": price, "oraclePx": price}
            )
        return [meta, scripted]

    def meta(self, query) -> dict:
        return self.seeded()["meta"][0]


def fires(is_buy: bool, tpsl: str, trigger: Decimal, mark: Decimal) -> bool:
    """Whether a trigger order has triggered at this mark: a stop that sells and a
    take-profit that buys trigger at or below their price, the others at or above."""
    below = (tpsl == "sl") != is_buy
    return mark <= trigger if below else mark >= trigger


def clipped(levels: list, size: Decimal) -> list:
    taken, remaining = [], size
    for price, take in levels:
        if remaining <= 0:
            break
        taken.append((price, min(take, remaining)))
        remaining -= taken[-1][1]
    return taken


EXCHANGE = {
    "updateLeverage": TradingModel.update_leverage,
    "updateIsolatedMargin": TradingModel.update_isolated_margin,
    "order": TradingModel.order,
    "cancel": TradingModel.cancel,
    "cancelByCloid": TradingModel.cancel_by_cloid,
    "scheduleCancel": TradingModel.schedule_cancel,
    "noop": TradingModel.noop,
}
