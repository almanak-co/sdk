"""Aster Pro Futures V3 REST client (gateway-side only).

Aster Pro is an off-chain order book: orders, positions and margin live in
Aster's ledger, reached through ``https://fapi.asterdex.com/fapi/v3``. Every
authenticated request is an EIP-712 ``Message{msg}`` signature, over the exact
query string sent, by an *agent* key that the main wallet has approved.

Agent lifecycle: each wallet has exactly two agents, a perp-only trading agent
and a withdraw-only agent, whose keys are derived from the main key (never
persisted). Aster caps the number of agents per account, so random per-start
keys would exhaust it after a few restarts. On first use the gateway lists the
wallet's agents and reuses a registration with the right scope and enough
remaining life; otherwise it deletes the stale one (a main-wallet ``DelAgent``
action) and registers it again (``registerAndApproveAgent``). A registration is
renewed the same way before it expires, so a long-running gateway never signs
with an expired agent.

The withdraw-only agent is bound by Aster to an IP whitelist; it is approved
lazily and only when the operator configured the gateway's egress IP(s). Every
withdrawal also carries a main-wallet EIP-712 ``Action`` signature naming the
receiver, so the agent alone cannot move funds.

This module performs network egress and holds keys, so it lives under the
connector's ``gateway/`` package and must never be imported strategy-side.
"""

from __future__ import annotations

import asyncio
import logging
import time
import urllib.parse
from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Any

import aiohttp
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_account.signers.local import LocalAccount
from eth_utils import keccak

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://fapi.asterdex.com"

# EIP-712 domain chain ids: trading/USER_DATA requests sign with Aster Chain
# mainnet (1666); agent management signs with ``signatureChainId`` (56). Both
# verified against the live API on 2026-10-05.
REQUEST_SIGNATURE_CHAIN_ID = 1666
AGENT_SIGNATURE_CHAIN_ID = 56

# Aster rejects agent expiries below ~7 days ("Agent expired time too short").
AGENT_TTL_SECONDS = 30 * 24 * 3600
AGENT_RENEW_MARGIN_SECONDS = 7 * 24 * 3600
_AGENT_KEY_DOMAIN = b"almanak/aster-pro/agent/v1/"
# Withdrawal Action domain: name "Aster", chainId of the destination chain.
WITHDRAW_CHAIN_ID = 56
_WITHDRAW_CHAIN_NAME = "BSC"
AGENT_NAME_PREFIX = "almanak"

_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
_HTTP_TIMEOUT = aiohttp.ClientTimeout(total=20)
_EXCHANGE_INFO_TTL_SECONDS = 300.0
# Binance-dialect codes meaning "sent, execution status unknown" (-1006 unexpected bus response, -1007 backend timeout).
_UNKNOWN_OUTCOME_CODES = frozenset({-1006, -1007})


class AsterApiError(Exception):
    """Aster returned a definitive error: the request was NOT executed."""

    def __init__(self, message: str, *, code: int | None = None, http_status: int | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.http_status = http_status


class AsterUnknownOutcomeError(Exception):
    """The request may or may not have executed (timeout, 5xx).

    Callers placing orders must reconcile by ``newClientOrderId`` before
    retrying — resubmitting blindly can double a position.
    """


@dataclass(frozen=True)
class SymbolRules:
    """Order-size rules for one symbol, from ``exchangeInfo``."""

    symbol: str
    status: str
    step_size: Decimal
    min_qty: Decimal
    max_market_qty: Decimal
    min_notional: Decimal
    quantity_precision: int
    tick_size: Decimal


def _typed_message(chain_id: int, msg: str) -> dict[str, Any]:
    return {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "Message": [{"name": "msg", "type": "string"}],
        },
        "primaryType": "Message",
        "domain": {
            "name": "AsterSignTransaction",
            "version": "1",
            "chainId": chain_id,
            "verifyingContract": _ZERO_ADDRESS,
        },
        "message": {"msg": msg},
    }


def sign_message(account: LocalAccount, chain_id: int, msg: str) -> str:
    """Sign ``msg`` as an Aster ``Message`` and return a 0x-prefixed signature."""
    signature = account.sign_message(encode_typed_data(full_message=_typed_message(chain_id, msg))).signature.hex()
    return signature if signature.startswith("0x") else f"0x{signature}"


def derive_agent(main_account: LocalAccount, role: str) -> LocalAccount:
    """The wallet's agent key for ``role``: the same key on every gateway start."""
    return Account.from_key(keccak(_AGENT_KEY_DOMAIN + role.encode() + bytes(main_account.key)))


def sign_main_action(account: LocalAccount, primary_type: str, values: dict[str, Any]) -> str:
    """Sign a main-wallet account action (e.g. ``DelAgent``) as Aster's dynamic EIP-712 struct.

    The struct is named ``primary_type``; its fields are ``values`` in order, each
    name capitalized, typed ``bool`` / ``uint256`` (int) / ``string``. Verified
    against the live API on 2026-10-05.
    """
    message = {key[:1].upper() + key[1:]: value for key, value in values.items()}
    fields = [
        {"name": key, "type": "bool" if isinstance(value, bool) else "uint256" if isinstance(value, int) else "string"}
        for key, value in message.items()
    ]
    typed = _typed_message(AGENT_SIGNATURE_CHAIN_ID, "")
    typed["types"] = {"EIP712Domain": typed["types"]["EIP712Domain"], primary_type: fields}
    typed["primaryType"] = primary_type
    typed["message"] = message
    signature = account.sign_message(encode_typed_data(full_message=typed)).signature.hex()
    return signature if signature.startswith("0x") else f"0x{signature}"


class _MicrosecondNonce:
    """Strictly increasing microsecond nonce.

    Aster keeps the last 100 nonces per agent and rejects reuse, so two
    requests in the same microsecond (or a clock step backwards) must still
    get distinct, increasing values.
    """

    def __init__(self) -> None:
        self._last = 0

    def next(self) -> int:
        candidate = time.time_ns() // 1_000
        self._last = max(candidate, self._last + 1)
        return self._last


def format_quantity(quantity: Decimal, rules: SymbolRules) -> str:
    """Render ``quantity`` at the symbol's step precision, without exponent."""
    exponent = rules.step_size.normalize().as_tuple().exponent
    places = -exponent if isinstance(exponent, int) and exponent < 0 else 0
    return f"{quantity:.{places}f}"


def protected_price(mark_price: Decimal, *, side: str, max_slippage: Decimal, rules: SymbolRules) -> Decimal:
    """Worst acceptable fill price for a ``side`` order within ``max_slippage`` of the mark.

    Rounds toward the mark (BUY down, SELL up) so tick alignment never widens
    the bound the strategy asked for.
    """
    if not Decimal(0) < max_slippage < Decimal(1):
        raise ValueError(f"max_slippage must be a fraction in (0, 1), got {max_slippage}")
    if mark_price <= 0:
        raise ValueError(f"mark price must be positive, got {mark_price}")
    if side == "BUY":
        raw, rounding = mark_price * (1 + max_slippage), ROUND_DOWN
    elif side == "SELL":
        raw, rounding = mark_price * (1 - max_slippage), ROUND_UP
    else:
        raise ValueError(f"unknown order side {side!r}")
    price = (raw / rules.tick_size).to_integral_value(rounding=rounding) * rules.tick_size
    if price <= 0:
        raise ValueError(f"{rules.symbol}: protected price {price} is not positive")
    return price


def format_price(price: Decimal, rules: SymbolRules) -> str:
    """Render ``price`` at the symbol's tick precision, without exponent."""
    exponent = rules.tick_size.normalize().as_tuple().exponent
    places = -exponent if isinstance(exponent, int) and exponent < 0 else 0
    return f"{price:.{places}f}"


def quantity_for_notional(notional_usd: Decimal, mark_price: Decimal, rules: SymbolRules) -> Decimal:
    """Largest step-aligned quantity whose notional does not exceed ``notional_usd``.

    Rounds DOWN so the order never exceeds the size the strategy asked for, then
    refuses (rather than silently upsizing) when the result is below the venue
    minimum quantity or minimum notional.
    """
    if mark_price <= 0:
        raise ValueError(f"mark price must be positive, got {mark_price}")
    if notional_usd <= 0:
        raise ValueError(f"notional must be positive, got {notional_usd}")
    raw = notional_usd / mark_price
    steps = (raw / rules.step_size).to_integral_value(rounding=ROUND_DOWN)
    quantity = steps * rules.step_size
    if quantity < rules.min_qty:
        raise ValueError(
            f"{rules.symbol}: ${notional_usd} at mark {mark_price} is {raw} contracts, below the "
            f"minimum quantity {rules.min_qty} (≈ ${rules.min_qty * mark_price:.2f})"
        )
    if quantity * mark_price < rules.min_notional:
        raise ValueError(
            f"{rules.symbol}: order notional ${quantity * mark_price:.4f} is below the venue minimum "
            f"${rules.min_notional}; raise size_usd"
        )
    if quantity > rules.max_market_qty:
        raise ValueError(f"{rules.symbol}: quantity {quantity} exceeds the market-order maximum {rules.max_market_qty}")
    return quantity


def _tick_size(symbol: str, price_filter: dict[str, Any]) -> Decimal:
    tick = price_filter.get("tickSize")
    if not tick or Decimal(str(tick)) <= 0:
        raise AsterApiError(f"{symbol}: exchangeInfo has no PRICE_FILTER tickSize; cannot bound an order price")
    return Decimal(str(tick))


def _parse_symbol_rules(entry: dict[str, Any]) -> SymbolRules:
    filters = {f.get("filterType"): f for f in entry.get("filters", [])}
    lot = filters.get("LOT_SIZE", {})
    market_lot = filters.get("MARKET_LOT_SIZE", lot)
    notional = filters.get("MIN_NOTIONAL", {})
    price_filter = filters.get("PRICE_FILTER", {})
    step = Decimal(str(market_lot.get("stepSize") or lot.get("stepSize")))
    min_qty = max(Decimal(str(lot.get("minQty", "0"))), Decimal(str(market_lot.get("minQty", "0"))))
    return SymbolRules(
        symbol=entry["symbol"],
        status=str(entry.get("status", "")),
        step_size=step,
        min_qty=min_qty,
        max_market_qty=Decimal(str(market_lot.get("maxQty") or lot.get("maxQty"))),
        min_notional=Decimal(str(notional.get("notional", "0"))),
        quantity_precision=int(entry.get("quantityPrecision", 0)),
        tick_size=_tick_size(entry["symbol"], price_filter),
    )


class AsterProApiClient:
    """Async Aster Pro Futures V3 client bound to one main wallet."""

    def __init__(
        self,
        main_account: LocalAccount,
        *,
        base_url: str = DEFAULT_BASE_URL,
        withdraw_ip_whitelist: str | None = None,
    ) -> None:
        self._main = main_account
        self._withdraw_ip_whitelist = (withdraw_ip_whitelist or "").strip()
        self._withdraw_agent: LocalAccount | None = None
        self._withdraw_agent_expires_at = 0.0
        self._base_url = base_url.rstrip("/")
        self._session: aiohttp.ClientSession | None = None
        self._agent: LocalAccount | None = None
        self._agent_expires_at = 0.0
        self._agent_lock = asyncio.Lock()
        self._withdraw_agent_lock = asyncio.Lock()
        self._nonce = _MicrosecondNonce()
        self._exchange_info: dict[str, SymbolRules] = {}
        self._exchange_info_at = 0.0

    @property
    def user_address(self) -> str:
        return self._main.address

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def _http(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=_HTTP_TIMEOUT)
        return self._session

    async def _send(self, method: str, path: str, query: str, *, body: bool) -> Any:
        session = await self._http()
        url = f"{self._base_url}{path}"
        try:
            if body:
                async with session.request(
                    method,
                    url,
                    data=query,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                ) as response:
                    return await self._decode(response, method, path)
            async with session.request(method, f"{url}?{query}" if query else url) as response:
                return await self._decode(response, method, path)
        except (TimeoutError, aiohttp.ClientConnectionError) as exc:
            raise AsterUnknownOutcomeError(f"{method} {path}: transport failure ({exc})") from exc

    @staticmethod
    async def _decode(response: aiohttp.ClientResponse, method: str, path: str) -> Any:
        text = await response.text()
        if response.status >= 500 or response.status == 408:
            raise AsterUnknownOutcomeError(f"{method} {path}: HTTP {response.status} {text[:200]}")
        try:
            payload = await response.json(content_type=None)
        except ValueError as exc:
            raise AsterApiError(
                f"{method} {path}: non-JSON response HTTP {response.status}: {text[:200]}",
                http_status=response.status,
            ) from exc
        code = payload.get("code") if isinstance(payload, dict) else None
        if code in _UNKNOWN_OUTCOME_CODES:
            raise AsterUnknownOutcomeError(f"{method} {path}: {payload.get('msg')} (code={code})")
        if response.status >= 400 or (isinstance(payload, dict) and "error" in payload):
            message = payload.get("msg") or payload.get("error") if isinstance(payload, dict) else text
            raise AsterApiError(
                f"{method} {path}: {message} (code={code}, http={response.status})",
                code=code if isinstance(code, int) else None,
                http_status=response.status,
            )
        if isinstance(payload, dict) and isinstance(payload.get("code"), int) and payload["code"] < 0:
            raise AsterApiError(f"{method} {path}: {payload.get('msg')}", code=payload["code"])
        return payload

    async def _public(self, path: str, params: dict[str, str] | None = None) -> Any:
        return await self._send("GET", path, urllib.parse.urlencode(params or {}), body=False)

    async def _signed(
        self, method: str, path: str, params: dict[str, str] | None = None, *, agent: LocalAccount | None = None
    ) -> Any:
        agent = agent or await self._ensure_agent()
        ordered: dict[str, str] = dict(params or {})
        ordered["nonce"] = str(self._nonce.next())
        ordered["user"] = self._main.address
        ordered["signer"] = agent.address
        msg = urllib.parse.urlencode(ordered)
        query = f"{msg}&signature={sign_message(agent, REQUEST_SIGNATURE_CHAIN_ID, msg)}"
        return await self._send(method, path, query, body=method != "GET")

    @staticmethod
    def _fresh(expires_at: float) -> bool:
        return expires_at - time.time() > AGENT_RENEW_MARGIN_SECONDS

    async def _ensure_agent(self) -> LocalAccount:
        if self._agent is not None and self._fresh(self._agent_expires_at):
            return self._agent
        async with self._agent_lock:
            if self._agent is None or not self._fresh(self._agent_expires_at):
                agent = derive_agent(self._main, "perp")
                self._agent_expires_at = await self._approve(agent, lister=agent, withdraw=False)
                self._agent = agent
        return self._agent

    async def _ensure_withdraw_agent(self) -> LocalAccount:
        if not self._withdraw_ip_whitelist:
            raise AsterApiError(
                "Aster withdrawals need the gateway's egress IP: set ALMANAK_GATEWAY_ASTER_PERPS_WITHDRAW_IP_WHITELIST"
            )
        if self._withdraw_agent is not None and self._fresh(self._withdraw_agent_expires_at):
            return self._withdraw_agent
        lister = await self._ensure_agent()
        async with self._withdraw_agent_lock:
            if self._withdraw_agent is None or not self._fresh(self._withdraw_agent_expires_at):
                agent = derive_agent(self._main, "withdraw")
                self._withdraw_agent_expires_at = await self._approve(agent, lister=lister, withdraw=True)
                self._withdraw_agent = agent
        return self._withdraw_agent

    async def _approve(self, agent: LocalAccount, *, lister: LocalAccount, withdraw: bool) -> float:
        """Make ``agent`` an approved agent of the wallet; return its expiry (epoch seconds).

        ``lister`` signs the agent listing; an agent that is not registered yet
        cannot list, which reads as "no registration".
        """
        try:
            listed = await self._signed("GET", "/fapi/v3/agent", agent=lister)
        except AsterApiError:
            listed = []
        current = next(
            (a for a in listed or [] if str(a.get("agentAddress", "")).lower() == agent.address.lower()),
            None,
        )
        if current is not None and self._scope_matches(current, withdraw=withdraw):
            expires_at = int(current.get("expired") or 0) / 1000
            if self._fresh(expires_at):
                logger.info("Reusing Aster agent %s for %s", agent.address, self._main.address)
                return expires_at
        try:
            await self._delete_agent(agent.address)
        except AsterApiError as exc:
            if current is not None:
                raise
            logger.debug("No stale Aster agent %s to delete: %s", agent.address, exc)
        return await self._register_agent(agent, withdraw=withdraw)

    def _scope_matches(self, listed: dict[str, Any], *, withdraw: bool) -> bool:
        scope = (bool(listed.get("canSpotTrade")), bool(listed.get("canPerpTrade")), bool(listed.get("canWithdraw")))
        if withdraw:
            whitelist = " ".join(str(listed.get("ipWhitelist") or "").split())
            return scope == (False, False, True) and whitelist == " ".join(self._withdraw_ip_whitelist.split())
        return scope == (False, True, False)

    async def _delete_agent(self, agent_address: str) -> None:
        values: dict[str, Any] = {
            "agentAddress": agent_address,
            "asterChain": "Mainnet",
            "user": self._main.address,
            "nonce": self._nonce.next(),
        }
        form = {key: str(value) for key, value in values.items()}
        form["signature"] = sign_main_action(self._main, "DelAgent", values)
        form["signatureChainId"] = str(AGENT_SIGNATURE_CHAIN_ID)
        await self._send("DELETE", "/fapi/v3/agent", urllib.parse.urlencode(form), body=True)

    async def _register_agent(self, agent: LocalAccount, *, withdraw: bool = False) -> float:
        expires_at = time.time() + AGENT_TTL_SECONDS
        params = {
            "user": self._main.address,
            "nonce": str(self._nonce.next()),
            "agentName": f"{AGENT_NAME_PREFIX}{'wd' if withdraw else 'perp'}",
            "agentAddress": agent.address,
            "expired": str(int(expires_at * 1000)),
            "signatureChainId": str(AGENT_SIGNATURE_CHAIN_ID),
            "canSpotTrade": "false",
            "canPerpTrade": "false" if withdraw else "true",
            "canWithdraw": "true" if withdraw else "false",
        }
        if withdraw:
            params["ipWhitelist"] = self._withdraw_ip_whitelist
        msg = urllib.parse.urlencode(params)
        query = f"{msg}&signature={sign_message(self._main, AGENT_SIGNATURE_CHAIN_ID, msg)}"
        await self._send("POST", "/fapi/v3/registerAndApproveAgent", query, body=True)
        scope = f"withdraw-only, IPs {self._withdraw_ip_whitelist}" if withdraw else "perp-only, no withdraw"
        logger.info("Aster agent %s approved for %s (%s)", agent.address, self._main.address, scope)
        return expires_at

    async def symbol_rules(self, symbol: str) -> SymbolRules:
        now = time.monotonic()
        if not self._exchange_info or now - self._exchange_info_at > _EXCHANGE_INFO_TTL_SECONDS:
            payload = await self._public("/fapi/v3/exchangeInfo")
            parsed: dict[str, SymbolRules] = {}
            for entry in payload.get("symbols", []):
                try:
                    parsed[entry["symbol"]] = _parse_symbol_rules(entry)
                except (AsterApiError, KeyError, ArithmeticError, ValueError, TypeError) as exc:
                    # One malformed symbol must not take every market down; it is unknown.
                    logger.warning("Skipping Aster symbol %s: %s", entry.get("symbol"), exc)
            self._exchange_info = parsed
            self._exchange_info_at = now
        rules = self._exchange_info.get(symbol)
        if rules is None:
            raise AsterApiError(f"Unknown Aster symbol {symbol!r}")
        return rules

    async def mark_price(self, symbol: str) -> Decimal:
        payload = await self._public("/fapi/v3/premiumIndex", {"symbol": symbol})
        price = Decimal(str(payload["markPrice"]))
        if price <= 0:
            raise AsterApiError(f"Aster returned a non-positive mark price for {symbol}: {price}")
        return price

    async def balances(self) -> list[dict[str, Any]]:
        return await self._signed("GET", "/fapi/v3/balance")

    async def positions(self, symbol: str | None = None) -> list[dict[str, Any]]:
        return await self._signed("GET", "/fapi/v3/positionRisk", {"symbol": symbol} if symbol else None)

    async def is_hedge_mode(self) -> bool:
        payload = await self._signed("GET", "/fapi/v3/positionSide/dual")
        return bool(payload.get("dualSidePosition"))

    async def set_leverage(self, symbol: str, leverage: int) -> dict[str, Any]:
        return await self._signed("POST", "/fapi/v3/leverage", {"symbol": symbol, "leverage": str(leverage)})

    async def place_ioc_order(
        self,
        *,
        symbol: str,
        side: str,
        quantity: str,
        price: str,
        reduce_only: bool,
        client_order_id: str,
    ) -> dict[str, Any]:
        """Immediate-or-cancel LIMIT order: fills at ``price`` or better, the rest expires."""
        params = {
            "symbol": symbol,
            "side": side,
            "type": "LIMIT",
            "timeInForce": "IOC",
            "quantity": quantity,
            "price": price,
            "newClientOrderId": client_order_id,
            "newOrderRespType": "RESULT",
        }
        if reduce_only:
            params["reduceOnly"] = "true"
        return await self._signed("POST", "/fapi/v3/order", params)

    async def get_order(self, *, symbol: str, client_order_id: str) -> dict[str, Any]:
        return await self._signed("GET", "/fapi/v3/order", {"symbol": symbol, "origClientOrderId": client_order_id})

    async def transfer_history(self) -> list[dict[str, Any]]:
        payload = await self._signed("POST", "/fapi/v3/aster/deposit-withdraw-history")
        return payload if isinstance(payload, list) else []

    async def withdraw_info(self) -> dict[str, Any]:
        return await self._signed("POST", "/fapi/v3/aster/user-withdraw-info")

    async def withdraw(self, *, asset: str, amount: str, fee: str) -> dict[str, Any]:
        """Withdraw ``amount`` (fee included) of ``asset`` to the main wallet on BSC."""
        agent = await self._ensure_withdraw_agent()
        user_nonce = self._nonce.next()
        action = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "Action": [
                    {"name": "type", "type": "string"},
                    {"name": "destination", "type": "address"},
                    {"name": "destination Chain", "type": "string"},
                    {"name": "token", "type": "string"},
                    {"name": "amount", "type": "string"},
                    {"name": "fee", "type": "string"},
                    {"name": "nonce", "type": "uint256"},
                    {"name": "aster chain", "type": "string"},
                ],
            },
            "primaryType": "Action",
            "domain": {
                "name": "Aster",
                "version": "1",
                "chainId": WITHDRAW_CHAIN_ID,
                "verifyingContract": _ZERO_ADDRESS,
            },
            "message": {
                "type": "Withdraw",
                "destination": self._main.address,
                "destination Chain": _WITHDRAW_CHAIN_NAME,
                "token": asset,
                "amount": amount,
                "fee": fee,
                "nonce": user_nonce,
                "aster chain": "Mainnet",
            },
        }
        signature = self._main.sign_message(encode_typed_data(full_message=action)).signature.hex()
        params = {
            "chainId": str(WITHDRAW_CHAIN_ID),
            "asset": asset,
            "amount": amount,
            "fee": fee,
            "receiver": self._main.address,
            "userNonce": str(user_nonce),
            "userSignature": signature if signature.startswith("0x") else f"0x{signature}",
        }
        return await self._signed("POST", "/fapi/v3/aster/user-withdraw", params, agent=agent)

    async def user_trades(self, *, symbol: str, order_id: int) -> list[dict[str, Any]]:
        return await self._signed("GET", "/fapi/v3/userTrades", {"symbol": symbol, "orderId": str(order_id)})


__all__ = [
    "AGENT_SIGNATURE_CHAIN_ID",
    "derive_agent",
    "DEFAULT_BASE_URL",
    "REQUEST_SIGNATURE_CHAIN_ID",
    "AsterApiError",
    "AsterProApiClient",
    "AsterUnknownOutcomeError",
    "SymbolRules",
    "format_price",
    "format_quantity",
    "protected_price",
    "quantity_for_notional",
    "sign_message",
]
