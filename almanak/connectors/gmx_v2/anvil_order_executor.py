"""Managed-Anvil execution for GMX V2 keeper-settled orders.

Production GMX orders are executed by an authorized keeper with signed oracle
payloads. A historical Anvil fork has neither the keeper process nor a matching
off-chain oracle archive. This module reproduces the same on-chain execution
entrypoint without weakening production behavior:

* every mutation is routed through :class:`GatewayWeb3Provider`;
* the caller must explicitly name the ``anvil`` network;
* the exact submitted order key must still belong to the strategy wallet;
* controller and keeper authority are verified against the forked RoleStore;
* prices come from the gateway price service and are scaled using measured
  on-chain token decimals;
* Oracle state, impersonation, and temporary balances are cleaned up.

The gateway rejects all Anvil mutation methods on mainnet, providing a second
boundary beyond the connector-level network guard.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, InvalidOperation
from typing import Any

from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode
from eth_typing import HexStr
from eth_utils import function_signature_to_4byte_selector, keccak, to_checksum_address
from web3 import Web3
from web3.types import RPCEndpoint

from almanak.connectors.gmx_v2.adapter import GMX_V2_ADDRESSES
from almanak.connectors.gmx_v2.teardown_reads import read_pending_orders
from almanak.framework.web3.gateway_provider import GatewayWeb3Provider
from almanak.gateway.proto import gateway_pb2

logger = logging.getLogger(__name__)

_GMX_USD_DECIMALS = 30
_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
_TX_RECEIPT_TIMEOUT_SECONDS = 30

_CONTROLLER_ROLE = keccak(abi_encode(["string"], ["CONTROLLER"]))
_ORDER_KEEPER_ROLE = keccak(abi_encode(["string"], ["ORDER_KEEPER"]))

_GET_ADDRESS_SIGNATURES = {
    "oracle": "oracle()",
    "role_store": "roleStore()",
    "data_store": "dataStore()",
}
_GET_MARKET_SIGNATURE = "getMarket(address,address)"
_GET_ROLE_MEMBER_COUNT_SIGNATURE = "getRoleMemberCount(bytes32)"
_GET_ROLE_MEMBERS_SIGNATURE = "getRoleMembers(bytes32,uint256,uint256)"
_HAS_ROLE_SIGNATURE = "hasRole(address,bytes32)"
_DECIMALS_SIGNATURE = "decimals()"
_GET_TOKENS_WITH_PRICES_COUNT_SIGNATURE = "getTokensWithPricesCount()"
_SET_PRIMARY_PRICE_SIGNATURE = "setPrimaryPrice(address,(uint256,uint256))"
_SET_TIMESTAMPS_SIGNATURE = "setTimestamps(uint256,uint256)"
_CLEAR_ALL_PRICES_SIGNATURE = "clearAllPrices()"
_EXECUTE_ORDER_SIGNATURE = "executeOrder(bytes32,(address[],address[],bytes[]))"


class GmxAnvilOrderExecutionError(RuntimeError):
    """Raised when the managed fork cannot safely execute a pending order."""


@dataclass(frozen=True)
class GmxAnvilOrderExecutionResult:
    """Measured result of one managed-fork execution pass."""

    ok: bool
    executed_order_keys: tuple[str, ...] = ()
    transaction_hashes: tuple[str, ...] = ()
    reason: str | None = None


@dataclass(frozen=True)
class _GmxDependencies:
    order_handler: str
    oracle: str
    role_store: str
    data_store: str
    reader: str


def _selector(signature: str) -> bytes:
    return function_signature_to_4byte_selector(signature)


def _calldata(signature: str, types: list[str] | None = None, values: list[Any] | None = None) -> str:
    encoded = abi_encode(types or [], values or [])
    return "0x" + (_selector(signature) + encoded).hex()


def _rpc(provider: GatewayWeb3Provider, method: str, params: list[Any]) -> Any:
    response = provider.make_request(RPCEndpoint(method), params)
    error = response.get("error")
    if error:
        message = error.get("message", str(error)) if isinstance(error, dict) else str(error)
        raise GmxAnvilOrderExecutionError(f"{method} failed: {message}")
    if "result" not in response:
        raise GmxAnvilOrderExecutionError(f"{method} returned no result")
    return response["result"]


def _eth_call(provider: GatewayWeb3Provider, to: str, data: str) -> Any:
    return _rpc(provider, "eth_call", [{"to": to_checksum_address(to), "data": data}, "latest"])


def _decode_address(raw: Any, label: str) -> str:
    try:
        decoded = abi_decode(["address"], bytes.fromhex(str(raw).removeprefix("0x")))[0]
        address = to_checksum_address(decoded)
    except Exception as exc:
        raise GmxAnvilOrderExecutionError(f"Could not decode GMX {label} address") from exc
    if address.lower() == _ZERO_ADDRESS:
        raise GmxAnvilOrderExecutionError(f"GMX {label} address is zero")
    return address


def _decode_uint(raw: Any, label: str) -> int:
    try:
        return int(abi_decode(["uint256"], bytes.fromhex(str(raw).removeprefix("0x")))[0])
    except Exception as exc:
        raise GmxAnvilOrderExecutionError(f"Could not decode GMX {label}") from exc


def _read_address_getter(provider: GatewayWeb3Provider, contract: str, name: str) -> str:
    signature = _GET_ADDRESS_SIGNATURES[name]
    return _decode_address(_eth_call(provider, contract, _calldata(signature)), name)


def _load_dependencies(provider: GatewayWeb3Provider, chain: str) -> _GmxDependencies:
    addresses = GMX_V2_ADDRESSES.get(chain.lower())
    if addresses is None:
        raise GmxAnvilOrderExecutionError(f"GMX V2 managed-order execution is not configured for {chain}")

    order_handler = to_checksum_address(addresses["order_handler"])
    data_store = _read_address_getter(provider, order_handler, "data_store")
    configured_data_store = to_checksum_address(addresses["data_store"])
    if data_store != configured_data_store:
        raise GmxAnvilOrderExecutionError(
            f"GMX OrderHandler DataStore mismatch: handler={data_store} configured={configured_data_store}"
        )

    return _GmxDependencies(
        order_handler=order_handler,
        oracle=_read_address_getter(provider, order_handler, "oracle"),
        role_store=_read_address_getter(provider, order_handler, "role_store"),
        data_store=data_store,
        reader=to_checksum_address(addresses["synthetics_reader"]),
    )


def _has_role(provider: GatewayWeb3Provider, role_store: str, account: str, role: bytes) -> bool:
    raw = _eth_call(
        provider,
        role_store,
        _calldata(_HAS_ROLE_SIGNATURE, ["address", "bytes32"], [to_checksum_address(account), role]),
    )
    try:
        return bool(abi_decode(["bool"], bytes.fromhex(str(raw).removeprefix("0x")))[0])
    except Exception as exc:
        raise GmxAnvilOrderExecutionError("Could not decode GMX RoleStore.hasRole result") from exc


def _find_order_keeper(provider: GatewayWeb3Provider, role_store: str) -> str:
    count_raw = _eth_call(
        provider,
        role_store,
        _calldata(_GET_ROLE_MEMBER_COUNT_SIGNATURE, ["bytes32"], [_ORDER_KEEPER_ROLE]),
    )
    count = _decode_uint(count_raw, "ORDER_KEEPER member count")
    if count <= 0:
        raise GmxAnvilOrderExecutionError("GMX RoleStore has no ORDER_KEEPER members")

    members_raw = _eth_call(
        provider,
        role_store,
        _calldata(
            _GET_ROLE_MEMBERS_SIGNATURE,
            ["bytes32", "uint256", "uint256"],
            [_ORDER_KEEPER_ROLE, 0, count],
        ),
    )
    try:
        members = tuple(
            to_checksum_address(member)
            for member in abi_decode(["address[]"], bytes.fromhex(str(members_raw).removeprefix("0x")))[0]
        )
    except Exception as exc:
        raise GmxAnvilOrderExecutionError("Could not decode GMX ORDER_KEEPER members") from exc
    if not members:
        raise GmxAnvilOrderExecutionError("GMX RoleStore returned an empty ORDER_KEEPER member list")

    keeper = members[0]
    if not _has_role(provider, role_store, keeper, _ORDER_KEEPER_ROLE):
        raise GmxAnvilOrderExecutionError(f"Enumerated GMX keeper {keeper} does not hold ORDER_KEEPER")
    return keeper


def _read_market_tokens(provider: GatewayWeb3Provider, dependencies: _GmxDependencies, market: str) -> tuple[str, ...]:
    raw = _eth_call(
        provider,
        dependencies.reader,
        _calldata(
            _GET_MARKET_SIGNATURE,
            ["address", "address"],
            [dependencies.data_store, to_checksum_address(market)],
        ),
    )
    try:
        _market_token, index_token, long_token, short_token = abi_decode(
            ["(address,address,address,address)"],
            bytes.fromhex(str(raw).removeprefix("0x")),
        )[0]
    except Exception as exc:
        raise GmxAnvilOrderExecutionError(f"Could not decode GMX market {market}") from exc

    tokens = {
        to_checksum_address(token)
        for token in (index_token, long_token, short_token)
        if str(token).lower() != _ZERO_ADDRESS
    }
    if not tokens:
        raise GmxAnvilOrderExecutionError(f"GMX market {market} returned no oracle tokens")
    return tuple(sorted(tokens))


def _read_token_decimals(provider: GatewayWeb3Provider, token: str) -> int:
    decimals = _decode_uint(
        _eth_call(provider, token, _calldata(_DECIMALS_SIGNATURE)),
        f"token decimals for {token}",
    )
    if decimals > _GMX_USD_DECIMALS:
        raise GmxAnvilOrderExecutionError(
            f"Token {token} has {decimals} decimals, above GMX's {_GMX_USD_DECIMALS}-decimal USD scale"
        )
    return decimals


def _read_price_bounds(gateway_client: Any, provider: GatewayWeb3Provider, chain: str, token: str) -> tuple[int, int]:
    response = gateway_client.market.GetPrice(gateway_pb2.PriceRequest(token=token, quote="USD", chain=chain.lower()))
    if bool(getattr(response, "stale", False)):
        raise GmxAnvilOrderExecutionError(f"Gateway price for GMX oracle token {token} is stale")
    try:
        price = Decimal(str(response.price))
    except (InvalidOperation, ValueError) as exc:
        raise GmxAnvilOrderExecutionError(f"Gateway returned an invalid price for GMX oracle token {token}") from exc
    if not price.is_finite() or price <= 0:
        raise GmxAnvilOrderExecutionError(f"Gateway returned a non-positive price for GMX oracle token {token}")

    decimals = _read_token_decimals(provider, token)
    scaled = price * (Decimal(10) ** (_GMX_USD_DECIMALS - decimals))
    minimum = int(scaled.to_integral_value(rounding=ROUND_FLOOR))
    maximum = int(scaled.to_integral_value(rounding=ROUND_CEILING))
    if minimum <= 0 or maximum <= 0:
        raise GmxAnvilOrderExecutionError(f"Scaled GMX oracle price for {token} is non-positive")
    return minimum, maximum


def _latest_block_timestamp(provider: GatewayWeb3Provider) -> int:
    block = _rpc(provider, "eth_getBlockByNumber", ["latest", False])
    if not isinstance(block, dict) or "timestamp" not in block:
        raise GmxAnvilOrderExecutionError("Latest Anvil block did not include a timestamp")
    timestamp = block["timestamp"]
    return int(timestamp, 16) if isinstance(timestamp, str) else int(timestamp)


def _normalize_order_key(order_key: str) -> str:
    try:
        key = bytes.fromhex(str(order_key).strip().removeprefix("0x"))
    except ValueError as exc:
        raise GmxAnvilOrderExecutionError(f"Invalid GMX order key: {order_key!r}") from exc
    if len(key) != 32 or not any(key):
        raise GmxAnvilOrderExecutionError(f"Invalid GMX order key: {order_key!r}")
    return "0x" + key.hex()


def _send_transaction(provider: GatewayWeb3Provider, sender: str, target: str, data: str) -> str:
    sender = to_checksum_address(sender)
    transaction = {
        "from": sender,
        "to": to_checksum_address(target),
        "data": data,
        "value": "0x0",
    }
    gas_estimate_raw = _rpc(provider, "eth_estimateGas", [transaction])
    try:
        gas_estimate = int(gas_estimate_raw, 16) if isinstance(gas_estimate_raw, str) else int(gas_estimate_raw)
    except (TypeError, ValueError) as exc:
        raise GmxAnvilOrderExecutionError(
            f"Anvil eth_estimateGas returned an invalid result: {gas_estimate_raw!r}"
        ) from exc
    if gas_estimate <= 0:
        raise GmxAnvilOrderExecutionError(f"Anvil eth_estimateGas returned a non-positive result: {gas_estimate}")

    gas_price_raw = _rpc(provider, "eth_gasPrice", [])
    balance_raw = _rpc(provider, "eth_getBalance", [sender, "latest"])
    try:
        gas_price = int(gas_price_raw, 16) if isinstance(gas_price_raw, str) else int(gas_price_raw)
        balance = int(balance_raw, 16) if isinstance(balance_raw, str) else int(balance_raw)
    except (TypeError, ValueError) as exc:
        raise GmxAnvilOrderExecutionError("Anvil returned an invalid gas price or sender balance") from exc
    required_balance = gas_estimate * gas_price
    if balance < required_balance:
        _rpc(provider, "anvil_setBalance", [sender, hex(required_balance)])

    transaction_with_gas = {
        **transaction,
        "gas": hex(gas_estimate),
        "gasPrice": hex(gas_price),
    }

    tx_hash = _rpc(
        provider,
        "eth_sendTransaction",
        [transaction_with_gas],
    )
    if not isinstance(tx_hash, str) or not tx_hash.startswith("0x"):
        raise GmxAnvilOrderExecutionError("Anvil eth_sendTransaction returned an invalid transaction hash")

    receipt = Web3(provider).eth.wait_for_transaction_receipt(
        HexStr(tx_hash),
        timeout=_TX_RECEIPT_TIMEOUT_SECONDS,
    )
    if int(receipt["status"]) != 1:
        gas_used = int(receipt["gasUsed"])
        raise GmxAnvilOrderExecutionError(
            f"GMX managed-Anvil transaction reverted: {tx_hash} "
            f"(gas_used={gas_used}, measured_gas_limit={gas_estimate})"
        )
    return tx_hash


@contextmanager
def _impersonated(provider: GatewayWeb3Provider, account: str) -> Iterator[None]:
    account = to_checksum_address(account)
    original_balance = _rpc(provider, "eth_getBalance", [account, "latest"])
    impersonating = False
    try:
        _rpc(provider, "anvil_impersonateAccount", [account])
        impersonating = True
        yield
    finally:
        if impersonating:
            try:
                _rpc(provider, "anvil_setBalance", [account, original_balance])
            finally:
                _rpc(provider, "anvil_stopImpersonatingAccount", [account])


def _seed_oracle_prices(
    *,
    gateway_client: Any,
    provider: GatewayWeb3Provider,
    dependencies: _GmxDependencies,
    chain: str,
    markets: tuple[str, ...],
) -> tuple[str, ...]:
    count = _decode_uint(
        _eth_call(
            provider,
            dependencies.oracle,
            _calldata(_GET_TOKENS_WITH_PRICES_COUNT_SIGNATURE),
        ),
        "Oracle tokens-with-prices count",
    )
    if count != 0:
        raise GmxAnvilOrderExecutionError(
            f"GMX Oracle already has {count} transient price token(s); refusing to overwrite execution state"
        )

    oracle_tokens: set[str] = set()
    for market in markets:
        oracle_tokens.update(_read_market_tokens(provider, dependencies, market))

    hashes: list[str] = []
    for token in sorted(oracle_tokens):
        minimum, maximum = _read_price_bounds(gateway_client, provider, chain, token)
        hashes.append(
            _send_transaction(
                provider,
                dependencies.order_handler,
                dependencies.oracle,
                _calldata(
                    _SET_PRIMARY_PRICE_SIGNATURE,
                    ["address", "(uint256,uint256)"],
                    [token, (minimum, maximum)],
                ),
            )
        )

    timestamp = _latest_block_timestamp(provider)
    hashes.append(
        _send_transaction(
            provider,
            dependencies.order_handler,
            dependencies.oracle,
            _calldata(
                _SET_TIMESTAMPS_SIGNATURE,
                ["uint256", "uint256"],
                [timestamp, timestamp],
            ),
        )
    )
    return tuple(hashes)


def _oracle_price_count(provider: GatewayWeb3Provider, dependencies: _GmxDependencies) -> int:
    return _decode_uint(
        _eth_call(
            provider,
            dependencies.oracle,
            _calldata(_GET_TOKENS_WITH_PRICES_COUNT_SIGNATURE),
        ),
        "Oracle tokens-with-prices count",
    )


def _clear_oracle_prices(provider: GatewayWeb3Provider, dependencies: _GmxDependencies) -> str:
    return _send_transaction(
        provider,
        dependencies.order_handler,
        dependencies.oracle,
        _calldata(_CLEAR_ALL_PRICES_SIGNATURE),
    )


def _execute_order(
    provider: GatewayWeb3Provider,
    dependencies: _GmxDependencies,
    keeper: str,
    order_key: str,
) -> str:
    key = bytes.fromhex(_normalize_order_key(order_key).removeprefix("0x"))
    return _send_transaction(
        provider,
        keeper,
        dependencies.order_handler,
        _calldata(
            _EXECUTE_ORDER_SIGNATURE,
            ["bytes32", "(address[],address[],bytes[])"],
            [key, ([], [], [])],
        ),
    )


def _prepare_execution_request(
    *,
    gateway_client: Any,
    chain: str,
    wallet_address: str,
    orders: tuple[Any, ...],
    network: str,
) -> GmxAnvilOrderExecutionResult | tuple[tuple[str, ...], dict[str, str]]:
    """Validate the managed-fork request and measure executable order markets."""
    if str(network or "").strip().lower() != "anvil":
        return GmxAnvilOrderExecutionResult(
            ok=False,
            reason="GMX local order execution is restricted to the managed Anvil network",
        )
    if gateway_client is None:
        return GmxAnvilOrderExecutionResult(ok=False, reason="GMX local order execution requires a gateway client")

    try:
        requested_keys = tuple(
            dict.fromkeys(_normalize_order_key(str(getattr(order, "order_id", "") or "")) for order in orders)
        )
    except GmxAnvilOrderExecutionError as exc:
        return GmxAnvilOrderExecutionResult(ok=False, reason=str(exc))
    if not requested_keys:
        return GmxAnvilOrderExecutionResult(
            ok=False, reason="GMX local order execution requires exact bytes32 order keys"
        )

    pending = read_pending_orders(gateway_client, chain, wallet_address)
    if not pending.ok:
        return GmxAnvilOrderExecutionResult(
            ok=False,
            reason=pending.error or "GMX pending-order ownership could not be measured",
        )

    pending_by_key = {str(order.order_key).lower(): order for order in pending.orders if order.order_key}
    pending_keys = {str(key).lower() for key in pending.order_keys}
    missing = [key for key in requested_keys if key not in pending_keys]
    if missing and pending.truncated:
        return GmxAnvilOrderExecutionResult(
            ok=False,
            reason="GMX pending-order set was truncated; exact order ownership could not be proven",
        )

    executable_keys = tuple(key for key in requested_keys if key in pending_keys)
    if not executable_keys:
        return GmxAnvilOrderExecutionResult(ok=True)

    market_by_key: dict[str, str] = {}
    for key in executable_keys:
        detail = pending_by_key.get(key)
        market = str(getattr(detail, "market", "") or "")
        if not market or market.lower() == _ZERO_ADDRESS:
            return GmxAnvilOrderExecutionResult(
                ok=False,
                reason=f"GMX order {key} has no measured market detail",
            )
        market_by_key[key] = to_checksum_address(market)
    return executable_keys, market_by_key


def execute_pending_orders_on_anvil(
    *,
    gateway_client: Any,
    chain: str,
    wallet_address: str,
    orders: tuple[Any, ...],
    network: str,
) -> GmxAnvilOrderExecutionResult:
    """Execute the exact submitted GMX orders in the current managed fork."""
    prepared = _prepare_execution_request(
        gateway_client=gateway_client,
        chain=chain,
        wallet_address=wallet_address,
        orders=orders,
        network=network,
    )
    if isinstance(prepared, GmxAnvilOrderExecutionResult):
        return prepared
    executable_keys, market_by_key = prepared

    provider = GatewayWeb3Provider(gateway_client, chain=chain)
    try:
        dependencies = _load_dependencies(provider, chain)
        if not _has_role(provider, dependencies.role_store, dependencies.order_handler, _CONTROLLER_ROLE):
            raise GmxAnvilOrderExecutionError(f"GMX OrderHandler {dependencies.order_handler} does not hold CONTROLLER")
        keeper = _find_order_keeper(provider, dependencies.role_store)

        transaction_hashes: list[str] = []
        with _impersonated(provider, dependencies.order_handler), _impersonated(provider, keeper):
            for key in executable_keys:
                oracle_state_owned = False
                try:
                    initial_count = _oracle_price_count(provider, dependencies)
                    if initial_count != 0:
                        raise GmxAnvilOrderExecutionError(
                            f"GMX Oracle already has {initial_count} transient price token(s); "
                            "refusing to overwrite execution state"
                        )
                    oracle_state_owned = True
                    transaction_hashes.extend(
                        _seed_oracle_prices(
                            gateway_client=gateway_client,
                            provider=provider,
                            dependencies=dependencies,
                            chain=chain,
                            markets=(market_by_key[key],),
                        )
                    )
                    transaction_hashes.append(_execute_order(provider, dependencies, keeper, key))
                finally:
                    if oracle_state_owned and _oracle_price_count(provider, dependencies) != 0:
                        transaction_hashes.append(_clear_oracle_prices(provider, dependencies))

        logger.info(
            "GMX managed-Anvil keeper executed %d exact order(s): keys=%s transactions=%s",
            len(executable_keys),
            ", ".join(executable_keys),
            ", ".join(transaction_hashes),
        )
        return GmxAnvilOrderExecutionResult(
            ok=True,
            executed_order_keys=executable_keys,
            transaction_hashes=tuple(transaction_hashes),
        )
    except Exception as exc:
        logger.warning("GMX managed-Anvil order execution failed: %s", exc, exc_info=True)
        return GmxAnvilOrderExecutionResult(ok=False, reason=f"{type(exc).__name__}: {exc}")


__all__ = [
    "GmxAnvilOrderExecutionError",
    "GmxAnvilOrderExecutionResult",
    "execute_pending_orders_on_anvil",
]
