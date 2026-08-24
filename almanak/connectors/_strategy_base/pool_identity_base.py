"""Shared plumbing for address-form pool identification (ALM-3368).

Connectors declare an ``identity_probe`` on their ``PoolReaderSpec``; each
probe answers "is this address one of mine, and what is it?" for its own ABI
family and reverse-verifies against its own factory/registry — any contract
can mimic an ABI, so identification without provenance is spoofable. The
framework only iterates probes and applies the protocol-neutral ERC-20
fallback (:func:`identify_erc20`).

The V3-shaped family shares one probe (:func:`identify_clamm_pool`): the
declaring spec parameterizes it (protocol name, factory, getPool selector),
so forks sharing the ABI are disambiguated by which factory acknowledges the
address.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from almanak.connectors._strategy_base.pool_validation_base import ZERO_ADDRESS, eth_call

if TYPE_CHECKING:
    from almanak.framework.gateway_client import GatewayClient

TOKEN0_SELECTOR = "0x0dfe1681"
TOKEN1_SELECTOR = "0xd21220a7"
FEE_SELECTOR = "0xddca3f43"
TICK_SPACING_SELECTOR = "0xd0c93a7c"
STABLE_SELECTOR = "0x22be3de1"
DECIMALS_SELECTOR = "0x313ce567"
SYMBOL_SELECTOR = "0x95d89b41"
TOTAL_SUPPLY_SELECTOR = "0x18160ddd"


def decode_word_address(raw: bytes | None) -> str | None:
    if raw is None or len(raw) < 32:
        return None
    address = "0x" + raw[12:32].hex()
    return None if address == ZERO_ADDRESS else address


def decode_word_uint(raw: bytes | None) -> int | None:
    if raw is None or len(raw) < 32:
        return None
    return int.from_bytes(raw[:32], "big")


def decode_word_int(raw: bytes | None) -> int | None:
    value = decode_word_uint(raw)
    if value is None:
        return None
    return value - 2**256 if value >= 2**255 else value


def decode_word_string(raw: bytes | None) -> str | None:
    if raw is None or len(raw) == 0:
        return None
    try:
        if len(raw) == 32:
            return raw.rstrip(b"\x00").decode("utf-8", errors="replace") or None
        offset = int.from_bytes(raw[0:32], "big")
        length = int.from_bytes(raw[offset : offset + 32], "big")
        return raw[offset + 32 : offset + 32 + length].decode("utf-8", errors="replace") or None
    except Exception:  # noqa: BLE001 — a malformed string return is "no symbol", not a fault
        return None


def probe_call(
    chain: str,
    to: str,
    data: str,
    *,
    gateway_client: GatewayClient | None = None,
    rpc_url: str | None = None,
    timeout: float = 10.0,
) -> bytes | None:
    return eth_call(rpc_url or "", to, data, timeout=timeout, chain=chain, gateway_client=gateway_client)


def identify_clamm_pool(
    spec,
    chain: str,
    address: str,
    *,
    gateway_client: GatewayClient | None = None,
    rpc_url: str | None = None,
    timeout: float = 10.0,
) -> dict | None:
    """Identify + factory-verify a V3-shaped pool for the declaring spec.

    Claims the address only when the spec's own factory acknowledges it —
    the same ABI is shared across forks, so factory provenance is the sole
    trustworthy discriminator. Returns ``None`` (not my pool) otherwise.
    """
    from almanak.connectors._strategy_base.v3_pool_abi import encode_get_pool

    def call(to: str, data: str) -> bytes | None:
        return probe_call(chain, to, data, gateway_client=gateway_client, rpc_url=rpc_url, timeout=timeout)

    factory = spec.factory_addresses.get(chain.lower())
    if not factory:
        return None
    token0 = decode_word_address(call(address, TOKEN0_SELECTOR))
    token1 = decode_word_address(call(address, TOKEN1_SELECTOR))
    if token0 is None or token1 is None:
        return None
    fee = decode_word_uint(call(address, FEE_SELECTOR))
    tick_spacing = decode_word_int(call(address, TICK_SPACING_SELECTOR))
    if decode_word_uint(call(address, STABLE_SELECTOR)) is not None:
        return None  # Solidly-shaped; not a CL pool

    keys: list[int] = []
    if spec.discriminator_kind.value == "tick_spacing":
        keys = [k for k in (tick_spacing,) if k is not None]
    else:
        keys = [k for k in (fee,) if k is not None]
    for pool_key in keys:
        raw = call(factory, encode_get_pool(spec.get_pool_selector, token0, token1, pool_key))
        acknowledged = decode_word_address(raw)
        if acknowledged is not None and acknowledged.lower() == address.lower():
            return {
                "kind": "pool",
                "family": "clamm",
                "protocol": spec.protocol,
                "pool_address": address.lower(),
                "token0": token0,
                "token1": token1,
                "fee_tier": fee,
                "tick_spacing": tick_spacing,
                "factory_verified": "verified",
                "identified_via": "abi-probe+factory",
            }
    return None


def identify_erc20(
    chain: str,
    address: str,
    *,
    gateway_client: GatewayClient | None = None,
    rpc_url: str | None = None,
    timeout: float = 10.0,
) -> dict | None:
    def call(to: str, data: str) -> bytes | None:
        return probe_call(chain, to, data, gateway_client=gateway_client, rpc_url=rpc_url, timeout=timeout)

    decimals = decode_word_uint(call(address, DECIMALS_SELECTOR))
    total_supply = decode_word_uint(call(address, TOTAL_SUPPLY_SELECTOR))
    if decimals is None or decimals > 77 or total_supply is None:
        return None
    return {
        "kind": "erc20",
        "pool_address": address.lower(),
        "symbol": decode_word_string(call(address, SYMBOL_SELECTOR)),
        "decimals": decimals,
        "factory_verified": "unverified",
        "identified_via": "abi-probe",
        "notes": [
            "ERC-20 interface only — a vault share, receipt, or LP token rather than a pool; "
            "not usable as a pool execution target."
        ],
    }
