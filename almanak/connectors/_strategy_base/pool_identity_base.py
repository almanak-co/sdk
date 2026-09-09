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
from almanak.connectors._strategy_base.rpc import looks_like_revert, looks_like_transport

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

# ERC-4626 (vault share token) — ``asset()`` / ``totalAssets()`` are the
# interface's two mandatory, argument-free reads; a plain ERC-20 answers
# neither, so both answering is the vault signature.
ERC4626_ASSET_SELECTOR = "0x38d52e0f"
ERC4626_TOTAL_ASSETS_SELECTOR = "0x01e1d114"
# Morpho generation fingerprints. MetaMorpho v1 exposes its allocation as
# on-chain queues; Morpho Vault V2 replaced them with an adapter list, and
# neither generation answers the other's read (both revert).
METAMORPHO_V1_WITHDRAW_QUEUE_LENGTH_SELECTOR = "0x33f91ebb"  # selector of the v1 withdrawQueueLength read
MORPHO_VAULT_V2_ADAPTERS_LENGTH_SELECTOR = "0x5aa22bc8"  # selector of the V2 adaptersLength read

ERC4626_VAULT_KIND = "erc4626_vault"
MORPHO_VAULT_PROTOCOL = "metamorpho"
# Names the CLI needs when it promotes a Morpho vault (``ax vault``): the
# gateway token-index source that answers for Morpho vault shares, and the
# lending protocol whose isolated markets a MetaMorpho v1 withdrawQueue holds.
# Kept here (a canonical skeleton home) so ``almanak/framework/cli`` never
# spells a connector name itself.
MORPHO_VAULT_LOOKUP_SOURCE = "morpho_vault"
MORPHO_BLUE_LENDING_PROTOCOL = "morpho_blue"
MORPHO_VAULT_VERSION_V1 = "v1"
MORPHO_VAULT_VERSION_V2 = "v2"


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
    """eth_call for identity probes: revert/empty/junk → ``None``; transport raises.

    A contract revert (selector missing, ``execution reverted``) is a definitive
    "this ABI is not present" and must abstain. A timeout, gateway-down, or
    rate-limit is inconclusive: swallowing it as ``None`` lets a later ERC-20
    fallback classify a real vault as a plain token. Callers that need a
    mandatory read (ERC-4626 ``asset()`` / ``totalAssets()``) therefore let
    transport errors propagate so ``ax vault`` can exit 4 (inconclusive).

    Anything that is neither a revert nor a transport failure (malformed
    hex, MagicMock AttributeError, TypeError from a fake gateway) is also
    an abstain — it is not evidence the RPC is down.
    """
    try:
        return eth_call(
            rpc_url or "",
            to,
            data,
            timeout=timeout,
            chain=chain,
            gateway_client=gateway_client,
            raise_errors=True,
            gateway_raise_on_error=True,
        )
    except Exception as exc:
        text = str(exc)
        if looks_like_revert(text):
            return None
        if looks_like_transport(text):
            raise
        return None


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

    factories = spec.factories_for(chain)
    if not factories:
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
    for factory in factories:
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
                    "factory": factory.lower(),
                    "factory_verified": "verified",
                    "identified_via": "abi-probe+factory",
                }
    return None


def identify_erc4626_vault(
    chain: str,
    address: str,
    *,
    gateway_client: GatewayClient | None = None,
    rpc_url: str | None = None,
    timeout: float = 10.0,
) -> dict | None:
    """Classify an ERC-4626 vault share token, before the bare ERC-20 fallback.

    Answers the question the ERC-20 fallback cannot: "this address is a
    share/receipt token — a receipt for WHAT?". A DeFiLlama ``poolTokenAddress``
    for a curated Morpho vault is the vault itself, and the SDK primitive is
    ``Intent.vault_deposit(vault_address=<this address>)`` — not a Morpho Blue
    ``market_id``, which does not exist for a vault.

    Signature: ``asset()`` returns a non-zero address AND ``totalAssets()``
    answers. Then the Morpho generation is fingerprinted on-chain —
    ``withdrawQueueLength()`` (MetaMorpho v1) or ``adaptersLength()``
    (Morpho Vault V2) — so ``protocol``/``vault_version`` are provenance
    from the contract's own ABI, not inferred from the symbol. A vault
    answering neither fingerprint is still reported as a generic ERC-4626
    vault with ``protocol=None``. Mandatory ``asset()`` / ``totalAssets()``
    reads propagate transport failures (they do not abstain); a revert is
    still ``None``.

    Provenance is ``factory_verified: "unverified"``: no factory/registry
    reverse-check exists for vaults here. ``almanak ax vault <address>`` is
    the promotion step (listing check + allocation), mirroring
    ``ax lending-reserves`` -> ``ax lending-market`` for markets.
    """

    def call(to: str, data: str) -> bytes | None:
        return probe_call(chain, to, data, gateway_client=gateway_client, rpc_url=rpc_url, timeout=timeout)

    underlying = decode_word_address(call(address, ERC4626_ASSET_SELECTOR))
    if underlying is None:
        return None
    total_assets = decode_word_uint(call(address, ERC4626_TOTAL_ASSETS_SELECTOR))
    if total_assets is None:
        return None

    symbol = decode_word_string(call(address, SYMBOL_SELECTOR))
    decimals = decode_word_uint(call(address, DECIMALS_SELECTOR))
    underlying_symbol = decode_word_string(call(underlying, SYMBOL_SELECTOR))
    underlying_decimals = decode_word_uint(call(underlying, DECIMALS_SELECTOR))

    protocol: str | None = None
    vault_version: str | None = None
    if decode_word_uint(call(address, METAMORPHO_V1_WITHDRAW_QUEUE_LENGTH_SELECTOR)) is not None:
        protocol, vault_version = MORPHO_VAULT_PROTOCOL, MORPHO_VAULT_VERSION_V1
    elif decode_word_uint(call(address, MORPHO_VAULT_V2_ADAPTERS_LENGTH_SELECTOR)) is not None:
        protocol, vault_version = MORPHO_VAULT_PROTOCOL, MORPHO_VAULT_VERSION_V2

    label = symbol or address
    if protocol is not None:
        # Both Morpho generations round-trip through the morpho_vault connector
        # (V2 since the generation-aware redeem path); the generation note below
        # carries the V2 liquidity caveat.
        target_note = (
            "The vault IS the execution target — use "
            f'Intent.vault_deposit(protocol="{protocol}", vault_address="{address}", ...) / '
            "Intent.vault_redeem(...)."
        )
    else:
        # No Morpho fingerprint: naming a connector here would route a foreign
        # ERC-4626 vault (Lagoon, Yearn, Beefy, ...) to the wrong executor.
        target_note = (
            "The vault IS the execution target (Intent.vault_deposit / Intent.vault_redeem with the "
            "vault address), but which vault connector owns it is not established — see the generation note."
        )
    notes = [
        f"ERC-4626 vault share token: {label} is a receipt for {underlying_symbol or underlying} "
        f"held by the vault at this address. {target_note} It is NOT a lending market: do not look for, "
        "derive, or ask the user for a Morpho Blue market_id (a curated vault allocates "
        "across many markets and has no single id).",
        "Promotion step: `almanak ax vault <address>` verifies the vault on-chain, checks the "
        "Morpho listing, and prints its generation + allocation before you pin the address in config.",
    ]
    if vault_version == MORPHO_VAULT_VERSION_V1:
        notes.append(
            "Morpho generation: MetaMorpho v1 (withdrawQueue on-chain) — supported by the morpho_vault connector."
        )
    elif vault_version == MORPHO_VAULT_VERSION_V2:
        notes.append(
            "Morpho generation: Morpho Vault V2 (adapter-based) — supported by the morpho_vault connector "
            "(deposit + redeem; redeem-all sizes from balanceOf and is simulated before send). Exit liquidity "
            "on V2 comes from idle assets plus one liquidity adapter and is NOT guaranteed: a redeem that "
            "cannot be covered fails closed at compile time (VaultIlliquidError) rather than reverting on-chain; "
            "the penalised forceDeallocate escape hatch is not automated. Size positions with that in mind."
        )
    else:
        notes.append(
            "Generation fingerprint did not match MetaMorpho v1 or Morpho Vault V2 — a non-Morpho ERC-4626 "
            "vault. Check the supported vault connectors (lagoon, yearn, beefy, ...) before targeting it."
        )

    return {
        "kind": ERC4626_VAULT_KIND,
        "family": "erc4626",
        "protocol": protocol,
        "vault_version": vault_version,
        "pool_address": address.lower(),
        "lp_token": address.lower(),
        "symbol": symbol,
        "decimals": decimals,
        "underlying_asset": underlying,
        "underlying_symbol": underlying_symbol,
        "underlying_decimals": underlying_decimals,
        "total_assets": total_assets,
        "factory_verified": "unverified",
        "identified_via": "abi-probe",
        "notes": notes,
    }


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
            "ERC-20 interface only — a receipt or LP token whose parent product no registered "
            "probe recognised (it answered neither a pool ABI nor ERC-4626 asset()/totalAssets()); "
            "not usable as a pool execution target."
        ],
    }
