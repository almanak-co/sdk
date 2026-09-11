"""Offline identity adaptation for persisted inventory revaluation inputs."""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass, field
from functools import cache
from typing import Any

from almanak.core.asset_identity import AssetIdentity, AssetNamespace
from almanak.core.chains import ChainRegistry
from almanak.core.chains._helpers import native_symbols_for
from almanak.core.enums import ChainFamily
from almanak.framework.data.tokens.defaults import DEFAULT_TOKENS, NATIVE_SENTINEL, SYMBOL_ALIASES


class InventoryIdentityError(ValueError):
    """Persisted inputs cannot establish one unambiguous holding identity."""


def _address_identity(address: str, chain: str) -> AssetIdentity:
    descriptor = ChainRegistry.resolve(chain)
    if descriptor.family is ChainFamily.EVM:
        if address.lower() == NATIVE_SENTINEL.lower():
            return AssetIdentity.native(descriptor.name)
        return AssetIdentity(descriptor.name, AssetNamespace.ERC20, address)
    return AssetIdentity(descriptor.name, AssetNamespace.TOKEN, address)


@cache
def _bundled_aliases() -> dict[tuple[str, str], frozenset[AssetIdentity]]:
    aliases: dict[tuple[str, str], set[AssetIdentity]] = {}
    for token in DEFAULT_TOKENS:
        for chain in token.chains:
            descriptor = ChainRegistry.try_resolve(chain)
            if descriptor is None:
                continue
            address = token.get_address(chain)
            if not address:
                continue
            try:
                identity = _address_identity(address, descriptor.name)
            except ValueError:
                continue
            aliases.setdefault((descriptor.name, token.symbol.upper()), set()).add(identity)
    for (chain, symbol), address in SYMBOL_ALIASES.items():
        descriptor = ChainRegistry.try_resolve(chain)
        if descriptor is None:
            continue
        identity = _address_identity(address, descriptor.name)
        aliases.setdefault((descriptor.name, symbol.upper()), set()).add(identity)
    return {key: frozenset(values) for key, values in aliases.items()}


def _asset_key(token: str, chain: str) -> tuple[str, str]:
    descriptor = ChainRegistry.resolve(chain)
    if token.upper() in native_symbols_for(descriptor.name):
        identity = AssetIdentity.native(descriptor.name)
    elif "/" in token and ":" in token:
        identity = AssetIdentity.from_caip19(token)
        if identity.chain != descriptor.name:
            raise InventoryIdentityError("Asset chain conflicts with its holding scope")
    elif token.lower().startswith("0x") or (descriptor.family is ChainFamily.SOLANA and len(token) >= 32):
        identity = _address_identity(token, descriptor.name)
    else:
        identities = _bundled_aliases().get((descriptor.name, token.upper()), frozenset())
        if len(identities) > 1:
            raise InventoryIdentityError("Bundled symbol identifies multiple contracts on this chain")
        if not identities:
            return "legacy_symbol", token.upper()
        identity = next(iter(identities))
    return identity.asset_namespace.value, identity.asset_reference


def _encoded_asset(deployment: str, chain: str, wallet: str, asset: tuple[str, str]) -> str:
    # FIFO lowercases token keys and splits on colons; hex preserves every identity byte.
    return "inventory_" + json.dumps([deployment, chain, wallet, *asset], separators=(",", ":")).encode().hex()


def _decoded(raw: Any, expected: type) -> Any:
    value = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(value, expected):
        raise InventoryIdentityError("Persisted inventory input has an invalid shape")
    return value


def _scope(chain: Any, wallet: Any) -> tuple[str, str]:
    if not isinstance(chain, str) or not isinstance(wallet, str) or not chain or not wallet:
        raise InventoryIdentityError("Holding chain and wallet were not observed")
    descriptor = ChainRegistry.resolve(chain)
    namespace = AssetNamespace.ERC20 if descriptor.family is ChainFamily.EVM else AssetNamespace.TOKEN
    normalized = AssetIdentity(descriptor.name, namespace, wallet).asset_reference
    return descriptor.name, normalized


def _swap_position_claims(
    chain: Any, wallet: Any, event: dict[str, Any], payload: dict[str, Any]
) -> list[tuple[str, str]]:
    claims: list[tuple[str, str]] = []
    for key in (event.get("position_key"), payload.get("swap_position_key")):
        if isinstance(key, str) and key.startswith("swap:"):
            parts = key.split(":")
            if len(parts) != 3:
                raise InventoryIdentityError("Invalid fungible wallet position scope")
            # Swap handlers persist swap:{chain.lower()}:{wallet.lower()} on every
            # family. That encoding is the same holding, not a second wallet.
            if (
                isinstance(chain, str)
                and isinstance(wallet, str)
                and key.lower() == f"swap:{chain.strip().lower()}:{wallet.strip().lower()}"
            ):
                continue
            claims.append(_scope(parts[1], parts[2]))
    if payload.get("swap_position_key") and not str(payload["swap_position_key"]).startswith("swap:"):
        raise InventoryIdentityError("Invalid fungible wallet position key")
    return claims


def _event_scope(event: dict[str, Any], payload: dict[str, Any]) -> tuple[str, str]:
    claims: list[tuple[str, str]] = []
    chain, wallet = event.get("chain"), event.get("wallet_address")
    if chain and wallet:
        claims.append(_scope(chain, wallet))
    claims.extend(_swap_position_claims(chain, wallet, event, payload))
    if not claims or len(set(claims)) != 1:
        raise InventoryIdentityError("Event wallet scope is missing or conflicting")
    result = claims[0]
    if chain and ChainRegistry.resolve(chain).name != result[0]:
        raise InventoryIdentityError("Event chain conflicts with its position scope")
    if wallet and _scope(result[0], wallet)[1] != result[1]:
        raise InventoryIdentityError("Event wallet conflicts with its position scope")
    if payload.get("chain") and ChainRegistry.resolve(payload["chain"]).name != result[0]:
        raise InventoryIdentityError("Payload chain conflicts with event scope")
    for key in ("wallet_address", "wallet"):
        if payload.get(key) and _scope(result[0], payload[key])[1] != result[1]:
            raise InventoryIdentityError("Payload wallet conflicts with event scope")
    return result


_FUNGIBLE_FIELDS = ("token_in", "token_out", "token0", "token1", "asset")
_REPLAY_EVENTS = frozenset(
    {
        "SWAP",
        "WALLET_MOVEMENT",
        "LP_OPEN",
        "LP_CLOSE",
        "LP_COLLECT_FEES",
        "BORROW",
        "SUPPLY",
        "WITHDRAW",
        "REPAY",
        "DELEVERAGE",
        "PT_BUY",
        "PT_SELL",
        "PT_REDEEM",
    }
)


@dataclass
class ScopedInventoryInputs:
    deployment_id: str
    labels: dict[str, str] = field(default_factory=dict)
    principal_positions: dict[str, str] = field(default_factory=dict)
    display_labels: dict[str, str] = field(default_factory=dict)
    observed_scopes: set[tuple[str, str]] = field(default_factory=set)
    asset_lanes: dict[tuple[str, ...], bool] = field(default_factory=dict)
    endpoint_scopes: list[set[tuple[str, str]]] = field(default_factory=list)

    def token(self, value: Any, scope: tuple[str, str], *, principal_token: bool = False) -> str:
        if not isinstance(value, str) or not value.strip():
            raise InventoryIdentityError("Inventory asset identity is missing")
        token = value.strip()
        if principal_token and token.upper().startswith("PT-"):
            asset = ("principal_token", token.upper())
        else:
            asset = _asset_key(token, scope[0])
            physical_key = (*scope, *asset)
            previous_lane = self.asset_lanes.get(physical_key)
            if previous_lane is not None and previous_lane != principal_token:
                raise InventoryIdentityError("One physical holding spans incompatible inventory lanes")
            self.asset_lanes[physical_key] = principal_token
            if principal_token:
                asset = ("principal_" + asset[0], asset[1])
        encoded = _encoded_asset(self.deployment_id, *scope, asset)
        if principal_token:
            encoded = "PT-" + encoded
        self.labels[encoded.upper()] = ":".join((*scope, *asset))
        self.observed_scopes.add(scope)
        if not token.startswith("0x") and len(token) < 32:
            self.display_labels[encoded.upper()] = token.upper()
        else:
            self.display_labels.setdefault(encoded.upper(), token)
        return encoded

    def snapshot(self, original: dict[str, Any] | None) -> dict[str, Any] | None:
        if original is None:
            raise InventoryIdentityError("Inventory endpoint is missing")
        if original.get("deployment_id") != self.deployment_id:
            raise InventoryIdentityError("Snapshot deployment scope does not match")
        snapshot = deepcopy(original)
        raw_positions = snapshot.get("positions_json") or "[]"
        container = json.loads(raw_positions) if isinstance(raw_positions, str) else raw_positions
        metadata = container.get("metadata", {}) if isinstance(container, dict) else {}
        if not isinstance(metadata, dict):
            raise InventoryIdentityError("Endpoint metadata has an invalid shape")
        rows = _decoded(snapshot.get("wallet_balances_json", "[]"), list)
        positions = container.get("positions", []) if isinstance(container, dict) else container
        explicit_pt_scope = isinstance(positions, list) and any(
            isinstance(position, dict)
            and isinstance(position.get("details"), dict)
            and position["details"].get("source") == "pt_inventory_lots"
            and (position["details"].get("wallet_address") or position["details"].get("wallet"))
            for position in positions
        )
        scopes, endpoint_scopes = self._bind_wallet_observations(metadata.get("wallet_scope", {}), rows)
        if not rows and not scopes and not explicit_pt_scope:
            raise InventoryIdentityError("Empty endpoint has no observed wallet scope")
        snapshot["wallet_balances_json"] = json.dumps(rows)
        snapshot["positions_json"] = self._positions(snapshot.get("positions_json", "[]"), scopes, endpoint_scopes)
        if snapshot.get("chain") and ChainRegistry.resolve(snapshot["chain"]).name not in {
            chain for chain, _ in endpoint_scopes
        }:
            raise InventoryIdentityError("Snapshot chain conflicts with its observed endpoint scope")
        self.endpoint_scopes.append(endpoint_scopes)
        if len(self.endpoint_scopes) == 2 and self.endpoint_scopes[0] != endpoint_scopes:
            raise InventoryIdentityError("Endpoint wallet scope continuity is unproven")
        return snapshot

    def _bind_wallet_observations(self, proof: Any, rows: list[Any]) -> tuple[dict[str, str], set[tuple[str, str]]]:
        scopes: dict[str, str] = {}
        if proof:
            if (
                not isinstance(proof, dict)
                or type(proof.get("schema_version")) is not int
                or proof["schema_version"] != 1
                or not isinstance(proof.get("chain_wallets"), dict)
            ):
                raise InventoryIdentityError("Unsupported endpoint wallet scope evidence")
            for chain, wallet in proof["chain_wallets"].items():
                normalized_chain, normalized_wallet = _scope(chain, wallet)
                if normalized_chain in scopes and scopes[normalized_chain] != normalized_wallet:
                    raise InventoryIdentityError("Conflicting endpoint wallet scope evidence")
                scopes[normalized_chain] = normalized_wallet
        endpoint_scopes = set(scopes.items())
        seen: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                raise InventoryIdentityError("Wallet observation has an invalid shape")
            scope = _scope(row.get("chain"), row.get("wallet_address"))
            if scope[0] in scopes and scopes[scope[0]] != scope[1]:
                raise InventoryIdentityError("Wallet observation conflicts with its endpoint scope")
            endpoint_scopes.add(scope)
            token = self.token(row.get("address") or row.get("symbol"), scope)
            if token in seen:
                raise InventoryIdentityError("Duplicate wallet observation identity")
            seen.add(token)
            row["symbol"] = token
        return scopes, endpoint_scopes

    def _positions(self, raw: Any, scopes: dict[str, str], endpoint_scopes: set[tuple[str, str]]) -> str:
        container = json.loads(raw) if isinstance(raw, str) else deepcopy(raw)
        if container is None:
            return "[]"
        rows = container.get("positions", []) if isinstance(container, dict) else container
        if not isinstance(rows, list):
            raise InventoryIdentityError("Position observations have an invalid shape")
        seen: set[str] = set()
        for position in rows:
            if not isinstance(position, dict):
                raise InventoryIdentityError("Position observation has an invalid shape")
            details = position.get("details") or {}
            if not isinstance(details, dict):
                raise InventoryIdentityError("Position details have an invalid shape")
            if details.get("source") != "pt_inventory_lots":
                continue
            chain = ChainRegistry.resolve(position.get("chain") or "").name
            wallet = details.get("wallet_address") or details.get("wallet") or scopes.get(chain)
            scope = _scope(chain, wallet)
            for key in ("wallet_address", "wallet"):
                if details.get(key) and _scope(chain, details[key]) != scope:
                    raise InventoryIdentityError("Principal-token position carries conflicting wallets")
            if chain in scopes and scopes[chain] != scope[1]:
                raise InventoryIdentityError("Principal-token position conflicts with endpoint wallet scope")
            endpoint_scopes.add(scope)
            token = self.token(details.get("pt_symbol") or details.get("asset"), scope, principal_token=True)
            if details.get("pt_symbol") and details.get("asset"):
                if self.token(details["asset"], scope, principal_token=True) != token:
                    raise InventoryIdentityError("Principal-token position carries conflicting assets")
            if token in seen:
                raise InventoryIdentityError("Duplicate principal-token observation identity")
            seen.add(token)
            details["pt_symbol"] = token
            details["asset"] = token
        return json.dumps(container)

    def events(self, originals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for original in originals:
            if not isinstance(original, dict) or original.get("deployment_id") != self.deployment_id:
                continue
            event = deepcopy(original)
            event_type = str(event.get("event_type") or "").upper()
            if event_type not in _REPLAY_EVENTS:
                events.append(event)
                continue
            payload = _decoded(event.get("payload_json") or event.get("payload") or "{}", dict)
            if event.get("payload") is not None and event.get("payload_json"):
                if _decoded(event["payload"], dict) != payload:
                    raise InventoryIdentityError("Event carries contradictory payload bodies")
            scope = _event_scope(event, payload)
            event["chain"], event["wallet_address"] = scope[0], scope[1].lower()
            swap_key = f"swap:{scope[0]}:{scope[1].lower()}"
            if payload.get("swap_position_key"):
                payload["swap_position_key"] = swap_key
            if str(event.get("position_key") or "").startswith("swap:"):
                event["position_key"] = swap_key
            for key in _FUNGIBLE_FIELDS:
                if payload.get(key):
                    payload[key] = self.token(payload[key], scope)
            if payload.get("coin_symbols"):
                coins = payload["coin_symbols"]
                if not isinstance(coins, list | tuple):
                    raise InventoryIdentityError("LP coin identities have an invalid shape")
                payload["coin_symbols"] = [self.token(token, scope) if token else token for token in coins]
            if payload.get("pt_token"):
                payload["pt_token"] = self.token(payload["pt_token"], scope, principal_token=True)
                position = str(event.get("position_key") or "")
                prior = self.principal_positions.setdefault(payload["pt_token"], position)
                if not position or prior != position:
                    raise InventoryIdentityError("Principal-token identity spans ambiguous position keys")
            event["payload_json"] = json.dumps(payload)
            event.pop("payload", None)
            events.append(event)
        return events

    def label(self, key: str) -> str:
        token, separator, suffix = key.partition(":")
        canonical = token.upper()
        display = self.display_labels.get(canonical)
        unique = display is not None and sum(value == display for value in self.display_labels.values()) == 1
        if len(self.observed_scopes) == 1 and unique:
            return str(display) + separator + suffix
        return self.labels.get(canonical, token) + separator + suffix
