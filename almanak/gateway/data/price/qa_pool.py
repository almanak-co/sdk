"""Exclusive measured-pool input for an owned Local/Anvil E2E experiment.

This is an experiment input route, not a live oracle or an aggregator fallback.
No scalar from this route may survive loss of its bound fork or evidence sink.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
from datetime import UTC, datetime
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import urlsplit

from eth_abi import decode, encode
from pydantic import BaseModel, ConfigDict, Field
from web3 import Web3
from web3.types import HexStr, RPCEndpoint

from almanak.connectors._base.gateway_capabilities import GatewayAddressCapability
from almanak.connectors._base.types import ProtocolName
from almanak.connectors._gateway_registry import GATEWAY_REGISTRY
from almanak.core.chains import ChainRegistry
from almanak.core.rpc_network import Network
from almanak.framework.data.interfaces import PriceResult
from almanak.framework.deployment import is_local
from almanak.framework.local_paths import local_db_path
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.utils.rpc_provider import get_rpc_url


class PoolInputManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1]
    run_id: Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9-]{7,79}$")]
    protocol: Annotated[str, Field(min_length=1)] | None = None
    preparation_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    config_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    fork_block: Annotated[int, Field(strict=True, gt=0)]
    fork_hash: Annotated[str, Field(pattern=r"^0x[0-9a-f]{64}$")]
    database_path: Path
    stimulus_wallet: Annotated[str, Field(pattern=r"^0x[0-9a-fA-F]{40}$")] | None = None
    stimulus_usdc_raw: Annotated[int, Field(strict=True, gt=0, le=10**13)] | None = None


def _canonical(value: dict) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def _owned_bytes(path: Path) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"Pool input requires a regular owned file: {path.name}")
    return path.read_bytes()


def _retain(path: Path, raw: bytes) -> None:
    if path.exists() or path.is_symlink():
        if _owned_bytes(path) != raw:
            raise ValueError("Pool input evidence identity changed")
        return
    with path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class PoolInputRoute:
    def __init__(self, settings: GatewaySettings):
        self.settings = settings
        chain = ChainRegistry.by_id(42161).name
        if not is_local() or settings.network != Network.ANVIL or list(settings.chains) != [chain]:
            raise ValueError("Pool input route requires Local SDK and exactly one Arbitrum Anvil gateway")
        if settings.enable_manual_price_overrides:
            raise ValueError("Pool input route cannot coexist with manual price overrides")
        path = settings.qa_pool_price_manifest
        if path is None or not path.is_absolute() or path.parent.is_symlink():
            raise ValueError("Pool input manifest must have an owned absolute path")
        self.path = path
        self.root = path.parent.resolve()
        self.raw_manifest = _owned_bytes(path)
        self.manifest = PoolInputManifest.model_validate_json(self.raw_manifest)
        self.manifest_hash = hashlib.sha256(self.raw_manifest).hexdigest()
        database = self.manifest.database_path
        if not database.is_absolute() or database.resolve() != local_db_path().resolve():
            raise ValueError("Pool input belongs to a different strategy database")
        self.config_path = database.parent / "config.json"
        self._assert_inputs()
        self.evidence = path.parent / "price-observations"
        if self.evidence.is_symlink():
            raise ValueError("Pool observation directory cannot be a symlink")
        self.evidence.mkdir(exist_ok=True)
        self._lock = threading.Lock()

    async def provision_stimulus(self, manager, subject_wallet: str) -> None:
        """Seed the declared external actor before the managed strategy can dispatch."""
        import asyncio

        from almanak.framework.anvil.accounts import anvil_default_address
        from almanak.framework.data.tokens import get_token_resolver

        wallet = self.manifest.stimulus_wallet
        amount = self.manifest.stimulus_usdc_raw
        if wallet is None and amount is None:
            return
        if wallet is None or amount is None or wallet.lower() != anvil_default_address(1).lower():
            raise ValueError("Pool stimulus must use its declared deterministic Anvil-only actor")
        if wallet.lower() == subject_wallet.lower():
            raise ValueError("Pool stimulus wallet must differ from the subject")
        self._assert_inputs()
        chain = ChainRegistry.by_id(42161).name
        if manager.anvil_port != urlsplit(get_rpc_url(chain, network=Network.ANVIL)).port:
            raise ValueError("Stimulus provisioner does not own the bound fork RPC")
        identity = await asyncio.to_thread(self._identity, self._client())
        _retain(
            self.root / "gateway-startup.json",
            _canonical(
                {
                    "schema_version": 1,
                    "scope": "qa_managed_fork_startup",
                    "run_id": self.manifest.run_id,
                    "fork_identity": identity,
                    "rpc_url": str(getattr(self._client().provider, "endpoint_uri", "")),
                    "subject_wallet": Web3.to_checksum_address(subject_wallet),
                    "database_path": str(self.manifest.database_path),
                    "funding_status": "UNMEASURED",
                    "execution_status": "UNMEASURED",
                }
            ),
        )
        chain = ChainRegistry.by_id(42161).name
        usdc = get_token_resolver().resolve("USDC", chain, skip_gateway=True).address
        if not await manager.fund_wallet(wallet, Decimal(1)):
            raise ValueError("Could not seed the declared stimulus gas reserve")
        failed = await manager.fund_tokens_report(wallet, {usdc: Decimal(amount) / Decimal(10**6)})
        if failed:
            raise ValueError("Could not seed the declared stimulus USDC reserve")
        balances = await asyncio.to_thread(self._stimulus_balances, wallet, usdc)
        if await asyncio.to_thread(self._identity, self._client()) != identity:
            raise ValueError("Stimulus fork identity changed during provisioning")
        if balances["usdc_raw"] != str(amount) or balances["native_wei"] != str(10**18):
            raise ValueError("Stimulus balances disagree with the declared provisioning caps")
        _retain(
            self.evidence / "stimulus-provisioning.json",
            _canonical(
                {
                    "schema_version": 1,
                    "fork_identity": identity,
                    "wallet": wallet,
                    "usdc": usdc,
                    **balances,
                    "stage": "managed_fork_provisioning",
                    "method": "RollingForkManager funding",
                }
            ),
        )
        from almanak.gateway.qa_wallet_balances import capture_wallet_balances

        descriptor = ChainRegistry.by_id(42161)
        weth = get_token_resolver().resolve("WETH", descriptor.name, skip_gateway=True).address
        connector = self._address_connector()
        if not isinstance(connector, GatewayAddressCapability):
            raise ValueError("Pool input requires registered position manager addresses")
        position_manager = connector.addresses_for(descriptor.name).get("position_manager")
        if not position_manager:
            raise ValueError("Pool input position manager address is unmeasured")
        subject_balances = await asyncio.to_thread(
            capture_wallet_balances,
            self._client(),
            subject_wallet,
            [usdc, weth, position_manager],
        )
        if await asyncio.to_thread(self._identity, self._client()) != identity:
            raise ValueError("Subject initial inventory belongs to a changed fork")
        _retain(
            self.root / "subject-initial-balances.json",
            _canonical(
                {
                    "schema_version": 1,
                    "scope": "pre_dispatch_subject_inventory",
                    "run_id": self.manifest.run_id,
                    "manifest_sha256": self.manifest_hash,
                    "fork_identity": identity,
                    "balances": subject_balances,
                }
            ),
        )

    def _stimulus_balances(self, wallet: str, usdc: str) -> dict:
        client = self._client()
        block = client.eth.get_block("latest")
        number = int(block["number"])
        raw = client.eth.call(
            {
                "to": Web3.to_checksum_address(usdc),
                "data": HexStr("0x70a08231" + wallet[2:].lower().rjust(64, "0")),
            },
            block_identifier=number,
        )
        if len(raw) != 32:
            raise ValueError("Stimulus funding balance is unmeasured")
        native = int(client.eth.get_balance(Web3.to_checksum_address(wallet), number))
        nonce = int(client.eth.get_transaction_count(Web3.to_checksum_address(wallet), number))
        if int(client.eth.get_transaction_count(Web3.to_checksum_address(wallet), "pending")) != nonce:
            raise ValueError("Stimulus actor has pending submissions during provisioning")
        if client.eth.get_block(number)["hash"] != block["hash"]:
            raise ValueError("Stimulus provisioning observation block changed")
        return {
            "block_number": number,
            "block_hash": Web3.to_hex(block["hash"]),
            "usdc_raw": str(int.from_bytes(raw, "big")),
            "native_wei": str(native),
            "nonce": nonce,
        }

    def _assert_inputs(self) -> None:
        chain = ChainRegistry.by_id(42161).name
        if not is_local() or self.settings.network != Network.ANVIL or list(self.settings.chains) != [chain]:
            raise ValueError("Pool input gateway execution surface changed")
        if self.settings.enable_manual_price_overrides:
            raise ValueError("Pool input cannot coexist with manual price overrides")
        if self.path.parent.resolve() != self.root or self.path.parent.is_symlink():
            raise ValueError("Pool input evidence ownership changed")
        if (self.root / "price-observations").is_symlink():
            raise ValueError("Pool observation directory cannot be a symlink")
        if self.manifest.database_path.resolve() != local_db_path().resolve():
            raise ValueError("Pool input strategy database identity changed")
        if _owned_bytes(self.path) != self.raw_manifest:
            raise ValueError("Pool input manifest changed after initialization")
        if hashlib.sha256(_owned_bytes(self.config_path)).hexdigest() != self.manifest.config_sha256:
            raise ValueError("Subject config changed after pool input was frozen")

    def _address_connector(self) -> GatewayAddressCapability:
        """Resolve the address capability for the frozen pool without a connector import edge."""
        if self.manifest.protocol:
            connector = GATEWAY_REGISTRY.get(ProtocolName(self.manifest.protocol))
            if isinstance(connector, GatewayAddressCapability):
                return connector
        descriptor = ChainRegistry.by_id(42161)
        for candidate in GATEWAY_REGISTRY.all():
            if not isinstance(candidate, GatewayAddressCapability):
                continue
            try:
                addresses = candidate.addresses_for(descriptor.name)
            except (KeyError, ValueError):
                continue
            if addresses.get("factory") and addresses.get("position_manager"):
                return candidate
        raise ValueError("Pool input requires a registered factory/position-manager address capability")

    def _client(self) -> Web3:
        url = get_rpc_url(ChainRegistry.by_id(42161).name, network=Network.ANVIL)
        parsed = urlsplit(url)
        if (
            parsed.scheme != "http"
            or parsed.hostname not in {"127.0.0.1", "::1"}
            or not parsed.port
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise ValueError("Pool input RPC must be a literal loopback Anvil endpoint")
        return Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 10}))

    def _identity(self, client: Web3) -> dict:
        metadata = client.provider.make_request(RPCEndpoint("anvil_metadata"), []).get("result")
        if not isinstance(metadata, dict) or not metadata.get("instanceId"):
            raise ValueError("Pool input cannot establish Anvil instance identity")
        fork = metadata.get("forkedNetwork") or {}
        number = fork.get("forkBlockNumber")
        if isinstance(number, str):
            number = int(number, 16 if number.startswith("0x") else 10)
        if number != self.manifest.fork_block or int(client.eth.chain_id) != 42161:
            raise ValueError("Pool input chain or fork origin disagrees with the frozen manifest")
        block_hash = Web3.to_hex(client.eth.get_block(number)["hash"]).lower()
        if block_hash != self.manifest.fork_hash:
            raise ValueError("Pool input fork hash disagrees with the frozen manifest")
        return {
            "instance_id": metadata["instanceId"],
            "fork_block": number,
            "fork_hash": block_hash,
            "chain_id": 42161,
            "manifest_sha256": self.manifest_hash,
        }

    def get_price(self, token: str, quote: str, chain: str) -> PriceResult:
        from almanak.framework.data.tokens import get_token_resolver

        descriptor = ChainRegistry.by_id(42161)
        if (chain and ChainRegistry.get(chain) != descriptor) or quote != "USD":
            raise ValueError("Pool input covers only Arbitrum USD quotes")
        connector = self._address_connector()
        if not isinstance(connector, GatewayAddressCapability):
            raise ValueError("Pool input requires registered factory addresses")
        factory = connector.addresses_for(descriptor.name).get("factory")
        if not factory or not Web3.is_address(factory):
            raise ValueError("Pool input factory address is unmeasured")
        resolver = get_token_resolver()
        weth = resolver.resolve("WETH", descriptor.name, skip_gateway=True).address.lower()
        usdc = resolver.resolve("USDC", descriptor.name, skip_gateway=True).address.lower()
        requested = token.lower()
        if requested not in {"eth", "weth", "usdc", weth, usdc}:
            raise ValueError("Pool input does not cover the requested token")
        with self._lock:
            self._assert_inputs()
            client = self._client()
            identity = self._identity(client)
            _retain(self.evidence / "binding.json", _canonical(identity))
            block = client.eth.get_block("latest")
            number = int(block["number"])
            if number < self.manifest.fork_block:
                raise ValueError("Pool observation precedes its fork origin")
            raw_reads = {}

            def call(label: str, address: str, selector: str, types: list[str]) -> tuple:
                raw = client.eth.call(
                    {"to": Web3.to_checksum_address(address), "data": HexStr(selector)}, block_identifier=number
                )
                if len(raw) != 32 * len(types):
                    raise ValueError(f"Pool input measurement missing: {label}")
                raw_reads[label] = HexStr(Web3.to_hex(raw))
                return decode(types, raw)

            calldata = HexStr(Web3.to_hex(Web3.keccak(text="getPool(address,address,uint24)")[:4]))
            calldata = HexStr(calldata + encode(["address", "address", "uint24"], [weth, usdc, 500]).hex())
            pool = call("factory_pool", factory, calldata, ["address"])[0]
            if int(pool, 16) == 0:
                raise ValueError("Pool input factory returned an absent pool")
            token0 = call("token0", pool, "0x0dfe1681", ["address"])[0].lower()
            token1 = call("token1", pool, "0xd21220a7", ["address"])[0].lower()
            fee = call("fee", pool, "0xddca3f43", ["uint24"])[0]
            if (token0, token1, fee) != (weth, usdc, 500):
                raise ValueError("Pool input resource does not match WETH/USDC/500")
            decimals0 = call("decimals0", weth, "0x313ce567", ["uint8"])[0]
            decimals1 = call("decimals1", usdc, "0x313ce567", ["uint8"])[0]
            if (decimals0, decimals1) != (18, 6):
                raise ValueError("Pool input token decimals changed")
            slot = call(
                "slot0", pool, "0x3850c7bd", ["uint160", "int24", "uint16", "uint16", "uint16", "uint8", "bool"]
            )
            if not slot[0] or not slot[6]:
                raise ValueError("Pool input price is absent or pool is locked")
            if self._identity(client) != identity or client.eth.get_block(number)["hash"] != block["hash"]:
                raise ValueError("Pool input fork or observation block changed during capture")
            with localcontext() as arithmetic:
                arithmetic.prec = 78
                price = Decimal(slot[0] * slot[0] * 10**12) / Decimal(2**192)
            if requested in {"usdc", usdc}:
                price = Decimal(1)
            now = datetime.now(UTC)
            observation = {
                "schema_version": 1,
                "run_id": self.manifest.run_id,
                "fork_identity": identity,
                "preparation_sha256": self.manifest.preparation_sha256,
                "pool": pool.lower(),
                "token": token,
                "quote": quote,
                "price": str(price),
                "block_number": number,
                "block_hash": Web3.to_hex(block["hash"]),
                "block_timestamp": int(block["timestamp"]),
                "observed_at": now.isoformat(),
                "raw_reads": raw_reads,
                "usd_anchor": "USDC_USD_1_ASSUMPTION",
                "native_anchor": "ETH_WETH_1_ASSUMPTION",
                "source": "qa_fork_pool",
                "measurement_policy": {
                    "confidence": "0.9",
                    "confidence_basis": "fixed_experiment_setting_not_calibrated",
                    "stale": False,
                    "stale_basis": "uncached_pinned_fork_read",
                    "freshness_basis": "gateway_observation_to_consumption_time",
                },
            }
            raw = _canonical(observation)
            observation_id = hashlib.sha256(raw).hexdigest()
            _retain(self.evidence / f"{observation_id}.json", raw)
            return PriceResult(
                price=price,
                source="qa_fork_pool",
                timestamp=now,
                confidence=0.9,
                stale=False,
                source_details={"observation_id": observation_id},
            )
