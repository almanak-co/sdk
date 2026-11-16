"""Reviewable generated authority, partitioned by the full account execution scope."""

import hashlib
from typing import Literal

from pydantic import Field

from almanak.gateway.accounts import HexData
from almanak.gateway.roles_conditions import FunctionGrant
from almanak.models import Address, Block, Hash32, Model, SafeRoles, UInt


class ContractIdentity(Model):
    address: Address
    code_hash: Hash32
    implementation: Address | None = None
    implementation_hash: Hash32 | None = None
    implementation_slot: UInt | None = None
    # Omit absent beacon fields so existing retained manifest digests stay stable.
    beacon: Address | None = Field(default=None, exclude_if=lambda value: value is None)
    beacon_hash: Hash32 | None = Field(default=None, exclude_if=lambda value: value is None)


class OwnerTransaction(Model):
    to: Address
    value: Literal["0"] = "0"
    data: HexData


class PermissionPartition(Model):
    account: str
    profile: SafeRoles
    grants: tuple[FunctionGrant, ...]
    contracts: tuple[ContractIdentity, ...]
    observation: Block
    transactions: tuple[OwnerTransaction, ...]


class PermissionManifest[Capabilities: Model](Model):
    compiler: Literal["almanak-v3-zodiac-2.1.0-v1"] = "almanak-v3-zodiac-2.1.0-v1"
    safe_version: Literal["1.4.1"] = "1.4.1"
    roles_version: Literal["2.1.0"] = "2.1.0"
    source_hash: str
    configuration_hash: str
    capabilities: Capabilities
    partitions: tuple[PermissionPartition, ...]
    runtime_checks: tuple[str, ...]

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()
