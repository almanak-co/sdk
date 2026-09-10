"""Pinned dependency observations for the reproduced Doppler/Rehype swap family."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from eth_abi import decode, encode
from eth_utils import keccak

from almanak.framework.venues import VenueTargetRole, VenueVerificationGateway

from .addresses import UNISWAP_V4
from .doppler import _ROBINHOOD_INITIALIZER_RUNTIME
from .pool_key import PoolKey
from .venue_verifier import address_ref

_REHYPE_RUNTIME = "0x5d33a1d867ba0d17cc7af077786b1356848c72f8e0bf960ef88aa15f7a6962d1"
_VIEW_QUOTER_RUNTIME = "0x6f47a0e49bab7e7d8a7feb99a3a1e470089b7cc920545cfdf8dc0a6fa34fe8cb"
_STATE_TYPES = ["address", "uint256", "address", "bytes", "uint8", "(address,address,uint24,int24,address)", "int24"]


@dataclass(frozen=True, slots=True)
class DopplerSwapDependencies:
    """Administrative continuity only; this is not executable swap admission."""

    digest: str
    block_number: int
    block_hash: str
    nested_hook: str
    fee_schedule: tuple[int, int, int, int]


class _PinnedReader:
    def __init__(self, gateway: VenueVerificationGateway, chain: str, block_number: int):
        self.gateway = gateway
        self.chain = chain
        self.block_number = block_number

    def read(self, target: str, signature: str, inputs: list[str], args: list[Any], outputs: list[str]) -> tuple:
        raw = self.gateway.read(
            chain=self.chain,
            target=address_ref(VenueTargetRole.PERMISSION_TARGET, target),
            payload=keccak(text=signature)[:4] + encode(inputs, args),
            block_number=self.block_number,
        )
        result = decode(outputs, raw)
        if encode(outputs, result) != raw:
            raise ValueError("Doppler dependency returned noncanonical ABI data")
        return result

    def runtime(self, target: str, expected: str) -> None:
        code = self.gateway.code(
            chain=self.chain,
            target=address_ref(VenueTargetRole.PERMISSION_TARGET, target),
            block_number=self.block_number,
        )
        if "0x" + keccak(code).hex() != expected:
            raise ValueError("Doppler dependency runtime is not qualified")


def observe_doppler_swap_dependencies(
    *, chain: str, key: PoolKey, gateway: VenueVerificationGateway, block_number: int
) -> DopplerSwapDependencies:
    """Bind mutable routing and fee policy without freezing ordinary market state."""
    if chain != "robinhood" or int(key.hooks, 16) & 0x3FFF != 0x2544 or not key.is_dynamic:
        raise ValueError("Pool is outside the reproduced Doppler dynamic swap family")
    reader = _PinnedReader(gateway, chain, block_number)
    reader.runtime(key.hooks, _ROBINHOOD_INITIALIZER_RUNTIME)
    manager = UNISWAP_V4[chain]["pool_manager"].lower()
    if reader.read(key.hooks, "poolManager()", [], [], ["address"])[0] != manager:
        raise ValueError("Doppler initializer targets a different PoolManager")
    expected_key = tuple(key.to_wire().values())
    matches = []
    for currency in (key.currency0, key.currency1):
        state = reader.read(key.hooks, "getState(address)", ["address"], [currency], _STATE_TYPES)
        if state[5] == expected_key:
            matches.append((currency, state))
    if len(matches) != 1:
        raise ValueError("Doppler asset state does not uniquely bind the full PoolKey")
    asset, state = matches[0]
    numeraire, _, nested, graduation_data, status, _, far_tick = state
    if {asset, numeraire} != {key.currency0, key.currency1} or status not in (1, 2):
        raise ValueError("Doppler pool is not an initialized trading pair")
    reader.runtime(nested, _REHYPE_RUNTIME)
    enabled = reader.read(key.hooks, "isDopplerHookEnabled(address)", ["address"], [nested], ["uint256"])[0]
    if enabled & 2 == 0 or enabled & ~7:
        raise ValueError("Doppler nested swap callback flags are not qualified")
    if reader.read(nested, "INITIALIZER()", [], [], ["address"])[0] != key.hooks:
        raise ValueError("Rehype dependency targets a different initializer")
    if reader.read(nested, "poolManager()", [], [], ["address"])[0] != manager:
        raise ValueError("Rehype dependency targets a different PoolManager")
    view_quoter = reader.read(nested, "quoter()", [], [], ["address"])[0]
    reader.runtime(view_quoter, _VIEW_QUOTER_RUNTIME)
    if reader.read(view_quoter, "poolManager()", [], [], ["address"])[0] != manager:
        raise ValueError("Rehype view quoter targets a different PoolManager")
    pool_id = bytes.fromhex(key.pool_id[2:])
    info = reader.read(nested, "getPoolInfo(bytes32)", ["bytes32"], [pool_id], ["address"] * 3)
    if info[:2] != (asset, numeraire):
        raise ValueError("Rehype pool assets conflict with the initializer")
    schedule = reader.read(
        nested, "getFeeSchedule(bytes32)", ["bytes32"], [pool_id], ["uint32", "uint24", "uint24", "uint24", "uint32"]
    )
    start_time, start_fee, end_fee, last_fee, duration = schedule
    if not 0 <= end_fee <= last_fee <= start_fee <= 800_000 or (start_fee != end_fee and duration == 0):
        raise ValueError("Rehype fee schedule is not valid")
    mode = reader.read(nested, "getFeeRoutingMode(bytes32)", ["bytes32"], [pool_id], ["uint8"])[0]
    weights = reader.read(nested, "getFeeDistributionInfo(bytes32)", ["bytes32"], [pool_id], ["uint256"] * 8)
    if mode not in (0, 1) or sum(weights[:4]) != 10**18 or sum(weights[4:]) != 10**18:
        raise ValueError("Rehype fee distribution is not valid")
    administrative_schedule = (start_time, start_fee, end_fee, duration)
    # lastFee decays during ordinary swaps; input caps and output minima bound price changes.
    policy = {
        "schema": 1,
        "chain": chain,
        "pool_key": key.to_wire(),
        "runtime": _ROBINHOOD_INITIALIZER_RUNTIME,
        "manager": manager,
        "asset": asset,
        "numeraire": numeraire,
        "nested": nested,
        "nested_runtime": _REHYPE_RUNTIME,
        "enabled": enabled,
        "view_quoter": view_quoter,
        "view_quoter_runtime": _VIEW_QUOTER_RUNTIME,
        "status": status,
        "graduation_data": graduation_data.hex(),
        "far_tick": far_tick,
        "beneficiary": info[2],
        "schedule": administrative_schedule,
        "routing_mode": mode,
        "distribution": weights,
    }
    return DopplerSwapDependencies(
        digest="0x" + keccak(text=json.dumps(policy, sort_keys=True, separators=(",", ":"))).hex(),
        block_number=block_number,
        block_hash=gateway.block_hash(chain=chain, block_number=block_number),
        nested_hook=nested,
        fee_schedule=administrative_schedule,
    )
