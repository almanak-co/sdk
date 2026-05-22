"""VIB-3749 Anvil-fork repro: Radiant V2 reserve-active pre-flight.

Exercises the production `_fetch_reserve_config` / `assert_lending_reserve_active`
helpers against forked mainnet state on Ethereum (the only chain where the
framework still routes Radiant V2 traffic). NO MOCKING of the data-provider;
we hit the real bytecode on Anvil so any address-table regression surfaces here.

Expected on-chain truth (verified by the implementer):

- Ethereum Radiant V2:
    Pool 0xA950...ED07 is active. WETH reserve is unfrozen
    (isActive=1, isFrozen=0). Pre-flight must NOT raise; strategy compiles
    and emits SUPPLY normally.

The Arbitrum Radiant V2 deployment is no longer exercised: its LendingPool
proxy at ``0xF4B1486DD74D07706052A33d31d7c0AAFD0659E1`` was reduced to a stub
implementation after the October 2024 attack and the framework now excludes
``radiant_v2`` from arbitrum entirely. The compile-time supply path returns
"not available on chain" before any reserve-active RPC happens.
See issues #1842 / #1847 / #1889 and the unit-test regression guards in
``tests/unit/connectors/test_radiant_v2.py`` and
``tests/unit/permissions/test_synthetic_intents.py``.

This module exercises **helper-only** Anvil-fork checks (it only invokes
``_fetch_reserve_config`` / ``assert_lending_reserve_active`` against a
forked node) and therefore lives under ``tests/integration/connectors/``
rather than ``tests/intents/`` — which is reserved for canonical
intent-test coverage that implements all four verification layers
(compile / fixture / execution / receipt).

Skipped when ``ANVIL_ETHEREUM_PORT`` isn't set.
Run with::

    ANVIL_ETHEREUM_PORT=8545 \
        uv run pytest tests/integration/connectors/test_radiant_v2_frozen_pool_preflight_anvil.py -v -s
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import pytest
from web3 import Web3

from almanak.framework.connectors.base.lending import aave_helpers as cl
from almanak.framework.connectors.base.lending.aave_helpers import assert_lending_reserve_active

# Token addresses (mainnet — same on the Anvil forks).
WETH_ETHEREUM = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"


@dataclass
class _RpcResponse:
    success: bool
    result: str
    error: str = ""


class _AnvilGatewayShim:
    """Minimal shim that translates `gateway.rpc.Call(eth_call …)` into a
    direct Web3 call. Lets the production `_fetch_reserve_config` code path
    run end-to-end against Anvil-forked state with zero gateway involvement.
    """

    def __init__(self, web3: Web3) -> None:
        self._web3 = web3
        self.is_connected = True
        self.rpc = self  # `gateway.rpc.Call(...)` is what the helper calls

    def Call(self, request: Any, timeout: float = 5.0) -> _RpcResponse:  # noqa: N802 — gRPC API name
        params = json.loads(request.params)
        call_obj, block = params[0], params[1]
        try:
            data = self._web3.eth.call(
                {"to": Web3.to_checksum_address(call_obj["to"]), "data": call_obj["data"]},
                block_identifier=block if isinstance(block, str) else int(block, 16),
            )
        except Exception as exc:  # ContractLogicError, etc.
            return _RpcResponse(
                success=False, result="", error=json.dumps({"code": 3, "message": f"execution reverted: {exc}"})
            )
        return _RpcResponse(success=True, result=json.dumps("0x" + data.hex()))


def _anvil_url(chain: str) -> str | None:
    port = os.environ.get(f"ANVIL_{chain.upper()}_PORT")
    if not port:
        return None
    return f"http://localhost:{port}"


def _build_compiler_against_anvil(chain: str) -> Any:
    """Build the minimum compiler object the pre-flight needs:
    `chain`, `rpc_timeout`, `_gateway_client` (with `is_connected` and `.rpc.Call`).
    """
    url = _anvil_url(chain)
    if url is None:
        pytest.skip(f"ANVIL_{chain.upper()}_PORT not set — skipping live-fork pre-flight test")
    web3 = Web3(Web3.HTTPProvider(url))
    if not web3.is_connected():
        pytest.skip(f"Anvil fork at {url} unreachable")

    # Compiler-shaped object — the helper only reads .chain, .rpc_timeout,
    # ._gateway_client, and stashes caches under `_lending_reserve_active_cache`.
    class _CompilerShim:
        chain = ""  # set below
        rpc_timeout = 10.0
        _gateway_client: Any = None

    shim = _CompilerShim()
    shim.chain = chain
    shim._gateway_client = _AnvilGatewayShim(web3)
    return shim


# =============================================================================
# Ethereum: pool is active — pre-flight must NOT raise.
# =============================================================================


@pytest.mark.ethereum
@pytest.mark.lending
def test_radiant_v2_ethereum_preflight_passes_on_active_pool():
    """Healthy reserve: the helper must return cleanly so the strategy
    proceeds to compile and submit a real SUPPLY.
    """
    compiler = _build_compiler_against_anvil("ethereum")
    # No exception → pre-flight green-lit the SUPPLY path.
    assert_lending_reserve_active(
        compiler,
        asset_address=WETH_ETHEREUM,
        asset_symbol="WETH",
        protocol="radiant_v2",
    )

    # Sanity-check that the cache recorded "no failure" so subsequent iters
    # don't re-hit the gateway (mirrors VIB-3701 cache semantics).
    # Important: assert the key *exists* in the cache before checking its
    # value — ``dict.get(key)`` returns ``None`` for both "cached active
    # reserve" and "cache miss", so a regression that stops caching successful
    # lookups would still pass under ``cache.get(key) is None``.
    cache = compiler._lending_reserve_active_cache
    cache_key = ("ethereum", "radiant_v2", WETH_ETHEREUM.lower())
    assert cache_key in cache, "Active-reserve lookup must be cached so subsequent iters short-circuit"
    assert cache[cache_key] is None  # None = "active and unfrozen"


@pytest.mark.ethereum
@pytest.mark.lending
def test_radiant_v2_ethereum_pool_data_provider_decodes_correctly():
    """Sanity: end-to-end ABI decode against live mainnet state.

    Pins the address-table entries: if the Ethereum Radiant V2
    AaveProtocolDataProvider address ever drifts, this test fails before any
    strategy ever submits a tx. Also verifies the WETH reserve is live.
    """
    compiler = _build_compiler_against_anvil("ethereum")
    config = cl._fetch_reserve_config(
        compiler,
        WETH_ETHEREUM,
        "WETH",
        protocol="radiant_v2",
        pre_flight_label="anvil",
    )
    assert config is not None, "Live data-provider call should succeed for Ethereum WETH"
    assert config.is_active is True
    assert config.is_frozen is False
    assert config.ltv > 0  # Pool is configured for borrowing
