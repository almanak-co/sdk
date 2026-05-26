"""VIB-3744 Anvil-fork repro: Aave V3 Linea WETH reserve-active pre-flight.

Exercises the production ``_fetch_reserve_config`` /
``assert_lending_reserve_active`` helpers against forked mainnet Linea state.
NO MOCKING of the data-provider; we hit the real bytecode on Anvil so any
address-table regression surfaces here before a strategy ever submits a tx.

Expected on-chain truth as of 2026-04-30 (verified by the implementer via
``cast call ... getReserveConfigurationData(WETH)``):

- Aave V3 Linea WETH reserve:
    isActive=True, isFrozen=True, ltv=0, usageAsCollateralEnabled=True,
    liquidationThreshold=8300, liquidationBonus=10600, reserveFactor=1500.

  Pre-flight MUST raise ``PoolReserveFrozenError`` for the WETH supply path,
  preventing the strategy from emitting a SUPPLY tx that would revert with
  the typed Aave V3 ``RESERVE_FROZEN`` (string ``"28"``) error and burn gas.

- Aave V3 Linea USDC reserve:
    isActive=True, isFrozen=False, ltv=7500, usageAsCollateralEnabled=True.

  Pre-flight MUST NOT raise — confirms the helper distinguishes the frozen
  asset from the rest of the market and is not over-broadly classifying the
  whole pool as frozen.

This module exercises **helper-only** Anvil-fork checks (it only invokes
``_fetch_reserve_config`` / ``assert_lending_reserve_active`` against a
forked node) and therefore lives under ``tests/integration/connectors/``,
mirroring the layout decision made for the Radiant V2 sibling test
(``test_radiant_v2_frozen_pool_preflight_anvil.py``) per CodeRabbit feedback
on PR #1971: ``tests/intents/`` is reserved for canonical intent-test
coverage that implements all four verification layers (compile / fixture /
execution / receipt).

Skipped when ``ANVIL_LINEA_PORT`` isn't set. For deterministic, governance-
proof runs, pin Anvil to the verified historical block::

    anvil --fork-url https://linea-rpc.publicnode.com \
          --fork-block-number 30447690 --port 18552 &
    ANVIL_LINEA_PORT=18552 uv run pytest \
        tests/integration/connectors/test_aave_v3_linea_frozen_pool_preflight_anvil.py -v -s

Why pinning matters: these tests assert ``isFrozen=True`` for WETH and
``isFrozen=False`` for USDC. If Aave Linea governance unfreezes WETH (or
freezes USDC) after the verified block, an unpinned fork at ``latest`` will
flip the assertions and the suite will go red even though
``assert_lending_reserve_active`` is still correct. The helper below enforces
the pinned block when ``ANVIL_LINEA_FORK_BLOCK`` is set, and warns (without
hard-failing) when the running fork is materially newer than the verified
state, so an operator running ``--fork-block-number latest`` gets a clear
signal rather than a confusing assertion mismatch.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any

import pytest
from web3 import Web3

from almanak.connectors._strategy_base.base.lending import aave_helpers as cl
from almanak.connectors._strategy_base.base.lending.aave_helpers import (
    PoolReserveFrozenError,
    assert_lending_reserve_active,
)

logger = logging.getLogger(__name__)

# Linea token addresses (verified against
# almanak/framework/intents/compiler_constants.py CHAIN_TOKENS["linea"]).
WETH_LINEA = "0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f"
USDC_LINEA = "0x176211869cA2b568f2A7D4EE941E073a821EE1ff"

# Linea block at which the on-chain reserve state was verified by the implementer
# (cast call ... getReserveConfigurationData(WETH/USDC) on 2026-04-30):
#   WETH: isFrozen=True, ltv=0
#   USDC: isFrozen=False, ltv=7500
# All assertions in this module are pinned to this block. If governance changes
# the WETH/USDC reserve config on Linea, re-verify and bump this constant.
LINEA_VERIFIED_FORK_BLOCK = 30447690

# Tolerance — number of blocks newer than the verified block before we warn.
# Linea avg block time ~2s, so 86400 blocks ≈ 48 hours of drift.
LINEA_FORK_BLOCK_DRIFT_WARN = 86400


@dataclass
class _RpcResponse:
    success: bool
    result: str
    error: str = ""


class _AnvilGatewayShim:
    """Minimal shim that translates ``gateway.rpc.Call(eth_call …)`` into a
    direct Web3 call. Lets the production ``_fetch_reserve_config`` code path
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
                success=False,
                result="",
                error=json.dumps({"code": 3, "message": f"execution reverted: {exc}"}),
            )
        return _RpcResponse(success=True, result=json.dumps("0x" + data.hex()))


def _anvil_url(chain: str) -> str | None:
    port = os.environ.get(f"ANVIL_{chain.upper()}_PORT")
    if not port:
        return None
    return f"http://localhost:{port}"


def _enforce_linea_fork_block_pin(web3: Web3) -> None:
    """Defensive guard against unpinned forks on Linea.

    When ``ANVIL_LINEA_FORK_BLOCK`` is set, hard-fail (skip with a clear
    message) if the running fork is at a different block. When it isn't set,
    warn loudly if the fork is materially newer than the verified state — the
    test will likely still pass at HEAD today, but the operator should know
    the run is not deterministic and that a future governance change will
    silently flip assertions.
    """
    fork_block = web3.eth.block_number
    pinned = os.environ.get("ANVIL_LINEA_FORK_BLOCK")
    if pinned:
        try:
            pinned_int = int(pinned)
        except ValueError:
            pytest.skip(f"ANVIL_LINEA_FORK_BLOCK={pinned!r} is not an integer")
        if fork_block != pinned_int:
            pytest.skip(
                f"Linea fork at block {fork_block}, expected pinned block {pinned_int} "
                "(set ANVIL_LINEA_FORK_BLOCK to the same value Anvil was started with, "
                f"or remove the env var to allow drift). Verified-state block is "
                f"{LINEA_VERIFIED_FORK_BLOCK}."
            )
        return
    drift = fork_block - LINEA_VERIFIED_FORK_BLOCK
    if drift > LINEA_FORK_BLOCK_DRIFT_WARN:
        logger.warning(
            "Linea fork at block %s is %s blocks newer than the verified state "
            "(block %s). If Aave Linea governance has changed WETH/USDC reserve "
            "config since then, the assertions in this module will go red even "
            "though ``assert_lending_reserve_active`` is still correct. Pin the "
            "fork via `--fork-block-number %s` for deterministic results.",
            fork_block,
            drift,
            LINEA_VERIFIED_FORK_BLOCK,
            LINEA_VERIFIED_FORK_BLOCK,
        )


def _build_compiler_against_anvil(chain: str) -> Any:
    """Build the minimum compiler object the pre-flight needs:
    ``chain``, ``rpc_timeout``, ``_gateway_client`` (with ``is_connected``
    and ``.rpc.Call``).
    """
    url = _anvil_url(chain)
    if url is None:
        pytest.skip(f"ANVIL_{chain.upper()}_PORT not set — skipping live-fork pre-flight test")
    web3 = Web3(Web3.HTTPProvider(url))
    if not web3.is_connected():
        pytest.skip(f"Anvil fork at {url} unreachable")

    if chain == "linea":
        _enforce_linea_fork_block_pin(web3)

    class _CompilerShim:
        chain = ""
        rpc_timeout = 10.0
        _gateway_client: Any = None

    shim = _CompilerShim()
    shim.chain = chain
    shim._gateway_client = _AnvilGatewayShim(web3)
    return shim


# =============================================================================
# Linea WETH: frozen reserve (governance-frozen, ltv=0). Pre-flight must raise.
# =============================================================================


@pytest.mark.linea
@pytest.mark.lending
def test_aave_v3_linea_weth_preflight_raises_on_frozen_reserve():
    """Headline VIB-3744 case: Aave V3 Linea WETH is frozen (isFrozen=True,
    ltv=0). Pre-flight MUST raise ``PoolReserveFrozenError`` so the strategy
    emits HOLD instead of submitting a SUPPLY that the on-chain Aave V3
    ``RESERVE_FROZEN`` guard reverts.
    """
    compiler = _build_compiler_against_anvil("linea")
    with pytest.raises(PoolReserveFrozenError) as exc_info:
        assert_lending_reserve_active(
            compiler,
            asset_address=WETH_LINEA,
            asset_symbol="WETH",
            protocol="aave_v3",
        )
    msg = str(exc_info.value)
    assert "aave_v3" in msg
    assert "linea" in msg
    assert "isFrozen=True" in msg


@pytest.mark.linea
@pytest.mark.lending
def test_aave_v3_linea_weth_preflight_caches_result():
    """A multi-iteration strategy run must pay the RPC tax exactly once."""
    compiler = _build_compiler_against_anvil("linea")
    shim = compiler._gateway_client
    original_call = shim.Call
    call_count = {"n": 0}

    def counted_call(request: Any, timeout: float = 5.0):
        call_count["n"] += 1
        return original_call(request, timeout=timeout)

    shim.Call = counted_call

    for _ in range(5):
        with pytest.raises(PoolReserveFrozenError):
            assert_lending_reserve_active(
                compiler,
                asset_address=WETH_LINEA,
                asset_symbol="WETH",
                protocol="aave_v3",
            )
    assert call_count["n"] == 1, "Cache must short-circuit subsequent calls"


@pytest.mark.linea
@pytest.mark.lending
def test_aave_v3_linea_weth_decoded_state_matches_onchain_truth():
    """ABI decode against live Linea fork state: pins the Linea entries in
    ``LENDING_POOL_DATA_PROVIDERS`` and ``CHAIN_TOKENS``. If either drifts,
    this test fails before any strategy ever submits a tx.

    Pinned values (2026-04-30):
        isActive=True, isFrozen=True, ltv=0, usageAsCollateralEnabled=True.
    """
    compiler = _build_compiler_against_anvil("linea")
    config = cl._fetch_reserve_config(
        compiler,
        WETH_LINEA,
        "WETH",
        protocol="aave_v3",
        pre_flight_label="anvil-vib3744",
    )
    assert config is not None, "Live data-provider call should succeed for Linea WETH"
    assert config.is_active is True
    assert config.is_frozen is True
    assert config.ltv == 0
    # Sanity-check that the ABI decode parsed all 10 words: a regression that
    # truncated the response would leave usage_as_collateral_enabled at False.
    assert config.usage_as_collateral_enabled is True


# =============================================================================
# Linea USDC: healthy reserve. Pre-flight must NOT raise.
# =============================================================================


@pytest.mark.linea
@pytest.mark.lending
def test_aave_v3_linea_usdc_preflight_passes_on_active_reserve():
    """Healthy reserve guard: confirms the helper distinguishes the frozen
    WETH asset from the rest of the market. Without this, a regression that
    flagged every Linea reserve as frozen would silently strand the strategy.
    """
    compiler = _build_compiler_against_anvil("linea")
    # No exception → pre-flight green-lit the SUPPLY path.
    assert_lending_reserve_active(
        compiler,
        asset_address=USDC_LINEA,
        asset_symbol="USDC",
        protocol="aave_v3",
    )

    # Sanity-check that the cache recorded "no failure" so subsequent iters
    # don't re-hit the gateway. Important: assert the key *exists* in the
    # cache before checking its value — ``dict.get(key)`` returns ``None``
    # for both "cached active reserve" and "cache miss", so a regression
    # that stops caching successful lookups would still pass under
    # ``cache.get(key) is None``.
    cache = compiler._lending_reserve_active_cache
    cache_key = ("linea", "aave_v3", USDC_LINEA.lower())
    assert cache_key in cache, "Active-reserve lookup must be cached so subsequent iters short-circuit"
    assert cache[cache_key] is None  # None = "active and unfrozen"


@pytest.mark.linea
@pytest.mark.lending
def test_aave_v3_linea_usdc_decoded_state_matches_onchain_truth():
    """Pin the live USDC reserve config so a config drift surfaces here."""
    compiler = _build_compiler_against_anvil("linea")
    config = cl._fetch_reserve_config(
        compiler,
        USDC_LINEA,
        "USDC",
        protocol="aave_v3",
        pre_flight_label="anvil-vib3744-usdc",
    )
    assert config is not None
    assert config.is_active is True
    assert config.is_frozen is False
    assert config.ltv > 0
    assert config.usage_as_collateral_enabled is True
