"""Shared fixtures and helpers for Intent tests.

This module provides common infrastructure for all per-chain Intent tests:
- Chain configuration (tokens, balance slots, RPC URLs)
- Anvil auto-management (start/stop per test session)
- Wallet funding utilities
- Token balance helpers
- Web3 connection management
- Price oracle with CoinGecko
"""

import inspect
import os
import sqlite3
import time
import weakref
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
import requests
from web3 import Web3
from web3.exceptions import TimeExhausted
from web3.providers.rpc.async_rpc import AsyncHTTPProvider

from almanak.connectors.uniswap_v3.slot0_fallback import (
    enrich_lp_close_with_slot0,
    enrich_lp_open_with_slot0,
)
from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.lp_accounting import _get_pool_address
from almanak.framework.accounting.processor import AccountingProcessor, write_outbox_entry
from almanak.framework.observability.ledger import build_ledger_entry
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore

# =============================================================================
# Test Timeouts (Fail Fast)
# =============================================================================

# Local Anvil RPC calls should return quickly; when they don't, the fork is usually stalled.
# Keep these defaults aggressive to avoid 10+ minute cascades when an Anvil instance hangs.
# Read timeout was bumped 10s -> 30s (#1738) to absorb the legitimate long calls the
# ethereum fork makes when executing LP close action bundles (eth_call heavy path);
# connect timeout stays tight because connect-stalls genuinely indicate a dead fork.
TEST_RPC_CONNECT_TIMEOUT_SECONDS = float(os.environ.get("ALMANAK_TEST_RPC_CONNECT_TIMEOUT_SECONDS", "3"))
TEST_RPC_READ_TIMEOUT_SECONDS = float(os.environ.get("ALMANAK_TEST_RPC_READ_TIMEOUT_SECONDS", "30"))
TEST_WEB3_DEFAULT_HTTP_TIMEOUT_SECONDS = float(os.environ.get("ALMANAK_TEST_WEB3_HTTP_TIMEOUT_SECONDS", "30"))
TEST_CAST_TIMEOUT_SECONDS = float(os.environ.get("ALMANAK_TEST_CAST_TIMEOUT_SECONDS", "15"))

# ExecutionOrchestrator / Submitter confirmation timeout (upper bound for receipt polling).
TEST_TX_TIMEOUT_SECONDS = float(os.environ.get("ALMANAK_TEST_TX_TIMEOUT_SECONDS", "30"))

# When local Anvil stalls, retries just waste time. Keep to 0 by default for intent tests.
TEST_SUBMITTER_MAX_RETRIES = int(os.environ.get("ALMANAK_TEST_SUBMITTER_MAX_RETRIES", "0"))

# requests-style (connect, read) timeouts for sync Web3 HTTPProvider
TEST_WEB3_REQUEST_TIMEOUT = (TEST_RPC_CONNECT_TIMEOUT_SECONDS, TEST_RPC_READ_TIMEOUT_SECONDS)

# Slow-fork read timeout: bumped 30s -> 60s. Some Anvil forks legitimately need
# >30s reads under CI load. Polygon consistently hit ReadTimeoutError at the 30s
# default set by #1738 (#1804, PR #1798 run 24793717120); and after the weekly
# fork-block roll the cold RPC-proxy cache makes the FIRST eth_call / evm_revert
# on a chain re-fetch state from a slow upstream — that timed out arbitrum +
# mantle's pristine-reset (30s) and killed the job on the W24 roll (2026-06-08).
# Scoped to TEST_SLOW_FORK_CHAINS; other chains keep the aggressive 30s default so
# a genuinely-stalled fork still surfaces quickly. The legacy
# ALMANAK_TEST_POLYGON_RPC_READ_TIMEOUT_SECONDS env override is still honoured.
TEST_SLOW_FORK_RPC_READ_TIMEOUT_SECONDS = float(
    os.environ.get(
        "ALMANAK_TEST_SLOW_FORK_RPC_READ_TIMEOUT_SECONDS",
        os.environ.get("ALMANAK_TEST_POLYGON_RPC_READ_TIMEOUT_SECONDS", "60"),
    )
)
TEST_SLOW_FORK_WEB3_REQUEST_TIMEOUT = (
    TEST_RPC_CONNECT_TIMEOUT_SECONDS,
    TEST_SLOW_FORK_RPC_READ_TIMEOUT_SECONDS,
)
# Chains whose Anvil fork needs the bumped read timeout above.
TEST_SLOW_FORK_CHAINS = frozenset({"polygon", "arbitrum", "mantle"})

# Back-compat aliases — polygon/conftest.py imports these names directly.
TEST_POLYGON_RPC_READ_TIMEOUT_SECONDS = TEST_SLOW_FORK_RPC_READ_TIMEOUT_SECONDS
TEST_POLYGON_WEB3_REQUEST_TIMEOUT = TEST_SLOW_FORK_WEB3_REQUEST_TIMEOUT


def web3_request_timeout(chain_name: str) -> tuple[float, float]:
    """Return the (connect, read) HTTP timeout for a chain's intent-test web3.

    Slow-fork chains (``TEST_SLOW_FORK_CHAINS``) get the bumped 60s read timeout so
    the first eth_call / evm_revert after a cold RPC-proxy cache (the weekly fork
    roll) can complete instead of timing out; every other chain keeps the
    aggressive 30s default that surfaces a stalled fork quickly.
    """
    if chain_name in TEST_SLOW_FORK_CHAINS:
        return TEST_SLOW_FORK_WEB3_REQUEST_TIMEOUT
    return TEST_WEB3_REQUEST_TIMEOUT


# Retry config for Anvil RPC calls during wallet funding.
# Only applies to setup-time RPC calls (anvil_setBalance, anvil_setStorageAt, evm_mine),
# NOT to test-time execution. Zero overhead on happy path.
TEST_FUNDING_RPC_MAX_RETRIES = int(os.environ.get("ALMANAK_TEST_FUNDING_RPC_MAX_RETRIES", "3"))
TEST_FUNDING_RPC_BACKOFF_SECONDS = float(os.environ.get("ALMANAK_TEST_FUNDING_RPC_BACKOFF_SECONDS", "2.0"))

# Health check timeout for recovery path (generous, since the fork is already degraded).
TEST_RECOVERY_HEALTH_TIMEOUT_SECONDS = float(os.environ.get("ALMANAK_TEST_RECOVERY_HEALTH_TIMEOUT_SECONDS", "15.0"))

# Fixed Anvil recovery policy for intent tests
TEST_ANVIL_RECOVERY_MAX_RESTARTS = 2
TEST_ANVIL_RECOVERY_SETTLE_SECONDS = 0.5
TEST_ANVIL_RECOVERY_PROBE_TIMEOUT_SECONDS = 3.0
TEST_ANVIL_PROBE_SENTINEL_WALLET = "0x000000000000000000000000000000000000dEaD"

# =============================================================================
# Constants
# =============================================================================

# Default max slippage for swap intent tests (20%).
# High tolerance because CoinGecko oracle prices can diverge from on-chain pool prices.
SWAP_MAX_SLIPPAGE = Decimal("0.20")

# Default Anvil port
ANVIL_PORT = 8545
ANVIL_URL = f"http://localhost:{ANVIL_PORT}"


@dataclass(frozen=True)
class Layer5AccountingHarness:
    """Per-test Layer-5 accounting persistence harness."""

    db_path: Path
    store: SQLiteStore
    basis_store: FIFOBasisStore
    processor: AccountingProcessor


@dataclass(frozen=True)
class Layer5Persisted:
    """Test-owned accounting persistence result."""

    ledger_entry_id: str
    outbox_id: str | None
    drained: Any = None


@dataclass(frozen=True)
class AnvilEthCallAdapter:
    """Test-scoped gateway-shaped eth_call adapter backed by the Anvil Web3."""

    web3: Web3
    # VIB-4483: the V4 PoolKey whose getSlot0 this adapter reads for
    # ``query_v4_position_state`` (native-pool tests set it via
    # ``with_v4_pool_key``). ``None`` ⇒ no V4 position-state read available.
    _v4_pool_key: Any = None

    def with_v4_pool_key(self, pool_key: Any) -> "AnvilEthCallAdapter":
        """Return a copy of this adapter bound to ``pool_key`` for getSlot0 reads."""
        import dataclasses

        return dataclasses.replace(self, _v4_pool_key=pool_key)

    def eth_call(self, chain: str, to: str, data: str) -> str | None:
        del chain
        result = self.web3.eth.call(
            {
                "to": Web3.to_checksum_address(to),
                "data": data,
            }
        )
        return Web3.to_hex(result)

    def query_native_balance(self, chain: str, wallet_address: str, block: int | str | None = None) -> int | None:
        """Gateway-shaped block-pinned native balance, backed by the Anvil Web3.

        VIB-5121 — mirrors ``GatewayClient.query_native_balance`` so the intent
        test can exercise the production ``StrategyRunner._capture_native_lp_*``
        balance-bracket capture end-to-end over the fork. Block-tag handling
        mirrors the production client exactly (reject bool / negative int; pass a
        str tag through; ``None`` → ``"latest"``) so the harness cannot mask a
        real validation difference in the capture path; a backend read failure
        returns ``None`` (the production degraded contract), never raises.
        """
        del chain
        if isinstance(block, bool):
            raise ValueError(f"query_native_balance block must not be bool, got {block!r}")
        if isinstance(block, int):
            if block < 0:
                raise ValueError(f"query_native_balance block must be non-negative, got {block}")
            block_id: int | str = block
        elif block is None:
            block_id = "latest"
        else:
            block_id = block
        try:
            return int(self.web3.eth.get_balance(Web3.to_checksum_address(wallet_address), block_identifier=block_id))
        except Exception:  # noqa: BLE001 — gateway-shaped degraded contract: read failure → None
            return None

    def query_v4_position_state(
        self,
        *,
        chain: str,
        position_manager: str,
        state_view: str,
        token_id: int,
    ) -> "V4PositionState | None":
        """Gateway-shaped V4 position-state read, backed by raw eth_call (VIB-4483).

        This is the gateway INTERFACE (same signature + return type as
        ``GatewayClient.query_v4_position_state``) implemented over the test
        Anvil fork's ``eth_call`` — NOT a re-implementation of the gateway's
        position-state logic. It composes the connector's EXISTING encoders /
        decoders:

        * ``StateView.getSlot0(bytes32 poolId)`` via the VIB-5038-fixed
          ``build_get_slot0_calldata`` / ``decode_slot0_response`` →
          ``sqrt_price_x96`` + ``current_tick``.
        * ``PositionManager.getPositionLiquidity(uint256)`` (selector
          ``0x1efeed33``) → ``liquidity``.

        Returns a :class:`V4PositionState` carrying those live fields, including
        the position's REAL ``tick_lower`` / ``tick_upper`` decoded from
        ``PositionManager.getPoolAndPositionInfo(tokenId)`` (selector ``0x7ba03aad``)
        — the SAME read + decoder the production gateway uses
        (``rpc_service._decode_v4_pool_and_position_info``). The VIB-4483 OPEN
        capture sources ticks from the mint receipt's ``LPOpenData`` so it would
        tolerate placeholders, but the VIB-5117 CLOSE native-PRINCIPAL capture
        derives amounts from the position's tick RANGE off this state read alone
        (the pre-burn read predates ``LPCloseData``). A degenerate
        ``tick_lower == tick_upper`` placeholder would collapse the native
        principal to a measured ``0`` (silent understatement); the real bounds keep
        the derived proceeds exact. Returns ``None`` on any missing/failed read so
        the capture leaves the native leg unmeasured (Empty ≠ Zero), never
        fabricating a zero.

        The PoolKey for ``getSlot0`` is reconstructed from the V4 LP pool this
        suite exercises — see ``v4_pool_key`` below; tests that use a different
        V4 pool override it via the fixture.
        """
        from almanak.connectors.uniswap_v4.hooks import (
            build_get_slot0_calldata,
            decode_slot0_response,
        )
        from almanak.framework.gateway_client import V4PositionState

        pool_key = getattr(self, "_v4_pool_key", None)
        if pool_key is None:
            return None

        # getSlot0(bytes32 poolId) — VIB-5038-fixed selector via the connector encoder.
        try:
            slot0_hex = self.eth_call(chain, state_view, build_get_slot0_calldata(pool_key))
        except Exception:
            return None
        if not slot0_hex:
            return None
        pool_state = decode_slot0_response(slot0_hex)
        if not pool_state.exists or pool_state.sqrt_price_x96 <= 0:
            return None

        # getPositionLiquidity(uint256) selector 0x1efeed33.
        liq_calldata = "0x1efeed33" + format(int(token_id), "064x")
        try:
            liq_hex = self.eth_call(chain, position_manager, liq_calldata)
        except Exception:
            return None
        if not liq_hex:
            return None
        try:
            liquidity = int(liq_hex, 16)
        except (TypeError, ValueError):
            return None

        # getPoolAndPositionInfo(uint256) selector 0x7ba03aad → decode the
        # position's REAL tick_lower/tick_upper. Reuse the production gateway's
        # packed-PositionInfo decoder so the int24 bit layout is single-sourced
        # (never duplicated/drifting). The VIB-5117 close native-principal capture
        # derives proceeds from this tick RANGE, so a degenerate placeholder range
        # would silently zero the native leg. Fail closed (None) on a missing/
        # malformed read rather than fall back to a degenerate range.
        from almanak.gateway.services.rpc_service import _decode_v4_pool_and_position_info

        try:
            info_hex = self.eth_call(chain, position_manager, "0x7ba03aad" + format(int(token_id), "064x"))
        except Exception:
            return None
        if not info_hex:
            return None
        try:
            _pool_key_words, tick_lower, tick_upper, _pool_id = _decode_v4_pool_and_position_info(info_hex)
        except (ValueError, TypeError):
            return None

        return V4PositionState(
            liquidity=liquidity,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            current_tick=pool_state.tick,
            sqrt_price_x96=pool_state.sqrt_price_x96,
            pool_id="",
        )


@pytest.fixture
def anvil_eth_call_adapter(web3: Web3) -> AnvilEthCallAdapter:
    return AnvilEthCallAdapter(web3)


def _reset_sqlite_file(db_path: Path) -> None:
    """Drop all SDK-owned tables so setup recreates a clean schema in-place."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        tables = [
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        ]
        for table in tables:
            conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.commit()
    finally:
        conn.close()


@pytest_asyncio.fixture
async def layer5_accounting_harness(
    tmp_path_factory: pytest.TempPathFactory, worker_id: str
) -> Layer5AccountingHarness:
    """Throwaway accounting SQLite for Layer-5 intent-test assertions.

    The path is keyed by xdist worker and reset in setup. We intentionally do
    not clean up after the test so a failing run leaves the DB for post-mortem.
    """
    worker = worker_id or "master"
    db_dir = tmp_path_factory.getbasetemp() / f"layer5-accounting-{worker}"
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / "accounting.sqlite"
    _reset_sqlite_file(db_path)

    store = SQLiteStore(SQLiteConfig(db_path=str(db_path)))
    await store.initialize()
    basis_store = FIFOBasisStore()
    processor = AccountingProcessor(store, basis_store, deployment_id="layer5-intent-test")
    harness = Layer5AccountingHarness(
        db_path=db_path,
        store=store,
        basis_store=basis_store,
        processor=processor,
    )
    try:
        yield harness
    finally:
        await store.close()


def _intent_type_str(intent: Any) -> str:
    intent_type = getattr(intent, "intent_type", None)
    if intent_type is None:
        return ""
    return intent_type.value if hasattr(intent_type, "value") else str(intent_type)


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _default_compute_position_key(
    intent: Any, *, chain: str, wallet_address: str, resolved_pool: str | None = None
) -> tuple[str, str]:
    intent_type = _intent_type_str(intent)
    protocol = (getattr(intent, "protocol", "") or "").lower()
    if intent_type in {"LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"} and "pendle" not in protocol:
        # VIB-3946: mirror the runner — prefer the compiler-resolved canonical pool
        # label (action_bundle.metadata["pool_name"]) so a Curve asset-set intent
        # keys off "3pool" instead of the raw "USDT/USDC/DAI" string.
        pool_address = _get_pool_address(intent, resolved_pool)
        return f"lp:{protocol}:{chain.lower()}:{wallet_address.lower()}:{pool_address}", pool_address
    if intent_type == "SWAP":
        return f"swap:{chain.lower()}:{wallet_address.lower()}", ""
    # Lending (SUPPLY / BORROW / REPAY / DELEVERAGE / WITHDRAW): mirror the runner's
    # _compute_outbox_position_key so per-market protocols (Morpho Blue, fluid_vault)
    # derive the canonical market-scoped key and the vault segment is not dropped.
    if intent_type in {"SUPPLY", "BORROW", "REPAY", "DELEVERAGE", "WITHDRAW"}:
        from almanak.framework.accounting.lending_accounting import (
            _derive_position_key,
            _intent_asset,
            _intent_market_id,
        )

        market_id = _intent_market_id(intent) or ""
        asset = _intent_asset(intent)
        position_key = _derive_position_key(protocol, chain, wallet_address, market_id or None, asset)
        return position_key, market_id
    return "", ""


def _capture_v4_lp_open_native_for_intent_test(
    *,
    intent: Any,
    chain: str,
    result: Any,
    eth_call_reader: Any | None,
) -> tuple[int | None, int | None] | None:
    """Layer-5 mirror of the runner's VIB-4483 native-amount capture.

    Delegates to the SAME ``StrategyRunner._capture_v4_lp_open_native_amounts_safe``
    static method the production runner uses (no logic duplication), passing the
    test eth_call adapter as the ``gateway_client``. The adapter implements the
    gateway ``query_v4_position_state`` interface over the Anvil fork, so this
    exercises the real stamp path. ``None`` (gateway-less harness / non-native /
    failed read) leaves the native leg unmeasured (Empty ≠ Zero).
    """
    if eth_call_reader is None:
        return None
    from almanak.framework.runner.strategy_runner import StrategyRunner

    return StrategyRunner._capture_v4_lp_open_native_amounts_safe(
        intent=intent,
        chain=chain,
        result=result,
        gateway_client=eth_call_reader,
    )


def capture_v4_lp_close_native_principal(
    *,
    intent: Any,
    chain: str,
    eth_call_reader: Any | None,
) -> tuple[int | None, int | None] | None:
    """Layer-5 mirror of the runner's VIB-5117 PRE-burn native-principal capture.

    The native-ETH leg of a V4 close is returned via ``TAKE_PAIR`` (no ERC-20
    Transfer), so the burn receipt leaves ``LPCloseData.amount{0,1}_collected =
    None`` (Empty ≠ Zero) and the runner fills it from a PRE-burn
    ``QueryV4PositionState`` read. This delegates to the SAME
    ``StrategyRunner._capture_v4_lp_close_native_principal_safe`` static method the
    production runner uses (no logic duplication), passing the test eth_call
    adapter as the ``gateway_client``.

    **Tests MUST call this BEFORE executing the close** — a post-burn read returns
    zero liquidity → zero principal. The captured pair is then threaded into
    :func:`assert_accounting_persisted` via ``v4_lp_close_native_principal`` so the
    Layer-5 LP_CLOSE row carries the real native proceeds (exact realized PnL
    instead of a fail-closed ``None`` under VIB-5131). ``None`` (gateway-less
    harness / non-native / failed read) leaves the native leg unmeasured.
    """
    if eth_call_reader is None:
        return None
    from almanak.framework.runner.strategy_runner import StrategyRunner

    return StrategyRunner._capture_v4_lp_close_native_principal_safe(
        intent=intent,
        chain=chain,
        gateway_client=eth_call_reader,
    )


def _maybe_enrich_with_slot0(result: Any, *, chain: str, eth_call_reader: Any | None) -> None:
    if eth_call_reader is None:
        return
    extracted_data = getattr(result, "extracted_data", None)
    if not isinstance(extracted_data, dict):
        return

    lp_open = enrich_lp_open_with_slot0(
        extracted_data.get("lp_open_data"),
        gateway_client=eth_call_reader,
        chain=chain,
    )
    if lp_open is not None:
        extracted_data["lp_open_data"] = lp_open

    lp_close = enrich_lp_close_with_slot0(
        extracted_data.get("lp_close_data"),
        gateway_client=eth_call_reader,
        chain=chain,
    )
    if lp_close is not None:
        extracted_data["lp_close_data"] = lp_close
        if hasattr(result, "lp_close_data"):
            result.lp_close_data = lp_close


async def _persist_and_drain_for_intent_test(
    *,
    state_manager: Any,
    accounting_processor: AccountingProcessor,
    deployment_id: str,
    cycle_id: str,
    execution_mode: str,
    chain: str,
    wallet_address: str,
    intent: Any,
    result: Any,
    success: bool,
    error: str = "",
    price_oracle: dict[str, Decimal] | None = None,
    eth_call_reader: Any | None = None,
    pre_state: dict[str, Any] | None = None,
    post_state: dict[str, Any] | None = None,
    resolved_pool: str | None = None,
    v4_lp_close_native_principal: tuple[int | None, int | None] | None = None,
    lp_close_native_amounts: tuple[int | None, int | None] | None = None,
) -> Layer5Persisted:
    """Persist one real intent result through the production outbox path.

    This stays test-scoped on purpose: Layer-5 intent tests need to assert
    what the existing accounting processor writes without adding a new
    production helper or changing runner behavior.

    ``pre_state`` / ``post_state`` mirror the runner's lending-state capture
    (``capture_lending_pre_state`` / ``capture_lending_post_state`` serialized
    via ``lending_state_to_dict``). When supplied they flow into
    ``build_ledger_entry`` so the lending category handler reads real
    collateral/debt/health-factor and emits ``confidence=HIGH`` rows — the
    LP slot0 lane has no equivalent (it enriches ``extracted_data`` instead).
    """
    _maybe_enrich_with_slot0(result, chain=chain, eth_call_reader=eth_call_reader)

    # VIB-4483: mirror the runner's POST-mint native-ETH leg capture so the
    # Layer-5 native-pool LP_OPEN row carries the real native amount0 (the leg
    # the receipt parser left None — no ERC-20 Transfer). The eth_call adapter
    # implements the gateway ``query_v4_position_state`` interface over the fork,
    # so this exercises the exact production stamp path end-to-end. Empty ≠ Zero:
    # a gateway-less harness (eth_call_reader=None) yields None and the leg stays
    # unmeasured, never a fabricated zero.
    v4_lp_open_native_amounts = _capture_v4_lp_open_native_for_intent_test(
        intent=intent,
        chain=chain,
        result=result,
        eth_call_reader=eth_call_reader,
    )

    # VIB-5117 / VIB-5121 — close-side native-leg principal. Unlike the OPEN
    # capture above (a post-mint read the harness performs itself), the V4 close
    # principal MUST be read PRE-burn, so the test captures it before executing
    # the close (``capture_v4_lp_close_native_principal``) and passes it in. The
    # ``lp_close_native_amounts`` twin carries the Fluid/fungible balance-bracket
    # close. Both mirror the runner's ``build_ledger_entry`` close-stamp wiring so
    # the Layer-5 native LP_CLOSE row records the real proceeds (exact realized
    # PnL) instead of a VIB-5131 fail-closed ``None``.

    entry = build_ledger_entry(
        deployment_id=deployment_id,
        cycle_id=cycle_id,
        intent=intent,
        result=result,
        chain=chain,
        success=success,
        error=error,
        price_oracle=price_oracle,
        pre_state=pre_state,
        post_state=post_state,
        lp_open_native_amounts=v4_lp_open_native_amounts,
        v4_lp_close_native_principal=v4_lp_close_native_principal,
        lp_close_native_amounts=lp_close_native_amounts,
    )
    entry.execution_mode = execution_mode
    await _maybe_await(state_manager.save_ledger_entry(entry))

    if not success:
        return Layer5Persisted(ledger_entry_id=entry.id, outbox_id=None)

    position_key, market_id = _default_compute_position_key(
        intent,
        chain=chain,
        wallet_address=wallet_address,
        resolved_pool=resolved_pool,
    )
    accounting_processor._deployment_id = deployment_id
    outbox_id = await write_outbox_entry(
        state_manager,
        deployment_id=deployment_id,
        cycle_id=cycle_id,
        ledger_entry_id=entry.id,
        intent_type=_intent_type_str(intent),
        wallet_address=wallet_address,
        position_key=position_key,
        market_id=market_id,
    )
    drained = await _maybe_await(accounting_processor.drain_one(entry.id)) if outbox_id else None
    return Layer5Persisted(ledger_entry_id=entry.id, outbox_id=outbox_id, drained=drained)


async def assert_accounting_persisted(
    harness: Layer5AccountingHarness,
    *,
    intent: Any,
    result: Any,
    chain: str,
    wallet_address: str,
    expected_event_type: str,
    price_oracle: dict[str, Decimal] | None = None,
    deployment_id: str = "layer5-intent-test",
    cycle_id: str = "layer5-cycle",
    execution_mode: str = "paper",
    eth_call_reader: Any | None = None,
    pre_state: dict[str, Any] | None = None,
    post_state: dict[str, Any] | None = None,
    resolved_pool: str | None = None,
    v4_lp_close_native_principal: tuple[int | None, int | None] | None = None,
    lp_close_native_amounts: tuple[int | None, int | None] | None = None,
) -> dict[str, Any]:
    """Persist a real execution result through Layer 5 and return the event row.

    ``pre_state`` / ``post_state`` are the runner-shaped lending-state dicts
    (see ``_persist_and_drain_for_intent_test``); LP callers omit them.

    ``resolved_pool`` (VIB-3946) is the compiler-resolved canonical pool label
    (``action_bundle.metadata["pool_name"]``); when set it threads into the
    position-key derivation so e.g. a Curve asset-set intent keys off the
    canonical ``"3pool"`` label instead of the raw ``"USDT/USDC/DAI"`` string.
    """
    persisted = await _persist_and_drain_for_intent_test(
        state_manager=harness.store,
        accounting_processor=harness.processor,
        deployment_id=deployment_id,
        cycle_id=cycle_id,
        execution_mode=execution_mode,
        chain=chain,
        wallet_address=wallet_address,
        intent=intent,
        result=result,
        success=bool(getattr(result, "success", False)),
        price_oracle=price_oracle,
        eth_call_reader=eth_call_reader,
        pre_state=pre_state,
        post_state=post_state,
        resolved_pool=resolved_pool,
        v4_lp_close_native_principal=v4_lp_close_native_principal,
        lp_close_native_amounts=lp_close_native_amounts,
    )
    assert persisted.outbox_id is not None, "Layer-5 helper must write accounting_outbox"
    assert persisted.drained is True, "AccountingProcessor.drain_one must process the row"

    rows = await harness.store.get_accounting_events(deployment_id, event_type=expected_event_type, limit=20)
    matching = [row for row in rows if row.get("ledger_entry_id") == persisted.ledger_entry_id]
    assert len(matching) == 1, (
        f"expected exactly one {expected_event_type} accounting_event for ledger "
        f"{persisted.ledger_entry_id}, got {len(matching)}"
    )

    # Idempotency: re-draining the same outbox row must not duplicate the typed event.
    redrained = await harness.processor.drain_one(persisted.ledger_entry_id)
    assert redrained is True
    rows_after = await harness.store.get_accounting_events(deployment_id, event_type=expected_event_type, limit=20)
    matching_after = [row for row in rows_after if row.get("ledger_entry_id") == persisted.ledger_entry_id]
    assert len(matching_after) == 1, "drain_one must be idempotent for Layer-5 rows"
    return matching_after[0]


async def assert_no_accounting_on_failure(
    harness: Layer5AccountingHarness,
    *,
    intent: Any | None = None,
    result: Any | None = None,
    chain: str = "",
    wallet_address: str = "",
    price_oracle: dict[str, Decimal] | None = None,
    deployment_id: str = "layer5-intent-test",
    cycle_id: str = "layer5-cycle",
    execution_mode: str = "paper",
    eth_call_reader: Any | None = None,
) -> None:
    """Assert the failure-path accounting contract: no typed events written.

    Drives the failed result through the same Layer-5 persist path as the
    success helper so the assertion is not vacuous on a fresh harness: a
    failed entry writes a ledger row but must enqueue no outbox row, drain
    nothing, and produce no typed ``accounting_events`` row for its
    ``ledger_entry_id``.
    """
    assert intent is not None, "failure assertion requires the intent"
    assert result is not None, "failure assertion requires the execution result"
    assert not bool(getattr(result, "success", False)), "use assert_accounting_persisted() for successful results"

    persisted = await _persist_and_drain_for_intent_test(
        state_manager=harness.store,
        accounting_processor=harness.processor,
        deployment_id=deployment_id,
        cycle_id=cycle_id,
        execution_mode=execution_mode,
        chain=chain,
        wallet_address=wallet_address,
        intent=intent,
        result=result,
        success=False,
        error=str(getattr(result, "error", "") or ""),
        price_oracle=price_oracle,
        eth_call_reader=eth_call_reader,
    )
    assert persisted.outbox_id is None, "failed execution must not enqueue accounting_outbox"
    assert persisted.drained is None, "failed execution must not drain a typed event"

    rows = await harness.store.get_accounting_events(deployment_id, limit=20)
    matching = [row for row in rows if row.get("ledger_entry_id") == persisted.ledger_entry_id]
    assert matching == [], (
        f"failed execution must not write accounting_events rows for {persisted.ledger_entry_id}; got {matching!r}"
    )


# Chain configurations
CHAIN_CONFIGS = {
    "base": {
        "rpc_url": "https://mainnet.base.org",
        "chain_id": 8453,
        "alchemy_key": "base",
        "tokens": {
            "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "WETH": "0x4200000000000000000000000000000000000006",
            "USDbC": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
            "wstETH": "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452",
        },
        "balance_slots": {
            "USDC": 9,
            "WETH": 3,
            "USDbC": 9,
            "wstETH": 1,
        },
    },
    "avalanche": {
        "rpc_url": "https://api.avax.network/ext/bc/C/rpc",
        "chain_id": 43114,
        "alchemy_key": "avax",
        "tokens": {
            "USDC": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
            "WAVAX": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
            "USDT": "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7",
            "USDC.e": "0xA7D7079b0FEaD91F3e65f86E8915Cb59c1a4C664",
            "USDT.e": "0xc7198437980c041c805A1EDcbA50c1Ce5db95118",
        },
        "balance_slots": {
            "USDC": 9,
            "WAVAX": 3,
            "USDT": 2,
            "USDC.e": 0,
            "USDT.e": 0,
        },
    },
    "ethereum": {
        "rpc_url": "https://eth.llamarpc.com",
        "chain_id": 1,
        "alchemy_key": "eth",
        "tokens": {
            "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
            "wstETH": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",
        },
        "balance_slots": {
            "USDC": 9,
            "WETH": 3,
            "USDT": 2,
            "wstETH": 0,
        },
    },
    "arbitrum": {
        "rpc_url": "https://arb1.arbitrum.io/rpc",
        "chain_id": 42161,
        "alchemy_key": "arb",
        "tokens": {
            "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
            "wstETH": "0x5979D7b546E38E414F7E9822514be443A4800529",
        },
        "balance_slots": {
            "USDC": 9,
            "WETH": 51,
            "USDT": 51,
            # Arbitrum wstETH (OFT bridged): OpenZeppelin-style ERC20 with `_balances`
            # mapping at storage slot 1. Slot verified 2026-04-17 by computing
            # keccak256(abi.encode(holder, slot=1)) for a known holder and confirming
            # the resulting storage value matched balanceOf(). Holder-independent.
            "wstETH": 1,
        },
    },
    "optimism": {
        "rpc_url": "https://mainnet.optimism.io",
        "chain_id": 10,
        "alchemy_key": "opt",
        "tokens": {
            "USDC": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
            "WETH": "0x4200000000000000000000000000000000000006",
            "USDT": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
        },
        "balance_slots": {
            "USDC": 9,
            "WETH": 3,
            "USDT": 2,
        },
    },
    "polygon": {
        "rpc_url": "https://polygon-rpc.com",
        "chain_id": 137,
        "alchemy_key": "polygon",
        "tokens": {
            "USDC": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
            # USDC.e — PoS-bridged USDC, the base asset of the Compound V3
            # Polygon Comet (the only deployed market on polygon). Native USDC
            # cannot be used because the Comet's baseToken() == this address.
            "USDC.e": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
            "WETH": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
            "USDT": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
            "WBTC": "0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6",
        },
        "balance_slots": {
            "USDC": 9,
            # USDC.e is UChildERC20Proxy like the other Polygon PoS-bridged
            # tokens (USDT, WBTC); slot 0 holds the OpenZeppelin _balances map.
            "USDC.e": 0,
            "WETH": 0,  # UChildERC20Proxy (PoS bridge): _balances is slot 0 in ERC20 base
            "USDT": 0,  # UChildERC20Proxy (PoS bridge): _balances is slot 0 in ERC20 base
            # Polygon WBTC (PoS-bridged) uses slot 0 for `_balances`. Verified
            # 2026-04-17 by computing keccak256(abi.encode(holder, slot=0)) for
            # Morpho Blue (a known holder with ~143 WBTC) and confirming storage
            # value matched balanceOf(). Holder-independent.
            "WBTC": 0,
        },
    },
    "bsc": {
        "rpc_url": "https://bsc-dataseed.binance.org",
        "chain_id": 56,
        "alchemy_key": "bnb",
        "tokens": {
            "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
            "WBNB": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
            "USDT": "0x55d398326f99059fF775485246999027B3197955",
        },
        "balance_slots": {
            "USDC": 1,  # Binance-Peg USDC uses slot 1
            "WBNB": 3,
            "USDT": 1,  # Binance-Peg USDT uses slot 1
        },
    },
    "bnb": {  # Alias for bsc (canonical name used by framework)
        "rpc_url": "https://bsc-dataseed.binance.org",
        "chain_id": 56,
        "alchemy_key": "bnb",
        "tokens": {
            "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
            "WBNB": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
            "USDT": "0x55d398326f99059fF775485246999027B3197955",
        },
        "balance_slots": {
            "USDC": 1,  # Binance-Peg USDC uses slot 1
            "WBNB": 3,
            "USDT": 1,  # Binance-Peg USDT uses slot 1
        },
    },
    "linea": {
        "rpc_url": "https://rpc.linea.build",
        "chain_id": 59144,
        "alchemy_key": "linea",
        "tokens": {
            "USDC": "0x176211869cA2b568f2A7D4EE941E073a821EE1ff",
            "WETH": "0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f",
            "USDT": "0xA219439258ca9da29E9Cc4cE5596924745e12B93",
        },
        # Verified on a Linea mainnet fork (2026-05-27): writing the balance
        # storage slot keccak256(abi.encode(holder, slot)) credits balanceOf()
        # only at these slots — USDC is a bridged FiatTokenV2-style proxy with
        # _balances at slot 9 (slot 0 is a no-op), matching fork_manager.py's
        # LINEA balance-slot table (VIB-2724). WETH/USDT confirmed at slot 3/51.
        "balance_slots": {
            "USDC": 9,
            "WETH": 3,
            "USDT": 51,
        },
    },
    "blast": {
        "rpc_url": "https://rpc.blast.io",
        "chain_id": 81457,
        "alchemy_key": None,  # No Alchemy support, uses public RPC
        "tokens": {
            "USDB": "0x4300000000000000000000000000000000000003",
            "WETH": "0x4300000000000000000000000000000000000004",
        },
        "balance_slots": {
            "USDB": 0,
            "WETH": 0,
        },
    },
    "mantle": {
        "rpc_url": "https://rpc.mantle.xyz",
        "chain_id": 5000,
        "alchemy_key": "mantle",  # Alchemy mainnet supported (mantle-mainnet.g.alchemy.com)
        "tokens": {
            "WMNT": "0x78c1b0C915c4FAA5FffA6CAbf0219DA63d7f4cb8",
            "USDC": "0x09Bc4E0D864854c6aFB6eB9A9cdF58aC190D0dF9",
            "WETH": "0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111",
            "USDT": "0x201EBa5CC46D216Ce6DC03F6a759e8E766e956aE",
        },
        "balance_slots": {
            "WMNT": 0,  # Unused — wraps from native MNT
            "USDC": 9,  # Bridged USDC uses slot 9 (verified via cast index + cast storage)
            "WETH": 0,  # L2 predeploy WETH uses slot 0
            "USDT": 0,  # Bridged USDT uses slot 0
        },
    },
    "monad": {
        # Public Monad RPC (verified 2026-04-18 to serve historical state for Anvil forking).
        # Alchemy Monad mainnet requires per-app enablement; use public RPC as the default
        # to keep intent tests self-contained. User can override via MONAD_RPC_URL env var.
        "rpc_url": "https://rpc.monad.xyz",
        "chain_id": 143,
        "alchemy_key": None,  # Optional — requires per-app enablement on Alchemy dashboard
        "tokens": {
            # Addresses match almanak/core/contracts.py (MORPHO_BLUE_TOKENS["monad"]).
            "WMON": "0x3bd359C1119dA7Da1D913D1C4D2B7c461115433A",
            "WETH": "0xEE8c0E9f1BFFb4Eb878d8f15f368A02a35481242",
            "USDC": "0x754704Bc059F8C67012fEd69BC8A327a5aafb603",
        },
        "balance_slots": {
            # WMON wraps native MON — funded via _wrap_native_token, slot unused but set
            # for consistency with how base handles WETH.
            "WMON": 3,
            # WETH9-canonical layout (bridged WETH on Monad). Slot 3 per standard.
            "WETH": 3,
            # USDC on Monad uses OpenZeppelin upgradeable pattern. Slot 9 is Circle's
            # standard across Arbitrum/Base/Ethereum/Polygon; assumed here, probe in
            # fixture seeding if it fails.
            "USDC": 9,
        },
    },
    "xlayer": {
        "rpc_url": "https://rpc.xlayer.tech",
        "chain_id": 196,
        "alchemy_key": "xlayer",  # Alchemy mainnet supported (xlayer-mainnet.g.alchemy.com)
        "tokens": {
            "USDC": "0x74b7F16337b8972027F6196A17a631aC6dE26d22",
            "WETH": "0x5A77f1443D16ee5761d310e38b62f77f726bC71c",
            "USDT0": "0x779Ded0c9e1022225f8E0630b35a9b54bE713736",
            # USDG (Gravity USD) — added so the test_uniswap_v3_swap.py shard can
            # use the only liquid stablecoin pair on xlayer Uniswap V3
            # (USDT0/USDG @ fee=100, ~0.02% price impact for a 100-unit swap).
            # See issue #2106 for the on-chain liquidity audit.
            "USDG": "0x4ae46a509F6b1D9056937BA4500cb143933D2dc8",
        },
        "balance_slots": {
            "USDC": 9,  # Native Circle USDC uses slot 9
            "WETH": 0,  # Bridged WETH
            "USDT0": 51,  # OpenZeppelin upgradeable pattern
            "USDG": 1,  # OpenZeppelin v5 ERC-20 (verified via cast index against the USDT0/USDG pool's balance)
        },
    },
    "zerog": {
        "rpc_url": "https://0g-rpc.publicnode.com",
        "chain_id": 16661,
        "alchemy_key": None,  # 0G uses public RPC only
        "tokens": {
            "W0G": "0x1Cd0690fF9a693f5EF2dD976660a8dAFc81A109c",
            "USDC.e": "0x1f3AA82227281cA364bFb3d253B0f1af1Da6473E",
        },
        # Storage slots not yet mapped for 0G tokens — native-only funding.
        # Tests that need ERC20 acquire it by swapping from native via Jaine.
        "balance_slots": {},
    },
}
# Import Anvil fixtures and constants from shared gateway conftest.
# Note: We do NOT import CHAIN_CONFIGS from conftest_gateway to avoid conflict with local definition
from tests.conftest_gateway import (
    CHAIN_ANVIL_PORTS,
    TEST_PRIVATE_KEY,
    TEST_WALLET,
    # Anvil fixtures (session-scoped, auto-started)
    anvil_arbitrum,
    anvil_avalanche,
    anvil_base,
    anvil_bsc,
    anvil_ethereum,
    anvil_mantle,
    anvil_monad,
    anvil_optimism,
    anvil_polygon,
    anvil_xlayer,
    anvil_zerog,
    get_anvil_rpc_url,
)

# Re-export for test files that import from this module
__all__ = [
    # Anvil fixtures (auto-started, session-scoped)
    "anvil_arbitrum",
    "anvil_avalanche",
    "anvil_base",
    "anvil_bsc",
    "anvil_ethereum",
    "anvil_mantle",
    "anvil_monad",
    "anvil_optimism",
    "anvil_polygon",
    "anvil_xlayer",
    "anvil_zerog",
    # Price oracle fixtures (session-scoped per chain)
    "price_oracle_arbitrum",
    "price_oracle_avalanche",
    "price_oracle_base",
    "price_oracle_monad",
    "price_oracle_bsc",
    "price_oracle_bnb",
    "price_oracle_ethereum",
    "price_oracle_mantle",
    "price_oracle_optimism",
    "price_oracle_polygon",
    "price_oracle_xlayer",
    # Utilities
    "fund_native_token",
    "fund_erc20_token",
    "get_anvil_rpc_url",
    "is_anvil_running",
    # Constants
    "TEST_WALLET",
    "TEST_PRIVATE_KEY",
    "TEST_RPC_CONNECT_TIMEOUT_SECONDS",
    "TEST_RPC_READ_TIMEOUT_SECONDS",
    "TEST_WEB3_DEFAULT_HTTP_TIMEOUT_SECONDS",
    "TEST_WEB3_REQUEST_TIMEOUT",
    "TEST_POLYGON_RPC_READ_TIMEOUT_SECONDS",
    "TEST_POLYGON_WEB3_REQUEST_TIMEOUT",
    "TEST_SLOW_FORK_RPC_READ_TIMEOUT_SECONDS",
    "TEST_SLOW_FORK_WEB3_REQUEST_TIMEOUT",
    "TEST_SLOW_FORK_CHAINS",
    "web3_request_timeout",
    "TEST_CAST_TIMEOUT_SECONDS",
    "TEST_TX_TIMEOUT_SECONDS",
    "TEST_SUBMITTER_MAX_RETRIES",
    "SWAP_MAX_SLIPPAGE",
    "CHAIN_CONFIGS",
    "CHAIN_ANVIL_PORTS",
    # Helper functions
    "get_token_balance",
    "get_token_decimals",
    "format_token_amount",
]

# ERC20 ABI for balance/allowance checks
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [
            {"name": "_owner", "type": "address"},
            {"name": "_spender", "type": "address"},
        ],
        "name": "allowance",
        "outputs": [{"name": "remaining", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function",
    },
]


# =============================================================================
# Helper Functions
# =============================================================================

_ASYNC_HTTP_PROVIDERS: weakref.WeakSet[AsyncHTTPProvider] = weakref.WeakSet()


def _enable_async_http_provider_tracking() -> None:
    """Track AsyncHTTPProvider instances so we can close leaked aiohttp sessions.

    Web3's AsyncHTTPProvider caches aiohttp ClientSessions per event loop.
    In intent tests we create lots of short-lived submitters/providers; if those
    sessions aren't closed, we can end up with many open connections and noisy
    "Unclosed client session" warnings (and, in worst cases, resource pressure).
    """

    if getattr(AsyncHTTPProvider, "_almanak_tracking_enabled", False):
        return

    original_init = AsyncHTTPProvider.__init__

    def tracked_init(self, *args: Any, **kwargs: Any) -> None:  # type: ignore[no-untyped-def]
        original_init(self, *args, **kwargs)
        _ASYNC_HTTP_PROVIDERS.add(self)

    AsyncHTTPProvider.__init__ = tracked_init  # type: ignore[method-assign]
    AsyncHTTPProvider._almanak_tracking_enabled = True  # type: ignore[attr-defined]


_enable_async_http_provider_tracking()


@pytest.fixture(scope="session", autouse=True)
def configure_web3_default_http_timeout():
    """Reduce Web3's default HTTP timeout for intent tests.

    Web3 defaults to 30s per HTTP request. When a forked Anvil instance stalls,
    those 30s timeouts compound across many RPC calls and make failures take
    minutes. In intent tests we prefer failing fast and restarting Anvil.
    """
    try:
        from web3._utils import http as web3_http
    except Exception:
        yield
        return

    original = web3_http.DEFAULT_HTTP_TIMEOUT
    web3_http.DEFAULT_HTTP_TIMEOUT = TEST_WEB3_DEFAULT_HTTP_TIMEOUT_SECONDS
    try:
        yield
    finally:
        web3_http.DEFAULT_HTTP_TIMEOUT = original


def make_intent_test_web3(rpc_url: str) -> Web3:
    """Create a Web3 HTTP provider using intent-test timeout defaults."""
    return Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": TEST_WEB3_REQUEST_TIMEOUT}))


def _is_timeout_chain_error(error: BaseException) -> bool:
    """Return True if error/cause chain indicates an RPC timeout."""
    current: BaseException | None = error
    visited: set[int] = set()

    while current is not None and id(current) not in visited:
        visited.add(id(current))
        if isinstance(current, TimeoutError | requests.exceptions.Timeout):
            return True
        message = str(current).lower()
        if "read timed out" in message or "timed out" in message:
            return True
        current = current.__cause__ or current.__context__

    return False


def _rpc_response_success(response: Any) -> bool:
    """Return True when a JSON-RPC response does not contain an error."""
    if isinstance(response, dict):
        return "error" not in response
    return True


def _probe_anvil_admin_rpc(rpc_url: str) -> bool:
    """Probe admin RPC methods required by intent fixture seeding.

    A healthy fork for our setup path must answer both anvil_setBalance and
    evm_mine, not just eth_chainId/eth_blockNumber.
    """
    probe_timeout = (
        min(TEST_RPC_CONNECT_TIMEOUT_SECONDS, TEST_ANVIL_RECOVERY_PROBE_TIMEOUT_SECONDS),
        TEST_ANVIL_RECOVERY_PROBE_TIMEOUT_SECONDS,
    )
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": probe_timeout}))
    if not w3.is_connected():
        return False

    set_balance_resp = w3.provider.make_request("anvil_setBalance", [TEST_ANVIL_PROBE_SENTINEL_WALLET, "0x0"])
    if not _rpc_response_success(set_balance_resp):
        return False

    mine_resp = w3.provider.make_request("evm_mine", [])
    return _rpc_response_success(mine_resp)


def _force_restart_anvil(anvil_instance: Any, chain_name: str, attempt: int) -> tuple[bool, str]:
    """Force-restart an Anvil fixture and verify admin RPC readiness."""
    restart = getattr(anvil_instance, "restart", None)
    get_rpc_url = getattr(anvil_instance, "get_rpc_url", None)

    if not callable(restart) or not callable(get_rpc_url):
        print(f"WARNING: {chain_name} recovery attempt {attempt}: missing restart/get_rpc_url on fixture")
        return (False, "")

    print(f"WARNING: {chain_name} recovery attempt {attempt}/{TEST_ANVIL_RECOVERY_MAX_RESTARTS}: forcing Anvil restart")
    restarted = restart(health_timeout_seconds=TEST_RECOVERY_HEALTH_TIMEOUT_SECONDS)
    if not restarted:
        print(f"WARNING: {chain_name} recovery attempt {attempt}: Anvil restart failed")
        return (False, "")

    time.sleep(TEST_ANVIL_RECOVERY_SETTLE_SECONDS)
    recovered_rpc_url = get_rpc_url()
    try:
        admin_ready = _probe_anvil_admin_rpc(recovered_rpc_url)
    except Exception as e:
        print(f"WARNING: {chain_name} recovery attempt {attempt}: admin RPC probe raised {type(e).__name__}: {e}")
        return (False, recovered_rpc_url)

    if not admin_ready:
        print(f"WARNING: {chain_name} recovery attempt {attempt}: admin RPC probe failed at {recovered_rpc_url}")
        return (False, recovered_rpc_url)

    print(f"WARNING: {chain_name} recovery attempt {attempt}: restart+probe succeeded at {recovered_rpc_url}")
    return (True, recovered_rpc_url)


def seed_wallet_state_with_recovery(
    *,
    seed_wallet_state: Callable[[Web3, str], str],
    web3: Web3,
    rpc_url: str,
    anvil_instance: Any,
    chain_name: str,
) -> str:
    """Seed wallet state with forced restart recovery on local Anvil timeout."""
    active_web3 = web3
    active_rpc_url = rpc_url
    last_timeout_error: Exception | None = None

    for attempt in range(TEST_ANVIL_RECOVERY_MAX_RESTARTS + 1):
        try:
            return seed_wallet_state(active_web3, active_rpc_url)
        except Exception as e:
            if not _is_timeout_chain_error(e):
                raise
            last_timeout_error = e

            if attempt >= TEST_ANVIL_RECOVERY_MAX_RESTARTS:
                break

            restart_attempt = attempt + 1
            restarted, recovered_rpc_url = _force_restart_anvil(anvil_instance, chain_name, restart_attempt)
            if not restarted:
                continue

            active_rpc_url = recovered_rpc_url
            active_web3 = make_intent_test_web3(active_rpc_url)

    if last_timeout_error is None:
        raise RuntimeError(f"{chain_name} Anvil recovery failed without timeout error context")

    raise RuntimeError(
        f"{chain_name} Anvil wallet seed failed after {TEST_ANVIL_RECOVERY_MAX_RESTARTS} forced restart attempts "
        f"(rpc_url={active_rpc_url}, last_error={type(last_timeout_error).__name__}: {last_timeout_error})"
    ) from last_timeout_error


def get_latest_block(rpc_url: str) -> int:
    """Get the latest block number from an RPC endpoint.

    Uses in-process Web3 provider instead of subprocess (cast).
    """
    w3 = make_intent_test_web3(rpc_url)
    return w3.eth.block_number


def is_anvil_running(rpc_url: str = ANVIL_URL) -> bool:
    """Check if Anvil is running and responding."""
    try:
        web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 2}))
        return web3.is_connected()
    except Exception:
        return False


def _retry_on_network_error(
    func: Callable[..., Any],
    description: str,
    max_retries: int = TEST_FUNDING_RPC_MAX_RETRIES,
    backoff_seconds: float = TEST_FUNDING_RPC_BACKOFF_SECONDS,
) -> Any:
    """Retry a callable on transient network errors with linear backoff.

    Catches ReadTimeout, ConnectionError, and web3 TimeExhausted.
    Zero overhead on happy path - returns immediately on first success.

    Args:
        func: Zero-argument callable to retry
        description: Human-readable label for log messages
        max_retries: Maximum number of attempts (0 treated as 1)
        backoff_seconds: Base backoff between retries (multiplied by attempt number)

    Returns:
        Return value of func()

    Raises:
        Last caught exception if all retries exhausted
    """
    attempts = max(1, max_retries)
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError, TimeExhausted) as e:
            last_error = e
            if attempt < attempts:
                delay = backoff_seconds * attempt
                print(
                    f"  [retry] {description} attempt {attempt}/{attempts} failed "
                    f"({type(e).__name__}), retrying in {delay:.0f}s..."
                )
                time.sleep(delay)
            else:
                print(f"  [retry] {description} failed after {attempts} attempts ({type(e).__name__}: {e})")
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"{description} failed without a captured retryable exception")


def _retry_rpc_call(
    w3: Web3,
    method: str,
    params: list,
    max_retries: int = TEST_FUNDING_RPC_MAX_RETRIES,
    backoff_seconds: float = TEST_FUNDING_RPC_BACKOFF_SECONDS,
) -> Any:
    """Retry an Anvil RPC call with linear backoff and error checking.

    Delegates to _retry_on_network_error for transient failures.
    Additionally checks for JSON-RPC error payloads and raises on failure.

    Args:
        w3: Web3 instance connected to Anvil
        method: RPC method name
        params: RPC parameters
        max_retries: Maximum number of attempts
        backoff_seconds: Base backoff between retries (multiplied by attempt number)

    Returns:
        RPC response

    Raises:
        RuntimeError: If the RPC response contains an error field
        Last caught network exception if all retries exhausted
    """
    response = _retry_on_network_error(
        lambda: w3.provider.make_request(method, params),
        description=method,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
    )
    if not _rpc_response_success(response):
        error_payload = response.get("error") if isinstance(response, dict) else response
        raise RuntimeError(f"{method} returned RPC error: {error_payload}")
    return response


def fund_native_token(wallet: str, amount_wei: int, rpc_url: str) -> None:
    """Fund a wallet with native token (ETH/AVAX/etc).

    Uses in-process Web3 provider RPC instead of subprocess (cast).
    Retries on transient network errors (ReadTimeout, ConnectionError).
    """
    w3 = make_intent_test_web3(rpc_url)
    checksum_wallet = Web3.to_checksum_address(wallet)
    current_balance = w3.eth.get_balance(checksum_wallet)
    if current_balance >= amount_wei:
        return

    amount_hex = hex(amount_wei)
    _retry_rpc_call(w3, "anvil_setBalance", [wallet, amount_hex])


def _calculate_mapping_slot(wallet: str, balance_slot: int) -> str:
    """Calculate the storage slot for a mapping entry (balanceOf).

    Equivalent to `cast index address <wallet> <slot>` but in-process.

    Uses keccak256(abi.encode(key, slot)) per Solidity storage layout.
    """
    from eth_hash.auto import keccak as keccak256

    # Pad wallet address to 32 bytes
    key_padded = wallet.lower().replace("0x", "").zfill(64)
    # Pad slot number to 32 bytes
    slot_padded = hex(balance_slot)[2:].zfill(64)
    # Concatenate and hash
    concat = bytes.fromhex(key_padded + slot_padded)
    return "0x" + keccak256(concat).hex()


def fund_erc20_token(
    wallet: str,
    token_address: str,
    amount: int,
    balance_slot: int,
    rpc_url: str,
) -> None:
    """Fund a wallet with ERC20 tokens using storage manipulation.

    Uses in-process Web3 provider and keccak256 instead of subprocess (cast).
    """
    w3 = make_intent_test_web3(rpc_url)

    # Calculate storage slot in-process (replaces `cast index`)
    storage_slot = _calculate_mapping_slot(wallet, balance_slot)

    # Format amount as 32-byte hex
    amount_hex = f"0x{amount:064x}"

    # Set storage via Anvil RPC (with retry for transient failures)
    _retry_rpc_call(w3, "anvil_setStorageAt", [token_address, storage_slot, amount_hex])

    # Mine a block to apply changes
    _retry_rpc_call(w3, "evm_mine", [])


# Arbitrum sUSDai (Staked USDai) — funds the live Pendle sUSDai-market intent
# tests after the Arbitrum wstETH Pendle market expired 2026-06-25. sUSDai is
# deliberately NOT a CHAIN_CONFIGS["arbitrum"]["tokens"] entry because the
# session price-oracle fixture requires every token there to carry a CoinGecko
# id and sUSDai has none — so it is seeded via this dedicated helper instead,
# from BOTH the EOA seed (arbitrum/conftest.py) and the Zodiac Safe seed
# (_build_zodiac_context), keeping a single source of truth for its address/slot.
#
# sUSDai is an OpenZeppelin v5 upgradeable ERC20 using ERC-7201 namespaced
# storage, so its ``_balances`` mapping does NOT live at a small integer slot.
# The base slot below is the OZ ERC20 namespaced location; verified 2026-06-29
# by computing keccak256(abi.encode(holder, base)) against the SY contract
# holder (0x30Ccf4Bb...) and confirming it matched balanceOf().
# ``_calculate_mapping_slot`` accepts this big int as the slot argument.
ARBITRUM_SUSDAI_ADDRESS = "0x0B2b2B2076d95dda7817e785989fE353fe955ef9"
ARBITRUM_SUSDAI_BALANCES_SLOT = 0x52C63247E1F47DB19D5CE0460030C497F067CA4CEBF71BA98EEADABE20BACE00


def seed_arbitrum_susdai(wallet: str, web3: Web3, rpc_url: str) -> None:
    """Fund ``wallet`` with sUSDai on an Arbitrum fork (best-effort).

    Used for the live Pendle sUSDai-market intent tests. No-op-on-error so a
    seeding failure surfaces as a clear ERC20InsufficientBalance at execute
    rather than crashing fixture setup.
    """
    try:
        decimals = get_token_decimals(web3, ARBITRUM_SUSDAI_ADDRESS)
        fund_erc20_token(
            wallet,
            ARBITRUM_SUSDAI_ADDRESS,
            100_000 * (10**decimals),
            ARBITRUM_SUSDAI_BALANCES_SLOT,
            rpc_url,
        )
    except Exception as exc:  # noqa: BLE001 — best-effort seeding
        print(f"Warning: could not fund sUSDai for {wallet}: {exc}")


def get_token_balance(web3: Web3, token_address: str, wallet: str) -> int:
    """Get ERC20 token balance for a wallet.

    Works with any Web3 instance (gateway-backed or direct).
    """
    contract = web3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC20_ABI)
    return contract.functions.balanceOf(Web3.to_checksum_address(wallet)).call()


def get_token_decimals(web3: Web3, token_address: str) -> int:
    """Get ERC20 token decimals.

    Works with any Web3 instance (gateway-backed or direct).
    """
    contract = web3.eth.contract(address=Web3.to_checksum_address(token_address), abi=ERC20_ABI)
    return contract.functions.decimals().call()


def format_token_amount(amount: int, decimals: int) -> Decimal:
    """Convert raw token amount to decimal representation."""
    return Decimal(amount) / Decimal(10**decimals)


# =============================================================================
# L3 Semantic Verification Helpers
# =============================================================================


def assert_swap_semantic_match(
    intent_amount: Decimal,
    intent_from_token: str,
    intent_to_token: str,
    swap_result: object,
    *,
    tolerance_bps: int = 200,
    chain: str | None = None,
) -> None:
    """L3 semantic verification: cross-check intent params against receipt parser output.

    Catches the case where a TX succeeds and balance deltas look plausible,
    but the wrong operation executed (e.g., wrong token pair, wrong amount).

    Args:
        intent_amount: The amount from the SwapIntent (in token units, e.g. 100 USDC)
        intent_from_token: The from_token symbol from the SwapIntent
        intent_to_token: The to_token symbol from the SwapIntent
        swap_result: The parse_result.swap_result from a receipt parser
        tolerance_bps: Maximum acceptable deviation in basis points (default: 200 = 2%)
        chain: Chain name for token address resolution (optional)
    """
    # 1. Amount match: receipt amount_in should be close to intent amount
    actual_in = getattr(swap_result, "amount_in_decimal", None)
    if actual_in is not None:
        assert actual_in > 0, f"L3 semantic: receipt amount_in must be positive, got {actual_in}"
        expected = Decimal(str(intent_amount))
        deviation_bps = abs(actual_in - expected) / expected * 10000
        assert deviation_bps <= tolerance_bps, (
            f"L3 semantic: receipt amount_in ({actual_in}) deviates from intent amount ({expected}) "
            f"by {deviation_bps} bps (tolerance: {tolerance_bps} bps)"
        )

    # 2. Effective price sanity: must be positive and finite
    effective_price = getattr(swap_result, "effective_price", None)
    if effective_price is not None:
        assert effective_price > 0, f"L3 semantic: effective_price must be positive, got {effective_price}"

    # 3. Token address match (if receipt parser populates token_in/token_out)
    receipt_token_in = getattr(swap_result, "token_in", None)
    receipt_token_out = getattr(swap_result, "token_out", None)
    if chain and (receipt_token_in or receipt_token_out):
        try:
            from almanak.framework.data.tokens import get_token_resolver

            resolver = get_token_resolver()
            if receipt_token_in:
                expected_in = resolver.resolve_for_swap(intent_from_token, chain)
                if expected_in:
                    assert receipt_token_in.lower() == expected_in.address.lower(), (
                        f"L3 semantic: receipt token_in ({receipt_token_in}) != "
                        f"intent from_token resolved ({expected_in.address})"
                    )
            if receipt_token_out:
                expected_out = resolver.resolve_for_swap(intent_to_token, chain)
                if expected_out:
                    assert receipt_token_out.lower() == expected_out.address.lower(), (
                        f"L3 semantic: receipt token_out ({receipt_token_out}) != "
                        f"intent to_token resolved ({expected_out.address})"
                    )
        except ImportError:
            pass  # Token resolver not available — skip address check

    # 4. Receipt amounts must be bilateral (non-zero on both sides)
    actual_out = getattr(swap_result, "amount_out_decimal", None)
    if actual_out is not None:
        assert actual_out > 0, f"L3 semantic: receipt amount_out must be positive, got {actual_out}"


def assert_swap_bilateral_deltas(
    web3: Web3,
    token_in: str,
    token_out: str,
    wallet: str,
    in_balance_before: int,
    out_balance_before: int,
    expected_in_spent: int,
    *,
    in_decimals: int = 18,
    out_decimals: int = 18,
) -> tuple[int, int]:
    """Assert bilateral balance deltas for a successful swap.

    Verifies BOTH sides of a swap:
    - Input token MUST decrease by the exact expected amount
    - Output token MUST increase by at least 1 unit (no-op guard)

    Returns (amount_spent, amount_received) for further assertions.
    """
    in_after = get_token_balance(web3, token_in, wallet)
    out_after = get_token_balance(web3, token_out, wallet)

    amount_spent = in_balance_before - in_after
    amount_received = out_after - out_balance_before

    assert amount_spent == expected_in_spent, (
        f"Input token must decrease by exact swap amount. "
        f"Expected: {format_token_amount(expected_in_spent, in_decimals)}, "
        f"Got: {format_token_amount(amount_spent, in_decimals)}"
    )
    assert amount_received > 0, (
        f"Output token must increase (no-op guard). Got delta: {format_token_amount(amount_received, out_decimals)}"
    )
    return amount_spent, amount_received


def assert_swap_conservation(
    web3: Web3,
    token_in: str,
    token_out: str,
    wallet: str,
    in_balance_before: int,
    out_balance_before: int,
) -> None:
    """Assert bilateral balance conservation after a failed swap.

    Verifies BOTH tokens are unchanged:
    - Input token balance must be identical to before
    - Output token balance must be identical to before
    """
    in_after = get_token_balance(web3, token_in, wallet)
    out_after = get_token_balance(web3, token_out, wallet)

    assert in_after == in_balance_before, (
        f"Input token balance must be unchanged after failed swap. Before: {in_balance_before}, After: {in_after}"
    )
    assert out_after == out_balance_before, (
        f"Output token balance must be unchanged after failed swap. Before: {out_balance_before}, After: {out_after}"
    )


def get_chain_name_from_id(chain_id: int) -> str:
    """Get chain name from chain ID."""
    chain_id_to_name = {
        1: "ethereum",
        10: "optimism",
        56: "bsc",
        137: "polygon",
        8453: "base",
        42161: "arbitrum",
        43114: "avalanche",
    }
    return chain_id_to_name.get(chain_id, f"unknown_{chain_id}")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def test_wallet() -> str:
    """Return the default test wallet address."""
    return TEST_WALLET


def _wrap_native_token(wallet: str, weth_address: str, amount: int, rpc_url: str) -> None:
    """Wrap native tokens to get WETH/WAVAX/etc.

    Uses in-process Web3 transaction from an unlocked (auto-impersonate) wallet
    instead of subprocess (cast send).

    This is more reliable than storage slot manipulation because WETH
    storage layouts can vary across chains and implementations.
    """
    w3 = make_intent_test_web3(rpc_url)
    checksum_wallet = Web3.to_checksum_address(wallet)
    checksum_weth = Web3.to_checksum_address(weth_address)

    def _wrap_call() -> None:
        tx_hash = w3.eth.send_transaction(
            {
                "from": checksum_wallet,
                "to": checksum_weth,
                "value": amount,
            }
        )
        w3.eth.wait_for_transaction_receipt(tx_hash, timeout=TEST_RPC_READ_TIMEOUT_SECONDS)

    _retry_on_network_error(_wrap_call, description="wrap_native_token")


# =============================================================================
# Test Markers
# =============================================================================


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers."""
    config.addinivalue_line("markers", "base: Tests that run on Base chain")
    config.addinivalue_line("markers", "avalanche: Tests that run on Avalanche chain")
    config.addinivalue_line("markers", "ethereum: Tests that run on Ethereum chain")
    config.addinivalue_line("markers", "arbitrum: Tests that run on Arbitrum chain")
    config.addinivalue_line("markers", "optimism: Tests that run on Optimism chain")
    config.addinivalue_line("markers", "polygon: Tests that run on Polygon chain")
    config.addinivalue_line("markers", "bsc: Tests that run on BSC chain")
    config.addinivalue_line("markers", "linea: Tests that run on Linea chain")
    config.addinivalue_line("markers", "blast: Tests that run on Blast chain")
    config.addinivalue_line("markers", "mantle: Tests that run on Mantle chain")
    config.addinivalue_line("markers", "xlayer: Tests that run on X-Layer chain")
    config.addinivalue_line("markers", "zerog: Tests that run on 0G Chain (Jaine DEX)")
    config.addinivalue_line("markers", "swap: Tests for SwapIntent")
    config.addinivalue_line("markers", "lp: Tests for LP intents (Open/Close)")
    config.addinivalue_line("markers", "lending: Tests for lending intents")
    config.addinivalue_line("markers", "perps: Tests for perps intents")
    config.addinivalue_line("markers", "supply: Tests for supply intents")
    config.addinivalue_line("markers", "borrow: Tests for borrow intents")
    config.addinivalue_line("markers", "repay: Tests for repay intents")
    config.addinivalue_line("markers", "withdraw: Tests for withdraw intents")
    config.addinivalue_line(
        "markers", "l3_semantic: L3 semantic verification — cross-checks intent params against receipt"
    )


# =============================================================================
# Module-Scoped Baseline Snapshot / Revert for Test Isolation
# =============================================================================

# Baseline map: (chain_id, module_path) -> baseline_snapshot_id
# Captured once per module after funding is complete, re-armed after each revert.
_module_baselines: dict[tuple[int, str], str] = {}

# Session pristine map: chain_id -> pristine_snapshot_id
# Captured lazily on first module's setup per chain and re-captured after each
# revert (Anvil consumes snapshot IDs on revert). Used by `reset_fork_to_pristine`
# to give every test module a clean fork independent of prior modules on the
# same chain's session-scoped Anvil fork (VIB-3059).
_session_pristine: dict[int, str] = {}


class _PristineTransportError(RuntimeError):
    """Raised by `_ensure_pristine_and_rearm` when an evm_snapshot/evm_revert RPC
    call fails with a transport-level exception on a path where retrying is safe
    (i.e. the session pristine map has not yet been committed to a new state, so
    a second attempt cannot corrupt isolation).

    Distinct from a `False` return, which means the helper completed but could
    not guarantee pristine state for the current module — retrying `False` would
    risk flipping a definitive isolation-loss verdict into a misleading `True`.
    """


def _ensure_pristine_and_rearm(web3_instance: Web3, chain_id: int) -> bool:
    """Revert fork to session pristine state, then recapture pristine for next module.

    On the first call for a chain: captures current fork state as the pristine
    baseline and returns immediately (no revert — caller is expected to invoke
    this at the start of the first module before any seeding mutates the fork).

    On subsequent calls: reverts to the stored pristine snapshot and then
    immediately recaptures a new pristine snapshot at the just-reverted state
    so the NEXT module can revert too.

    Also purges stale `_module_baselines` entries for this chain, since Anvil's
    `evm_revert` invalidates all snapshots taken after the reverted one.

    Returns:
        True if pristine state is now active (captured for the first time, or
        successfully reverted + recaptured). False if the fork is unhealthy
        enough that pristine state could not be established; callers may
        continue but cross-module isolation is degraded.

    Raises:
        _PristineTransportError: if an evm_snapshot/evm_revert RPC call fails
            with a transport-level exception on a path where retrying is safe.
            The caller (`reset_fork_to_pristine`) catches this to drive its
            retry-with-backoff loop against transient RPC flakes.
    """
    snap_id = _session_pristine.get(chain_id)

    if snap_id is None:
        # First time for this chain — capture current state as pristine.
        # A transport exception here is safe to retry: no state has been
        # mutated yet and `_session_pristine[chain_id]` has not been written.
        try:
            resp = web3_instance.provider.make_request("evm_snapshot", [])
        except Exception as e:
            raise _PristineTransportError(f"initial pristine snapshot transport error for chain {chain_id}: {e}") from e
        new_snap = resp.get("result") if isinstance(resp, dict) else None
        if new_snap is None:
            print(f"WARNING: evm_snapshot returned no result for chain {chain_id}: {resp}")
            return False
        _session_pristine[chain_id] = new_snap
        print(f"  [pristine] Captured session pristine {new_snap} for chain {chain_id}")
        return True

    # Revert to pristine. `evm_revert` consumes snap_id AND any snapshots taken
    # after it on this fork, so stale module baselines for this chain are now
    # invalid and must be purged regardless of revert outcome.
    #
    # A transport exception on evm_revert here is safe to retry: either the
    # RPC call never reached Anvil (snap_id still valid for a retry) or it
    # did reach Anvil and consumed the snapshot (in which case the retry will
    # find `reverted=False` and fall through to the best-effort recapture
    # branch deterministically). Either way, retrying cannot silently upgrade
    # a definitive `False` into `True`.
    try:
        resp = web3_instance.provider.make_request("evm_revert", [snap_id])
        reverted = bool(resp.get("result")) if isinstance(resp, dict) else False
    except Exception as e:
        raise _PristineTransportError(f"pristine revert transport error for chain {chain_id}: {e}") from e

    for old_key in list(_module_baselines):
        if old_key[0] == chain_id:
            del _module_baselines[old_key]

    if not reverted:
        # Pristine snapshot gone (fork was restarted mid-session, or anvil_revert
        # returned false). Recapture current state so the NEXT module at least
        # gets a stable reference; cross-module isolation for THIS module is
        # degraded. We intentionally do NOT re-raise transport errors here:
        # the current module's pristine guarantee is already lost once we know
        # `evm_revert` did not succeed, so a retry cannot restore it — retrying
        # would only risk masking this definitive isolation-loss with a later
        # lucky `True`. Keep it deterministic: on any recapture trouble, clear
        # the map and return False.
        print(
            f"WARNING: pristine snapshot {snap_id} for chain {chain_id} invalid; "
            "recapturing current state as best-effort pristine"
        )
        try:
            resp = web3_instance.provider.make_request("evm_snapshot", [])
            new_snap = resp.get("result") if isinstance(resp, dict) else None
        except Exception as e:
            print(f"WARNING: could not recapture pristine after failed revert for chain {chain_id}: {e}")
            _session_pristine.pop(chain_id, None)
            return False
        if new_snap is None:
            _session_pristine.pop(chain_id, None)
            return False
        _session_pristine[chain_id] = new_snap
        return False

    # Recapture pristine at the just-reverted state so the next module can revert.
    # A transport exception here is safe to retry: the revert already succeeded,
    # `snap_id` has been consumed by Anvil, and `_session_pristine[chain_id]`
    # still holds the now-stale id. On retry, the next attempt will see
    # `evm_revert(stale_id) -> False` and fall deterministically into the
    # best-effort recapture branch above — no path exists where a retry turns
    # a definitive failure into an accidental `True`. So raise and let the
    # retry loop absorb the transient flake.
    try:
        resp = web3_instance.provider.make_request("evm_snapshot", [])
    except Exception as e:
        raise _PristineTransportError(
            f"post-revert pristine recapture transport error for chain {chain_id}: {e}"
        ) from e
    new_snap = resp.get("result") if isinstance(resp, dict) else None
    if new_snap is None:
        print(f"WARNING: evm_snapshot returned no result after revert for chain {chain_id}: {resp}")
        _session_pristine.pop(chain_id, None)
        return False
    _session_pristine[chain_id] = new_snap
    print(f"  [pristine] Re-armed session pristine {new_snap} for chain {chain_id}")
    return True


def reset_fork_to_pristine(
    web3_instance: Web3,
    *,
    strict: bool = True,
    attempts: int = 3,
    backoff_s: float = 5.0,
) -> bool:
    """Helper for per-chain `funded_wallet` fixtures: revert to session pristine.

    Call this at the top of a module-scoped `funded_wallet` fixture, BEFORE
    seeding tokens, so each module sees a clean fork independent of prior
    modules on the same chain (VIB-3059).

    On first call per chain: captures current state as pristine (no revert).
    On subsequent calls: reverts fork to the captured pristine state and
    recaptures pristine for the next module.

    By default `strict=True`, so the function raises ``RuntimeError`` if the
    pristine reset cannot be guaranteed — this is the intent-test convention:
    surface infrastructure problems rather than silently running with degraded
    isolation. Pass ``strict=False`` if the caller wants to attempt best-effort
    seeding on a partially healthy fork (returns False on failure in that case).

    To absorb intermittent RPC read-timeouts against long-running Anvil forks
    (#1739), the pristine reset is retried up to ``attempts`` times with a
    fixed ``backoff_s`` delay between attempts — BUT ONLY on a
    ``_PristineTransportError`` raised from inside
    ``_ensure_pristine_and_rearm`` (the transient-flake signal the helper emits
    on paths where a retry is provably safe: no state mutation has been
    committed yet, or the committed mutation makes the retry deterministically
    fall into the best-effort-recapture branch).

    A ``False`` return from ``_ensure_pristine_and_rearm`` is NOT retried: that
    already means the module's pristine-revert guarantee was lost (the session
    pristine was either cleared or only recaptured for future modules), so
    retrying it cannot restore isolation for the current module — it would only
    risk masking a genuine isolation failure by accidentally returning ``True``
    on a subsequent attempt. Narrowing the retry to ``_PristineTransportError``
    preserves the strict isolation contract intent tests depend on.
    """
    try:
        chain_id = int(web3_instance.eth.chain_id)
    except Exception as e:
        msg = f"could not determine chain_id for pristine reset: {e}"
        print(f"WARNING: {msg}")
        if strict:
            raise RuntimeError(msg) from e
        return False

    last_err: BaseException | None = None
    ok = False
    for attempt in range(max(1, attempts)):
        try:
            ok = _ensure_pristine_and_rearm(web3_instance, chain_id)
            # Any result (True OR False) is a definitive verdict from the
            # helper and must be returned as-is. False already means the
            # pristine-revert guarantee is lost for this module; retrying
            # can only hide that failure.
            break
        except _PristineTransportError as e:
            last_err = e
            print(
                f"WARNING: pristine reset attempt {attempt + 1}/{attempts} "
                f"hit transport flake for chain {chain_id}: {e}"
            )
            if attempt + 1 < attempts:
                print(
                    f"  [pristine] retrying reset for chain {chain_id} in {backoff_s}s "
                    f"(attempt {attempt + 2}/{attempts})"
                )
                time.sleep(backoff_s)

    if strict and not ok:
        msg = (
            f"pristine reset could not be established for chain_id={chain_id} "
            f"after {attempts} attempt(s); fork appears unhealthy and module "
            "isolation cannot be guaranteed"
        )
        if last_err is not None:
            raise RuntimeError(msg) from last_err
        raise RuntimeError(msg)
    return ok


def _get_baseline_key(request: pytest.FixtureRequest) -> tuple[int, str]:
    """Build a baseline map key from the current test request.

    Returns:
        Tuple of (chain_id_or_-1, module_path)
    """
    chain_id = -1
    try:
        chain_id = int(request.getfixturevalue("chain_id"))
    except Exception:
        try:
            web3 = request.getfixturevalue("web3")
            if web3 is not None:
                chain_id = int(web3.eth.chain_id)
        except Exception:
            pass
    module_path = request.fspath.strpath if hasattr(request, "fspath") else str(request.node.module)
    return (chain_id, module_path)


def _capture_baseline(web3_instance: Any) -> str | None:
    """Capture a baseline snapshot on the Anvil fork.

    Returns:
        Snapshot ID or None on failure
    """
    try:
        resp = web3_instance.provider.make_request("evm_snapshot", [])
        snapshot_id = resp.get("result")
        if snapshot_id is None:
            print(f"WARNING: evm_snapshot returned no result: {resp}")
        return snapshot_id
    except Exception as e:
        print(f"WARNING: baseline capture failed ({type(e).__name__}: {e})")
        return None


def _revert_to_baseline(web3_instance: Any, snapshot_id: str) -> bool:
    """Revert to a baseline snapshot.

    Returns:
        True if revert succeeded
    """
    try:
        resp = web3_instance.provider.make_request("evm_revert", [snapshot_id])
        return bool(resp.get("result"))
    except Exception as e:
        print(f"WARNING: baseline revert failed ({type(e).__name__}: {e})")
        return False


@pytest.fixture(autouse=True)
def anvil_snapshot(request):
    """Snapshot/revert Anvil state around each test using module baselines.

    On first test in a module: captures baseline after funding is complete
    (late-binding web3 and funded_wallet fixtures).

    On each test: reverts to baseline, then re-arms a new snapshot.

    On revert failure: attempts fork restart -> reseed -> new baseline.

    Requires ``web3`` fixture to be available in the test's scope.
    Tests without a ``web3`` fixture run without snapshot isolation.
    """
    # Skip if no web3 fixture available
    if "web3" not in request.fixturenames:
        yield
        return

    try:
        web3 = request.getfixturevalue("web3")
    except Exception:
        yield
        return

    if web3 is None:
        yield
        return

    key = _get_baseline_key(request)

    # Ensure baseline exists for this module (late-bind funded_wallet)
    if key not in _module_baselines:
        # Trigger funded_wallet if available (ensures funding is done before baseline)
        if "funded_wallet" in request.fixturenames:
            try:
                request.getfixturevalue("funded_wallet")
            except Exception:
                pass

        baseline_id = _capture_baseline(web3)
        if baseline_id is None:
            print("WARNING: Could not capture module baseline; running without isolation")
            yield
            return
        _module_baselines[key] = baseline_id
        print(f"  [baseline] Captured module baseline {baseline_id} for {key[1]}")

    # Revert to baseline before this test
    baseline_id = _module_baselines[key]
    reverted = _revert_to_baseline(web3, baseline_id)

    if not reverted:
        # Attempt recovery: restart fork, reseed, rebuild baseline
        print(f"WARNING: Baseline revert failed for {key[1]}, attempting recovery...")
        recovered = _attempt_recovery(request, web3, key)
        if not recovered:
            pytest.fail(
                f"Anvil recovery failed for module {key[1]} (chain_id={key[0]}). "
                "Fork is unhealthy and state isolation cannot be guaranteed."
            )

    # Re-arm: capture new snapshot for next test's revert
    new_baseline = _capture_baseline(web3)
    if new_baseline is not None:
        _module_baselines[key] = new_baseline
    else:
        print(f"WARNING: Failed to re-arm baseline for {key[1]}; next test may trigger recovery")

    yield

    # No teardown revert needed; the NEXT test's setup reverts to baseline


def _attempt_recovery(request: pytest.FixtureRequest, web3_instance: Any, key: tuple[int, str]) -> bool:
    """Attempt to recover from a failed baseline revert.

    Tries to restart the Anvil fork via the anvil_instance fixture,
    reseed the wallet, and capture a new baseline.

    Returns:
        True if recovery succeeded
    """
    try:
        anvil = request.getfixturevalue("anvil_instance")
    except Exception as e:
        print(f"WARNING: Recovery unavailable (anvil_instance fixture not found): {e}")
        return False

    try:
        chain_name = str(getattr(anvil, "chain", key[0]))
        recovered = False
        for attempt in range(1, TEST_ANVIL_RECOVERY_MAX_RESTARTS + 1):
            recovered, _ = _force_restart_anvil(anvil, chain_name, attempt)
            if recovered:
                break
        if not recovered:
            print("WARNING: Fork restart failed during recovery")
            return False

        try:
            reseed_wallet_state = request.getfixturevalue("reseed_wallet_state")
        except Exception as e:
            print(f"WARNING: Recovery unavailable (reseed_wallet_state fixture not found): {e}")
            return False

        if not callable(reseed_wallet_state):
            print("WARNING: Recovery unavailable (reseed_wallet_state is not callable)")
            return False

        try:
            reseed_wallet_state()
        except Exception as e:
            print(f"WARNING: Re-funding failed during recovery: {e}")
            return False

        # Capture new baseline after restart + reseed
        recovered_rpc_url = anvil.get_rpc_url()
        recovered_web3 = make_intent_test_web3(recovered_rpc_url)
        new_baseline = _capture_baseline(recovered_web3)
        if new_baseline is not None:
            _module_baselines[key] = new_baseline
            print(f"  [baseline] Recovery successful, new baseline {new_baseline}")
            return True
        return False
    except Exception as e:
        print(f"WARNING: Recovery attempt failed: {e}")
        return False


@pytest_asyncio.fixture(autouse=True)
async def close_web3_async_http_sessions():
    """Close leaked aiohttp ClientSessions created by web3 AsyncHTTPProvider between tests."""
    yield

    for provider in list(_ASYNC_HTTP_PROVIDERS):
        manager = getattr(provider, "_request_session_manager", None)
        if manager is None:
            continue

        # Close cached async sessions (aiohttp ClientSession)
        for _, session in manager.session_cache.items():
            closed = getattr(session, "closed", True)
            if not closed:
                try:
                    await session.close()
                except Exception:
                    # Best-effort cleanup; don't fail tests on teardown.
                    pass

        manager.session_cache.clear()


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def web3() -> Web3:
    """Web3 connection to Anvil."""
    if not is_anvil_running(ANVIL_URL):
        pytest.skip("Anvil is not running. Start Anvil first.")

    w3 = Web3(Web3.HTTPProvider(ANVIL_URL, request_kwargs={"timeout": TEST_WEB3_REQUEST_TIMEOUT}))
    assert w3.is_connected(), "Failed to connect to Anvil"
    return w3


@pytest.fixture(scope="module")
def chain_id(web3: Web3) -> int:
    """Get the chain ID from Anvil."""
    return web3.eth.chain_id


@pytest.fixture(scope="module")
def chain_name(chain_id: int) -> str:
    """Get chain name from chain ID."""
    for name, config in CHAIN_CONFIGS.items():
        if config["chain_id"] == chain_id:
            return name
    pytest.skip(f"Unsupported chain ID: {chain_id}")
    return ""  # Unreachable, but needed for type checker


@pytest.fixture(scope="module")
def chain_config(chain_name: str) -> dict:
    """Get chain configuration."""
    return CHAIN_CONFIGS[chain_name]


@pytest.fixture(scope="module")
def test_private_key() -> str:
    """Private key for test wallet."""
    return TEST_PRIVATE_KEY


@pytest.fixture(scope="module")
def funded_wallet(
    web3: Web3,
    chain_config: dict,
) -> str:
    """Fund test wallet with native token and ERC20 tokens."""
    wallet = TEST_WALLET

    # Fund with native token (10 ETH/AVAX/etc)
    native_amount = 10 * 10**18
    fund_native_token(wallet, native_amount, ANVIL_URL)

    # Fund with all configured tokens
    tokens = chain_config.get("tokens", {})
    balance_slots = chain_config.get("balance_slots", {})

    for token_symbol, token_address in tokens.items():
        if token_symbol not in balance_slots:
            print(f"Warning: No balance slot for {token_symbol}, skipping funding")
            continue
        balance_slot = balance_slots[token_symbol]

        # Get token decimals
        decimals = get_token_decimals(web3, token_address)

        # Fund with 1 million tokens
        amount = 1_000_000 * (10**decimals)
        fund_erc20_token(wallet, token_address, amount, balance_slot, ANVIL_URL)

        # Verify funding
        balance = get_token_balance(web3, token_address, wallet)
        print(f"  Funded {token_symbol}: {format_token_amount(balance, decimals)}")

    return wallet


# =============================================================================
# Session-Scoped Price Oracles (One Per Chain)
# =============================================================================
#
# Similar to Anvil fixtures, we create session-scoped price oracle fixtures
# per chain. This ensures prices are fetched ONCE at session start, aligned
# with when the Anvil fork is created. This eliminates flakiness caused by
# price divergence between CoinGecko (live) and Anvil fork (frozen state).
#
# NOTE: These fixtures use a direct CoinGecko HTTP call (no gateway dependency)
# so that only the tested chain's Anvil fork needs to start.
# =============================================================================

from almanak.gateway.data.price.coingecko import GLOBAL_TOKEN_IDS


def _fetch_prices_sync(chain_name: str) -> dict[str, Decimal]:
    """Fetch prices synchronously via direct CoinGecko HTTP call.

    Uses GLOBAL_TOKEN_IDS to resolve token symbols to CoinGecko IDs,
    then makes a single batch /simple/price request. Supports both
    free and pro CoinGecko API via COINGECKO_API_KEY env var.

    Args:
        chain_name: Chain name to fetch prices for

    Returns:
        Dict mapping token symbols to USD prices
    """
    config = CHAIN_CONFIGS.get(chain_name, {})
    token_symbols = list(config.get("tokens", {}).keys())

    # Resolve symbols to CoinGecko IDs
    symbol_to_cg_id: dict[str, str] = {}
    for symbol in token_symbols:
        cg_id = GLOBAL_TOKEN_IDS.get(symbol.upper())
        if cg_id is None:
            raise ValueError(
                f"No CoinGecko ID found for token '{symbol}'. "
                f"Add it to GLOBAL_TOKEN_IDS in almanak/gateway/data/price/coingecko.py"
            )
        symbol_to_cg_id[symbol] = cg_id

    # Deduplicate CoinGecko IDs (e.g. USDC and USDC.E both map to "usd-coin")
    unique_cg_ids = sorted(set(symbol_to_cg_id.values()))

    # Determine API host and headers
    api_key = os.environ.get("COINGECKO_API_KEY", "")
    if api_key:
        base_url = "https://pro-api.coingecko.com/api/v3/simple/price"
        headers = {"x-cg-pro-api-key": api_key}
    else:
        base_url = "https://api.coingecko.com/api/v3/simple/price"
        headers = {}

    params = {"ids": ",".join(unique_cg_ids), "vs_currencies": "usd"}

    # Fetch with retry on 429
    print(f"\n  Fetching prices for {chain_name} via direct CoinGecko HTTP:")
    resp = None
    for attempt in range(3):
        resp = requests.get(base_url, params=params, headers=headers, timeout=15)
        if resp.status_code == 429:
            backoff = attempt + 1  # 1s, 2s
            print(f"    Rate limited (429), retrying in {backoff}s (attempt {attempt + 1}/3)...")
            time.sleep(backoff)
            continue
        resp.raise_for_status()
        break
    else:
        raise RuntimeError(
            f"CoinGecko rate limited after 3 attempts for {chain_name}. "
            f"Set COINGECKO_API_KEY env var for higher limits."
        )

    data = resp.json()

    # Build symbol -> price map
    prices: dict[str, Decimal] = {}
    missing = []
    for symbol, cg_id in symbol_to_cg_id.items():
        entry = data.get(cg_id, {})
        usd_price = entry.get("usd")
        if usd_price is None:
            missing.append(f"{symbol} (cg_id={cg_id})")
            continue
        prices[symbol] = Decimal(str(usd_price))
        print(f"    {symbol}: ${usd_price}")

    if missing:
        raise RuntimeError(
            f"CoinGecko returned no USD price for: {', '.join(missing)}. Response keys: {list(data.keys())}"
        )

    return prices


def _create_price_oracle_fixture(chain_name: str):
    """Factory function to create session-scoped price oracle per chain.

    Similar to Anvil fixture pattern in conftest_gateway.py, creates a
    separate fixture per chain that fetches prices once per session.

    Args:
        chain_name: Chain name (e.g., "arbitrum", "base", "bsc")

    Returns:
        A pytest fixture function
    """

    @pytest.fixture(scope="session")
    def price_oracle_fixture() -> dict[str, Decimal]:
        """Fetch prices once per session for all tokens in this chain.

        Uses direct CoinGecko HTTP call to avoid gateway dependency.

        Returns:
            Dict mapping token symbols to USD prices
        """
        return _fetch_prices_sync(chain_name)

    return price_oracle_fixture


# Create session-scoped price oracle fixtures for each supported chain
# (matches the chains that have Anvil fixtures in conftest_gateway.py)
price_oracle_arbitrum = _create_price_oracle_fixture("arbitrum")
price_oracle_base = _create_price_oracle_fixture("base")
price_oracle_ethereum = _create_price_oracle_fixture("ethereum")
price_oracle_avalanche = _create_price_oracle_fixture("avalanche")
price_oracle_bsc = _create_price_oracle_fixture("bsc")
price_oracle_bnb = _create_price_oracle_fixture("bnb")  # Alias for bsc
price_oracle_mantle = _create_price_oracle_fixture("mantle")
price_oracle_optimism = _create_price_oracle_fixture("optimism")
price_oracle_polygon = _create_price_oracle_fixture("polygon")
price_oracle_linea = _create_price_oracle_fixture("linea")
price_oracle_monad = _create_price_oracle_fixture("monad")
price_oracle_xlayer = _create_price_oracle_fixture("xlayer")


# =============================================================================
# Backward-Compatible Price Oracle Selector
# =============================================================================


@pytest.fixture(scope="module")
def price_oracle(chain_name: str, request) -> dict[str, Decimal]:
    """Select the appropriate session-scoped price oracle for this chain.

    This fixture maintains backward compatibility with existing tests
    while routing to the session-scoped oracle for the specific chain.

    The session-scoped oracles fetch prices once at session start,
    ensuring alignment with the Anvil fork block state.

    Args:
        chain_name: Chain name from the chain_name fixture
        request: Pytest request object for fixture access

    Returns:
        Dict mapping token symbols to USD prices
    """
    # Map chain names to their session-scoped fixtures
    fixture_map = {
        "arbitrum": "price_oracle_arbitrum",
        "base": "price_oracle_base",
        "ethereum": "price_oracle_ethereum",
        "mantle": "price_oracle_mantle",
        "monad": "price_oracle_monad",
        "avalanche": "price_oracle_avalanche",
        "bsc": "price_oracle_bsc",
        "bnb": "price_oracle_bnb",
        "optimism": "price_oracle_optimism",
        "polygon": "price_oracle_polygon",
        "linea": "price_oracle_linea",
        "xlayer": "price_oracle_xlayer",
    }

    fixture_name = fixture_map.get(chain_name)
    if not fixture_name:
        pytest.skip(f"No price oracle fixture for chain: {chain_name}")

    return request.getfixturevalue(fixture_name)


# =============================================================================
# Zodiac Fixture (Phase G.1 pilot)
# =============================================================================
#
# Tests opt into Safe+Zodiac execution by marking themselves with
# ``@pytest.mark.uses_zodiac(protocols=[...], intent_types=[...], config={...})``.
# The marker declares the manifest scope — the same triple ``generate_manifest``
# takes.
#
# When a test has the marker, the ``zodiac_safe`` fixture below:
#   1. Deploys a fresh Safe + Roles Modifier on the per-chain Anvil fork.
#   2. Assigns the member EOA (``TEST_WALLET`` / ``TEST_PRIVATE_KEY``, reused
#      from the existing EOA path so the test's signing key is unchanged) to a
#      per-test role key on the Roles Modifier.
#   3. Generates the manifest from the marker's ``protocols`` / ``intent_types``
#      / ``config`` kwargs and applies its targets under the role key.
#   4. Seeds the Safe with the same CHAIN_CONFIGS ERC-20 balances that
#      ``funded_wallet`` would have received on the EOA path.
#
# The per-chain conftests (e.g. ``tests/intents/arbitrum/conftest.py``) then
# override ``funded_wallet`` (to return the Safe) and ``orchestrator`` (to
# route through ``Roles.execTransactionWithRole``) when the marker is present.
# Unmarked tests see no change: ``zodiac_safe`` yields ``None`` for them.
#
# Design decisions (see G.1 PR body for rationale):
#   - Fixture scope: per-test (default). Safe + Roles deploy ≈ 1-2s/test; the
#     safety of per-test isolation outweighs the saved wall-clock for the
#     pilot. G.2 may re-evaluate per-class / session scope once overheads
#     accumulate.
#   - Manifest generation: eager, from ``marker.kwargs``. Mirrors how a
#     strategy declares its permission surface at config time.
#   - Token funding: eager — seed every ``CHAIN_CONFIGS[chain].tokens`` token
#     the EOA would have received. Storage-slot writes are cheap and it keeps
#     the pilot's marker payload minimal.
#   - Marker kwargs shape: ``protocols: list[str]``, ``intent_types: list[str]``,
#     ``config: dict[str, Any]``. Mirrors ``generate_manifest``.
# =============================================================================


# Anvil's second pre-funded account (mnemonic "test test test test test test
# test test test test test junk", account index 1). Used as the Safe owner so
# that the *member* (the role-holder who signs ``execTransactionWithRole``)
# remains ``TEST_WALLET`` and existing tests' signing keys keep working
# unchanged. Safe owner vs role-member separation is a correctness property of
# Zodiac Roles — the Safe's execTransaction calls (enableModule, scopeTarget,
# allowFunction, revokeTarget, assignRoles, setDefaultRole) MUST be signed by
# the Safe owner, not by the role member.
ZODIAC_OWNER_ADDRESS = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"
ZODIAC_OWNER_PRIVATE_KEY = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"


from dataclasses import dataclass as _zodiac_dataclass


@_zodiac_dataclass
class ZodiacContext:
    """Per-test Safe+Zodiac harness wiring for ``uses_zodiac``-marked tests.

    Fields:
        safe_address: Newly-deployed Safe v1.4.1 proxy. Holds the tokens the
            test spends; ``funded_wallet`` returns this when the marker is
            present.
        roles_address: Newly-deployed Zodiac Roles Modifier v2 proxy,
            ``owner == avatar == target == safe_address``, enabled on the Safe.
        role_key: 32-byte role key the manifest's targets were applied under
            and the member's membership was granted for. Also the role key
            passed to ``execTransactionWithRole``.
        owner_eoa / owner_private_key: EOA that signed the Safe administrative
            transactions (``enableModule``, ``scopeTarget``, ``allowFunction``,
            etc.). Distinct from the member to enforce the owner/member split.
        member_eoa / member_private_key: EOA whose signature goes into each
            ``execTransactionWithRole`` call. Defaults to ``TEST_WALLET`` /
            ``TEST_PRIVATE_KEY`` so tests' existing signing keys keep working.
        manifest_targets: Output of ``PermissionManifest.to_zodiac_targets()``
            — the target list that was applied on-chain. Retained so negative
            tests (future G-phase work) can identify which target to revoke.
    """

    safe_address: str
    roles_address: str
    role_key: bytes
    owner_eoa: str
    owner_private_key: str
    member_eoa: str
    member_private_key: str
    manifest_targets: list[dict]


def _build_zodiac_context(
    web3: Web3,
    chain: str,
    anvil_rpc_url: str,
    *,
    member_eoa: str = TEST_WALLET,
    member_private_key: str = TEST_PRIVATE_KEY,
) -> "ZodiacContext":
    """Deploy Safe+Roles, assign role, seed tokens. Manifest is applied later.

    Phase G post-pivot (opt-out model): the manifest is generated and applied
    at execute-time from the intents the test actually compiles, not at
    fixture setup from marker kwargs. This helper is therefore stripped to
    just the on-chain plumbing — Safe deploy, Roles deploy, role assignment,
    eager token seeding. The ``ZodiacOrchestrator`` consumes the recorded
    intents and calls ``apply_manifest_targets`` itself before each execute.

    Side effects:
      - ``anvil_setBalance`` funds ``ZODIAC_OWNER_ADDRESS`` with 10 native
        tokens so the owner can pay gas for Safe admin txs.
      - ``fund_erc20_token`` best-effort seeds the Safe with every token+slot
        pair in ``CHAIN_CONFIGS[chain]``. Without a marker, the fixture has
        no list of "required" tokens — failures during setup log a warning;
        if a token is genuinely needed, the test surfaces a clear "balance 0"
        error at execute time.
    """
    # Local imports to keep the shared conftest from taking a hard import
    # dependency on Safe machinery at collection time. Only tests that
    # actually use ``zodiac_safe`` pay the cost.
    from tests.intents._zodiac_helpers import (
        assign_role_to_member,
        deploy_test_safe,
        deploy_test_zodiac_roles,
    )

    # Owner gas. ``anvil_setBalance`` is idempotent and cheap.
    fund_native_token(ZODIAC_OWNER_ADDRESS, 10 * 10**18, anvil_rpc_url)

    safe = deploy_test_safe(web3, ZODIAC_OWNER_ADDRESS, ZODIAC_OWNER_PRIVATE_KEY)
    # Fund the Safe itself with native tokens. Intents that send ETH value
    # (wrap, native swaps, native-leg LP mints) would otherwise revert with an
    # opaque insufficient-balance error — the Safe's own native balance matters
    # regardless of what the member EOA holds.
    fund_native_token(safe, 10 * 10**18, anvil_rpc_url)
    roles = deploy_test_zodiac_roles(web3, safe, ZODIAC_OWNER_ADDRESS, ZODIAC_OWNER_PRIVATE_KEY)
    # Single fixed role label per chain — the manifest is keyed by this role
    # at apply-time, and the orchestrator passes the same key into
    # ``execTransactionWithRole``. Was previously per-protocol-set keyed off
    # the marker; under late-binding the protocol set isn't known yet, so the
    # label is just the chain name.
    role_label = f"zodiac-fixture:{chain}".encode()
    role_key = role_label[:32].ljust(32, b"\0")

    assign_role_to_member(
        web3,
        roles,
        safe,
        role_key,
        member_eoa=member_eoa,
        owner_eoa=ZODIAC_OWNER_ADDRESS,
        owner_private_key=ZODIAC_OWNER_PRIVATE_KEY,
    )

    # Eager token seeding — fund the Safe with the same tokens the EOA would
    # have received. The existing module-scoped ``funded_wallet`` seeding has
    # already run (it seeded ``TEST_WALLET``); here we mirror the same set
    # onto the Safe. Storage-slot writes are cheap (~20ms/token on Anvil), so
    # we don't bother filtering by what the test will actually spend.
    chain_cfg = CHAIN_CONFIGS[chain]
    for token_symbol, token_address in chain_cfg.get("tokens", {}).items():
        balance_slot = chain_cfg.get("balance_slots", {}).get(token_symbol)
        if balance_slot is None:
            continue
        try:
            decimals = get_token_decimals(web3, token_address)
        except Exception:  # noqa: BLE001 — best-effort
            continue
        if token_symbol in ("WETH", "WAVAX", "WMATIC", "WBNB", "WMON", "W0G"):
            # Wrapped-native tokens: use direct storage-slot writes for the
            # Safe (wrapping requires an EOA→WETH self-call, which would
            # need to go through execTransactionWithRole — a chicken/egg
            # problem during setup). Storage writes give the same end-state.
            amount = 10 * (10**decimals)
        else:
            amount = 100_000 * (10**decimals)
        try:
            fund_erc20_token(safe, token_address, amount, balance_slot, anvil_rpc_url)
        except Exception as exc:  # noqa: BLE001 — token-seeding failures shouldn't hide authz errors
            print(f"  [zodiac_safe] warning: could not fund Safe with {token_symbol}: {exc}")

    # sUSDai is not in CHAIN_CONFIGS["arbitrum"]["tokens"] (see seed_arbitrum_susdai)
    # so the loop above skips it; mirror the EOA seed onto the Safe explicitly.
    if chain == "arbitrum":
        seed_arbitrum_susdai(safe, web3, anvil_rpc_url)

    return ZodiacContext(
        safe_address=safe,
        roles_address=roles,
        role_key=role_key,
        owner_eoa=ZODIAC_OWNER_ADDRESS,
        owner_private_key=ZODIAC_OWNER_PRIVATE_KEY,
        member_eoa=member_eoa,
        member_private_key=member_private_key,
        # Empty: the orchestrator generates and applies targets at execute
        # time from the intents the test compiles. This list is retained on
        # the dataclass for backward compat with the runner-based negative
        # tests in ``tests/intents/<chain>/test_permission_onchain.py``.
        manifest_targets=[],
    )


def no_zodiac_marker(request: pytest.FixtureRequest) -> pytest.Mark | None:
    """Return the ``no_zodiac`` opt-out marker on the current test, or ``None``.

    The opt-out model (post-Phase-G pivot): every intent test runs through
    Safe+Zodiac by default. Tests that legitimately can't run that way —
    aggregator tests with non-deterministic routing, EOA-specific failure
    paths, infrastructure tests that bypass the orchestrator — opt out via
    ``@pytest.mark.no_zodiac(reason="...")``.
    """
    return request.node.get_closest_marker("no_zodiac")


def uses_zodiac_marker(request: pytest.FixtureRequest) -> pytest.Mark | None:
    """Return the legacy ``uses_zodiac`` marker on the current test, or ``None``.

    Retained for transition only — under the opt-out model, the marker has
    no effect; tests run under Zodiac by default. Will be removed once all
    stale ``uses_zodiac`` decorators are deleted from the test files.
    """
    return request.node.get_closest_marker("uses_zodiac")


@pytest.fixture
def _zodiac_intent_recorder(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> list[Any]:
    """Capture every intent passed to ``IntentCompiler.compile`` during the test.

    Returns a live ``list`` that the orchestrator reads at execute time to
    derive a manifest from the test's actual intents (late-binding). The
    monkey-patch is auto-reverted at fixture teardown via ``monkeypatch``.

    No-op for opt-out tests: the captured list is returned empty and no
    monkey-patch is installed, so the standard ``IntentCompiler.compile`` is
    untouched.
    """
    captured: list[Any] = []
    if no_zodiac_marker(request) is not None:
        return captured

    from almanak.framework.intents.compiler import IntentCompiler

    original_compile = IntentCompiler.compile

    def recording_compile(self, intent):  # type: ignore[no-redef]
        captured.append(intent)
        return original_compile(self, intent)

    monkeypatch.setattr(IntentCompiler, "compile", recording_compile)
    return captured


@pytest.fixture
def zodiac_safe(
    request: pytest.FixtureRequest,
) -> ZodiacContext | None:
    """Per-test Safe+Zodiac context for opt-in-by-default intent tests.

    Returns ``None`` only when the test carries ``@pytest.mark.no_zodiac(...)``;
    otherwise deploys a fresh Safe + Roles Modifier on the chain's Anvil fork,
    seeds the Safe with the chain's stock token balances, and returns the
    context that the chain conftest's ``funded_wallet`` / ``orchestrator``
    overrides consume.

    Manifest is NOT applied at fixture setup. The ``ZodiacOrchestrator`` reads
    intents the test compiles (captured via ``_zodiac_intent_recorder``) and
    calls ``apply_manifest_targets`` itself before each execute. This is the
    "late-binding" path — the manifest scope matches what the test actually
    does, not what a marker declares.

    Scope is per-test (``function``). A Safe + Roles deploy is ~1-2s on Anvil;
    per-test keeps state leakage impossible.
    """
    if no_zodiac_marker(request) is not None:
        return None

    # Chain-local fixtures. Delayed lookup — opt-out tests never run this
    # block, so they don't force the Anvil fork to spin up.
    web3: Web3 = request.getfixturevalue("web3")
    anvil_rpc_url: str = request.getfixturevalue("anvil_rpc_url")
    # Prefer the per-chain fixture; fall back to deriving from chain_id if a
    # future chain conftest skips the explicit ``chain_name`` fixture.
    try:
        chain: str = request.getfixturevalue("chain_name")
    except pytest.FixtureLookupError:
        chain_id_val: int = web3.eth.chain_id
        chain = get_chain_name_from_id(chain_id_val)

    return _build_zodiac_context(
        web3=web3,
        chain=chain,
        anvil_rpc_url=anvil_rpc_url,
    )


# =============================================================================
# Intent-Coverage Marker Hook (VIB-4303 / VIB-4298 Phase 2)
# =============================================================================
#
# Every test collected from ``tests/intents/`` MUST carry an
# ``@pytest.mark.intent(IntentType.X, ...)`` marker — function level,
# class level, or module-level ``pytestmark``. This runtime hook fails
# collection on any item missing the marker, complementing the static
# AST check in ``scripts/ci/check_intent_coverage.py``.
#
# Two layers because they catch different cases:
#
# * Static (``check_intent_coverage.py``): runs in ``make lint`` without
#   importing the test modules. Fast, AST-based, catches simple
#   missing-decorator cases.
# * Runtime (this hook): runs at pytest collection. Catches dynamically
#   generated test items — ``@pytest.mark.parametrize`` expansions,
#   fixture-generated tests, anything where the test function name only
#   exists after collection. The AST scan sees the source function once;
#   collection emits one ``Item`` per parameter set.
#
# Both layers enforce the same contract (marker present somewhere up the
# scope chain), so passing one and failing the other indicates a real
# bug in the scan logic, not a benign tooling difference.


def pytest_collection_modifyitems(config, items):  # type: ignore[no-untyped-def]
    """Fail collection if any item under tests/intents/ lacks an intent marker.

    Hard-fails by raising ``pytest.UsageError``, which pytest renders as
    "INTERNALERROR>" and prevents the session from continuing. We do this
    instead of marking items as failed so the user sees a single clear
    message rather than N test failures.
    """
    missing: list[tuple[str, str]] = []
    for item in items:
        path = str(item.fspath)
        # Only enforce on items physically under tests/intents/. Items
        # collected from outside (e.g. shared conftest plugins) are out
        # of scope for this gate.
        if "/tests/intents/" not in path and not path.endswith("/tests/intents"):
            continue
        if not list(item.iter_markers("intent")):
            missing.append((item.nodeid, path))

    if missing:
        lines = [
            "Intent-coverage marker hook (VIB-4303): "
            f"{len(missing)} test item(s) under tests/intents/ are missing the "
            "@pytest.mark.intent(IntentType.X, ...) marker.",
            "",
            "Add the marker at function, enclosing class, or module level:",
            "",
            "    import pytest",
            "    from almanak.framework.intents.vocabulary import IntentType",
            "",
            "    @pytest.mark.intent(IntentType.SWAP)",
            "    async def test_swap_usdc_to_weth(...):",
            "        ...",
            "",
            "Or module-level:",
            "",
            "    pytestmark = pytest.mark.intent(IntentType.SWAP)",
            "",
            "Offending items:",
        ]
        for nodeid, _path in missing:
            lines.append(f"  • {nodeid}")
        raise pytest.UsageError("\n".join(lines))
