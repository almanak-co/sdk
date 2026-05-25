"""Solana compilation helpers extracted from IntentCompiler.

These standalone functions receive the compiler instance as their first
parameter and implement Solana spot/lending compilation logic (Jupiter,
Kamino, Jupiter Lend). Drift perps live in the Drift connector compiler;
Raydium / Meteora / Orca LP live in their respective connector compilers.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from .compiler_models import CompilationResult, CompilationStatus

if TYPE_CHECKING:
    from .vocabulary import (
        BorrowIntent,
        RepayIntent,
        SupplyIntent,
        SwapIntent,
        WithdrawIntent,
    )

logger = logging.getLogger("almanak.framework.intents.compiler")


# =============================================================================
# Adapter caching helpers
# =============================================================================


def get_jupiter_adapter(compiler) -> Any:
    """Get or create a cached JupiterAdapter instance."""
    if compiler._cached_jupiter_adapter is None:
        from almanak.framework.connectors.jupiter import JupiterAdapter, JupiterConfig

        config = JupiterConfig(wallet_address=compiler.wallet_address)
        compiler._cached_jupiter_adapter = JupiterAdapter(
            config=config,
            price_provider=compiler.price_oracle,
            allow_placeholder_prices=compiler.price_oracle is None,
            token_resolver=compiler._token_resolver,
        )
    else:
        # Update price provider on cached adapter in case prices have changed
        compiler._cached_jupiter_adapter.price_provider = compiler.price_oracle
        compiler._cached_jupiter_adapter.allow_placeholder_prices = compiler.price_oracle is None
    return compiler._cached_jupiter_adapter


def get_kamino_adapter(compiler, *, needs_rpc: bool = False) -> Any:
    """Get or create a cached KaminoAdapter instance."""
    if needs_rpc:
        if compiler._cached_kamino_adapter_with_rpc is None:
            from almanak.framework.connectors.kamino import KaminoAdapter, KaminoConfig

            config = KaminoConfig(wallet_address=compiler.wallet_address)
            compiler._cached_kamino_adapter_with_rpc = KaminoAdapter(
                config=config, token_resolver=compiler._token_resolver
            )
        return compiler._cached_kamino_adapter_with_rpc
    if compiler._cached_kamino_adapter is None:
        from almanak.framework.connectors.kamino import KaminoAdapter, KaminoConfig

        config = KaminoConfig(wallet_address=compiler.wallet_address)
        compiler._cached_kamino_adapter = KaminoAdapter(config=config, token_resolver=compiler._token_resolver)
    return compiler._cached_kamino_adapter


def get_jupiter_lend_adapter(compiler) -> Any:
    """Get or create a cached JupiterLendAdapter instance."""
    if compiler._cached_jupiter_lend_adapter is None:
        from almanak.framework.connectors.jupiter_lend import JupiterLendAdapter, JupiterLendConfig

        config = JupiterLendConfig(wallet_address=compiler.wallet_address)
        compiler._cached_jupiter_lend_adapter = JupiterLendAdapter(
            config=config, token_resolver=compiler._token_resolver
        )
    return compiler._cached_jupiter_lend_adapter


# =============================================================================
# Jupiter swap
# =============================================================================


def compile_jupiter_swap(compiler, intent: SwapIntent) -> CompilationResult:
    """Compile a SWAP intent using Jupiter for Solana chains.

    Args:
        compiler: IntentCompiler instance
        intent: SwapIntent to compile

    Returns:
        CompilationResult with Jupiter ActionBundle
    """
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    try:
        adapter = get_jupiter_adapter(compiler)
        bundle = adapter.compile_swap_intent(intent, price_oracle=compiler.price_oracle)

        if bundle.metadata.get("error"):
            result.status = CompilationStatus.FAILED
            result.error = bundle.metadata["error"]
        else:
            result.action_bundle = bundle
    except Exception as e:
        logger.exception(f"Jupiter swap compilation failed: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)
    return result


# =============================================================================
# Kamino lending
# =============================================================================


def compile_kamino_supply(compiler, intent: SupplyIntent) -> CompilationResult:
    """Compile a SUPPLY intent using Kamino for Solana chains."""
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    try:
        adapter = get_kamino_adapter(compiler)
        bundle = adapter.compile_supply_intent(intent)

        if bundle.metadata.get("error"):
            result.status = CompilationStatus.FAILED
            result.error = bundle.metadata["error"]
        else:
            result.action_bundle = bundle
    except Exception as e:
        logger.exception(f"Kamino supply compilation failed: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)
    return result


def compile_kamino_borrow(compiler, intent: BorrowIntent) -> CompilationResult:
    """Compile a BORROW intent using Kamino for Solana chains."""
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    try:
        adapter = get_kamino_adapter(compiler)
        bundle = adapter.compile_borrow_intent(intent)

        if bundle.metadata.get("error"):
            result.status = CompilationStatus.FAILED
            result.error = bundle.metadata["error"]
        else:
            result.action_bundle = bundle
    except Exception as e:
        logger.exception(f"Kamino borrow compilation failed: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)
    return result


def compile_kamino_repay(compiler, intent: RepayIntent) -> CompilationResult:
    """Compile a REPAY intent using Kamino for Solana chains."""
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    try:
        adapter = get_kamino_adapter(compiler)
        bundle = adapter.compile_repay_intent(intent)

        if bundle.metadata.get("error"):
            result.status = CompilationStatus.FAILED
            result.error = bundle.metadata["error"]
        else:
            result.action_bundle = bundle
    except Exception as e:
        logger.exception(f"Kamino repay compilation failed: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)
    return result


def compile_kamino_withdraw(compiler, intent: WithdrawIntent) -> CompilationResult:
    """Compile a WITHDRAW intent using Kamino for Solana chains."""
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    try:
        adapter = get_kamino_adapter(compiler)
        bundle = adapter.compile_withdraw_intent(intent)

        if bundle.metadata.get("error"):
            result.status = CompilationStatus.FAILED
            result.error = bundle.metadata["error"]
        else:
            result.action_bundle = bundle
    except Exception as e:
        logger.exception(f"Kamino withdraw compilation failed: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)
    return result


# =============================================================================
# Jupiter Lend
# =============================================================================


def compile_jupiter_lend_supply(compiler, intent: SupplyIntent) -> CompilationResult:
    """Compile a SUPPLY intent using Jupiter Lend for Solana chains."""
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    try:
        adapter = get_jupiter_lend_adapter(compiler)
        bundle = adapter.compile_supply_intent(intent)

        if bundle.metadata.get("error"):
            result.status = CompilationStatus.FAILED
            result.error = bundle.metadata["error"]
        else:
            result.action_bundle = bundle
    except Exception as e:
        logger.exception(f"Jupiter Lend supply compilation failed: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)
    return result


def compile_jupiter_lend_borrow(compiler, intent: BorrowIntent) -> CompilationResult:
    """Compile a BORROW intent using Jupiter Lend for Solana chains."""
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    try:
        adapter = get_jupiter_lend_adapter(compiler)
        bundle = adapter.compile_borrow_intent(intent)

        if bundle.metadata.get("error"):
            result.status = CompilationStatus.FAILED
            result.error = bundle.metadata["error"]
        else:
            result.action_bundle = bundle
    except Exception as e:
        logger.exception(f"Jupiter Lend borrow compilation failed: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)
    return result


def compile_jupiter_lend_repay(compiler, intent: RepayIntent) -> CompilationResult:
    """Compile a REPAY intent using Jupiter Lend for Solana chains."""
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    try:
        adapter = get_jupiter_lend_adapter(compiler)
        bundle = adapter.compile_repay_intent(intent)

        if bundle.metadata.get("error"):
            result.status = CompilationStatus.FAILED
            result.error = bundle.metadata["error"]
        else:
            result.action_bundle = bundle
    except Exception as e:
        logger.exception(f"Jupiter Lend repay compilation failed: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)
    return result


def compile_jupiter_lend_withdraw(compiler, intent: WithdrawIntent) -> CompilationResult:
    """Compile a WITHDRAW intent using Jupiter Lend for Solana chains."""
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    try:
        adapter = get_jupiter_lend_adapter(compiler)
        bundle = adapter.compile_withdraw_intent(intent)

        if bundle.metadata.get("error"):
            result.status = CompilationStatus.FAILED
            result.error = bundle.metadata["error"]
        else:
            result.action_bundle = bundle
    except Exception as e:
        logger.exception(f"Jupiter Lend withdraw compilation failed: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)
    return result


# =============================================================================
# Raydium / Meteora / Orca LP — owned by connectors/{raydium,meteora,orca}/
# compiler.py. Dispatched via almanak.framework.connectors.compiler_registry.
# =============================================================================
