"""Solana compilation helpers extracted from IntentCompiler.

These standalone functions receive the compiler instance as their first
parameter and implement all Solana-related compilation logic (Jupiter,
Kamino, Raydium, Meteora, Orca, Drift).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from .compiler_models import CompilationResult, CompilationStatus

if TYPE_CHECKING:
    from .vocabulary import (
        BorrowIntent,
        LPCloseIntent,
        LPOpenIntent,
        PerpCloseIntent,
        PerpOpenIntent,
        RepayIntent,
        SupplyIntent,
        SwapIntent,
        WithdrawIntent,
    )

logger = logging.getLogger(__name__)


# =============================================================================
# Chain detection
# =============================================================================


def is_solana_chain(compiler) -> bool:
    """Check if the compiler's target chain is in the Solana family."""
    try:
        from almanak.core.enums import Chain, ChainFamily, get_chain_family

        chain_enum = Chain(compiler.chain.upper())
        return get_chain_family(chain_enum) == ChainFamily.SOLANA
    except (ValueError, KeyError):
        return False


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


def get_raydium_adapter(compiler, *, needs_rpc: bool = False) -> Any:
    """Get or create a cached RaydiumAdapter instance."""
    if needs_rpc:
        if compiler._cached_raydium_adapter_with_rpc is None:
            from almanak.framework.connectors.raydium import RaydiumAdapter, RaydiumConfig

            config = RaydiumConfig(wallet_address=compiler.wallet_address, rpc_url=compiler.rpc_url or "")
            compiler._cached_raydium_adapter_with_rpc = RaydiumAdapter(
                config=config, token_resolver=compiler._token_resolver
            )
        return compiler._cached_raydium_adapter_with_rpc
    if compiler._cached_raydium_adapter is None:
        from almanak.framework.connectors.raydium import RaydiumAdapter, RaydiumConfig

        config = RaydiumConfig(wallet_address=compiler.wallet_address)
        compiler._cached_raydium_adapter = RaydiumAdapter(config=config, token_resolver=compiler._token_resolver)
    return compiler._cached_raydium_adapter


def get_meteora_adapter(compiler, *, needs_rpc: bool = False) -> Any:
    """Get or create a cached MeteoraAdapter instance."""
    if needs_rpc:
        if compiler._cached_meteora_adapter_with_rpc is None:
            from almanak.framework.connectors.meteora import MeteoraAdapter, MeteoraConfig

            config = MeteoraConfig(wallet_address=compiler.wallet_address, rpc_url=compiler.rpc_url or "")
            compiler._cached_meteora_adapter_with_rpc = MeteoraAdapter(
                config=config, token_resolver=compiler._token_resolver
            )
        return compiler._cached_meteora_adapter_with_rpc
    if compiler._cached_meteora_adapter is None:
        from almanak.framework.connectors.meteora import MeteoraAdapter, MeteoraConfig

        config = MeteoraConfig(wallet_address=compiler.wallet_address)
        compiler._cached_meteora_adapter = MeteoraAdapter(config=config, token_resolver=compiler._token_resolver)
    return compiler._cached_meteora_adapter


def get_orca_adapter(compiler, *, needs_rpc: bool = False) -> Any:
    """Get or create a cached OrcaAdapter instance."""
    if needs_rpc:
        if compiler._cached_orca_adapter_with_rpc is None:
            from almanak.framework.connectors.orca import OrcaAdapter, OrcaConfig

            config = OrcaConfig(wallet_address=compiler.wallet_address, rpc_url=compiler.rpc_url or "")
            compiler._cached_orca_adapter_with_rpc = OrcaAdapter(config=config, token_resolver=compiler._token_resolver)
        return compiler._cached_orca_adapter_with_rpc
    if compiler._cached_orca_adapter is None:
        from almanak.framework.connectors.orca import OrcaAdapter, OrcaConfig

        config = OrcaConfig(wallet_address=compiler.wallet_address)
        compiler._cached_orca_adapter = OrcaAdapter(config=config, token_resolver=compiler._token_resolver)
    return compiler._cached_orca_adapter


def get_drift_adapter(compiler) -> Any:
    """Get or create a cached DriftAdapter instance."""
    if compiler._cached_drift_adapter is None:
        from almanak.framework.connectors.drift import DriftAdapter, DriftConfig

        config = DriftConfig(wallet_address=compiler.wallet_address)
        compiler._cached_drift_adapter = DriftAdapter(config=config, token_resolver=compiler._token_resolver)
    return compiler._cached_drift_adapter


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
# Raydium LP
# =============================================================================


def compile_raydium_lp_open(compiler, intent: LPOpenIntent) -> CompilationResult:
    """Compile an LP_OPEN intent using Raydium CLMM for Solana chains."""
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    try:
        adapter = get_raydium_adapter(compiler)
        bundle = adapter.compile_lp_open_intent(intent)

        if bundle.metadata.get("error"):
            result.status = CompilationStatus.FAILED
            result.error = bundle.metadata["error"]
        else:
            result.action_bundle = bundle
    except Exception as e:
        logger.exception(f"Raydium LP open compilation failed: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)
    return result


def compile_raydium_lp_close(compiler, intent: LPCloseIntent) -> CompilationResult:
    """Compile an LP_CLOSE intent using Raydium CLMM for Solana chains."""
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    try:
        adapter = get_raydium_adapter(compiler, needs_rpc=True)
        bundle = adapter.compile_lp_close_intent(intent)

        if bundle.metadata.get("error"):
            result.status = CompilationStatus.FAILED
            result.error = bundle.metadata["error"]
        else:
            result.action_bundle = bundle
    except Exception as e:
        logger.exception(f"Raydium LP close compilation failed: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)
    return result


# =============================================================================
# Meteora LP
# =============================================================================


def compile_meteora_lp_open(compiler, intent: LPOpenIntent) -> CompilationResult:
    """Compile an LP_OPEN intent using Meteora DLMM for Solana chains."""
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    try:
        adapter = get_meteora_adapter(compiler)
        bundle = adapter.compile_lp_open_intent(intent)

        if bundle.metadata.get("error"):
            result.status = CompilationStatus.FAILED
            result.error = bundle.metadata["error"]
        else:
            result.action_bundle = bundle
    except Exception as e:
        logger.exception(f"Meteora LP open compilation failed: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)
    return result


def compile_meteora_lp_close(compiler, intent: LPCloseIntent) -> CompilationResult:
    """Compile an LP_CLOSE intent using Meteora DLMM for Solana chains."""
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    try:
        adapter = get_meteora_adapter(compiler, needs_rpc=True)
        bundle = adapter.compile_lp_close_intent(intent)

        if bundle.metadata.get("error"):
            result.status = CompilationStatus.FAILED
            result.error = bundle.metadata["error"]
        else:
            result.action_bundle = bundle
    except Exception as e:
        logger.exception(f"Meteora LP close compilation failed: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)
    return result


# =============================================================================
# Orca LP
# =============================================================================


def compile_orca_lp_open(compiler, intent: LPOpenIntent) -> CompilationResult:
    """Compile an LP_OPEN intent using Orca Whirlpools for Solana chains."""
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    try:
        adapter = get_orca_adapter(compiler)
        bundle = adapter.compile_lp_open_intent(intent)

        if bundle.metadata.get("error"):
            result.status = CompilationStatus.FAILED
            result.error = bundle.metadata["error"]
        else:
            result.action_bundle = bundle
    except Exception as e:
        logger.exception(f"Orca LP open compilation failed: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)
    return result


def compile_orca_lp_close(compiler, intent: LPCloseIntent) -> CompilationResult:
    """Compile an LP_CLOSE intent using Orca Whirlpools for Solana chains."""
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    try:
        adapter = get_orca_adapter(compiler, needs_rpc=True)
        bundle = adapter.compile_lp_close_intent(intent)

        if bundle.metadata.get("error"):
            result.status = CompilationStatus.FAILED
            result.error = bundle.metadata["error"]
        else:
            result.action_bundle = bundle
    except Exception as e:
        logger.exception(f"Orca LP close compilation failed: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)
    return result


# =============================================================================
# Drift perpetuals
# =============================================================================


def compile_drift_perp_open(compiler, intent: PerpOpenIntent) -> CompilationResult:
    """Compile a PERP_OPEN intent using Drift for Solana chains."""
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    try:
        if not is_solana_chain(compiler):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="Drift is only supported on Solana",
                intent_id=intent.intent_id,
            )

        # Validate collateral_amount is not chained
        if intent.collateral_amount == "all":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="collateral_amount='all' must be resolved before compilation.",
                intent_id=intent.intent_id,
            )

        adapter = get_drift_adapter(compiler)
        bundle = adapter.compile_perp_open_intent(intent, price_oracle=compiler.price_oracle)

        if bundle.metadata.get("error"):
            result.status = CompilationStatus.FAILED
            result.error = bundle.metadata["error"]
        else:
            result.action_bundle = bundle

    except Exception as e:
        logger.exception(f"Drift perp open compilation failed: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)
    return result


def compile_drift_perp_close(compiler, intent: PerpCloseIntent) -> CompilationResult:
    """Compile a PERP_CLOSE intent using Drift for Solana chains."""
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    try:
        if not is_solana_chain(compiler):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="Drift is only supported on Solana",
                intent_id=intent.intent_id,
            )

        adapter = get_drift_adapter(compiler)
        bundle = adapter.compile_perp_close_intent(intent, price_oracle=compiler.price_oracle)

        if bundle.metadata.get("error"):
            result.status = CompilationStatus.FAILED
            result.error = bundle.metadata["error"]
        else:
            result.action_bundle = bundle

    except Exception as e:
        logger.exception(f"Drift perp close compilation failed: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)
    return result
