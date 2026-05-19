"""Connector-owned compiler for Fluid DEX."""

from __future__ import annotations

import logging
from typing import ClassVar

from almanak.framework.connectors.base.compiler import BaseCompilerContext, BaseProtocolCompiler
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TransactionData
from almanak.framework.intents.vocabulary import CollectFeesIntent, IntentType, LPCloseIntent, LPOpenIntent, SwapIntent
from almanak.framework.models.reproduction_bundle import ActionBundle

logger = logging.getLogger(__name__)


class FluidCompiler(BaseProtocolCompiler[BaseCompilerContext]):
    """Compiler for Fluid DEX's currently supported compile boundary."""

    protocols: ClassVar[frozenset[str]] = frozenset({"fluid"})
    intents: ClassVar[frozenset[IntentType]] = frozenset(
        {
            IntentType.SWAP,
            IntentType.LP_OPEN,
            IntentType.LP_CLOSE,
        }
    )
    chains: ClassVar[frozenset[str]] = frozenset({"arbitrum"})

    def compile_swap(self, ctx: BaseCompilerContext, intent: SwapIntent) -> CompilationResult:
        """Compile SWAP intent for Fluid DEX (currently disabled)."""
        return CompilationResult(
            status=CompilationStatus.FAILED,
            intent_id=intent.intent_id,
            error=(
                "Fluid DEX connector is disabled: all 20 Arbitrum T1 pools "
                "currently reject swaps at any amount (FluidDexSwapTooSmall / "
                "FluidDexLiquidityLimit). This is a protocol-level issue, not a "
                "compiler bug. Use uniswap_v3, sushiswap_v3, or camelot instead."
            ),
        )

    def compile_lp_open(self, ctx: BaseCompilerContext, intent: LPOpenIntent) -> CompilationResult:
        """Compile LP_OPEN intent for Fluid DEX T1 (currently unsupported)."""
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                "Fluid DEX LP_OPEN is not supported in phase 1. "
                "The Liquidity-layer routing causes on-chain reverts on all pools. "
                "LP deposit support is a follow-up. Use swap intents instead."
            ),
            intent_id=intent.intent_id,
        )

    def compile_lp_close(self, ctx: BaseCompilerContext, intent: LPCloseIntent) -> CompilationResult:
        """Compile LP_CLOSE intent for Fluid DEX T1 with the adapter encumbrance guard."""
        from almanak.framework.connectors.fluid import FluidAdapter, FluidConfig

        result = CompilationResult(
            status=CompilationStatus.SUCCESS,
            intent_id=intent.intent_id,
        )
        transactions: list[TransactionData] = []

        try:
            try:
                nft_id = int(intent.position_id)
            except ValueError:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Invalid Fluid position ID (must be integer): {intent.position_id}",
                    intent_id=intent.intent_id,
                )

            dex_address = intent.pool
            if not dex_address or not dex_address.startswith("0x"):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(f"Fluid LP_CLOSE requires pool address in pool field. Got pool={intent.pool}"),
                    intent_id=intent.intent_id,
                )

            gateway_client = ctx.gateway_client
            if gateway_client is not None and not getattr(gateway_client, "is_connected", False):
                gateway_client = None

            if gateway_client is None:
                rpc_url = ctx.rpc_url
                if not rpc_url:
                    raise ValueError("Connected gateway_client or RPC URL required for Fluid DEX adapter.")
            else:
                rpc_url = None

            config = FluidConfig(
                chain=ctx.chain,
                wallet_address=ctx.wallet_address,
                rpc_url=rpc_url,
                gateway_client=gateway_client,
            )
            fluid_adapter = FluidAdapter(config)

            lp_tx = fluid_adapter.build_remove_liquidity_transaction(
                dex_address=dex_address,
                nft_id=nft_id,
            )

            transactions.append(
                TransactionData(
                    to=lp_tx.to,
                    value=lp_tx.value,
                    data=lp_tx.data,
                    gas_estimate=lp_tx.gas,
                    description=lp_tx.description,
                    tx_type="fluid_operate_close",
                )
            )

            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.LP_CLOSE.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "dex_address": dex_address,
                    "nft_id": nft_id,
                    "protocol": "fluid",
                    "chain": ctx.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas

            logger.info(
                f"Compiled Fluid LP_CLOSE intent: nft_id={nft_id}, pool={dex_address}, "
                f"{len(transactions)} txs, {total_gas} gas"
            )

        except Exception as e:
            logger.exception(f"Failed to compile Fluid LP_CLOSE intent: {e}")
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    def compile_collect_fees(self, ctx: BaseCompilerContext, intent: CollectFeesIntent) -> CompilationResult:
        """Fluid has no standalone fee-collection compile path."""
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error="Fluid does not support LP_COLLECT_FEES compilation.",
            intent_id=intent.intent_id,
        )


__all__ = ["FluidCompiler"]
