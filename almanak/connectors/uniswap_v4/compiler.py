"""Connector-owned compiler for Uniswap V4."""

from __future__ import annotations

import logging
from typing import Any, ClassVar

from almanak.connectors._strategy_base.base.compiler import (
    BaseCompilerContext,
    BaseProtocolCompiler,
    SwapCompilerContext,
)
from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus, TransactionData
from almanak.framework.intents.vocabulary import CollectFeesIntent, IntentType, LPCloseIntent, LPOpenIntent, SwapIntent

from .addresses import UNISWAP_V4

logger = logging.getLogger(__name__)


class UniswapV4Compiler(BaseProtocolCompiler[SwapCompilerContext]):
    """Compiler for Uniswap V4 singleton PoolManager intents.

    Declares :class:`SwapCompilerContext` (not the bare ``BaseCompilerContext``)
    so the swap pipeline's price-impact / placeholder knobs
    (``max_price_impact_pct``, ``using_placeholders``) reach the swap-safety guard
    (VIB-2058). V4 does not use the concentrated-liquidity adapter-factory
    machinery on ``CLCompilerContext`` — it owns its bespoke adapter — so it stops
    at ``SwapCompilerContext``.
    """

    context_type: ClassVar[type[BaseCompilerContext]] = SwapCompilerContext
    protocols: ClassVar[frozenset[str]] = frozenset({"uniswap_v4"})
    intents: ClassVar[frozenset[IntentType]] = frozenset(
        {
            IntentType.SWAP,
            IntentType.LP_OPEN,
            IntentType.LP_CLOSE,
            IntentType.LP_COLLECT_FEES,
        }
    )
    chains: ClassVar[frozenset[str]] = frozenset({"ethereum", "arbitrum", "base"})

    def compile(self, ctx: SwapCompilerContext, intent: Any) -> CompilationResult:
        invalid_ctx = self._check_context(ctx, intent)
        if invalid_ctx is not None:
            return invalid_ctx
        intent_type = getattr(intent, "intent_type", None)
        if intent_type == IntentType.SWAP:
            return self.compile_swap(ctx, intent)
        if intent_type == IntentType.LP_OPEN:
            return self.compile_lp_open(ctx, intent)
        if intent_type == IntentType.LP_CLOSE:
            return self.compile_lp_close(ctx, intent)
        if intent_type == IntentType.LP_COLLECT_FEES:
            return self.compile_collect_fees(ctx, intent)
        return self._unsupported(intent)

    def compile_swap(self, ctx: SwapCompilerContext, intent: SwapIntent) -> CompilationResult:
        """Compile SWAP intent for Uniswap V4."""
        try:
            if ctx.chain not in UNISWAP_V4:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Uniswap V4 is not supported on {ctx.chain}. Supported: {list(UNISWAP_V4.keys())}",
                    intent_id=intent.intent_id,
                )

            slippage_bps = int(intent.max_slippage * 10000)
            adapter = self._adapter(ctx, default_slippage_bps=slippage_bps)
            # VIB-2058: thread the swap-safety knobs from the runtime context so the
            # adapter can fail closed on a missing executable quote and run the
            # price-impact guard (parity with the V3 swap path). ``SwapCompilerContext``
            # (enforced by ``_check_context``) guarantees these fields, so direct
            # attribute access is safe.
            action_bundle = adapter.compile_swap_intent(
                intent,
                price_oracle=ctx.price_oracle,
                config_max_price_impact=ctx.max_price_impact_pct,
                permission_discovery=ctx.permission_discovery,
                using_placeholders=ctx.using_placeholders,
            )

            if not action_bundle.transactions:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=action_bundle.metadata.get(
                        "error",
                        "Uniswap V4 swap compilation returned no transactions",
                    ),
                    intent_id=intent.intent_id,
                )

            action_bundle.metadata["protocol"] = "uniswap_v4"
            transactions = []
            for tx_dict in action_bundle.transactions:
                desc = tx_dict.get("description", "")
                if "approve" in desc.lower() and "permit2" not in desc.lower():
                    tx_type = "approve"
                elif "permit2" in desc.lower():
                    tx_type = "permit2_approve"
                else:
                    tx_type = "swap"
                transactions.append(self._transaction_from_dict(tx_dict, tx_type=tx_type))

            return CompilationResult(
                status=CompilationStatus.SUCCESS,
                intent_id=intent.intent_id,
                action_bundle=action_bundle,
                transactions=transactions,
                total_gas_estimate=action_bundle.metadata.get("gas_estimate", 0),
            )

        except ValueError as e:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=str(e),
                intent_id=intent.intent_id,
            )
        except Exception as e:
            logger.exception("Failed to compile Uniswap V4 SWAP intent: %s", e)
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=str(e),
                intent_id=intent.intent_id,
            )

    # crap-allowlist: VIB-4688 — pre-existing logic (cc=6, well under threshold); coverage-driven score from phase-2 fold relocation. Unit-coverage backfill tracked in VIB-4688.
    def compile_lp_open(self, ctx: BaseCompilerContext, intent: LPOpenIntent) -> CompilationResult:
        """Compile LP_OPEN intent for Uniswap V4 via PositionManager."""
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)

        try:
            adapter = self._adapter(ctx)
            bundle = adapter.compile_lp_open_intent(intent, ctx.price_oracle)

            if not bundle.transactions:
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata.get("error", "Unknown error during V4 LP_OPEN compilation")
                return result

            result.action_bundle = bundle
            result.transactions = [
                self._transaction_from_dict(
                    tx,
                    tx_type="approve" if "approve" in tx.get("description", "").lower() else "lp_mint",
                )
                for tx in bundle.transactions
            ]
            result.total_gas_estimate = bundle.metadata.get("gas_estimate", 0)
            if bundle.metadata.get("warnings"):
                result.warnings = bundle.metadata["warnings"]

            logger.info(
                "Compiled V4 LP_OPEN intent: %d txs, %d gas, pool=%s",
                len(bundle.transactions),
                result.total_gas_estimate,
                intent.pool,
            )

        except Exception as e:
            logger.exception("Failed to compile V4 LP_OPEN intent: %s", e)
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    # crap-allowlist: VIB-4688 — pre-existing logic (cc=15, at threshold); coverage-driven score from phase-2 fold relocation. Unit-coverage backfill tracked in VIB-4688.
    def compile_lp_close(self, ctx: BaseCompilerContext, intent: LPCloseIntent) -> CompilationResult:
        """Compile LP_CLOSE intent for Uniswap V4 via PositionManager."""
        # VIB-5346 defense-in-depth: position_id is an NFT/identity token-id for
        # Uniswap V4, not a fungible amount. Reject amount="all" chaining via the
        # shared fail-closed allowlist (the runner gate is the primary control;
        # this guards the direct-compile path).
        from almanak.framework.strategies.lp_position_tracker import (
            lp_close_amount_chaining_supported,
        )

        protocol = getattr(intent, "protocol", None) or "uniswap_v4"
        if getattr(intent, "is_chained_amount", False) and not lp_close_amount_chaining_supported(protocol):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    "LP_CLOSE amount='all' chaining is not supported for uniswap_v4: "
                    "position_id is a position identity (NFT token-id), not a fungible amount"
                ),
                intent_id=intent.intent_id,
            )
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)

        try:
            adapter = self._adapter(ctx)
            liquidity = 0
            currency0 = ""
            currency1 = ""
            protocol_params = getattr(intent, "protocol_params", None) or {}
            if protocol_params:
                liquidity = int(protocol_params.get("liquidity", 0))
                currency0 = protocol_params.get("currency0", "")
                currency1 = protocol_params.get("currency1", "")

            # Resolve the close currencies from (in order) protocol_params, the
            # pool hint, then the position NFT on-chain (VIB-5361 close-by-id).
            currency0, currency1 = self._resolve_close_currencies(adapter, intent, ctx.rpc_url, currency0, currency1)

            if liquidity == 0:
                try:
                    token_id = int(intent.position_id)
                except (ValueError, TypeError):
                    result.status = CompilationStatus.FAILED
                    result.error = f"V4 LP_CLOSE: invalid position_id '{intent.position_id}' (must be numeric)"
                    return result
                try:
                    liquidity = adapter.get_position_liquidity(token_id, rpc_url=ctx.rpc_url)
                    logger.info("V4 LP_CLOSE: queried on-chain liquidity=%d for position %d", liquidity, token_id)
                except Exception as e:
                    result.status = CompilationStatus.FAILED
                    result.error = (
                        f"V4 LP_CLOSE: could not determine position liquidity. "
                        f"Either provide 'liquidity' in protocol_params or ensure RPC is available. Error: {e}"
                    )
                    return result
                if liquidity == 0:
                    result.status = CompilationStatus.FAILED
                    result.error = (
                        f"V4 LP_CLOSE: position {token_id} has zero liquidity on-chain. "
                        f"Provide 'liquidity' in protocol_params or ensure the position exists with liquidity > 0."
                    )
                    return result
            if not currency0 or not currency1:
                result.status = CompilationStatus.FAILED
                result.error = (
                    "V4 LP_CLOSE requires 'currency0' and 'currency1' in protocol_params "
                    "or a resolvable 'pool' string (e.g. 'WETH/USDC/3000')."
                )
                return result

            currency0, currency1 = self._canonical_currency_order(currency0, currency1)
            bundle = adapter.compile_lp_close_intent(
                intent,
                liquidity=liquidity,
                currency0=currency0,
                currency1=currency1,
            )

            if not bundle.transactions:
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata.get("error", "Unknown error during V4 LP_CLOSE compilation")
                return result

            result.action_bundle = bundle
            result.transactions = [self._transaction_from_dict(tx, tx_type="lp_close") for tx in bundle.transactions]
            result.total_gas_estimate = bundle.metadata.get("gas_estimate", 0)
            if bundle.metadata.get("warnings"):
                result.warnings = bundle.metadata["warnings"]

            logger.info(
                "Compiled V4 LP_CLOSE intent: position_id=%s, %d txs, %d gas",
                intent.position_id,
                len(bundle.transactions),
                result.total_gas_estimate,
            )

        except Exception as e:
            logger.exception("Failed to compile V4 LP_CLOSE intent: %s", e)
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    # crap-allowlist: VIB-4688 — pre-existing logic (cc=14, under threshold); coverage-driven score from phase-2 fold relocation. Unit-coverage backfill tracked in VIB-4688.
    def compile_collect_fees(self, ctx: BaseCompilerContext, intent: CollectFeesIntent) -> CompilationResult:
        """Compile LP_COLLECT_FEES intent for Uniswap V4 via PositionManager."""
        result = CompilationResult(status=CompilationStatus.SUCCESS, intent_id=intent.intent_id)

        try:
            adapter = self._adapter(ctx)
            protocol_params = getattr(intent, "protocol_params", None) or {}
            position_id = protocol_params.get("position_id") or getattr(intent, "position_id", None)
            if not position_id:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="V4 LP_COLLECT_FEES requires 'position_id' in protocol_params.",
                    intent_id=intent.intent_id,
                )

            currency0 = protocol_params.get("currency0", "")
            currency1 = protocol_params.get("currency1", "")
            if (not currency0 or not currency1) and intent.pool:
                currency0, currency1 = self._resolve_pool_currencies(adapter, intent.pool, currency0, currency1)

            if not currency0 or not currency1:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=(
                        "V4 LP_COLLECT_FEES requires 'currency0' and 'currency1' in protocol_params "
                        "or a resolvable 'pool' string (e.g. 'WETH/USDC/3000')."
                    ),
                    intent_id=intent.intent_id,
                )

            currency0, currency1 = self._canonical_currency_order(currency0, currency1)
            hook_data = b""
            hook_data_hex = protocol_params.get("hook_data", "")
            if hook_data_hex:
                hook_data = bytes.fromhex(hook_data_hex.replace("0x", ""))

            bundle = adapter.compile_collect_fees_intent(
                position_id=int(position_id),
                currency0=currency0,
                currency1=currency1,
                hook_data=hook_data,
            )

            if not bundle.transactions:
                result.status = CompilationStatus.FAILED
                result.error = bundle.metadata.get("error", "Unknown error during V4 LP_COLLECT_FEES compilation")
                return result

            result.action_bundle = bundle
            result.transactions = [
                self._transaction_from_dict(tx, tx_type="lp_collect_fees") for tx in bundle.transactions
            ]
            result.total_gas_estimate = bundle.metadata.get("gas_estimate", 0)
            if bundle.metadata.get("warnings"):
                result.warnings = bundle.metadata["warnings"]

            logger.info(
                "Compiled V4 LP_COLLECT_FEES intent: position_id=%s, %d txs",
                position_id,
                len(bundle.transactions),
            )

        except Exception as e:
            logger.exception("Failed to compile V4 LP_COLLECT_FEES intent: %s", e)
            result.status = CompilationStatus.FAILED
            result.error = str(e)

        return result

    @staticmethod
    def _adapter(ctx: BaseCompilerContext, *, default_slippage_bps: int | None = None) -> Any:
        from almanak.connectors.uniswap_v4.adapter import UniswapV4Adapter, UniswapV4Config

        kwargs: dict[str, Any] = {
            "chain": ctx.chain,
            "wallet_address": ctx.wallet_address,
            "rpc_url": ctx.rpc_url,
        }
        if default_slippage_bps is not None:
            kwargs["default_slippage_bps"] = default_slippage_bps
        config = UniswapV4Config(**kwargs)
        return UniswapV4Adapter(config=config, token_resolver=ctx.token_resolver, gateway_client=ctx.gateway_client)

    @staticmethod
    def _transaction_from_dict(tx: dict[str, Any], *, tx_type: str) -> TransactionData:
        value = tx.get("value", 0)
        if isinstance(value, str):
            value = int(value, 0) if value.startswith("0x") else int(value)
        return TransactionData(
            to=tx["to"],
            value=int(value),
            data=tx["data"],
            gas_estimate=tx.get("gas_estimate", 0),
            description=tx.get("description", ""),
            tx_type=tx_type,
        )

    # crap-allowlist: VIB-4688 — extracted helper from compile_lp_close during phase-2 fold; cc=6 (well under threshold); coverage-driven score. Unit-coverage backfill tracked in VIB-4688.
    @classmethod
    def _resolve_pool_currencies(cls, adapter: Any, pool: str, currency0: str, currency1: str) -> tuple[str, str]:
        try:
            parts = pool.split("/")
            if len(parts) >= 2:
                addr0, _ = adapter._resolve_token(parts[0], for_v4_pool=True)
                addr1, _ = adapter._resolve_token(parts[1], for_v4_pool=True)
                addr0, addr1 = cls._canonical_currency_order(addr0, addr1)
                currency0 = currency0 or addr0
                currency1 = currency1 or addr1
        except (ValueError, KeyError) as e:
            logger.debug("Could not resolve currencies from pool '%s': %s", type(e).__name__, e)
        except Exception as e:
            logger.warning("Failed to resolve currencies from pool '%s': %s", pool, e)
        return currency0, currency1

    @classmethod
    def _resolve_close_currencies(
        cls,
        adapter: Any,
        intent: LPCloseIntent,
        rpc_url: str | None,
        currency0: str,
        currency1: str,
    ) -> tuple[str, str]:
        """Resolve the V4 close currencies from pool hint, then on-chain by id.

        Precedence (each only runs when the currencies are still missing):

        1. ``intent.pool`` string (e.g. ``"WETH/USDC/3000"``) via
           :meth:`_resolve_pool_currencies`.
        2. The position NFT on-chain — ``PositionManager.getPoolAndPositionInfo``
           through ``adapter.get_position_currencies`` (VIB-5361 close-by-id). A V4
           position is keyed by a pool-id (not a self-describing NFT), so this read
           is the only way to close it from its id alone — e.g.
           ``ax lp-close <id> --protocol uniswap_v4``.

        Both rungs are best-effort: on any failure the inputs are returned
        unchanged so ``compile_lp_close`` emits its existing "currencies required"
        error rather than a lower-level one.
        """
        if currency0 and currency1:
            return currency0, currency1
        if intent.pool:
            currency0, currency1 = cls._resolve_pool_currencies(adapter, intent.pool, currency0, currency1)
        if currency0 and currency1:
            return currency0, currency1
        try:
            token_id = int(intent.position_id)
        except (ValueError, TypeError):
            return currency0, currency1
        try:
            resolved0, resolved1 = adapter.get_position_currencies(token_id, rpc_url=rpc_url)
        except Exception as e:
            logger.warning(
                "V4 LP_CLOSE: could not resolve currencies on-chain for position %s: %s", intent.position_id, e
            )
            return currency0, currency1
        logger.info(
            "V4 LP_CLOSE: resolved currencies on-chain for position %s: %s / %s",
            intent.position_id,
            resolved0,
            resolved1,
        )
        return resolved0 or currency0, resolved1 or currency1

    @staticmethod
    def _canonical_currency_order(currency0: str, currency1: str) -> tuple[str, str]:
        if int(currency0, 16) > int(currency1, 16):
            return currency1, currency0
        return currency0, currency1
