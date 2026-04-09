"""Lending compilation helpers extracted from IntentCompiler.

These standalone functions receive the compiler instance as their first
parameter and implement all lending-related compilation logic (borrow,
repay, supply, withdraw).
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from ..models.reproduction_bundle import ActionBundle
from ..utils.log_formatters import format_token_amount
from . import compiler_constants
from .compiler_models import CompilationResult, CompilationStatus, TransactionData
from .vocabulary import IntentType

if TYPE_CHECKING:
    from .vocabulary import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent

logger = logging.getLogger("almanak.framework.intents.compiler")

# Re-export constants used throughout this module via compiler_constants module
# reference so that mock patching works correctly.
AAVE_COMPATIBLE_PROTOCOLS = compiler_constants.AAVE_COMPATIBLE_PROTOCOLS
AAVE_VARIABLE_RATE_MODE = compiler_constants.AAVE_VARIABLE_RATE_MODE
MAX_UINT256 = compiler_constants.MAX_UINT256


def compile_borrow(compiler, intent: BorrowIntent) -> CompilationResult:
    """Compile a BORROW intent into an ActionBundle.

    This method:
    1. Resolves collateral and borrow token addresses
    2. Converts amounts to wei
    3. Builds approve TX for collateral
    4. Builds supply TX to deposit collateral
    5. Builds borrow TX to borrow tokens

    Args:
        compiler: IntentCompiler instance
        intent: BorrowIntent to compile

    Returns:
        CompilationResult with borrow ActionBundle
    """
    from .compiler_adapters import AaveV3Adapter

    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    transactions: list[TransactionData] = []
    warnings: list[str] = []

    try:
        protocol_lower = intent.protocol.lower()

        # =================================================================
        # SOLANA LENDING PATH (Kamino / Jupiter Lend)
        # =================================================================
        if protocol_lower == "jupiter_lend":
            if not compiler._is_solana_chain():
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error="Protocol 'jupiter_lend' is only available on Solana chains.",
                )
            return compiler._compile_jupiter_lend_borrow(intent)
        if protocol_lower == "kamino" or (
            compiler._is_solana_chain() and protocol_lower not in ("morpho", "morpho_blue", "jupiter_lend")
        ):
            if compiler._is_solana_chain() and protocol_lower not in ("kamino", ""):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error=f"Protocol '{intent.protocol}' is not supported for BORROW on Solana. Supported: kamino, jupiter_lend",
                )
            return compiler._compile_kamino_borrow(intent)

        # Step 1: Resolve token addresses (needed for both protocols)
        collateral_token = compiler._resolve_token(intent.collateral_token)
        borrow_token = compiler._resolve_token(intent.borrow_token)

        if collateral_token is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown collateral token: {intent.collateral_token}",
                intent_id=intent.intent_id,
            )
        if borrow_token is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown borrow token: {intent.borrow_token}",
                intent_id=intent.intent_id,
            )

        # Step 2: Check for chained amount
        if intent.collateral_amount == "all":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error="collateral_amount='all' must be resolved before compilation. Use Intent.set_resolved_amount() to resolve chained amounts.",
                intent_id=intent.intent_id,
            )
        collateral_amount_decimal: Decimal = intent.collateral_amount  # type: ignore[assignment]

        # =================================================================
        # MORPHO BLUE PATH
        # =================================================================
        if protocol_lower in ("morpho", "morpho_blue"):
            # Validate market_id is provided
            if not intent.market_id:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="market_id is required for Morpho Blue borrow",
                    intent_id=intent.intent_id,
                )

            # Lazy import to avoid circular import
            from ..connectors.morpho_blue.adapter import MorphoBlueAdapter, MorphoBlueConfig

            # Create Morpho adapter
            morpho_config = MorphoBlueConfig(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
            )
            morpho_adapter = MorphoBlueAdapter(morpho_config)

            # If collateral > 0, first supply collateral
            if collateral_amount_decimal > 0:
                # Build approve TX for Morpho Blue contract
                approve_txs = compiler._build_approve_tx(
                    collateral_token.address,
                    morpho_adapter.morpho_address,
                    int(collateral_amount_decimal * Decimal(10**collateral_token.decimals)),
                )
                transactions.extend(approve_txs)

                # Build supply collateral TX
                supply_result: Any = morpho_adapter.supply_collateral(
                    market_id=intent.market_id,
                    amount=collateral_amount_decimal,
                    on_behalf_of=compiler.wallet_address,
                )

                if not supply_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Morpho Blue supply collateral failed: {supply_result.error}",
                        intent_id=intent.intent_id,
                    )

                assert supply_result.tx_data is not None
                supply_tx = TransactionData(
                    to=supply_result.tx_data["to"],
                    value=supply_result.tx_data["value"],
                    data=supply_result.tx_data["data"],
                    gas_estimate=supply_result.gas_estimate,
                    description=supply_result.description
                    or f"Supply {collateral_amount_decimal} {collateral_token.symbol} as collateral",
                    tx_type="lending_supply_collateral",
                )
                transactions.append(supply_tx)
            else:
                warnings.append("No collateral supplied - borrowing against existing collateral")

            # Build borrow TX
            borrow_result: Any = morpho_adapter.borrow(
                market_id=intent.market_id,
                amount=intent.borrow_amount,
                on_behalf_of=compiler.wallet_address,
                receiver=compiler.wallet_address,
            )

            if not borrow_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Morpho Blue borrow failed: {borrow_result.error}",
                    intent_id=intent.intent_id,
                )

            assert borrow_result.tx_data is not None
            borrow_tx = TransactionData(
                to=borrow_result.tx_data["to"],
                value=borrow_result.tx_data["value"],
                data=borrow_result.tx_data["data"],
                gas_estimate=borrow_result.gas_estimate,
                description=borrow_result.description or f"Borrow {intent.borrow_amount} {borrow_token.symbol}",
                tx_type="lending_borrow",
            )
            transactions.append(borrow_tx)

            # Build ActionBundle for Morpho
            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.BORROW.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "morpho_address": morpho_adapter.morpho_address,
                    "market_id": intent.market_id,
                    "collateral_token": collateral_token.to_dict(),
                    "borrow_token": borrow_token.to_dict(),
                    "collateral_amount": str(collateral_amount_decimal),
                    "borrow_amount": str(intent.borrow_amount),
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled BORROW: {collateral_amount_decimal} {collateral_token.symbol} collateral -> {intent.borrow_amount} {borrow_token.symbol} on Morpho Blue"
            )
            return result

        # =================================================================
        # AAVE-COMPATIBLE PATH (Aave V3 + Radiant V2)
        # =================================================================
        elif protocol_lower in AAVE_COMPATIBLE_PROTOCOLS:
            # Get lending adapter
            adapter = AaveV3Adapter(compiler.chain, protocol_lower)
            pool_address = adapter.get_pool_address()

            if pool_address == "0x0000000000000000000000000000000000000000":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"{intent.protocol} not available on chain: {compiler.chain}",
                    intent_id=intent.intent_id,
                )

            collateral_amount = int(collateral_amount_decimal * Decimal(10**collateral_token.decimals))
            borrow_amount = int(intent.borrow_amount * Decimal(10**borrow_token.decimals))

            # Build approve TX and supply TX for collateral (if collateral > 0)
            if collateral_amount > 0:
                actual_collateral_address = collateral_token.address
                supply_value = 0

                if collateral_token.is_native:
                    weth_address = compiler._get_wrapped_native_address()
                    if weth_address:
                        actual_collateral_address = weth_address
                        warnings.append("Native token collateral: will wrap to WETH before supplying")
                    else:
                        return CompilationResult(
                            status=CompilationStatus.FAILED,
                            error="Cannot use native ETH as collateral - WETH address not found",
                            intent_id=intent.intent_id,
                        )

                if not collateral_token.is_native:
                    approve_txs = compiler._build_approve_tx(
                        actual_collateral_address,
                        pool_address,
                        collateral_amount,
                    )
                    transactions.extend(approve_txs)

                supply_calldata = adapter.get_supply_calldata(
                    asset=actual_collateral_address,
                    amount=collateral_amount,
                    on_behalf_of=compiler.wallet_address,
                )

                supply_tx = TransactionData(
                    to=pool_address,
                    value=supply_value,
                    data="0x" + supply_calldata.hex(),
                    gas_estimate=adapter.estimate_supply_gas(),
                    description=(
                        f"Supply {compiler._format_amount(collateral_amount, collateral_token.decimals)} {collateral_token.symbol} as collateral"
                    ),
                    tx_type="lending_supply",
                )
                transactions.append(supply_tx)
            else:
                warnings.append("No collateral supplied - borrowing against existing collateral")

            # Resolve interest rate mode: use intent value or default to variable
            # Note: stable rate is deprecated on Aave V3, rejected at intent layer
            aave_borrow_rate_mode = AAVE_VARIABLE_RATE_MODE
            borrow_rate_mode_label = "variable"

            # Build borrow TX
            borrow_calldata = adapter.get_borrow_calldata(
                asset=borrow_token.address,
                amount=borrow_amount,
                interest_rate_mode=aave_borrow_rate_mode,
                on_behalf_of=compiler.wallet_address,
            )

            borrow_tx = TransactionData(
                to=pool_address,
                value=0,
                data="0x" + borrow_calldata.hex(),
                gas_estimate=adapter.estimate_borrow_gas(),
                description=(
                    f"Borrow {compiler._format_amount(borrow_amount, borrow_token.decimals)} {borrow_token.symbol} ({borrow_rate_mode_label} rate)"
                ),
                tx_type="lending_borrow",
            )
            transactions.append(borrow_tx)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.BORROW.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "pool_address": pool_address,
                    "collateral_token": collateral_token.to_dict(),
                    "borrow_token": borrow_token.to_dict(),
                    "collateral_amount": str(collateral_amount),
                    "borrow_amount": str(borrow_amount),
                    "interest_rate_mode": aave_borrow_rate_mode,
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            collateral_fmt = format_token_amount(collateral_amount, collateral_token.symbol, collateral_token.decimals)
            borrow_fmt = format_token_amount(borrow_amount, borrow_token.symbol, borrow_token.decimals)

            logger.info(f"Compiled BORROW: Supply {collateral_fmt} (collateral) -> Borrow {borrow_fmt}")
            logger.info(f"   Protocol: {intent.protocol} | Txs: {len(transactions)} | Gas: {total_gas:,}")

        # =================================================================
        # SPARK PATH (Aave V3 fork with Spark-specific addresses)
        # =================================================================
        elif protocol_lower == "spark":
            from ..connectors.spark import (
                SPARK_POOL_ADDRESSES,
                SPARK_VARIABLE_RATE_MODE,
                SparkAdapter,
                SparkConfig,
            )

            if compiler.chain not in SPARK_POOL_ADDRESSES:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Spark not available on chain: {compiler.chain}. Supported: {list(SPARK_POOL_ADDRESSES.keys())}",
                    intent_id=intent.intent_id,
                )

            spark_config = SparkConfig(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
            )
            spark_adapter = SparkAdapter(spark_config)
            pool_address = spark_adapter.pool_address

            collateral_amount = int(collateral_amount_decimal * Decimal(10**collateral_token.decimals))
            borrow_amount = int(intent.borrow_amount * Decimal(10**borrow_token.decimals))

            # Build approve TX and supply TX for collateral (if collateral > 0)
            if collateral_amount > 0:
                actual_collateral_address = collateral_token.address
                supply_value = 0

                if collateral_token.is_native:
                    weth_address = compiler._get_wrapped_native_address()
                    if weth_address:
                        actual_collateral_address = weth_address
                        # Wrap native ETH -> WETH
                        wrap_tx = TransactionData(
                            to=weth_address,
                            value=collateral_amount,
                            data="0xd0e30db0",  # WETH.deposit()
                            gas_estimate=compiler_constants.get_gas_estimate(compiler.chain, "wrap_eth"),
                            description=f"Wrap {compiler._format_amount(collateral_amount, collateral_token.decimals)} {collateral_token.symbol} to WETH",
                            tx_type="wrap",
                        )
                        transactions.append(wrap_tx)
                        # Approve WETH for pool
                        approve_txs = compiler._build_approve_tx(
                            weth_address,
                            pool_address,
                            collateral_amount,
                        )
                        transactions.extend(approve_txs)
                        warnings.append("Native token collateral: wrapped to WETH before supplying")
                    else:
                        return CompilationResult(
                            status=CompilationStatus.FAILED,
                            error="Cannot use native ETH as collateral - WETH address not found",
                            intent_id=intent.intent_id,
                        )
                else:
                    approve_txs = compiler._build_approve_tx(
                        actual_collateral_address,
                        pool_address,
                        collateral_amount,
                    )
                    transactions.extend(approve_txs)

                # Build supply TX via Spark adapter
                supply_result = spark_adapter.supply(
                    asset=actual_collateral_address,
                    amount=collateral_amount_decimal,
                    on_behalf_of=compiler.wallet_address,
                )

                if not supply_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Spark supply collateral failed: {supply_result.error}",
                        intent_id=intent.intent_id,
                    )

                assert supply_result.tx_data is not None
                supply_data = supply_result.tx_data["data"]
                if not supply_data.startswith("0x"):
                    supply_data = "0x" + supply_data

                supply_value = int(supply_result.tx_data.get("value", 0))

                supply_tx = TransactionData(
                    to=supply_result.tx_data["to"],
                    value=supply_value,
                    data=supply_data,
                    gas_estimate=supply_result.gas_estimate,
                    description=(
                        f"Supply {compiler._format_amount(collateral_amount, collateral_token.decimals)} {collateral_token.symbol} as collateral to Spark"
                    ),
                    tx_type="lending_supply",
                )
                transactions.append(supply_tx)
            else:
                warnings.append("No collateral supplied - borrowing against existing collateral")

            # Resolve interest rate mode: use intent value or default to variable
            # Note: stable rate is deprecated on Spark, rejected at intent layer
            spark_borrow_rate_mode = SPARK_VARIABLE_RATE_MODE
            spark_borrow_rate_label = "variable"

            # Build borrow TX via Spark adapter
            borrow_result = spark_adapter.borrow(
                asset=borrow_token.address,
                amount=intent.borrow_amount,
                interest_rate_mode=spark_borrow_rate_mode,
                on_behalf_of=compiler.wallet_address,
            )

            if not borrow_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Spark borrow failed: {borrow_result.error}",
                    intent_id=intent.intent_id,
                )

            assert borrow_result.tx_data is not None
            borrow_data = borrow_result.tx_data["data"]
            if not borrow_data.startswith("0x"):
                borrow_data = "0x" + borrow_data

            borrow_tx = TransactionData(
                to=borrow_result.tx_data["to"],
                value=0,
                data=borrow_data,
                gas_estimate=borrow_result.gas_estimate,
                description=(
                    f"Borrow {compiler._format_amount(borrow_amount, borrow_token.decimals)} {borrow_token.symbol} from Spark ({spark_borrow_rate_label} rate)"
                ),
                tx_type="lending_borrow",
            )
            transactions.append(borrow_tx)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.BORROW.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "pool_address": pool_address,
                    "collateral_token": collateral_token.to_dict(),
                    "borrow_token": borrow_token.to_dict(),
                    "collateral_amount": str(collateral_amount),
                    "borrow_amount": str(borrow_amount),
                    "interest_rate_mode": spark_borrow_rate_mode,
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            collateral_fmt = format_token_amount(collateral_amount, collateral_token.symbol, collateral_token.decimals)
            borrow_fmt = format_token_amount(borrow_amount, borrow_token.symbol, borrow_token.decimals)

            logger.info(f"Compiled BORROW: Supply {collateral_fmt} (collateral) -> Borrow {borrow_fmt}")
            logger.info(f"   Protocol: Spark | Txs: {len(transactions)} | Gas: {total_gas:,}")

        # =================================================================
        # COMPOUND V3 PATH
        # =================================================================
        elif protocol_lower == "compound_v3":
            from ..connectors.compound_v3.adapter import (
                COMPOUND_V3_COMET_ADDRESSES,
                CompoundV3Adapter,
                CompoundV3Config,
            )

            market = intent.market_id or "usdc"

            if compiler.chain not in COMPOUND_V3_COMET_ADDRESSES:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Compound V3 not available on chain: {compiler.chain}. Supported: {list(COMPOUND_V3_COMET_ADDRESSES.keys())}",
                    intent_id=intent.intent_id,
                )

            available_markets = COMPOUND_V3_COMET_ADDRESSES.get(compiler.chain, {})
            if market not in available_markets:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Compound V3 market '{market}' not available on {compiler.chain}. Available: {list(available_markets.keys())}",
                    intent_id=intent.intent_id,
                )

            compound_config = CompoundV3Config(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
                market=market,
            )
            compound_adapter = CompoundV3Adapter(compound_config)

            # If collateral > 0, first supply collateral
            if collateral_amount_decimal > 0:
                collateral_amount_wei = int(collateral_amount_decimal * Decimal(10**collateral_token.decimals))

                # Build approve TX for Comet contract (collateral token)
                approve_txs = compiler._build_approve_tx(
                    collateral_token.address,
                    compound_adapter.comet_address,
                    collateral_amount_wei,
                )
                transactions.extend(approve_txs)

                # Build supply collateral TX
                # Determine collateral symbol for adapter
                collateral_symbol = collateral_token.symbol.upper()
                supply_result = compound_adapter.supply_collateral(
                    asset=collateral_symbol,
                    amount=collateral_amount_decimal,
                )

                if not supply_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Compound V3 supply collateral failed: {supply_result.error}",
                        intent_id=intent.intent_id,
                    )

                assert supply_result.tx_data is not None
                supply_data = supply_result.tx_data["data"]
                if not supply_data.startswith("0x"):
                    supply_data = "0x" + supply_data

                supply_tx = TransactionData(
                    to=supply_result.tx_data["to"],
                    value=int(supply_result.tx_data.get("value", 0)),
                    data=supply_data,
                    gas_estimate=supply_result.gas_estimate,
                    description=supply_result.description
                    or f"Supply {collateral_amount_decimal} {collateral_token.symbol} as collateral to Compound V3",
                    tx_type="lending_supply_collateral",
                )
                transactions.append(supply_tx)
            else:
                warnings.append("No collateral supplied - borrowing against existing collateral")

            # Build borrow TX
            borrow_result = compound_adapter.borrow(amount=intent.borrow_amount)

            if not borrow_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Compound V3 borrow failed: {borrow_result.error}",
                    intent_id=intent.intent_id,
                )

            assert borrow_result.tx_data is not None
            borrow_data = borrow_result.tx_data["data"]
            if not borrow_data.startswith("0x"):
                borrow_data = "0x" + borrow_data

            borrow_tx = TransactionData(
                to=borrow_result.tx_data["to"],
                value=int(borrow_result.tx_data.get("value", 0)),
                data=borrow_data,
                gas_estimate=borrow_result.gas_estimate,
                description=borrow_result.description
                or f"Borrow {intent.borrow_amount} {borrow_token.symbol} from Compound V3",
                tx_type="lending_borrow",
            )
            transactions.append(borrow_tx)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.BORROW.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "comet_address": compound_adapter.comet_address,
                    "market": market,
                    "collateral_token": collateral_token.to_dict(),
                    "borrow_token": borrow_token.to_dict(),
                    "collateral_amount": str(collateral_amount_decimal),
                    "borrow_amount": str(intent.borrow_amount),
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            collateral_fmt = format_token_amount(
                int(collateral_amount_decimal * Decimal(10**collateral_token.decimals)),
                collateral_token.symbol,
                collateral_token.decimals,
            )
            borrow_fmt = format_token_amount(
                int(intent.borrow_amount * Decimal(10**borrow_token.decimals)),
                borrow_token.symbol,
                borrow_token.decimals,
            )

            logger.info(f"Compiled BORROW: Supply {collateral_fmt} (collateral) -> Borrow {borrow_fmt}")
            logger.info(f"   Protocol: Compound V3 ({market} market) | Txs: {len(transactions)} | Gas: {total_gas:,}")

        # =================================================================
        # BENQI PATH (Compound V2 fork on Avalanche)
        # =================================================================
        elif protocol_lower == "benqi":
            from ..connectors.benqi.adapter import (
                BENQI_QI_TOKENS,
                BenqiAdapter,
                BenqiConfig,
            )

            if compiler.chain != "avalanche":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"BENQI is only available on Avalanche, got: {compiler.chain}",
                    intent_id=intent.intent_id,
                )

            benqi_config = BenqiConfig(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
            )
            benqi_adapter = BenqiAdapter(benqi_config)

            # If collateral > 0, first supply collateral + enterMarkets
            if collateral_amount_decimal > 0:
                collateral_symbol = collateral_token.symbol.upper()
                collateral_market = benqi_adapter.get_market_info(collateral_symbol)

                if not collateral_market:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"BENQI does not support collateral asset: {collateral_symbol}. Supported: {list(BENQI_QI_TOKENS.keys())}",
                        intent_id=intent.intent_id,
                    )

                collateral_amount_wei = int(collateral_amount_decimal * Decimal(10**collateral_token.decimals))

                # Build approve TX for qiToken (skip for native AVAX)
                if not collateral_market.is_native:
                    approve_txs = compiler._build_approve_tx(
                        collateral_token.address,
                        collateral_market.qi_token_address,
                        collateral_amount_wei,
                    )
                    transactions.extend(approve_txs)

                # Build supply (mint) TX
                supply_result = benqi_adapter.supply(
                    asset=collateral_symbol,
                    amount=collateral_amount_decimal,
                )

                if not supply_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"BENQI supply collateral failed: {supply_result.error}",
                        intent_id=intent.intent_id,
                    )

                assert supply_result.tx_data is not None
                supply_data = supply_result.tx_data["data"]
                if not supply_data.startswith("0x"):
                    supply_data = "0x" + supply_data

                supply_tx = TransactionData(
                    to=supply_result.tx_data["to"],
                    value=int(supply_result.tx_data.get("value", 0)),
                    data=supply_data,
                    gas_estimate=supply_result.gas_estimate,
                    description=supply_result.description,
                    tx_type="lending_supply_collateral",
                )
                transactions.append(supply_tx)

                # Build enterMarkets TX to enable as collateral
                enter_result = benqi_adapter.enter_markets([collateral_symbol])
                if not enter_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"BENQI enterMarkets failed: {enter_result.error}",
                        intent_id=intent.intent_id,
                    )
                assert enter_result.tx_data is not None
                enter_data = enter_result.tx_data["data"]
                if not enter_data.startswith("0x"):
                    enter_data = "0x" + enter_data
                enter_tx = TransactionData(
                    to=enter_result.tx_data["to"],
                    value=0,
                    data=enter_data,
                    gas_estimate=enter_result.gas_estimate,
                    description=enter_result.description,
                    tx_type="lending_enter_markets",
                )
                transactions.append(enter_tx)
            else:
                warnings.append("No collateral supplied - borrowing against existing collateral")

            # Build borrow TX
            borrow_symbol = borrow_token.symbol.upper()
            borrow_result = benqi_adapter.borrow(asset=borrow_symbol, amount=intent.borrow_amount)

            if not borrow_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"BENQI borrow failed: {borrow_result.error}",
                    intent_id=intent.intent_id,
                )

            assert borrow_result.tx_data is not None
            borrow_data = borrow_result.tx_data["data"]
            if not borrow_data.startswith("0x"):
                borrow_data = "0x" + borrow_data

            borrow_tx = TransactionData(
                to=borrow_result.tx_data["to"],
                value=int(borrow_result.tx_data.get("value", 0)),
                data=borrow_data,
                gas_estimate=borrow_result.gas_estimate,
                description=borrow_result.description,
                tx_type="lending_borrow",
            )
            transactions.append(borrow_tx)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.BORROW.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "comptroller_address": benqi_adapter.comptroller_address,
                    "collateral_token": collateral_token.to_dict(),
                    "borrow_token": borrow_token.to_dict(),
                    "collateral_amount": str(collateral_amount_decimal),
                    "borrow_amount": str(intent.borrow_amount),
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            collateral_fmt = format_token_amount(
                int(collateral_amount_decimal * Decimal(10**collateral_token.decimals)),
                collateral_token.symbol,
                collateral_token.decimals,
            )
            borrow_fmt = format_token_amount(
                int(intent.borrow_amount * Decimal(10**borrow_token.decimals)),
                borrow_token.symbol,
                borrow_token.decimals,
            )

            logger.info(f"Compiled BORROW: Supply {collateral_fmt} (collateral) -> Borrow {borrow_fmt}")
            logger.info(f"   Protocol: BENQI | Txs: {len(transactions)} | Gas: {total_gas:,}")

        # =================================================================
        # JOE LEND PATH (Compound V2 fork on Avalanche — Banker Joe)
        # =================================================================
        elif protocol_lower == "joelend":
            from ..connectors.joelend.adapter import (
                JOELEND_J_TOKENS,
                JoeLendAdapter,
                JoeLendConfig,
            )

            if compiler.chain != "avalanche":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Joe Lend is only available on Avalanche, got: {compiler.chain}",
                    intent_id=intent.intent_id,
                )

            joelend_config = JoeLendConfig(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
            )
            joelend_adapter = JoeLendAdapter(joelend_config)

            # If collateral > 0, first supply collateral + enterMarkets
            if collateral_amount_decimal > 0:
                collateral_symbol = collateral_token.symbol.upper()
                jl_collateral_market = joelend_adapter.get_market_info(collateral_symbol)

                if not jl_collateral_market:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Joe Lend does not support collateral asset: {collateral_symbol}. Supported: {list(JOELEND_J_TOKENS.keys())}",
                        intent_id=intent.intent_id,
                    )

                collateral_amount_wei = int(collateral_amount_decimal * Decimal(10**collateral_token.decimals))

                # Build approve TX for jToken (skip for native AVAX)
                if not jl_collateral_market.is_native:
                    approve_txs = compiler._build_approve_tx(
                        collateral_token.address,
                        jl_collateral_market.j_token_address,
                        collateral_amount_wei,
                    )
                    transactions.extend(approve_txs)

                # Build supply (mint) TX
                jl_supply_result = joelend_adapter.supply(
                    asset=collateral_symbol,
                    amount=collateral_amount_decimal,
                )

                if not jl_supply_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Joe Lend supply collateral failed: {jl_supply_result.error}",
                        intent_id=intent.intent_id,
                    )

                assert jl_supply_result.tx_data is not None
                supply_data = jl_supply_result.tx_data["data"]
                if not supply_data.startswith("0x"):
                    supply_data = "0x" + supply_data

                supply_tx = TransactionData(
                    to=jl_supply_result.tx_data["to"],
                    value=int(jl_supply_result.tx_data.get("value", 0)),
                    data=supply_data,
                    gas_estimate=jl_supply_result.gas_estimate,
                    description=jl_supply_result.description,
                    tx_type="lending_supply_collateral",
                )
                transactions.append(supply_tx)

                # Build enterMarkets TX to enable as collateral
                jl_enter_result = joelend_adapter.enter_markets([collateral_symbol])
                if not jl_enter_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Joe Lend enterMarkets failed: {jl_enter_result.error}",
                        intent_id=intent.intent_id,
                    )
                assert jl_enter_result.tx_data is not None
                enter_data = jl_enter_result.tx_data["data"]
                if not enter_data.startswith("0x"):
                    enter_data = "0x" + enter_data
                enter_tx = TransactionData(
                    to=jl_enter_result.tx_data["to"],
                    value=0,
                    data=enter_data,
                    gas_estimate=jl_enter_result.gas_estimate,
                    description=jl_enter_result.description,
                    tx_type="lending_enter_markets",
                )
                transactions.append(enter_tx)
            else:
                warnings.append("No collateral supplied - borrowing against existing collateral")

            # Build borrow TX
            borrow_symbol = borrow_token.symbol.upper()
            borrow_result = joelend_adapter.borrow(asset=borrow_symbol, amount=intent.borrow_amount)

            if not borrow_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Joe Lend borrow failed: {borrow_result.error}",
                    intent_id=intent.intent_id,
                )

            assert borrow_result.tx_data is not None
            borrow_data = borrow_result.tx_data["data"]
            if not borrow_data.startswith("0x"):
                borrow_data = "0x" + borrow_data

            borrow_tx = TransactionData(
                to=borrow_result.tx_data["to"],
                value=int(borrow_result.tx_data.get("value", 0)),
                data=borrow_data,
                gas_estimate=borrow_result.gas_estimate,
                description=borrow_result.description,
                tx_type="lending_borrow",
            )
            transactions.append(borrow_tx)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.BORROW.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "joetroller_address": joelend_adapter.joetroller_address,
                    "collateral_token": collateral_token.to_dict(),
                    "borrow_token": borrow_token.to_dict(),
                    "collateral_amount": str(collateral_amount_decimal),
                    "borrow_amount": str(intent.borrow_amount),
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            collateral_fmt = format_token_amount(
                int(collateral_amount_decimal * Decimal(10**collateral_token.decimals)),
                collateral_token.symbol,
                collateral_token.decimals,
            )
            borrow_fmt = format_token_amount(
                int(intent.borrow_amount * Decimal(10**borrow_token.decimals)),
                borrow_token.symbol,
                borrow_token.decimals,
            )

            logger.info(f"Compiled BORROW: Supply {collateral_fmt} (collateral) -> Borrow {borrow_fmt}")
            logger.info(f"   Protocol: Joe Lend | Txs: {len(transactions)} | Gas: {total_gas:,}")

        # =================================================================
        # SILO V2 PATH (Isolated lending on Avalanche)
        # =================================================================
        elif protocol_lower == "silo_v2":
            from ..connectors.silo_v2.adapter import (
                SILO_V2_MARKETS,
                SiloV2Adapter,
                SiloV2Config,
            )

            if compiler.chain != "avalanche":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Silo V2 is only available on Avalanche, got: {compiler.chain}",
                    intent_id=intent.intent_id,
                )

            silo_config = SiloV2Config(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
            )
            silo_adapter = SiloV2Adapter(silo_config)

            collateral_symbol = collateral_token.symbol.upper()
            borrow_symbol = borrow_token.symbol.upper()

            sv2_market = silo_adapter.find_market(collateral_symbol, borrow_symbol)
            if not sv2_market:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"No Silo V2 market found for {collateral_symbol}/{borrow_symbol}. Available: {list(SILO_V2_MARKETS.keys())}",
                    intent_id=intent.intent_id,
                )

            # If collateral > 0, deposit into the collateral silo
            if collateral_amount_decimal > 0:
                collateral_amount_wei = int(collateral_amount_decimal * Decimal(10**collateral_token.decimals))

                sv2_silo_result = silo_adapter.find_silo_for_asset(collateral_symbol, sv2_market.market_name)
                if not sv2_silo_result:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Cannot find silo for collateral {collateral_symbol} in market {sv2_market.market_name}",
                        intent_id=intent.intent_id,
                    )
                _, collateral_silo_address, _ = sv2_silo_result

                approve_txs = compiler._build_approve_tx(
                    collateral_token.address,
                    collateral_silo_address,
                    collateral_amount_wei,
                )
                transactions.extend(approve_txs)

                sv2_supply_result = silo_adapter.supply(
                    asset=collateral_symbol,
                    amount=collateral_amount_decimal,
                    market_name=sv2_market.market_name,
                )

                if not sv2_supply_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Silo V2 deposit failed: {sv2_supply_result.error}",
                        intent_id=intent.intent_id,
                    )

                assert sv2_supply_result.tx_data is not None
                supply_data = sv2_supply_result.tx_data["data"]
                if not supply_data.startswith("0x"):
                    supply_data = "0x" + supply_data

                supply_tx = TransactionData(
                    to=sv2_supply_result.tx_data["to"],
                    value=int(sv2_supply_result.tx_data.get("value", 0)),
                    data=supply_data,
                    gas_estimate=sv2_supply_result.gas_estimate,
                    description=sv2_supply_result.description,
                    tx_type="lending_supply_collateral",
                )
                transactions.append(supply_tx)
            else:
                warnings.append("No collateral supplied - borrowing against existing collateral")

            borrow_result = silo_adapter.borrow(
                collateral_asset=collateral_symbol,
                borrow_asset=borrow_symbol,
                borrow_amount=intent.borrow_amount,
            )

            if not borrow_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Silo V2 borrow failed: {borrow_result.error}",
                    intent_id=intent.intent_id,
                )

            assert borrow_result.tx_data is not None
            borrow_data = borrow_result.tx_data["data"]
            if not borrow_data.startswith("0x"):
                borrow_data = "0x" + borrow_data

            borrow_tx = TransactionData(
                to=borrow_result.tx_data["to"],
                value=int(borrow_result.tx_data.get("value", 0)),
                data=borrow_data,
                gas_estimate=borrow_result.gas_estimate,
                description=borrow_result.description,
                tx_type="lending_borrow",
            )
            transactions.append(borrow_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.BORROW.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "silo_config": sv2_market.silo_config,
                    "market_name": sv2_market.market_name,
                    "collateral_token": collateral_token.to_dict(),
                    "borrow_token": borrow_token.to_dict(),
                    "collateral_amount": str(collateral_amount_decimal),
                    "borrow_amount": str(intent.borrow_amount),
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            collateral_fmt = format_token_amount(
                int(collateral_amount_decimal * Decimal(10**collateral_token.decimals)),
                collateral_token.symbol,
                collateral_token.decimals,
            )
            borrow_fmt = format_token_amount(
                int(intent.borrow_amount * Decimal(10**borrow_token.decimals)),
                borrow_token.symbol,
                borrow_token.decimals,
            )

            logger.info(f"Compiled BORROW: Supply {collateral_fmt} (collateral) -> Borrow {borrow_fmt}")
            logger.info(
                f"   Protocol: Silo V2 ({sv2_market.market_name}) | Txs: {len(transactions)} | Gas: {total_gas:,}"
            )

        # =================================================================
        # UNSUPPORTED PROTOCOL
        # =================================================================
        else:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unsupported lending protocol: {intent.protocol}. Supported: aave_v3, morpho, morpho_blue, spark, compound_v3, benqi, joelend, silo_v2",
                intent_id=intent.intent_id,
            )

    except Exception as e:
        logger.exception(f"Failed to compile BORROW intent: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)

    return result


def compile_repay(compiler, intent: RepayIntent) -> CompilationResult:
    """Compile a REPAY intent into an ActionBundle.

    This method:
    1. Resolves repay token address
    2. Converts amount to wei (or uses MAX_UINT256 for full repay)
    3. Builds approve TX for repay token
    4. Builds repay TX

    Args:
        compiler: IntentCompiler instance
        intent: RepayIntent to compile

    Returns:
        CompilationResult with repay ActionBundle
    """
    from .compiler_adapters import AaveV3Adapter

    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    transactions: list[TransactionData] = []
    warnings: list[str] = []

    try:
        protocol_lower = intent.protocol.lower()

        # =================================================================
        # SOLANA LENDING PATH (Kamino / Jupiter Lend)
        # =================================================================
        if protocol_lower == "jupiter_lend":
            if not compiler._is_solana_chain():
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error="Protocol 'jupiter_lend' is only available on Solana chains.",
                )
            return compiler._compile_jupiter_lend_repay(intent)
        if protocol_lower == "kamino" or (
            compiler._is_solana_chain() and protocol_lower not in ("morpho", "morpho_blue", "jupiter_lend")
        ):
            if compiler._is_solana_chain() and protocol_lower not in ("kamino", ""):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error=f"Protocol '{intent.protocol}' is not supported for REPAY on Solana. Supported: kamino, jupiter_lend",
                )
            return compiler._compile_kamino_repay(intent)

        # Step 1: Resolve token address
        repay_token = compiler._resolve_token(intent.token)
        if repay_token is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown repay token: {intent.token}",
                intent_id=intent.intent_id,
            )

        # Step 2: Calculate repay amount
        repay_amount_decimal: Decimal | None
        if intent.repay_full:
            repay_amount_decimal = None  # Will use shares-based repay for Morpho
            amount_description = "full debt"
            warnings.append("Repaying full debt - ensure sufficient balance to cover interest")
        elif intent.amount == "all":
            # amount="all" was not resolved by the amount resolver — fall back to repay_full
            logger.info(
                "amount='all' reached compiler unresolved for %s repay — using repay_full path",
                intent.protocol,
            )
            repay_amount_decimal = None
            intent = intent.model_copy(update={"repay_full": True})
            amount_description = "full debt"
            warnings.append("Repaying full debt (amount='all' fallback)")
        else:
            repay_amount_decimal = intent.amount  # type: ignore[assignment]
            amount_description = str(repay_amount_decimal)

        # =================================================================
        # MORPHO BLUE PATH
        # =================================================================
        if protocol_lower in ("morpho", "morpho_blue"):
            if not intent.market_id:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="market_id is required for Morpho Blue repay",
                    intent_id=intent.intent_id,
                )

            # Lazy import to avoid circular import
            from ..connectors.morpho_blue.adapter import MorphoBlueAdapter, MorphoBlueConfig

            # Use _get_chain_rpc_url() (not compiler.rpc_url) so Anvil fork URL is detected via
            # ANVIL_{CHAIN}_PORT env var when running on a fork. compiler.rpc_url is always None
            # in gateway mode, which caused the SDK to use Alchemy mainnet RPC even on Anvil,
            # returning borrow_shares=0 and breaking repay_full=True (VIB-587).
            morpho_rpc_url = compiler._get_chain_rpc_url()

            morpho_config = MorphoBlueConfig(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
                rpc_url=morpho_rpc_url,  # Pass RPC URL for on-chain queries (e.g., repay_full)
            )
            morpho_adapter = MorphoBlueAdapter(morpho_config)

            # Build approve TX for Morpho Blue contract
            if repay_amount_decimal is not None:
                approve_amount = int(repay_amount_decimal * Decimal(10**repay_token.decimals))
            else:
                approve_amount = MAX_UINT256  # Approve max for full repay

            approve_txs = compiler._build_approve_tx(
                repay_token.address,
                morpho_adapter.morpho_address,
                approve_amount,
            )
            transactions.extend(approve_txs)

            # Build repay TX
            repay_result: Any = morpho_adapter.repay(
                market_id=intent.market_id,
                amount=repay_amount_decimal if repay_amount_decimal else Decimal("0"),
                on_behalf_of=compiler.wallet_address,
                repay_all=intent.repay_full,
            )

            if not repay_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Morpho Blue repay failed: {repay_result.error}",
                    intent_id=intent.intent_id,
                )

            assert repay_result.tx_data is not None
            repay_tx = TransactionData(
                to=repay_result.tx_data["to"],
                value=repay_result.tx_data["value"],
                data=repay_result.tx_data["data"],
                gas_estimate=repay_result.gas_estimate,
                description=repay_result.description or f"Repay {amount_description} {repay_token.symbol}",
                tx_type="lending_repay",
            )
            transactions.append(repay_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.REPAY.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "morpho_address": morpho_adapter.morpho_address,
                    "market_id": intent.market_id,
                    "repay_token": repay_token.to_dict(),
                    "repay_amount": amount_description,
                    "repay_full": intent.repay_full,
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(f"Compiled REPAY: {amount_description} {repay_token.symbol} on Morpho Blue")
            return result

        # =================================================================
        # AAVE-COMPATIBLE PATH (Aave V3 + Radiant V2)
        # =================================================================
        elif protocol_lower in AAVE_COMPATIBLE_PROTOCOLS:
            adapter = AaveV3Adapter(compiler.chain, protocol_lower)
            pool_address = adapter.get_pool_address()

            if pool_address == "0x0000000000000000000000000000000000000000":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"{intent.protocol} not available on chain: {compiler.chain}",
                    intent_id=intent.intent_id,
                )

            if intent.repay_full:
                # Query wallet balance to use as repay amount — avoids InsufficientFunds()
                # when accrued interest causes debt to exceed the borrowed principal.
                # Aave accepts any amount up to the debt; we repay as much as the wallet holds.
                wallet_balance = compiler._query_erc20_balance(repay_token.address, compiler.wallet_address)
                if wallet_balance is None:
                    repay_amount = MAX_UINT256
                    logger.warning(
                        f"repay_full: could not query wallet balance for {repay_token.symbol}, "
                        f"falling back to MAX_UINT256 (may fail if interest accrued exceeds balance)"
                    )
                else:
                    repay_amount = wallet_balance
                    logger.debug(
                        f"repay_full: using on-chain wallet balance {repay_amount} wei for {repay_token.symbol}"
                    )
            else:
                assert repay_amount_decimal is not None
                repay_amount = int(repay_amount_decimal * Decimal(10**repay_token.decimals))

            approve_amount = repay_amount

            if not repay_token.is_native:
                approve_txs = compiler._build_approve_tx(
                    repay_token.address,
                    pool_address,
                    approve_amount,
                )
                transactions.extend(approve_txs)
            else:
                weth_address = compiler._get_wrapped_native_address()
                if weth_address:
                    approve_txs = compiler._build_approve_tx(
                        weth_address,
                        pool_address,
                        approve_amount,
                    )
                    transactions.extend(approve_txs)
                    warnings.append("Native token debt: using WETH for repayment")

            actual_repay_address = repay_token.address
            if repay_token.is_native:
                weth_address = compiler._get_wrapped_native_address()
                if weth_address:
                    actual_repay_address = weth_address

            # Resolve interest rate mode: use intent value or default to variable
            # Note: stable rate is deprecated on Aave V3, rejected at intent layer
            aave_rate_mode = AAVE_VARIABLE_RATE_MODE
            rate_mode_label = "variable"

            repay_calldata = adapter.get_repay_calldata(
                asset=actual_repay_address,
                amount=repay_amount,
                interest_rate_mode=aave_rate_mode,
                on_behalf_of=compiler.wallet_address,
            )

            repay_tx = TransactionData(
                to=pool_address,
                value=0,
                data="0x" + repay_calldata.hex(),
                gas_estimate=adapter.estimate_repay_gas(),
                description=(f"Repay {amount_description} {repay_token.symbol} ({rate_mode_label} rate)"),
                tx_type="lending_repay",
            )
            transactions.append(repay_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.REPAY.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "pool_address": pool_address,
                    "repay_token": repay_token.to_dict(),
                    "repay_amount": str(repay_amount),
                    "repay_full": intent.repay_full,
                    "interest_rate_mode": aave_rate_mode,
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled REPAY: {repay_token.symbol}, full={intent.repay_full}, {len(transactions)} txs, {total_gas} gas"
            )

        # =================================================================
        # SPARK PATH (Aave V3 fork with Spark-specific addresses)
        # =================================================================
        elif protocol_lower == "spark":
            from ..connectors.spark import (
                SPARK_POOL_ADDRESSES,
                SPARK_VARIABLE_RATE_MODE,
                SparkAdapter,
                SparkConfig,
            )

            if compiler.chain not in SPARK_POOL_ADDRESSES:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Spark not available on chain: {compiler.chain}. Supported: {list(SPARK_POOL_ADDRESSES.keys())}",
                    intent_id=intent.intent_id,
                )

            spark_config = SparkConfig(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
            )
            spark_adapter = SparkAdapter(spark_config)
            pool_address = spark_adapter.pool_address

            if intent.repay_full:
                # Same as Aave path: query wallet balance to avoid InsufficientFunds()
                # if accrued interest exceeds original principal in the wallet.
                wallet_balance = compiler._query_erc20_balance(repay_token.address, compiler.wallet_address)
                if wallet_balance is None:
                    repay_amount = MAX_UINT256
                    logger.warning(
                        f"repay_full: could not query wallet balance for {repay_token.symbol}, "
                        f"falling back to MAX_UINT256 (may fail if interest accrued exceeds balance)"
                    )
                else:
                    repay_amount = wallet_balance
                    logger.debug(
                        f"repay_full: using on-chain wallet balance {repay_amount} wei for {repay_token.symbol}"
                    )
            else:
                assert repay_amount_decimal is not None
                repay_amount = int(repay_amount_decimal * Decimal(10**repay_token.decimals))

            approve_amount = repay_amount

            actual_repay_address = repay_token.address
            if not repay_token.is_native:
                approve_txs = compiler._build_approve_tx(
                    repay_token.address,
                    pool_address,
                    approve_amount,
                )
                transactions.extend(approve_txs)
            else:
                weth_address = compiler._get_wrapped_native_address()
                if weth_address:
                    actual_repay_address = weth_address
                    approve_txs = compiler._build_approve_tx(
                        weth_address,
                        pool_address,
                        approve_amount,
                    )
                    transactions.extend(approve_txs)
                    warnings.append("Native token debt: using WETH for repayment")

            # Resolve interest rate mode: use intent value or default to variable
            # Note: stable rate is deprecated on Spark, rejected at intent layer
            spark_repay_rate_mode = SPARK_VARIABLE_RATE_MODE
            spark_repay_rate_label = "variable"

            # Build repay TX via Spark adapter.
            # When we have a concrete wallet balance, pass repay_all=False so the
            # adapter uses the exact amount instead of overriding with MAX_UINT256.
            spark_use_repay_all = repay_amount == MAX_UINT256
            spark_amount = (
                Decimal(repay_amount) / Decimal(10**repay_token.decimals)
                if not spark_use_repay_all
                else (repay_amount_decimal or Decimal("0"))
            )
            repay_result = spark_adapter.repay(
                asset=actual_repay_address,
                amount=spark_amount,
                interest_rate_mode=spark_repay_rate_mode,
                on_behalf_of=compiler.wallet_address,
                repay_all=spark_use_repay_all,
            )

            if not repay_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Spark repay failed: {repay_result.error}",
                    intent_id=intent.intent_id,
                )

            assert repay_result.tx_data is not None
            repay_data = repay_result.tx_data["data"]
            if not repay_data.startswith("0x"):
                repay_data = "0x" + repay_data

            repay_tx = TransactionData(
                to=repay_result.tx_data["to"],
                value=0,
                data=repay_data,
                gas_estimate=repay_result.gas_estimate,
                description=repay_result.description
                or f"Repay {amount_description} {repay_token.symbol} to Spark ({spark_repay_rate_label} rate)",
                tx_type="lending_repay",
            )
            transactions.append(repay_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.REPAY.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "pool_address": pool_address,
                    "repay_token": repay_token.to_dict(),
                    "repay_amount": str(repay_amount),
                    "repay_full": intent.repay_full,
                    "interest_rate_mode": spark_repay_rate_mode,
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled REPAY: {repay_token.symbol}, full={intent.repay_full}, {len(transactions)} txs, {total_gas} gas (Spark)"
            )

        # =================================================================
        # COMPOUND V3 PATH
        # =================================================================
        elif protocol_lower == "compound_v3":
            from ..connectors.compound_v3.adapter import (
                COMPOUND_V3_COMET_ADDRESSES,
                CompoundV3Adapter,
                CompoundV3Config,
            )

            market = intent.market_id or "usdc"

            if compiler.chain not in COMPOUND_V3_COMET_ADDRESSES:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Compound V3 not available on chain: {compiler.chain}. Supported: {list(COMPOUND_V3_COMET_ADDRESSES.keys())}",
                    intent_id=intent.intent_id,
                )

            available_markets = COMPOUND_V3_COMET_ADDRESSES.get(compiler.chain, {})
            if market not in available_markets:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Compound V3 market '{market}' not available on {compiler.chain}. Available: {list(available_markets.keys())}",
                    intent_id=intent.intent_id,
                )

            compound_config = CompoundV3Config(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
                market=market,
            )
            compound_adapter = CompoundV3Adapter(compound_config)

            # Build approve TX for Comet contract (repay token -> Comet)
            if repay_amount_decimal is not None:
                approve_amount = int(repay_amount_decimal * Decimal(10**repay_token.decimals))
            else:
                approve_amount = MAX_UINT256  # Approve max for full repay

            approve_txs = compiler._build_approve_tx(
                repay_token.address,
                compound_adapter.comet_address,
                approve_amount,
            )
            transactions.extend(approve_txs)

            # Build repay TX via Compound V3 adapter
            repay_result = compound_adapter.repay(
                amount=repay_amount_decimal if repay_amount_decimal else Decimal("0"),
                on_behalf_of=compiler.wallet_address,
                repay_all=intent.repay_full,
            )

            if not repay_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Compound V3 repay failed: {repay_result.error}",
                    intent_id=intent.intent_id,
                )

            assert repay_result.tx_data is not None
            repay_data = repay_result.tx_data["data"]
            if not repay_data.startswith("0x"):
                repay_data = "0x" + repay_data

            repay_tx = TransactionData(
                to=repay_result.tx_data["to"],
                value=int(repay_result.tx_data.get("value", 0)),
                data=repay_data,
                gas_estimate=repay_result.gas_estimate,
                description=repay_result.description
                or f"Repay {amount_description} {repay_token.symbol} to Compound V3",
                tx_type="lending_repay",
            )
            transactions.append(repay_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.REPAY.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "comet_address": compound_adapter.comet_address,
                    "market": market,
                    "repay_token": repay_token.to_dict(),
                    "repay_amount": amount_description,
                    "repay_full": intent.repay_full,
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled REPAY: {amount_description} {repay_token.symbol} to Compound V3 {market}, "
                f"full={intent.repay_full}, {len(transactions)} txs, {total_gas} gas"
            )

        # =================================================================
        # BENQI PATH (Compound V2 fork on Avalanche)
        # =================================================================
        elif protocol_lower == "benqi":
            from ..connectors.benqi.adapter import (
                BENQI_QI_TOKENS,
                BenqiAdapter,
                BenqiConfig,
            )

            if compiler.chain != "avalanche":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"BENQI is only available on Avalanche, got: {compiler.chain}",
                    intent_id=intent.intent_id,
                )

            benqi_config = BenqiConfig(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
            )
            benqi_adapter = BenqiAdapter(benqi_config)

            repay_symbol = repay_token.symbol.upper()
            repay_market = benqi_adapter.get_market_info(repay_symbol)

            if not repay_market:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"BENQI does not support asset: {repay_symbol}. Supported: {list(BENQI_QI_TOKENS.keys())}",
                    intent_id=intent.intent_id,
                )

            # Build approve TX for qiToken (skip for native AVAX)
            if not repay_market.is_native and not intent.repay_full:
                if repay_amount_decimal is None:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="BENQI repay requires an explicit amount (or use repay_full=True)",
                        intent_id=intent.intent_id,
                    )
                repay_amount_wei = int(repay_amount_decimal * Decimal(10**repay_token.decimals))
                approve_txs = compiler._build_approve_tx(
                    repay_token.address,
                    repay_market.qi_token_address,
                    repay_amount_wei,
                )
                transactions.extend(approve_txs)
            elif not repay_market.is_native and intent.repay_full:
                # For repay_full, approve MAX_UINT256
                from ..connectors.benqi.adapter import MAX_UINT256 as BENQI_MAX_UINT256

                approve_txs = compiler._build_approve_tx(
                    repay_token.address,
                    repay_market.qi_token_address,
                    BENQI_MAX_UINT256,
                )
                transactions.extend(approve_txs)

            # Build repay TX
            repay_result = benqi_adapter.repay(
                asset=repay_symbol,
                amount=repay_amount_decimal if repay_amount_decimal is not None else Decimal("0"),
                repay_all=intent.repay_full,
            )

            if not repay_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"BENQI repay failed: {repay_result.error}",
                    intent_id=intent.intent_id,
                )

            assert repay_result.tx_data is not None
            repay_data = repay_result.tx_data["data"]
            if not repay_data.startswith("0x"):
                repay_data = "0x" + repay_data

            amount_description = "all" if intent.repay_full else str(repay_amount_decimal)

            repay_tx = TransactionData(
                to=repay_result.tx_data["to"],
                value=int(repay_result.tx_data.get("value", 0)),
                data=repay_data,
                gas_estimate=repay_result.gas_estimate,
                description=repay_result.description,
                tx_type="lending_repay",
            )
            transactions.append(repay_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.REPAY.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "comptroller_address": benqi_adapter.comptroller_address,
                    "repay_token": repay_token.to_dict(),
                    "repay_amount": amount_description,
                    "repay_full": intent.repay_full,
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled REPAY: {amount_description} {repay_token.symbol} to BENQI, "
                f"full={intent.repay_full}, {len(transactions)} txs, {total_gas} gas"
            )

        # =================================================================
        # JOE LEND PATH (Compound V2 fork on Avalanche — Banker Joe)
        # =================================================================
        elif protocol_lower == "joelend":
            from ..connectors.joelend.adapter import (
                JOELEND_J_TOKENS,
                JoeLendAdapter,
                JoeLendConfig,
            )

            if compiler.chain != "avalanche":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Joe Lend is only available on Avalanche, got: {compiler.chain}",
                    intent_id=intent.intent_id,
                )

            joelend_config = JoeLendConfig(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
            )
            joelend_adapter = JoeLendAdapter(joelend_config)

            repay_symbol = repay_token.symbol.upper()
            jl_repay_market = joelend_adapter.get_market_info(repay_symbol)

            if not jl_repay_market:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Joe Lend does not support asset: {repay_symbol}. Supported: {list(JOELEND_J_TOKENS.keys())}",
                    intent_id=intent.intent_id,
                )

            # Build approve TX for jToken (skip for native AVAX)
            if not jl_repay_market.is_native and not intent.repay_full:
                if repay_amount_decimal is None:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="Joe Lend repay requires an explicit amount (or use repay_full=True)",
                        intent_id=intent.intent_id,
                    )
                repay_amount_wei = int(repay_amount_decimal * Decimal(10**repay_token.decimals))
                approve_txs = compiler._build_approve_tx(
                    repay_token.address,
                    jl_repay_market.j_token_address,
                    repay_amount_wei,
                )
                transactions.extend(approve_txs)
            elif not jl_repay_market.is_native and intent.repay_full:
                # For repay_full, approve MAX_UINT256
                from ..connectors.joelend.adapter import MAX_UINT256 as JOELEND_MAX_UINT256

                approve_txs = compiler._build_approve_tx(
                    repay_token.address,
                    jl_repay_market.j_token_address,
                    JOELEND_MAX_UINT256,
                )
                transactions.extend(approve_txs)

            # Fail fast: native AVAX repay_full requires an explicit amount
            # (the adapter uses amount as msg.value, MAX_UINT256 trick doesn't apply)
            if jl_repay_market.is_native and intent.repay_full and not repay_amount_decimal:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="Joe Lend native AVAX repay_full requires an explicit repay amount (query debt balance first)",
                    intent_id=intent.intent_id,
                )

            # Build repay TX
            repay_result = joelend_adapter.repay(
                asset=repay_symbol,
                amount=repay_amount_decimal if repay_amount_decimal is not None else Decimal("0"),
                repay_all=intent.repay_full,
            )

            if not repay_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Joe Lend repay failed: {repay_result.error}",
                    intent_id=intent.intent_id,
                )

            assert repay_result.tx_data is not None
            repay_data = repay_result.tx_data["data"]
            if not repay_data.startswith("0x"):
                repay_data = "0x" + repay_data

            amount_description = "all" if intent.repay_full else str(repay_amount_decimal)

            repay_tx = TransactionData(
                to=repay_result.tx_data["to"],
                value=int(repay_result.tx_data.get("value", 0)),
                data=repay_data,
                gas_estimate=repay_result.gas_estimate,
                description=repay_result.description,
                tx_type="lending_repay",
            )
            transactions.append(repay_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.REPAY.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "joetroller_address": joelend_adapter.joetroller_address,
                    "repay_token": repay_token.to_dict(),
                    "repay_amount": amount_description,
                    "repay_full": intent.repay_full,
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled REPAY: {amount_description} {repay_token.symbol} to Joe Lend, "
                f"full={intent.repay_full}, {len(transactions)} txs, {total_gas} gas"
            )

        # =================================================================
        elif protocol_lower == "silo_v2":
            from ..connectors.silo_v2.adapter import (
                MAX_UINT256 as SILO_MAX_UINT256,
            )
            from ..connectors.silo_v2.adapter import (
                SILO_V2_MARKETS,
                SiloV2Adapter,
                SiloV2Config,
            )

            if compiler.chain != "avalanche":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Silo V2 is only available on Avalanche, got: {compiler.chain}",
                    intent_id=intent.intent_id,
                )

            silo_config = SiloV2Config(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
            )
            silo_adapter = SiloV2Adapter(silo_config)

            repay_symbol = repay_token.symbol.upper()
            sv2_silo_result = silo_adapter.find_silo_for_asset(repay_symbol)

            if not sv2_silo_result:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"No Silo V2 market found for asset: {repay_symbol}. Available: {list(SILO_V2_MARKETS.keys())}",
                    intent_id=intent.intent_id,
                )

            sv2_market, silo_address, _ = sv2_silo_result

            # Build approve TX for the silo
            if not intent.repay_full:
                if repay_amount_decimal is None:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="Silo V2 repay requires an explicit amount (or use repay_full=True)",
                        intent_id=intent.intent_id,
                    )
                repay_amount_wei = int(repay_amount_decimal * Decimal(10**repay_token.decimals))
                approve_txs = compiler._build_approve_tx(
                    repay_token.address,
                    silo_address,
                    repay_amount_wei,
                )
                transactions.extend(approve_txs)
            else:
                # For repay_full, approve MAX_UINT256
                approve_txs = compiler._build_approve_tx(
                    repay_token.address,
                    silo_address,
                    SILO_MAX_UINT256,
                )
                transactions.extend(approve_txs)

            # Build repay TX
            repay_result = silo_adapter.repay(
                asset=repay_symbol,
                amount=repay_amount_decimal if repay_amount_decimal is not None else Decimal("0"),
                market_name=sv2_market.market_name,
                repay_all=intent.repay_full,
            )

            if not repay_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Silo V2 repay failed: {repay_result.error}",
                    intent_id=intent.intent_id,
                )

            assert repay_result.tx_data is not None
            repay_data = repay_result.tx_data["data"]
            if not repay_data.startswith("0x"):
                repay_data = "0x" + repay_data

            amount_description = "all" if intent.repay_full else str(repay_amount_decimal)

            repay_tx = TransactionData(
                to=repay_result.tx_data["to"],
                value=int(repay_result.tx_data.get("value", 0)),
                data=repay_data,
                gas_estimate=repay_result.gas_estimate,
                description=repay_result.description,
                tx_type="lending_repay",
            )
            transactions.append(repay_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.REPAY.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "silo_config": sv2_market.silo_config,
                    "market_name": sv2_market.market_name,
                    "repay_token": repay_token.to_dict(),
                    "repay_amount": amount_description,
                    "repay_full": intent.repay_full,
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled REPAY: {amount_description} {repay_token.symbol} to Silo V2 ({sv2_market.market_name}), "
                f"full={intent.repay_full}, {len(transactions)} txs, {total_gas} gas"
            )

        # =================================================================
        # UNSUPPORTED PROTOCOL
        # =================================================================
        else:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unsupported lending protocol: {intent.protocol}. Supported: aave_v3, morpho, morpho_blue, spark, compound_v3, benqi, joelend, silo_v2",
                intent_id=intent.intent_id,
            )

    except Exception as e:
        logger.exception(f"Failed to compile REPAY intent: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)

    return result


def compile_supply(compiler, intent: SupplyIntent) -> CompilationResult:
    """Compile a SUPPLY intent into an ActionBundle.

    This method:
    1. Resolves token address
    2. Converts amount to wei
    3. Builds approve TX for supply token
    4. Builds supply TX to deposit tokens

    Args:
        compiler: IntentCompiler instance
        intent: SupplyIntent to compile

    Returns:
        CompilationResult with supply ActionBundle
    """
    from .compiler_adapters import AaveV3Adapter

    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    transactions: list[TransactionData] = []
    warnings: list[str] = []

    try:
        protocol_lower = intent.protocol.lower()

        # =================================================================
        # SOLANA LENDING PATH (Kamino / Jupiter Lend)
        # =================================================================
        if protocol_lower == "jupiter_lend":
            if not compiler._is_solana_chain():
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error="Protocol 'jupiter_lend' is only available on Solana chains.",
                )
            return compiler._compile_jupiter_lend_supply(intent)
        if protocol_lower == "kamino" or (
            compiler._is_solana_chain() and protocol_lower not in ("morpho", "morpho_blue", "jupiter_lend")
        ):
            if compiler._is_solana_chain() and protocol_lower not in ("kamino", ""):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error=f"Protocol '{intent.protocol}' is not supported for SUPPLY on Solana. Supported: kamino, jupiter_lend",
                )
            return compiler._compile_kamino_supply(intent)

        # Step 1: Resolve token address (needed for both protocols)
        supply_token = compiler._resolve_token(intent.token)
        if supply_token is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown token: {intent.token}",
                intent_id=intent.intent_id,
            )

        # Step 2: Check for chained amount
        if intent.amount == "all":
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=(
                    "amount='all' for supply must be resolved to a wallet balance before compilation. "
                    "This should be done by the strategy runner or teardown manager."
                ),
                intent_id=intent.intent_id,
            )
        amount_decimal: Decimal = intent.amount  # type: ignore[assignment]

        # =================================================================
        # MORPHO BLUE PATH
        # =================================================================
        if protocol_lower in ("morpho", "morpho_blue"):
            # Validate market_id is provided
            if not intent.market_id:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="market_id is required for Morpho Blue supply",
                    intent_id=intent.intent_id,
                )

            # Lazy import to avoid circular import
            from ..connectors.morpho_blue.adapter import MorphoBlueAdapter, MorphoBlueConfig

            # Create Morpho adapter
            morpho_config = MorphoBlueConfig(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
            )
            morpho_adapter = MorphoBlueAdapter(morpho_config)

            # Build approve TX for Morpho Blue contract
            approve_txs = compiler._build_approve_tx(
                supply_token.address,
                morpho_adapter.morpho_address,
                int(amount_decimal * Decimal(10**supply_token.decimals)),
            )
            transactions.extend(approve_txs)

            # Morpho Blue has two supply paths:
            # - supply() for loan-token deposits (lending to earn interest)
            # - supply_collateral() for collateral deposits (to enable borrowing)
            # Route based on use_as_collateral flag: True -> collateral, False -> loan-token
            if intent.use_as_collateral:
                tx_result = morpho_adapter.supply_collateral(
                    market_id=intent.market_id,
                    amount=amount_decimal,
                    on_behalf_of=compiler.wallet_address,
                )
                tx_type_label = "lending_supply_collateral"
                description_suffix = "as collateral"
            else:
                tx_result = morpho_adapter.supply(
                    market_id=intent.market_id,
                    amount=amount_decimal,
                    on_behalf_of=compiler.wallet_address,
                )
                tx_type_label = "lending_supply"
                description_suffix = "as loan token"

            if not tx_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Morpho Blue supply failed: {tx_result.error}",
                    intent_id=intent.intent_id,
                )

            assert tx_result.tx_data is not None
            supply_tx = TransactionData(
                to=tx_result.tx_data["to"],
                value=tx_result.tx_data["value"],
                data=tx_result.tx_data["data"],
                gas_estimate=tx_result.gas_estimate,
                description=tx_result.description
                or f"Supply {amount_decimal} {supply_token.symbol} to Morpho Blue {description_suffix}",
                tx_type=tx_type_label,
            )
            transactions.append(supply_tx)

            # Build ActionBundle for Morpho
            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.SUPPLY.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "morpho_address": morpho_adapter.morpho_address,
                    "market_id": intent.market_id,
                    "supply_token": supply_token.to_dict(),
                    "supply_amount": str(amount_decimal),
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled SUPPLY: {amount_decimal} {supply_token.symbol} to Morpho Blue market {intent.market_id[:16]}..."
            )
            return result

        # =================================================================
        # AAVE-COMPATIBLE PATH (Aave V3 + Radiant V2)
        # =================================================================
        elif protocol_lower in AAVE_COMPATIBLE_PROTOCOLS:
            # Get lending adapter
            adapter = AaveV3Adapter(compiler.chain, protocol_lower)
            pool_address = adapter.get_pool_address()

            if pool_address == "0x0000000000000000000000000000000000000000":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"{intent.protocol} not available on chain: {compiler.chain}",
                    intent_id=intent.intent_id,
                )

            supply_amount = int(amount_decimal * Decimal(10**supply_token.decimals))

            # Handle native token vs ERC20
            actual_supply_address = supply_token.address
            supply_value = 0

            if supply_token.is_native:
                weth_address = compiler._get_wrapped_native_address()
                if weth_address:
                    actual_supply_address = weth_address
                    warnings.append("Native token supply: will wrap to WETH before supplying")
                else:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="Cannot supply native ETH - WETH address not found",
                        intent_id=intent.intent_id,
                    )

            # Build approve TX (skip for native token scenarios)
            if not supply_token.is_native:
                approve_txs = compiler._build_approve_tx(
                    actual_supply_address,
                    pool_address,
                    supply_amount,
                )
                transactions.extend(approve_txs)

            # Build supply TX
            supply_calldata = adapter.get_supply_calldata(
                asset=actual_supply_address,
                amount=supply_amount,
                on_behalf_of=compiler.wallet_address,
            )

            supply_tx = TransactionData(
                to=pool_address,
                value=supply_value,
                data="0x" + supply_calldata.hex(),
                gas_estimate=adapter.estimate_supply_gas(),
                description=(
                    f"Supply {compiler._format_amount(supply_amount, supply_token.decimals)} {supply_token.symbol} to {intent.protocol}"
                ),
                tx_type="lending_supply",
            )
            transactions.append(supply_tx)

            # Build setUserUseReserveAsCollateral TX if requested
            if intent.use_as_collateral:
                set_collateral_calldata = adapter.get_set_collateral_calldata(
                    asset=actual_supply_address,
                    use_as_collateral=True,
                )

                set_collateral_tx = TransactionData(
                    to=pool_address,
                    value=0,
                    data="0x" + set_collateral_calldata.hex(),
                    gas_estimate=adapter.estimate_set_collateral_gas(),
                    description=(f"Enable {supply_token.symbol} as collateral on {intent.protocol}"),
                    tx_type="lending_set_collateral",
                )
                transactions.append(set_collateral_tx)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.SUPPLY.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "pool_address": pool_address,
                    "supply_token": supply_token.to_dict(),
                    "supply_amount": str(supply_amount),
                    "use_as_collateral": intent.use_as_collateral,
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            # Format amounts for user-friendly logging
            supply_fmt = format_token_amount(supply_amount, supply_token.symbol, supply_token.decimals)
            collateral_str = " (as collateral)" if intent.use_as_collateral else ""

            logger.info(f"Compiled SUPPLY: {supply_fmt} to {intent.protocol}{collateral_str}")
            logger.info(f"   Txs: {len(transactions)} | Gas: {total_gas:,}")

        # =================================================================
        # SPARK PATH (Aave V3 fork with Spark-specific addresses)
        # =================================================================
        elif protocol_lower == "spark":
            from ..connectors.spark import (
                SPARK_POOL_ADDRESSES,
                SparkAdapter,
                SparkConfig,
            )

            if compiler.chain not in SPARK_POOL_ADDRESSES:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Spark not available on chain: {compiler.chain}. Supported: {list(SPARK_POOL_ADDRESSES.keys())}",
                    intent_id=intent.intent_id,
                )

            spark_config = SparkConfig(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
            )
            spark_adapter = SparkAdapter(spark_config)
            pool_address = spark_adapter.pool_address

            supply_amount = int(amount_decimal * Decimal(10**supply_token.decimals))

            # Handle native token vs ERC20
            actual_supply_address = supply_token.address
            supply_value = 0

            if supply_token.is_native:
                weth_address = compiler._get_wrapped_native_address()
                if weth_address:
                    actual_supply_address = weth_address
                    # Wrap native ETH -> WETH
                    wrap_tx = TransactionData(
                        to=weth_address,
                        value=supply_amount,
                        data="0xd0e30db0",  # WETH.deposit()
                        gas_estimate=compiler_constants.get_gas_estimate(compiler.chain, "wrap_eth"),
                        description=f"Wrap {compiler._format_amount(supply_amount, supply_token.decimals)} {supply_token.symbol} to WETH",
                        tx_type="wrap",
                    )
                    transactions.append(wrap_tx)
                    # Approve WETH for pool
                    approve_txs = compiler._build_approve_tx(
                        weth_address,
                        pool_address,
                        supply_amount,
                    )
                    transactions.extend(approve_txs)
                    warnings.append("Native token supply: wrapped to WETH before supplying")
                else:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="Cannot supply native ETH - WETH address not found",
                        intent_id=intent.intent_id,
                    )
            else:
                approve_txs = compiler._build_approve_tx(
                    actual_supply_address,
                    pool_address,
                    supply_amount,
                )
                transactions.extend(approve_txs)

            # Build supply TX via Spark adapter
            supply_result: Any = spark_adapter.supply(
                asset=actual_supply_address,
                amount=amount_decimal,
                on_behalf_of=compiler.wallet_address,
            )

            if not supply_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Spark supply failed: {supply_result.error}",
                    intent_id=intent.intent_id,
                )

            assert supply_result.tx_data is not None
            supply_data = supply_result.tx_data["data"]
            if not supply_data.startswith("0x"):
                supply_data = "0x" + supply_data

            supply_value = int(supply_result.tx_data.get("value", 0))

            supply_tx = TransactionData(
                to=supply_result.tx_data["to"],
                value=supply_value,
                data=supply_data,
                gas_estimate=supply_result.gas_estimate,
                description=supply_result.description
                or f"Supply {compiler._format_amount(supply_amount, supply_token.decimals)} {supply_token.symbol} to Spark",
                tx_type="lending_supply",
            )
            transactions.append(supply_tx)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.SUPPLY.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "pool_address": pool_address,
                    "supply_token": supply_token.to_dict(),
                    "supply_amount": str(supply_amount),
                    "use_as_collateral": intent.use_as_collateral,
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            supply_fmt = format_token_amount(supply_amount, supply_token.symbol, supply_token.decimals)
            collateral_str = " (as collateral)" if intent.use_as_collateral else ""

            logger.info(f"Compiled SUPPLY: {supply_fmt} to Spark{collateral_str}")
            logger.info(f"   Txs: {len(transactions)} | Gas: {total_gas:,}")

        # =================================================================
        # COMPOUND V3 PATH
        # =================================================================
        elif protocol_lower == "compound_v3":
            from ..connectors.compound_v3.adapter import (
                COMPOUND_V3_COMET_ADDRESSES,
                CompoundV3Adapter,
                CompoundV3Config,
            )

            market = intent.market_id or "usdc"

            if compiler.chain not in COMPOUND_V3_COMET_ADDRESSES:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Compound V3 not available on chain: {compiler.chain}. Supported: {list(COMPOUND_V3_COMET_ADDRESSES.keys())}",
                    intent_id=intent.intent_id,
                )

            available_markets = COMPOUND_V3_COMET_ADDRESSES.get(compiler.chain, {})
            if market not in available_markets:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Compound V3 market '{market}' not available on {compiler.chain}. Available: {list(available_markets.keys())}",
                    intent_id=intent.intent_id,
                )

            compound_config = CompoundV3Config(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
                market=market,
            )
            compound_adapter = CompoundV3Adapter(compound_config)

            supply_amount_wei = int(amount_decimal * Decimal(10**supply_token.decimals))

            # Build approve TX for Comet contract
            approve_txs = compiler._build_approve_tx(
                supply_token.address,
                compound_adapter.comet_address,
                supply_amount_wei,
            )
            transactions.extend(approve_txs)

            # Detect if the token is the base token or a collateral token.
            # Compound V3 uses supply() for the base asset and supply_collateral()
            # for collateral assets — they are different contract methods.
            # Use address comparison (not symbol) for reliable matching.
            # Fail closed if market_config is incomplete — we cannot safely route
            # without knowing the base token address.
            base_token_address = compound_adapter.market_config.get("base_token_address", "")
            if not base_token_address:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Compound V3 market config missing base_token_address for {market} on {compiler.chain} — cannot determine supply routing",
                    intent_id=intent.intent_id,
                )
            is_base_token = supply_token.address.lower() == base_token_address.lower()

            if is_base_token:
                # Supply base asset (earn interest)
                supply_result = compound_adapter.supply(amount=amount_decimal)
            else:
                # In Compound V3, non-base tokens can ONLY be supplied as collateral.
                # If the caller explicitly opted out of collateral, fail closed.
                if not intent.use_as_collateral:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Cannot supply {supply_token.symbol} to Compound V3 {market} market with use_as_collateral=False — non-base tokens can only be supplied as collateral in Compound V3",
                        intent_id=intent.intent_id,
                    )
                # Supply collateral asset (enable borrowing)
                supply_result = compound_adapter.supply_collateral(
                    asset=supply_token.symbol,
                    amount=amount_decimal,
                )

            if not supply_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Compound V3 supply failed: {supply_result.error}",
                    intent_id=intent.intent_id,
                )

            assert supply_result.tx_data is not None
            supply_data = supply_result.tx_data["data"]
            if not supply_data.startswith("0x"):
                supply_data = "0x" + supply_data

            supply_tx = TransactionData(
                to=supply_result.tx_data["to"],
                value=int(supply_result.tx_data.get("value", 0)),
                data=supply_data,
                gas_estimate=supply_result.gas_estimate,
                description=supply_result.description
                or f"Supply {amount_decimal} {supply_token.symbol} to Compound V3",
                tx_type="lending_supply" if is_base_token else "lending_supply_collateral",
            )
            transactions.append(supply_tx)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.SUPPLY.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "comet_address": compound_adapter.comet_address,
                    "market": market,
                    "supply_token": supply_token.to_dict(),
                    "supply_amount": str(amount_decimal),
                    "supply_type": "base" if is_base_token else "collateral",
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            supply_type = "base" if is_base_token else "collateral"
            supply_fmt = format_token_amount(supply_amount_wei, supply_token.symbol, supply_token.decimals)
            logger.info(f"Compiled SUPPLY ({supply_type}): {supply_fmt} to Compound V3 ({market} market)")
            logger.info(f"   Txs: {len(transactions)} | Gas: {total_gas:,}")

        # =================================================================
        # BENQI PATH (Compound V2 fork on Avalanche)
        # =================================================================
        elif protocol_lower == "benqi":
            from ..connectors.benqi.adapter import (
                BENQI_QI_TOKENS,
                BenqiAdapter,
                BenqiConfig,
            )

            if compiler.chain != "avalanche":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"BENQI is only available on Avalanche, got: {compiler.chain}",
                    intent_id=intent.intent_id,
                )

            benqi_config = BenqiConfig(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
            )
            benqi_adapter = BenqiAdapter(benqi_config)

            supply_symbol = supply_token.symbol.upper()
            supply_market = benqi_adapter.get_market_info(supply_symbol)

            if not supply_market:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"BENQI does not support asset: {supply_symbol}. Supported: {list(BENQI_QI_TOKENS.keys())}",
                    intent_id=intent.intent_id,
                )

            supply_amount_wei = int(amount_decimal * Decimal(10**supply_token.decimals))

            # Build approve TX for qiToken (skip for native AVAX)
            if not supply_market.is_native:
                approve_txs = compiler._build_approve_tx(
                    supply_token.address,
                    supply_market.qi_token_address,
                    supply_amount_wei,
                )
                transactions.extend(approve_txs)

            # Build supply (mint) TX
            supply_result = benqi_adapter.supply(
                asset=supply_symbol,
                amount=amount_decimal,
            )

            if not supply_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"BENQI supply failed: {supply_result.error}",
                    intent_id=intent.intent_id,
                )

            assert supply_result.tx_data is not None
            supply_data = supply_result.tx_data["data"]
            if not supply_data.startswith("0x"):
                supply_data = "0x" + supply_data

            supply_tx = TransactionData(
                to=supply_result.tx_data["to"],
                value=int(supply_result.tx_data.get("value", 0)),
                data=supply_data,
                gas_estimate=supply_result.gas_estimate,
                description=supply_result.description or f"Supply {amount_decimal} {supply_token.symbol} to BENQI",
                tx_type="lending_supply",
            )
            transactions.append(supply_tx)

            # Optionally enable as collateral via enterMarkets
            if intent.use_as_collateral:
                enter_result = benqi_adapter.enter_markets([supply_symbol])
                if not enter_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"BENQI enterMarkets failed: {enter_result.error}",
                        intent_id=intent.intent_id,
                    )
                assert enter_result.tx_data is not None
                enter_data = enter_result.tx_data["data"]
                if not enter_data.startswith("0x"):
                    enter_data = "0x" + enter_data
                enter_tx = TransactionData(
                    to=enter_result.tx_data["to"],
                    value=0,
                    data=enter_data,
                    gas_estimate=enter_result.gas_estimate,
                    description=enter_result.description,
                    tx_type="lending_enter_markets",
                )
                transactions.append(enter_tx)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.SUPPLY.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "comptroller_address": benqi_adapter.comptroller_address,
                    "qi_token_address": supply_market.qi_token_address,
                    "supply_token": supply_token.to_dict(),
                    "supply_amount": str(amount_decimal),
                    "use_as_collateral": intent.use_as_collateral,
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            supply_fmt = format_token_amount(supply_amount_wei, supply_token.symbol, supply_token.decimals)
            collateral_str = " (as collateral)" if intent.use_as_collateral else ""
            logger.info(f"Compiled SUPPLY: {supply_fmt} to BENQI{collateral_str}")
            logger.info(f"   Txs: {len(transactions)} | Gas: {total_gas:,}")

        # =================================================================
        # JOE LEND PATH (Compound V2 fork on Avalanche — Banker Joe)
        # =================================================================
        elif protocol_lower == "joelend":
            from ..connectors.joelend.adapter import (
                JOELEND_J_TOKENS,
                JoeLendAdapter,
                JoeLendConfig,
            )

            if compiler.chain != "avalanche":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Joe Lend is only available on Avalanche, got: {compiler.chain}",
                    intent_id=intent.intent_id,
                )

            joelend_config = JoeLendConfig(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
            )
            joelend_adapter = JoeLendAdapter(joelend_config)

            supply_symbol = supply_token.symbol.upper()
            jl_supply_market = joelend_adapter.get_market_info(supply_symbol)

            if not jl_supply_market:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Joe Lend does not support asset: {supply_symbol}. Supported: {list(JOELEND_J_TOKENS.keys())}",
                    intent_id=intent.intent_id,
                )

            supply_amount_wei = int(amount_decimal * Decimal(10**supply_token.decimals))

            # Build approve TX for jToken (skip for native AVAX)
            if not jl_supply_market.is_native:
                approve_txs = compiler._build_approve_tx(
                    supply_token.address,
                    jl_supply_market.j_token_address,
                    supply_amount_wei,
                )
                transactions.extend(approve_txs)

            # Build supply (mint) TX
            jl_supply_result = joelend_adapter.supply(
                asset=supply_symbol,
                amount=amount_decimal,
            )

            if not jl_supply_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Joe Lend supply failed: {jl_supply_result.error}",
                    intent_id=intent.intent_id,
                )

            assert jl_supply_result.tx_data is not None
            supply_data = jl_supply_result.tx_data["data"]
            if not supply_data.startswith("0x"):
                supply_data = "0x" + supply_data

            supply_tx = TransactionData(
                to=jl_supply_result.tx_data["to"],
                value=int(jl_supply_result.tx_data.get("value", 0)),
                data=supply_data,
                gas_estimate=jl_supply_result.gas_estimate,
                description=jl_supply_result.description
                or f"Supply {amount_decimal} {supply_token.symbol} to Joe Lend",
                tx_type="lending_supply",
            )
            transactions.append(supply_tx)

            # Optionally enable as collateral via enterMarkets
            if intent.use_as_collateral:
                jl_enter_result = joelend_adapter.enter_markets([supply_symbol])
                if not jl_enter_result.success:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error=f"Joe Lend enterMarkets failed: {jl_enter_result.error}",
                        intent_id=intent.intent_id,
                    )
                assert jl_enter_result.tx_data is not None
                enter_data = jl_enter_result.tx_data["data"]
                if not enter_data.startswith("0x"):
                    enter_data = "0x" + enter_data
                enter_tx = TransactionData(
                    to=jl_enter_result.tx_data["to"],
                    value=0,
                    data=enter_data,
                    gas_estimate=jl_enter_result.gas_estimate,
                    description=jl_enter_result.description,
                    tx_type="lending_enter_markets",
                )
                transactions.append(enter_tx)

            # Build ActionBundle
            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.SUPPLY.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "joetroller_address": joelend_adapter.joetroller_address,
                    "j_token_address": jl_supply_market.j_token_address,
                    "supply_token": supply_token.to_dict(),
                    "supply_amount": str(amount_decimal),
                    "use_as_collateral": intent.use_as_collateral,
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            supply_fmt = format_token_amount(supply_amount_wei, supply_token.symbol, supply_token.decimals)
            collateral_str = " (as collateral)" if intent.use_as_collateral else ""
            logger.info(f"Compiled SUPPLY: {supply_fmt} to Joe Lend{collateral_str}")
            logger.info(f"   Txs: {len(transactions)} | Gas: {total_gas:,}")

        # =================================================================
        elif protocol_lower == "silo_v2":
            from ..connectors.silo_v2.adapter import (
                SILO_V2_MARKETS,
                SiloV2Adapter,
                SiloV2Config,
            )

            if compiler.chain != "avalanche":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Silo V2 is only available on Avalanche, got: {compiler.chain}",
                    intent_id=intent.intent_id,
                )

            silo_config = SiloV2Config(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
            )
            silo_adapter = SiloV2Adapter(silo_config)

            supply_symbol = supply_token.symbol.upper()
            sv2_silo_result = silo_adapter.find_silo_for_asset(supply_symbol)

            if not sv2_silo_result:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"No Silo V2 market found for asset: {supply_symbol}. Available: {list(SILO_V2_MARKETS.keys())}",
                    intent_id=intent.intent_id,
                )

            sv2_market, silo_address, _ = sv2_silo_result
            supply_amount_wei = int(amount_decimal * Decimal(10**supply_token.decimals))

            # Build approve TX for the silo
            approve_txs = compiler._build_approve_tx(
                supply_token.address,
                silo_address,
                supply_amount_wei,
            )
            transactions.extend(approve_txs)

            # Build deposit TX
            sv2_supply_result = silo_adapter.supply(
                asset=supply_symbol,
                amount=amount_decimal,
                market_name=sv2_market.market_name,
            )

            if not sv2_supply_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Silo V2 supply failed: {sv2_supply_result.error}",
                    intent_id=intent.intent_id,
                )

            assert sv2_supply_result.tx_data is not None
            supply_data = sv2_supply_result.tx_data["data"]
            if not supply_data.startswith("0x"):
                supply_data = "0x" + supply_data

            supply_tx = TransactionData(
                to=sv2_supply_result.tx_data["to"],
                value=int(sv2_supply_result.tx_data.get("value", 0)),
                data=supply_data,
                gas_estimate=sv2_supply_result.gas_estimate,
                description=sv2_supply_result.description
                or f"Deposit {amount_decimal} {supply_token.symbol} to Silo V2",
                tx_type="lending_supply",
            )
            transactions.append(supply_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.SUPPLY.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "silo_config": sv2_market.silo_config,
                    "market_name": sv2_market.market_name,
                    "silo_address": silo_address,
                    "supply_token": supply_token.to_dict(),
                    "supply_amount": str(amount_decimal),
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            supply_fmt = format_token_amount(supply_amount_wei, supply_token.symbol, supply_token.decimals)
            logger.info(f"Compiled SUPPLY: {supply_fmt} to Silo V2 ({sv2_market.market_name})")
            logger.info(f"   Txs: {len(transactions)} | Gas: {total_gas:,}")

        # =================================================================
        # UNSUPPORTED PROTOCOL
        # =================================================================
        else:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unsupported lending protocol: {intent.protocol}. Supported: aave_v3, morpho, morpho_blue, spark, compound_v3, benqi, joelend, silo_v2",
                intent_id=intent.intent_id,
            )

    except Exception as e:
        logger.exception(f"Failed to compile SUPPLY intent: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)

    return result


def compile_withdraw(compiler, intent: WithdrawIntent) -> CompilationResult:
    """Compile a WITHDRAW intent into an ActionBundle.

    This method:
    1. Resolves token address
    2. Converts amount to wei (or uses MAX_UINT256 for withdraw all)
    3. Builds withdraw TX

    Args:
        compiler: IntentCompiler instance
        intent: WithdrawIntent to compile

    Returns:
        CompilationResult with withdraw ActionBundle
    """
    from .compiler_adapters import AaveV3Adapter

    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    transactions: list[TransactionData] = []
    warnings: list[str] = []

    try:
        protocol_lower = intent.protocol.lower()

        # =================================================================
        # SOLANA LENDING PATH (Kamino / Jupiter Lend)
        # =================================================================
        if protocol_lower == "jupiter_lend":
            if not compiler._is_solana_chain():
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error="Protocol 'jupiter_lend' is only available on Solana chains.",
                )
            return compiler._compile_jupiter_lend_withdraw(intent)
        if protocol_lower == "kamino" or (
            compiler._is_solana_chain() and protocol_lower not in ("morpho", "morpho_blue", "jupiter_lend")
        ):
            if compiler._is_solana_chain() and protocol_lower not in ("kamino", ""):
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    intent_id=intent.intent_id,
                    error=f"Protocol '{intent.protocol}' is not supported for WITHDRAW on Solana. Supported: kamino, jupiter_lend",
                )
            return compiler._compile_kamino_withdraw(intent)

        # Step 1: Resolve token address
        withdraw_token = compiler._resolve_token(intent.token)
        if withdraw_token is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown token: {intent.token}",
                intent_id=intent.intent_id,
            )

        # Step 2: Calculate amount
        withdraw_amount_decimal: Decimal | None
        if intent.withdraw_all:
            withdraw_amount_decimal = None  # Will use withdraw_all flag
            warnings.append("Withdrawing all available balance")
        elif intent.amount == "all":
            # amount="all" was not resolved by the amount resolver (no RPC, no reader, etc.)
            # Fall back to withdraw_all=True — let the adapter handle it.
            logger.info(
                "amount='all' reached compiler unresolved for %s — using withdraw_all path",
                intent.protocol,
            )
            withdraw_amount_decimal = None
            intent = intent.model_copy(update={"withdraw_all": True})
            warnings.append("Withdrawing all available balance (amount='all' fallback)")
        else:
            withdraw_amount_decimal = intent.amount  # type: ignore[assignment]

        # =================================================================
        # MORPHO BLUE PATH
        # =================================================================
        if protocol_lower in ("morpho", "morpho_blue"):
            if not intent.market_id:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error="market_id is required for Morpho Blue withdraw",
                    intent_id=intent.intent_id,
                )

            # Lazy import to avoid circular import
            from ..connectors.morpho_blue.adapter import MorphoBlueAdapter, MorphoBlueConfig

            # Resolve RPC URL with compiler's chain-aware fallback logic
            # (explicit rpc_url -> managed Anvil fork -> configured provider)
            morpho_rpc_url = compiler._get_chain_rpc_url()

            morpho_config = MorphoBlueConfig(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
                rpc_url=morpho_rpc_url,  # Pass RPC URL for on-chain queries (e.g., withdraw_all)
            )
            morpho_adapter = MorphoBlueAdapter(morpho_config)

            # Morpho Blue has two withdraw paths (mirrors supply):
            # - withdraw_collateral() for collateral withdrawals
            # - withdraw() for loan-token withdrawals (lender reclaiming supplied funds)
            # Route based on is_collateral flag (default True for backward compat)
            amount_for_adapter = withdraw_amount_decimal if withdraw_amount_decimal else Decimal("0")
            if intent.is_collateral:
                withdraw_result: Any = morpho_adapter.withdraw_collateral(
                    market_id=intent.market_id,
                    amount=amount_for_adapter,
                    receiver=compiler.wallet_address,
                    on_behalf_of=compiler.wallet_address,
                    withdraw_all=intent.withdraw_all,
                )
                tx_type_label = "lending_withdraw_collateral"
                description_suffix = "collateral"
            else:
                withdraw_result = morpho_adapter.withdraw(
                    market_id=intent.market_id,
                    amount=amount_for_adapter,
                    receiver=compiler.wallet_address,
                    on_behalf_of=compiler.wallet_address,
                    withdraw_all=intent.withdraw_all,
                )
                tx_type_label = "lending_withdraw"
                description_suffix = "loan token"

            if not withdraw_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Morpho Blue withdraw failed: {withdraw_result.error}",
                    intent_id=intent.intent_id,
                )

            amount_display = "all" if intent.withdraw_all else str(withdraw_amount_decimal)

            assert withdraw_result.tx_data is not None
            withdraw_tx = TransactionData(
                to=withdraw_result.tx_data["to"],
                value=withdraw_result.tx_data["value"],
                data=withdraw_result.tx_data["data"],
                gas_estimate=withdraw_result.gas_estimate,
                description=withdraw_result.description
                or f"Withdraw {amount_display} {withdraw_token.symbol} {description_suffix} from Morpho Blue",
                tx_type=tx_type_label,
            )
            transactions.append(withdraw_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)
            action_bundle = ActionBundle(
                intent_type=IntentType.WITHDRAW.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "morpho_address": morpho_adapter.morpho_address,
                    "market_id": intent.market_id,
                    "withdraw_token": withdraw_token.to_dict(),
                    "withdraw_amount": amount_display,
                    "withdraw_all": intent.withdraw_all,
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(f"Compiled WITHDRAW: {amount_display} {withdraw_token.symbol} from Morpho Blue")
            return result

        # =================================================================
        # AAVE-COMPATIBLE PATH (Aave V3 + Radiant V2)
        # =================================================================
        elif protocol_lower in AAVE_COMPATIBLE_PROTOCOLS:
            adapter = AaveV3Adapter(compiler.chain, protocol_lower)
            pool_address = adapter.get_pool_address()

            if pool_address == "0x0000000000000000000000000000000000000000":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"{intent.protocol} not available on chain: {compiler.chain}",
                    intent_id=intent.intent_id,
                )

            if intent.withdraw_all:
                withdraw_amount = MAX_UINT256
            else:
                assert withdraw_amount_decimal is not None
                withdraw_amount = int(withdraw_amount_decimal * Decimal(10**withdraw_token.decimals))

            actual_withdraw_address = withdraw_token.address

            if withdraw_token.is_native:
                weth_address = compiler._get_wrapped_native_address()
                if weth_address:
                    actual_withdraw_address = weth_address
                    warnings.append("Native token withdraw: will receive WETH (unwrap separately if needed)")
                else:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="Cannot withdraw native ETH - WETH address not found",
                        intent_id=intent.intent_id,
                    )

            withdraw_calldata = adapter.get_withdraw_calldata(
                asset=actual_withdraw_address,
                amount=withdraw_amount,
                to=compiler.wallet_address,
            )

            amount_display = (
                "all" if intent.withdraw_all else compiler._format_amount(withdraw_amount, withdraw_token.decimals)
            )

            withdraw_tx = TransactionData(
                to=pool_address,
                value=0,
                data="0x" + withdraw_calldata.hex(),
                gas_estimate=adapter.estimate_withdraw_gas(),
                description=(f"Withdraw {amount_display} {withdraw_token.symbol} from {intent.protocol}"),
                tx_type="lending_withdraw",
            )
            transactions.append(withdraw_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.WITHDRAW.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "pool_address": pool_address,
                    "withdraw_token": withdraw_token.to_dict(),
                    "withdraw_amount": str(withdraw_amount),
                    "withdraw_all": intent.withdraw_all,
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled WITHDRAW: {withdraw_token.symbol}, all={intent.withdraw_all}, {len(transactions)} txs, {total_gas} gas"
            )

        # =================================================================
        # SPARK PATH (Aave V3 fork with Spark-specific addresses)
        # =================================================================
        elif protocol_lower == "spark":
            from ..connectors.spark import (
                SPARK_POOL_ADDRESSES,
                SparkAdapter,
                SparkConfig,
            )

            if compiler.chain not in SPARK_POOL_ADDRESSES:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Spark not available on chain: {compiler.chain}. Supported: {list(SPARK_POOL_ADDRESSES.keys())}",
                    intent_id=intent.intent_id,
                )

            spark_config = SparkConfig(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
            )
            spark_adapter = SparkAdapter(spark_config)
            pool_address = spark_adapter.pool_address

            if intent.withdraw_all:
                withdraw_amount = MAX_UINT256
            else:
                assert withdraw_amount_decimal is not None
                withdraw_amount = int(withdraw_amount_decimal * Decimal(10**withdraw_token.decimals))

            actual_withdraw_address = withdraw_token.address

            if withdraw_token.is_native:
                weth_address = compiler._get_wrapped_native_address()
                if weth_address:
                    actual_withdraw_address = weth_address
                    warnings.append("Native token withdraw: will receive WETH (unwrap separately if needed)")
                else:
                    return CompilationResult(
                        status=CompilationStatus.FAILED,
                        error="Cannot withdraw native ETH - WETH address not found",
                        intent_id=intent.intent_id,
                    )

            # Build withdraw TX via Spark adapter
            withdraw_result = spark_adapter.withdraw(
                asset=actual_withdraw_address,
                amount=withdraw_amount_decimal if withdraw_amount_decimal else Decimal("0"),
                to=compiler.wallet_address,
                withdraw_all=intent.withdraw_all,
            )

            if not withdraw_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Spark withdraw failed: {withdraw_result.error}",
                    intent_id=intent.intent_id,
                )

            amount_display = (
                "all" if intent.withdraw_all else compiler._format_amount(withdraw_amount, withdraw_token.decimals)
            )

            assert withdraw_result.tx_data is not None
            withdraw_data = withdraw_result.tx_data["data"]
            if not withdraw_data.startswith("0x"):
                withdraw_data = "0x" + withdraw_data

            withdraw_tx = TransactionData(
                to=withdraw_result.tx_data["to"],
                value=0,
                data=withdraw_data,
                gas_estimate=withdraw_result.gas_estimate,
                description=withdraw_result.description
                or f"Withdraw {amount_display} {withdraw_token.symbol} from Spark",
                tx_type="lending_withdraw",
            )
            transactions.append(withdraw_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.WITHDRAW.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "pool_address": pool_address,
                    "withdraw_token": withdraw_token.to_dict(),
                    "withdraw_amount": str(withdraw_amount),
                    "withdraw_all": intent.withdraw_all,
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled WITHDRAW: {withdraw_token.symbol}, all={intent.withdraw_all}, {len(transactions)} txs, {total_gas} gas (Spark)"
            )

        # =================================================================
        # PENDLE REDEEM PATH
        # =================================================================
        elif protocol_lower == "pendle":
            return compiler._compile_pendle_redeem(intent)

        # =================================================================
        # COMPOUND V3 PATH
        # =================================================================
        elif protocol_lower == "compound_v3":
            from ..connectors.compound_v3.adapter import (
                COMPOUND_V3_COMET_ADDRESSES,
                CompoundV3Adapter,
                CompoundV3Config,
            )

            market = intent.market_id or "usdc"

            if compiler.chain not in COMPOUND_V3_COMET_ADDRESSES:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Compound V3 not available on chain: {compiler.chain}. Supported: {list(COMPOUND_V3_COMET_ADDRESSES.keys())}",
                    intent_id=intent.intent_id,
                )

            available_markets = COMPOUND_V3_COMET_ADDRESSES.get(compiler.chain, {})
            if market not in available_markets:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Compound V3 market '{market}' not available on {compiler.chain}. Available: {list(available_markets.keys())}",
                    intent_id=intent.intent_id,
                )

            # Resolve RPC URL for on-chain queries (needed for collateral withdraw_all)
            compound_rpc_url = compiler._get_chain_rpc_url() if intent.withdraw_all else None

            compound_config = CompoundV3Config(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
                market=market,
                rpc_url=compound_rpc_url,
            )
            compound_adapter = CompoundV3Adapter(compound_config)

            # Detect if the token is the base token or a collateral token.
            # Compound V3 uses withdraw() for the base asset and withdraw_collateral()
            # for collateral assets — they are different contract methods.
            # Compare by address (more robust than symbol, avoids alias ambiguity).
            base_token_address = compound_adapter.market_config.get("base_token_address")
            if not base_token_address:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Compound V3 market config missing base_token_address for market '{market}' on {compiler.chain}",
                    intent_id=intent.intent_id,
                )
            is_base_token = withdraw_token.address.lower() == base_token_address.lower()

            compound_withdraw_amount: Decimal = (
                withdraw_amount_decimal if withdraw_amount_decimal is not None else Decimal("0")
            )

            if is_base_token:
                # Withdraw base asset (reduce lending position)
                # Base token withdraw supports MAX_UINT256 for withdraw_all natively.
                withdraw_result = compound_adapter.withdraw(
                    amount=compound_withdraw_amount,
                    withdraw_all=intent.withdraw_all,
                )
            else:
                # Withdraw collateral asset.
                # For withdraw_all: use the intent's original amount if available, since
                # Compound V3 stores collateral as uint128 and MAX_UINT256 causes safe128() revert.
                # The on-chain query in the adapter is the primary path; the intent amount is
                # the fallback for when no RPC is available.
                collateral_amount = compound_withdraw_amount
                if intent.withdraw_all and collateral_amount == 0 and intent.amount not in (None, "all"):
                    try:
                        collateral_amount = Decimal(str(intent.amount))
                    except (TypeError, ValueError, ArithmeticError):
                        pass

                withdraw_result = compound_adapter.withdraw_collateral(
                    asset=withdraw_token.symbol,
                    amount=collateral_amount,
                    withdraw_all=intent.withdraw_all,
                )

            if not withdraw_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Compound V3 withdraw failed: {withdraw_result.error}",
                    intent_id=intent.intent_id,
                )

            # No-op: withdraw_all on zero collateral returns success with no tx_data.
            # Return a SUCCESS result with an empty ActionBundle so callers don't crash.
            if withdraw_result.tx_data is None:
                return CompilationResult(
                    status=CompilationStatus.SUCCESS,
                    action_bundle=ActionBundle(
                        intent_type=IntentType.WITHDRAW.value,
                        transactions=[],
                        metadata={
                            "protocol": intent.protocol,
                            "comet_address": compound_adapter.comet_address,
                            "market": market,
                            "withdraw_token": withdraw_token.to_dict(),
                            "withdraw_amount": "0",
                            "withdraw_all": intent.withdraw_all,
                            "withdraw_type": "collateral" if not is_base_token else "base",
                            "chain": compiler.chain,
                            "no_op": True,
                            "reason": withdraw_result.description or "Nothing to withdraw (balance is 0)",
                        },
                    ),
                    intent_id=intent.intent_id,
                )

            amount_display = "all" if intent.withdraw_all else str(withdraw_amount_decimal)
            withdraw_data = withdraw_result.tx_data["data"]
            if not withdraw_data.startswith("0x"):
                withdraw_data = "0x" + withdraw_data

            withdraw_tx = TransactionData(
                to=withdraw_result.tx_data["to"],
                value=int(withdraw_result.tx_data.get("value", 0)),
                data=withdraw_data,
                gas_estimate=withdraw_result.gas_estimate,
                description=withdraw_result.description
                or f"Withdraw {amount_display} {withdraw_token.symbol} from Compound V3",
                tx_type="lending_withdraw" if is_base_token else "lending_withdraw_collateral",
            )
            transactions.append(withdraw_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.WITHDRAW.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "comet_address": compound_adapter.comet_address,
                    "market": market,
                    "withdraw_token": withdraw_token.to_dict(),
                    "withdraw_amount": amount_display,
                    "withdraw_all": intent.withdraw_all,
                    "withdraw_type": "base" if is_base_token else "collateral",
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            withdraw_type = "base" if is_base_token else "collateral"
            logger.info(
                f"Compiled WITHDRAW ({withdraw_type}): {withdraw_token.symbol}, all={intent.withdraw_all}, {len(transactions)} txs, {total_gas} gas (Compound V3)"
            )

        # =================================================================
        # BENQI PATH (Compound V2 fork on Avalanche)
        # =================================================================
        elif protocol_lower == "benqi":
            from ..connectors.benqi.adapter import (
                BENQI_QI_TOKENS,
                BenqiAdapter,
                BenqiConfig,
            )

            if compiler.chain != "avalanche":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"BENQI is only available on Avalanche, got: {compiler.chain}",
                    intent_id=intent.intent_id,
                )

            benqi_config = BenqiConfig(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
            )
            benqi_adapter = BenqiAdapter(benqi_config)

            withdraw_symbol = withdraw_token.symbol.upper()
            withdraw_market = benqi_adapter.get_market_info(withdraw_symbol)

            if not withdraw_market:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"BENQI does not support asset: {withdraw_symbol}. Supported: {list(BENQI_QI_TOKENS.keys())}",
                    intent_id=intent.intent_id,
                )

            # Build withdraw (redeem) TX
            withdraw_result = benqi_adapter.withdraw(
                asset=withdraw_symbol,
                amount=withdraw_amount_decimal if withdraw_amount_decimal else Decimal("0"),
                withdraw_all=intent.withdraw_all,
            )

            if not withdraw_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"BENQI withdraw failed: {withdraw_result.error}",
                    intent_id=intent.intent_id,
                )

            amount_display = "all" if intent.withdraw_all else str(withdraw_amount_decimal)

            assert withdraw_result.tx_data is not None
            withdraw_data = withdraw_result.tx_data["data"]
            if not withdraw_data.startswith("0x"):
                withdraw_data = "0x" + withdraw_data

            withdraw_tx = TransactionData(
                to=withdraw_result.tx_data["to"],
                value=int(withdraw_result.tx_data.get("value", 0)),
                data=withdraw_data,
                gas_estimate=withdraw_result.gas_estimate,
                description=withdraw_result.description
                or f"Withdraw {amount_display} {withdraw_token.symbol} from BENQI",
                tx_type="lending_withdraw",
            )
            transactions.append(withdraw_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.WITHDRAW.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "comptroller_address": benqi_adapter.comptroller_address,
                    "qi_token_address": withdraw_market.qi_token_address,
                    "withdraw_token": withdraw_token.to_dict(),
                    "withdraw_amount": amount_display,
                    "withdraw_all": intent.withdraw_all,
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled WITHDRAW: {withdraw_token.symbol}, all={intent.withdraw_all}, {len(transactions)} txs, {total_gas} gas (BENQI)"
            )

        # =================================================================
        # JOE LEND PATH (Compound V2 fork on Avalanche — Banker Joe)
        # =================================================================
        elif protocol_lower == "joelend":
            from ..connectors.joelend.adapter import (
                JOELEND_J_TOKENS,
                JoeLendAdapter,
                JoeLendConfig,
            )

            if compiler.chain != "avalanche":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Joe Lend is only available on Avalanche, got: {compiler.chain}",
                    intent_id=intent.intent_id,
                )

            joelend_config = JoeLendConfig(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
            )
            joelend_adapter = JoeLendAdapter(joelend_config)

            withdraw_symbol = withdraw_token.symbol.upper()
            jl_withdraw_market = joelend_adapter.get_market_info(withdraw_symbol)

            if not jl_withdraw_market:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Joe Lend does not support asset: {withdraw_symbol}. Supported: {list(JOELEND_J_TOKENS.keys())}",
                    intent_id=intent.intent_id,
                )

            # Build withdraw (redeem) TX
            jl_withdraw_result = joelend_adapter.withdraw(
                asset=withdraw_symbol,
                amount=withdraw_amount_decimal if withdraw_amount_decimal else Decimal("0"),
                withdraw_all=intent.withdraw_all,
            )

            if not jl_withdraw_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Joe Lend withdraw failed: {jl_withdraw_result.error}",
                    intent_id=intent.intent_id,
                )

            amount_display = "all" if intent.withdraw_all else str(withdraw_amount_decimal)

            assert jl_withdraw_result.tx_data is not None
            withdraw_data = jl_withdraw_result.tx_data["data"]
            if not withdraw_data.startswith("0x"):
                withdraw_data = "0x" + withdraw_data

            withdraw_tx = TransactionData(
                to=jl_withdraw_result.tx_data["to"],
                value=int(jl_withdraw_result.tx_data.get("value", 0)),
                data=withdraw_data,
                gas_estimate=jl_withdraw_result.gas_estimate,
                description=jl_withdraw_result.description
                or f"Withdraw {amount_display} {withdraw_token.symbol} from Joe Lend",
                tx_type="lending_withdraw",
            )
            transactions.append(withdraw_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.WITHDRAW.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "joetroller_address": joelend_adapter.joetroller_address,
                    "j_token_address": jl_withdraw_market.j_token_address,
                    "withdraw_token": withdraw_token.to_dict(),
                    "withdraw_amount": amount_display,
                    "withdraw_all": intent.withdraw_all,
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled WITHDRAW: {withdraw_token.symbol}, all={intent.withdraw_all}, {len(transactions)} txs, {total_gas} gas (Joe Lend)"
            )

        # =================================================================
        elif protocol_lower == "silo_v2":
            from ..connectors.silo_v2.adapter import (
                SILO_V2_MARKETS,
                SiloV2Adapter,
                SiloV2Config,
            )

            if compiler.chain != "avalanche":
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Silo V2 is only available on Avalanche, got: {compiler.chain}",
                    intent_id=intent.intent_id,
                )

            silo_config = SiloV2Config(
                chain=compiler.chain,
                wallet_address=compiler.wallet_address,
            )
            silo_adapter = SiloV2Adapter(silo_config)

            withdraw_symbol = withdraw_token.symbol.upper()
            sv2_silo_result = silo_adapter.find_silo_for_asset(withdraw_symbol)

            if not sv2_silo_result:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"No Silo V2 market found for asset: {withdraw_symbol}. Available: {list(SILO_V2_MARKETS.keys())}",
                    intent_id=intent.intent_id,
                )

            sv2_market, silo_address, _ = sv2_silo_result

            # Build withdraw TX
            withdraw_result = silo_adapter.withdraw(
                asset=withdraw_symbol,
                amount=withdraw_amount_decimal if withdraw_amount_decimal else Decimal("0"),
                market_name=sv2_market.market_name,
                withdraw_all=intent.withdraw_all,
            )

            if not withdraw_result.success:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Silo V2 withdraw failed: {withdraw_result.error}",
                    intent_id=intent.intent_id,
                )

            amount_display = "all" if intent.withdraw_all else str(withdraw_amount_decimal)

            assert withdraw_result.tx_data is not None
            withdraw_data = withdraw_result.tx_data["data"]
            if not withdraw_data.startswith("0x"):
                withdraw_data = "0x" + withdraw_data

            withdraw_tx = TransactionData(
                to=withdraw_result.tx_data["to"],
                value=int(withdraw_result.tx_data.get("value", 0)),
                data=withdraw_data,
                gas_estimate=withdraw_result.gas_estimate,
                description=withdraw_result.description
                or f"Withdraw {amount_display} {withdraw_token.symbol} from Silo V2",
                tx_type="lending_withdraw",
            )
            transactions.append(withdraw_tx)

            total_gas = sum(tx.gas_estimate for tx in transactions)

            action_bundle = ActionBundle(
                intent_type=IntentType.WITHDRAW.value,
                transactions=[tx.to_dict() for tx in transactions],
                metadata={
                    "protocol": intent.protocol,
                    "silo_config": sv2_market.silo_config,
                    "market_name": sv2_market.market_name,
                    "silo_address": silo_address,
                    "withdraw_token": withdraw_token.to_dict(),
                    "withdraw_amount": amount_display,
                    "withdraw_all": intent.withdraw_all,
                    "chain": compiler.chain,
                },
            )

            result.action_bundle = action_bundle
            result.transactions = transactions
            result.total_gas_estimate = total_gas
            result.warnings = warnings

            logger.info(
                f"Compiled WITHDRAW: {withdraw_token.symbol}, all={intent.withdraw_all}, {len(transactions)} txs, {total_gas} gas (Silo V2 {sv2_market.market_name})"
            )

        # =================================================================
        # UNSUPPORTED PROTOCOL
        # =================================================================
        else:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unsupported lending protocol: {intent.protocol}. Supported: aave_v3, morpho, morpho_blue, spark, pendle, compound_v3, benqi, joelend, silo_v2",
                intent_id=intent.intent_id,
            )

    except Exception as e:
        logger.exception(f"Failed to compile WITHDRAW intent: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)

    return result
