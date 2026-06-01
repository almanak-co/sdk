"""Flash loan compilation helpers extracted from IntentCompiler.

These standalone functions receive the compiler instance as their first
parameter and implement all flash-loan-related compilation logic (Aave,
Balancer, Morpho).
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING

from almanak.connectors._strategy_flash_loan_registry import FLASH_LOAN_PROVIDER_REGISTRY

from ..models.reproduction_bundle import ActionBundle
from .compiler_models import CompilationResult, CompilationStatus, TransactionData
from .flash_loan_selector import FlashLoanSelector, NoProviderAvailableError
from .vocabulary import AnyIntent, Intent, IntentType, SwapIntent

if TYPE_CHECKING:
    from .vocabulary import FlashLoanIntent

logger = logging.getLogger("almanak.framework.intents.compiler")


_ZERO_ADDRESS = "0x" + "0" * 40


def _build_flash_loan_selector(chain: str) -> FlashLoanSelector:
    """Construct the cross-protocol selector with all registered providers.

    Each provider is self-contained inside its protocol connector and opts
    in via ``FLASH_LOAN_PROVIDER_REGISTRY`` (populated in
    ``almanak/connectors/_strategy_flash_loan_registry.py``). This function
    names no connector — adding one is a registration line in that boot file,
    with no edit here. (VIB-4837.)
    """
    return FlashLoanSelector(
        chain=chain,
        providers=FLASH_LOAN_PROVIDER_REGISTRY.providers(),
    )


def _check_wallet_for_flash_loan(
    compiler,
    intent: FlashLoanIntent,
    warnings: list[str],
) -> CompilationResult | None:
    """Verify the wallet can receive flash-loan callbacks.

    Flash loans require a contract wallet (e.g. Safe) because the lending
    protocol calls back into the recipient during the same transaction.
    EOA wallets have no bytecode and cannot handle the callback. The
    zero-address sentinel is used by permission-discovery synthetic
    intents and is skipped.

    Returns:
        A FAILED ``CompilationResult`` for an EOA wallet, or ``None`` if
        the wallet is a contract / cannot be verified. Pushes a warning
        into ``warnings`` when verification is inconclusive.
    """
    if compiler.wallet_address == _ZERO_ADDRESS:
        return None
    is_contract = compiler._is_wallet_contract()
    if is_contract is False:
        return CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                # VIB-3826: phrasing includes "not supported" so the state machine
                # classifies this as COMPILATION_PERMANENT (state_machine.py:1007)
                # and skips pointless retries. EOA-ness will not change between
                # attempts; retrying always produces the same revert.
                "Flash loans not supported for EOA wallets — flash-loan providers call back "
                "into the recipient during the same transaction (Balancer's receiveFlashLoan, "
                f"Aave's executeOperation), which EOAs cannot handle. Wallet {compiler.wallet_address} "
                "is an EOA (no bytecode); deploy a compatible flash-loan receiver contract."
            ),
            intent_id=intent.intent_id,
        )
    if is_contract is None:
        warnings.append(
            "Could not verify wallet bytecode (no RPC available). "
            "Flash loans will revert if the wallet is an EOA (not a contract)."
        )
    return None


def _resolve_flash_loan_provider(
    compiler,
    intent: FlashLoanIntent,
) -> tuple[str | None, CompilationResult | None]:
    """Resolve ``intent.provider`` to a concrete provider name.

    Returns ``(provider_name, None)`` on success or ``(None, failed_result)``
    when the request cannot be fulfilled. Handles ``"auto"`` by invoking
    the selector with a fee-first priority.
    """
    if intent.provider == "auto":
        try:
            selector = _build_flash_loan_selector(compiler.chain)
            selection_result = selector.select_provider(
                token=intent.token,
                amount=intent.amount,
                priority="fee",  # Prefer lower fees (Balancer / Morpho are zero)
            )
        except NoProviderAvailableError as e:
            return None, CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"No flash loan provider available: {e}",
                intent_id=intent.intent_id,
            )
        if selection_result.selection_reasoning:
            logger.info(f"Flash loan provider auto-selected: {selection_result.selection_reasoning}")
        effective_provider = selection_result.provider
    else:
        effective_provider = intent.provider

    # ``effective_provider`` is ``str | None`` (the selector's ``provider`` is
    # optional); a ``None`` falls into the same "unsupported" path it did under
    # the legacy ``not in`` tuple check, and the ``or`` short-circuit narrows the
    # ``has`` argument (and the success return) to ``str``.
    if effective_provider is None or not FLASH_LOAN_PROVIDER_REGISTRY.has(effective_provider):
        return None, CompilationResult(
            status=CompilationStatus.FAILED,
            error=(
                f"Unsupported flash loan provider: {intent.provider}. "
                f"Supported providers: {', '.join(FLASH_LOAN_PROVIDER_REGISTRY.names())}."
            ),
            intent_id=intent.intent_id,
        )
    return effective_provider, None


def compile_flash_loan(compiler, intent: FlashLoanIntent) -> CompilationResult:  # noqa: C901
    """Compile a FLASH_LOAN intent into an ActionBundle.

    This method:
    1. Validates the provider (Aave or Balancer)
    2. Resolves the flash loan token
    3. Compiles nested callback intents
    4. Encodes callbacks as flash loan params
    5. Builds the flash loan transaction

    For atomic arbitrage strategies, the flash loan must be repaid within
    the same transaction. The callback_intents should return sufficient
    tokens to repay the loan plus fees (0.09% for Aave, 0% for Balancer).

    Args:
        compiler: IntentCompiler instance
        intent: FlashLoanIntent to compile

    Returns:
        CompilationResult with flash loan ActionBundle
    """
    result = CompilationResult(
        status=CompilationStatus.SUCCESS,
        intent_id=intent.intent_id,
    )
    transactions: list[TransactionData] = []
    warnings: list[str] = []

    try:
        # Step 0: Check wallet can handle flash loan callbacks
        wallet_failure = _check_wallet_for_flash_loan(compiler, intent, warnings)
        if wallet_failure is not None:
            return wallet_failure

        # Step 1: Validate and resolve provider (auto-selects via FlashLoanSelector
        # when intent.provider == "auto")
        effective_provider, provider_failure = _resolve_flash_loan_provider(compiler, intent)
        if provider_failure is not None:
            return provider_failure
        # Contract of _resolve_flash_loan_provider: a None failure means a
        # concrete provider name was resolved. Narrow for the build dispatch.
        assert effective_provider is not None

        # Step 2: Resolve flash loan token
        token_info = compiler._resolve_token(intent.token)
        if token_info is None:
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=f"Unknown flash loan token: {intent.token}",
                intent_id=intent.intent_id,
            )

        # Step 3: Calculate flash loan amount in wei
        amount_wei = int(intent.amount * Decimal(10**token_info.decimals))

        # Step 4: Compile callback intents to get their transactions
        # For flash loan callbacks, amount='all' means "use the full output from the
        # previous callback." We estimate this at compile time using the price oracle,
        # since the exact amount is only known on-chain at execution time.
        callback_transactions: list[TransactionData] = []
        callback_gas_total = 0
        # Seed with flash loan's own borrow amount/token so callback 1 can use amount='all'
        prev_output_amount: Decimal | None = intent.amount
        prev_output_token: str | None = intent.token

        for i, callback_intent in enumerate(intent.callback_intents):
            # Resolve amount='all' using estimated output from previous callback
            resolved_intent: AnyIntent = callback_intent
            if (
                hasattr(callback_intent, "amount")
                and callback_intent.amount == "all"
                and prev_output_amount is not None
            ):
                # Validate token compatibility: the callback's input token must match
                # the previous callback's output token to use amount='all'.
                # Resolve both tokens to addresses to handle symbol/address/alias equivalence.
                callback_from = getattr(callback_intent, "from_token", None)
                if callback_from and prev_output_token:
                    resolved_from = compiler._resolve_token(callback_from)
                    resolved_prev = compiler._resolve_token(prev_output_token)
                    if (
                        resolved_from
                        and resolved_prev
                        and resolved_from.address.lower() != resolved_prev.address.lower()
                    ):
                        return CompilationResult(
                            status=CompilationStatus.FAILED,
                            error=(
                                f"Flash loan callback {i + 1}: amount='all' expects token "
                                f"'{prev_output_token}' (output of previous callback) but "
                                f"from_token is '{callback_from}'. Use an explicit amount instead."
                            ),
                            intent_id=intent.intent_id,
                        )
                resolved_intent = Intent.set_resolved_amount(callback_intent, prev_output_amount)
                logger.info(
                    f"Flash loan callback {i + 1}: resolved amount='all' to "
                    f"{prev_output_amount} {prev_output_token} (estimated from previous callback)"
                )

            callback_result = compiler.compile(resolved_intent)
            if callback_result.status != CompilationStatus.SUCCESS:
                return CompilationResult(
                    status=CompilationStatus.FAILED,
                    error=f"Failed to compile callback intent {i + 1}: {callback_result.error}",
                    intent_id=intent.intent_id,
                )
            if callback_result.transactions:
                callback_transactions.extend(callback_result.transactions)
                callback_gas_total += callback_result.total_gas_estimate or 0

            # Estimate output for next callback's amount='all' resolution
            prev_output_amount, prev_output_token = estimate_callback_output(
                compiler, resolved_intent, prev_output_amount, prev_output_token
            )

        # Step 5: Encode callback transactions as params
        callback_params = encode_flash_loan_callbacks(callback_transactions)

        # Step 6: Build flash loan transaction via the registered provider.
        # The provider's connector owns the build (Balancer/Morpho zero-fee,
        # Aave V3 0.09%); this dispatch names none of them.
        flash_loan_result = FLASH_LOAN_PROVIDER_REGISTRY.build(
            effective_provider,
            compiler,
            token_info=token_info,
            amount_wei=amount_wei,
            callback_params=callback_params,
            callback_gas_total=callback_gas_total,
        )

        if flash_loan_result.get("error"):
            return CompilationResult(
                status=CompilationStatus.FAILED,
                error=flash_loan_result["error"],
                intent_id=intent.intent_id,
            )

        transactions.append(flash_loan_result["transaction"])

        # Step 7: Build ActionBundle
        total_gas = sum(tx.gas_estimate for tx in transactions)

        action_bundle = ActionBundle(
            intent_type=IntentType.FLASH_LOAN.value,
            transactions=[tx.to_dict() for tx in transactions],
            metadata={
                "provider": effective_provider,
                "pool_address": flash_loan_result["pool_address"],
                "token": token_info.to_dict(),
                "amount": str(amount_wei),
                "amount_formatted": str(intent.amount),
                "premium_bps": flash_loan_result["premium_bps"],
                "premium_amount": str(flash_loan_result["premium_amount"]),
                "total_repay": str(flash_loan_result["total_repay"]),
                "callback_count": len(intent.callback_intents),
                "callback_gas_estimate": callback_gas_total,
                "chain": compiler.chain,
            },
        )

        result.action_bundle = action_bundle
        result.transactions = transactions
        result.total_gas_estimate = total_gas
        result.warnings = warnings

        logger.info(
            f"Compiled FLASH_LOAN intent: {intent.amount} {intent.token} via {effective_provider}, {len(intent.callback_intents)} callbacks, {len(transactions)} txs, {total_gas} gas"
        )

    except Exception as e:
        logger.exception(f"Failed to compile FLASH_LOAN intent: {e}")
        result.status = CompilationStatus.FAILED
        result.error = str(e)

    return result


def estimate_callback_output(
    compiler,
    callback_intent: AnyIntent,
    prev_output_amount: Decimal | None,
    prev_output_token: str | None,
) -> tuple[Decimal | None, str | None]:
    """Estimate the output token and amount from a compiled callback intent.

    Used by compile_flash_loan to resolve amount='all' in subsequent callbacks.
    The estimate is based on the price oracle and is approximate -- the actual
    amount is only known on-chain at execution time.

    Args:
        compiler: IntentCompiler instance
        callback_intent: The callback intent (after amount='all' resolution)
        prev_output_amount: Previous callback's estimated output amount
        prev_output_token: Previous callback's output token symbol

    Returns:
        Tuple of (estimated_output_amount, output_token_symbol).
        Returns (None, None) for unsupported intent types.

    Raises:
        ValueError: If price data is unavailable for token resolution.
    """
    if not isinstance(callback_intent, SwapIntent):
        intent_type = getattr(callback_intent, "intent_type", "unknown")
        logger.warning(
            f"Cannot estimate output for non-swap callback intent type {intent_type}. "
            f"Subsequent amount='all' callbacks will fail to resolve."
        )
        return None, None

    from_token_info = compiler._resolve_token(callback_intent.from_token)
    to_token_info = compiler._resolve_token(callback_intent.to_token)
    if not from_token_info or not to_token_info:
        raise ValueError(
            f"Cannot resolve tokens for callback output estimate: "
            f"{callback_intent.from_token} -> {callback_intent.to_token}"
        )

    # Determine input amount in wei
    amount_in_wei: int | None = None
    if callback_intent.amount_usd is not None:
        amount_in_wei = compiler._usd_to_token_amount(callback_intent.amount_usd, from_token_info)
    elif callback_intent.amount is not None and callback_intent.amount != "all":
        amount_decimal = (
            callback_intent.amount
            if isinstance(callback_intent.amount, Decimal)
            else Decimal(str(callback_intent.amount))
        )
        amount_in_wei = int(amount_decimal * Decimal(10**from_token_info.decimals))

    if amount_in_wei is not None:
        expected_out_wei = compiler._calculate_expected_output(amount_in_wei, from_token_info, to_token_info)
        return (
            Decimal(str(expected_out_wei)) / Decimal(10**to_token_info.decimals),
            callback_intent.to_token,
        )
    return None, None


def encode_flash_loan_callbacks(
    callback_transactions: list[TransactionData],
) -> bytes:
    """Encode callback transactions for flash loan params.

    The encoded data will be passed to the receiver contract's executeOperation
    function. The receiver contract is responsible for decoding and executing
    these transactions atomically.

    Format: ABI-encoded array of (address to, uint256 value, bytes data) tuples

    Args:
        callback_transactions: List of transactions to encode

    Returns:
        ABI-encoded bytes for the params field
    """
    if not callback_transactions:
        return b""

    # Simple encoding: concatenate transaction data
    # In production, this would use proper ABI encoding
    # Format for each tx: to(20 bytes) + value(32 bytes) + data_length(32 bytes) + data
    encoded_parts: list[bytes] = []

    for tx in callback_transactions:
        # Extract address (remove 0x prefix, pad to 20 bytes)
        to_addr = bytes.fromhex(tx.to.lower().replace("0x", "").zfill(40))

        # Value as 32-byte big-endian
        value_bytes = tx.value.to_bytes(32, "big")

        # Data (remove 0x prefix if present)
        data_hex = tx.data.lower().replace("0x", "") if tx.data else ""
        data_bytes = bytes.fromhex(data_hex) if data_hex else b""

        # Data length as 32-byte big-endian
        data_len_bytes = len(data_bytes).to_bytes(32, "big")

        # Combine: to + value + data_length + data
        encoded_parts.append(to_addr + value_bytes + data_len_bytes + data_bytes)

    # Prepend count of transactions
    count_bytes = len(callback_transactions).to_bytes(32, "big")
    return count_bytes + b"".join(encoded_parts)
