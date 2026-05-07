"""Balancer Flash Loan Arbitrage Demo Strategy.

Exercises the Balancer flash loan connector on Arbitrum -- the first kitchenloop
test of this connector across 65 iterations. The Balancer connector in the SDK
is flash-loan only (not a DEX swap adapter), so this strategy tests flash loan
intent compilation with Enso swap callbacks.

WHAT THIS TESTS:
1. FlashLoanIntent compilation with provider="balancer"
2. Balancer Vault calldata generation (zero-fee flash loan)
3. Enso swap callbacks inside flash loan context
4. Fallback to simple Enso swap when flash loan isn't needed

BALANCER FLASH LOANS:
- Zero fees (unlike Aave's 0.09%)
- Borrowed via Balancer Vault (same address on all chains)
- Must repay borrowed amount in same transaction (no fee)
- Ideal for arbitrage where profit covers gas only

IMPORTANT LIMITATION:
Flash loans require a receiver contract that implements the provider callback
(e.g., Balancer's receiveFlashLoan or Aave's executeOperation). The flash loan
provider calls back into the recipient during the same transaction, which reverts
on EOA wallets (no bytecode). The compiler will detect this and fail at compile
time with a clear error. To run this strategy, deploy a compatible flash-loan
receiver contract.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.utils.log_formatters import format_usd

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary

logger = logging.getLogger(__name__)

_ACTION_SWAP_PROTOCOL = "enso"
_DEFAULT_TEARDOWN_PROTOCOL = "uniswap_v3"
_SUPPORTED_TEARDOWN_PROTOCOLS = frozenset({_ACTION_SWAP_PROTOCOL, _DEFAULT_TEARDOWN_PROTOCOL})


@almanak_strategy(
    name="demo_balancer_flash_arb",
    description="Demo: Balancer flash loan with Enso swap callbacks on Arbitrum",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "balancer", "flash-loan", "arbitrage", "enso"],
    supported_chains=["arbitrum"],
    default_chain="arbitrum",
    supported_protocols=["balancer", "enso", "uniswap_v3"],
    intent_types=["FLASH_LOAN", "SWAP", "HOLD"],
)
class BalancerFlashArbStrategy(IntentStrategy):
    """Demo strategy testing Balancer flash loan intent compilation.

    CONFIGURATION (from config.json):
        flash_loan_amount_usd: USD value to flash loan
        max_slippage_pct: Max slippage for swap callbacks
        base_token: Token to trade (e.g., "WETH")
        quote_token: Quote token (e.g., "USDC")
        teardown_protocol: Protocol used only for teardown exit swap
        force_action: Force "flash_loan" or "swap" for testing
    """

    def supports_teardown(self) -> bool:
        return True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.flash_loan_amount_usd = Decimal(str(self.get_config("flash_loan_amount_usd", "1000")))
        self.max_slippage_pct = float(self.get_config("max_slippage_pct", 1.0))
        self.base_token = self.get_config("base_token", "WETH")
        self.quote_token = self.get_config("quote_token", "USDC")
        self.teardown_protocol = self._resolve_teardown_protocol()
        # Normalize force_action once: boolean/truthy -> "swap", strings lowercased
        raw_action = self.get_config("force_action", None)
        if raw_action is None:
            self.force_action = None
        else:
            action = str(raw_action).lower().strip()
            if action in ("true", "1"):
                self.force_action = "swap"
            else:
                self.force_action = action or None
        self._trades_executed = 0
        self._fell_back_to_swap = False

        # flash_loan_amount_usd is passed as raw token units to Intent.flash_loan,
        # so quote_token must be a dollar-pegged stablecoin for the amount to make sense.
        _USD_TOKENS = {"USDC", "USDT", "DAI", "USDC.E", "USDBC"}
        if self.quote_token.upper() not in _USD_TOKENS:
            logger.warning(
                f"quote_token '{self.quote_token}' is not a known USD stablecoin. "
                f"flash_loan_amount_usd ({self.flash_loan_amount_usd}) will be used as raw token units."
            )

        logger.info(
            f"BalancerFlashArbStrategy initialized: "
            f"flash_loan={format_usd(self.flash_loan_amount_usd)}, "
            f"pair={self.base_token}/{self.quote_token}, "
            f"teardown_protocol={self.teardown_protocol}"
        )

    def _resolve_teardown_protocol(self) -> str:
        raw_protocol = self.get_config("teardown_protocol", _DEFAULT_TEARDOWN_PROTOCOL)
        protocol = str(raw_protocol).strip().lower()
        if protocol not in _SUPPORTED_TEARDOWN_PROTOCOLS:
            supported = ", ".join(sorted(_SUPPORTED_TEARDOWN_PROTOCOLS))
            raise ValueError(
                f"Unsupported balancer teardown_protocol={raw_protocol!r}; "
                f"expected one of: {supported}"
            )
        return protocol

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Decide: emit flash loan intent or simple swap for testing.

        In force_action="flash_loan" mode, creates a Balancer flash loan
        that borrows USDC and swaps through Enso (round-trip arbitrage pattern).
        NOTE: Flash loans require a smart contract receiver — EOA wallets will
        revert because they can't implement the receiveFlashLoan callback.
        Since the gateway compiler is not available at decide() time, flash_loan
        mode always falls back to swap on local/Anvil runs (EOA wallets).

        In force_action="swap" mode, creates a simple Enso swap as fallback.
        """
        if self.force_action == "flash_loan":
            # Flash loans revert on EOA wallets (no receiveFlashLoan callback).
            # The compiler (with gateway RPC) is not available at decide() time,
            # so we cannot do an on-chain eth_getCode check here. Instead, check
            # using the compiler's cached result if available, otherwise assume EOA.
            if not self._is_contract_wallet():
                logger.warning(
                    "flash_loan requested but wallet is an EOA (or wallet type unknown) — "
                    "flash loans require a smart contract receiver with "
                    "receiveFlashLoan() callback. Falling back to swap mode."
                )
                self._fell_back_to_swap = True
                return self._create_swap_intent()
            logger.info("Force action: Balancer flash loan with Enso swap callbacks")
            return self._create_flash_loan_intent()
        elif self.force_action == "swap":
            logger.info("Force action: simple Enso swap (fallback)")
            return self._create_swap_intent()
        else:
            return Intent.hold(reason="No action forced -- set force_action in config.json")

    def _is_contract_wallet(self) -> bool:
        """Check if the wallet address is a smart contract (e.g., Safe).

        Returns False (assumes EOA) if the check fails or no gateway is available.
        Flash loans require a contract wallet with callback support.

        NOTE: At decide() time, self._compiler is typically None because the
        runner creates the compiler after decide() returns. This means this
        method will return False for most local/Anvil runs, which is the safe
        default (fall back to swap instead of guaranteed-revert flash loan).
        """
        compiler = getattr(self, "_compiler", None)
        if compiler is None:
            return False
        gateway_client = getattr(compiler, "_gateway_client", None)
        if gateway_client is None:
            return False

        import json

        try:
            from almanak.gateway.proto import gateway_pb2

            response = gateway_client.rpc.Call(
                gateway_pb2.RpcRequest(
                    chain=self.chain,
                    method="eth_getCode",
                    params=json.dumps([self.wallet_address, "latest"]),
                    id="check-eoa",
                ),
                timeout=5.0,
            )
            if response.success and response.result:
                code = json.loads(response.result)
                return code not in (None, "0x", "0x0")
        except Exception:  # noqa: BLE001
            logger.debug("Could not check wallet bytecode, assuming EOA")
        return False

    def _create_flash_loan_intent(self) -> Intent:
        """Create a Balancer flash loan intent with swap callbacks.

        Pattern: Borrow USDC via Balancer -> swap USDC->WETH via Enso ->
        swap WETH->USDC via Enso -> repay USDC (zero fee).

        This is a round-trip that should return approximately the same amount
        (minus swap fees/slippage). Real arbitrage would use different DEX
        routes for a profit.
        """
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")

        self._trades_executed += 1

        return Intent.flash_loan(
            provider="balancer",
            token=self.quote_token,
            amount=self.flash_loan_amount_usd,
            callback_intents=[
                Intent.swap(
                    from_token=self.quote_token,
                    to_token=self.base_token,
                    amount=self.flash_loan_amount_usd,
                    max_slippage=max_slippage,
                    protocol=_ACTION_SWAP_PROTOCOL,
                ),
                Intent.swap(
                    from_token=self.base_token,
                    to_token=self.quote_token,
                    amount="all",
                    max_slippage=max_slippage,
                    protocol=_ACTION_SWAP_PROTOCOL,
                ),
            ],
            chain="arbitrum",
        )

    def _create_swap_intent(self) -> Intent:
        """Create a simple Enso swap as a fallback test."""
        max_slippage = Decimal(str(self.max_slippage_pct)) / Decimal("100")
        self._trades_executed += 1
        return Intent.swap(
            from_token=self.quote_token,
            to_token=self.base_token,
            amount_usd=Decimal("3"),
            max_slippage=max_slippage,
            protocol=_ACTION_SWAP_PROTOCOL,
        )

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_balancer_flash_arb",
            "chain": self.chain,
            "action_protocol": _ACTION_SWAP_PROTOCOL,
            "trades_executed": self._trades_executed,
            "teardown_protocol": self.teardown_protocol,
        }

    def to_dict(self) -> dict[str, Any]:
        metadata = self.get_metadata()
        config_dict = self.config if isinstance(self.config, dict) else {}
        return {
            "strategy_name": self.__class__.STRATEGY_NAME,
            "chain": self.chain,
            "wallet_address": self.wallet_address,
            "config": config_dict,
            "config_version": self.get_current_config_version(),
            "current_intent": self._current_intent.serialize() if self._current_intent else None,
            "metadata": metadata.to_dict() if metadata else None,
        }

    # Teardown support

    # Dust threshold expressed in base_token native units. Enso "swap all" leaves
    # ~1e-12 to 1e-9 of base_token behind from rounding; 1e-6 is well above that
    # ceiling and well below any real position (1e-6 WETH ≈ $0.003). This is
    # token-decimals dependent — for an 8-decimal high-priced asset (WBTC at
    # ~$60k → 1e-6 WBTC ≈ $0.06) it's still safe; for a sub-cent memecoin a
    # smaller floor would be wanted. Demo strategy assumes WETH/stablecoin
    # pairs. VIB-3738.
    _BASE_TOKEN_DUST_THRESHOLD = Decimal("0.000001")

    def _query_base_token_balance(self, market=None) -> tuple[Decimal, Decimal] | None:
        """Read on-chain wallet balance for base_token.

        Returns (balance_amount, balance_usd) on success or `None` on query
        failure. Callers MUST treat None as "unknown — assume position may
        still exist" (fail closed). Returning a sentinel zero would let RPC
        outages silently bypass teardown verification (Codex P1 / Claude #4).
        """
        try:
            snapshot = market or self.create_market_snapshot()
            balance = snapshot.balance(self.base_token)
            # MarketSnapshot.balance() returns TokenBalance; the hasattr fallback
            # is defensive against alternative balance providers that might
            # return a bare Decimal.
            amount = balance.balance if hasattr(balance, "balance") else Decimal(str(balance))
            value_usd = getattr(balance, "balance_usd", None) or Decimal("0")
            return Decimal(str(amount)), Decimal(str(value_usd))
        except (ValueError, KeyError, ConnectionError, TimeoutError) as exc:
            # ValueError covers "Cannot determine balance for X" from MarketSnapshot;
            # ConnectionError/TimeoutError cover transient gateway/RPC issues.
            # Anything else propagates so genuine logic bugs aren't masked.
            logger.warning(
                f"Unable to query on-chain {self.base_token} balance for teardown: {exc!r}"
            )
            return None

    def _has_likely_open_position(self) -> bool:
        """Cached-state fallback signal — only consulted when on-chain query fails.

        The strategy executed a SWAP (forced or fallen-back from flash_loan)
        and we have no way to confirm the wallet is flat, so the safe assumption
        is "position may still be open." Errs on the side of attempting an
        unwind rather than silently bypassing teardown.
        """
        return self._trades_executed > 0 and (self.force_action == "swap" or self._fell_back_to_swap)

    def get_open_positions(self) -> TeardownPositionSummary:
        """Detect open positions via on-chain wallet balance, with cached
        fallback when the query fails.

        After a successful teardown SWAP, the wallet should hold approximately
        zero base_token. The previous implementation read cached
        `_trades_executed` / `_fell_back_to_swap` flags that were never reset,
        so verification always reported the position as still open. VIB-3738.

        On-chain query failure → fall back to cached state and report a
        position so the framework retries / blocks teardown completion
        (Codex P1 / Claude #4 — fail closed).
        """
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        result = self._query_base_token_balance()
        if result is None:
            # On-chain truth unavailable — fall back to cached state.
            if self._has_likely_open_position():
                # Estimate value_usd from the swap intent's hardcoded amount
                # ($3 in `_create_swap_intent`). Encoding "unknown" as $0 here
                # is misleading: `TeardownPositionSummary.__post_init__` would
                # total the position at zero and `safety_guard.py` would derive
                # a $0 acceptable-loss floor from it (CodeRabbit feedback on
                # PR #1964). Using the swap-intent's amount produces a sensible
                # loss tolerance for the operator.
                estimated_value_usd = Decimal("3")  # matches _create_swap_intent amount_usd
                logger.warning(
                    "Falling back to cached state for teardown verification "
                    f"(on-chain {self.base_token} balance unavailable). "
                    f"Reporting position with estimated value ${estimated_value_usd} "
                    "so framework can retry."
                )
                positions.append(
                    PositionInfo(
                        position_type=PositionType.TOKEN,
                        position_id="balancer_flash_arb_token_0",
                        chain=self.chain,
                        protocol=self.teardown_protocol,
                        value_usd=estimated_value_usd,
                        details={
                            "asset": self.base_token,
                            "source_protocol": _ACTION_SWAP_PROTOCOL,
                            "teardown_protocol": self.teardown_protocol,
                            "valuation_source": "cached_fallback_estimate",
                        },
                    )
                )
        else:
            balance_amount, balance_usd = result
            if balance_amount > self._BASE_TOKEN_DUST_THRESHOLD:
                positions.append(
                    PositionInfo(
                        position_type=PositionType.TOKEN,
                        position_id="balancer_flash_arb_token_0",
                        chain=self.chain,
                        protocol=self.teardown_protocol,
                        value_usd=balance_usd,
                        details={
                            "asset": self.base_token,
                            "balance": str(balance_amount),
                            "source_protocol": _ACTION_SWAP_PROTOCOL,
                            "teardown_protocol": self.teardown_protocol,
                        },
                    )
                )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_balancer_flash_arb"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: TeardownMode, market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        # Skip the teardown swap only if we KNOW the wallet has no base_token
        # (on-chain query succeeded and balance is below dust). On query
        # failure or cached-positive, we emit the swap to avoid leaving funds
        # behind (Codex P1 / Claude #4).
        result = self._query_base_token_balance(market)
        if result is not None:
            balance_amount, _ = result
            if balance_amount <= self._BASE_TOKEN_DUST_THRESHOLD:
                logger.info(
                    f"No {self.base_token} balance to unwind ({balance_amount}) — skipping teardown swap"
                )
                return []
        elif not self._has_likely_open_position():
            # Query failed AND no record of executing a trade — nothing to unwind.
            logger.info(
                f"On-chain {self.base_token} balance unknown and no trade executed — skipping teardown swap"
            )
            return []
        else:
            logger.warning(
                f"On-chain {self.base_token} balance unknown but trade was executed — emitting teardown swap"
            )

        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal(str(self.max_slippage_pct)) / Decimal("100")
        return [
            Intent.swap(
                from_token=self.base_token,
                to_token=self.quote_token,
                amount="all",
                max_slippage=max_slippage,
                protocol=self.teardown_protocol,
                chain=self.chain,
            )
        ]
