"""Base contract for connector-owned intent compilers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar, Protocol

if TYPE_CHECKING:
    from almanak.framework.intents.compiler_models import CompilationResult, TokenInfo, TransactionData
    from almanak.framework.intents.vocabulary import CollectFeesIntent, LPCloseIntent, LPOpenIntent, SwapIntent


class CompilerServices(Protocol):
    """Typed boundary for framework services a connector compiler may use."""

    def resolve_token(self, token: str) -> TokenInfo | None: ...

    def require_token_price(self, symbol: str) -> Decimal: ...

    def usd_to_token_amount(self, usd_amount: Decimal, token: TokenInfo) -> int: ...

    def calculate_expected_output(self, amount_in: int, from_token: TokenInfo, to_token: TokenInfo) -> int: ...

    def build_approve_tx(self, token_address: str, spender: str, amount: int) -> list[TransactionData]: ...

    def validate_pool(self, result: Any, intent_id: str) -> CompilationResult | None: ...

    def format_amount(self, amount: int, decimals: int) -> str: ...

    def parse_pool_info(self, pool: str) -> tuple[TokenInfo, TokenInfo, int, bool] | None: ...

    def price_to_tick(self, price: Decimal, *, token0_decimals: int, token1_decimals: int) -> int: ...

    def get_tick_spacing(self, fee_tier: int) -> int: ...

    def get_wrapped_native_address(self) -> str | None: ...

    def query_position_liquidity(self, position_manager: str, token_id: int) -> int | None: ...

    def query_position_tokens_owed(self, position_manager: str, token_id: int) -> tuple[int | None, int | None]: ...

    def query_erc20_balance(self, token_address: str, wallet_address: str) -> int | None: ...


@dataclass(frozen=True)
class BaseCompilerContext:
    """Primitive-agnostic context passed from the framework compiler to connectors."""

    chain: str
    wallet_address: str
    rpc_url: str | None
    rpc_timeout: float
    permission_discovery: bool
    allow_placeholder_prices: bool
    token_resolver: Any
    gateway_client: Any
    price_oracle: Any
    cache: Any
    services: CompilerServices


@dataclass(frozen=True)
class CLCompilerContext(BaseCompilerContext):
    """Concentrated-liquidity compiler context."""

    protocol: str
    default_swap_adapter_factory: Callable[[str], Any]
    lp_adapter_factory: Callable[[str], Any]
    swap_pool_selection_mode: str
    fixed_swap_fee_tier: int | None
    default_deadline_seconds: int
    default_lp_slippage: Decimal
    max_price_impact_pct: Decimal
    using_placeholders: bool


class BaseProtocolCompiler[CompilerContextT: BaseCompilerContext](ABC):
    """Root ABC for connector-owned intent compilers."""

    protocols: ClassVar[frozenset[str]]
    intents: ClassVar[frozenset[Any]]
    chains: ClassVar[frozenset[str] | None] = None
    context_type: ClassVar[type[BaseCompilerContext]] = BaseCompilerContext

    def compile(self, ctx: CompilerContextT, intent: Any) -> CompilationResult:
        """Dispatch one intent to the primitive method implemented by a subclass."""
        if not isinstance(ctx, self.context_type):
            return _failed_result(
                getattr(intent, "intent_id", ""),
                f"{type(self).__name__} requires {self.context_type.__name__}, got {type(ctx).__name__}",
            )
        intent_type = getattr(intent, "intent_type", None)
        intent_value = getattr(intent_type, "value", intent_type)
        if isinstance(intent_value, str):
            method = self._dispatch_methods().get(intent_value)
            if method is not None:
                return method(ctx, intent)
        return _failed_result(
            getattr(intent, "intent_id", ""),
            f"{type(self).__name__} does not support intent type {intent_type}",
        )

    def _dispatch_methods(self) -> dict[str, Callable[[CompilerContextT, Any], CompilationResult]]:
        return {
            "SWAP": self.compile_swap,
            "LP_OPEN": self.compile_lp_open,
            "LP_CLOSE": self.compile_lp_close,
            "LP_COLLECT_FEES": self.compile_collect_fees,
        }

    @abstractmethod
    def compile_swap(self, ctx: CompilerContextT, intent: SwapIntent) -> CompilationResult:
        """Compile a swap intent."""

    @abstractmethod
    def compile_lp_open(self, ctx: CompilerContextT, intent: LPOpenIntent) -> CompilationResult:
        """Compile an LP open intent."""

    @abstractmethod
    def compile_lp_close(self, ctx: CompilerContextT, intent: LPCloseIntent) -> CompilationResult:
        """Compile an LP close intent."""

    @abstractmethod
    def compile_collect_fees(self, ctx: CompilerContextT, intent: CollectFeesIntent) -> CompilationResult:
        """Compile a fee collection intent."""


class BaseConcentratedLiquidityCompiler(BaseProtocolCompiler[CLCompilerContext]):
    """Base class for concentrated-liquidity connector compilers."""

    context_type: ClassVar[type[BaseCompilerContext]] = CLCompilerContext

    @abstractmethod
    def compile_swap(self, ctx: CLCompilerContext, intent: SwapIntent) -> CompilationResult:
        """Compile a CL swap intent."""

    @abstractmethod
    def compile_lp_open(self, ctx: CLCompilerContext, intent: LPOpenIntent) -> CompilationResult:
        """Compile a CL LP open intent."""

    @abstractmethod
    def compile_lp_close(self, ctx: CLCompilerContext, intent: LPCloseIntent) -> CompilationResult:
        """Compile a CL LP close intent."""

    @abstractmethod
    def compile_collect_fees(self, ctx: CLCompilerContext, intent: CollectFeesIntent) -> CompilationResult:
        """Compile a CL fee collection intent."""


__all__ = [
    "BaseCompilerContext",
    "BaseConcentratedLiquidityCompiler",
    "BaseProtocolCompiler",
    "CLCompilerContext",
    "CompilerServices",
]


def _failed_result(intent_id: str, error: str) -> CompilationResult:
    from almanak.framework.intents.compiler_models import CompilationResult, CompilationStatus

    return CompilationResult(status=CompilationStatus.FAILED, intent_id=intent_id, error=error)
