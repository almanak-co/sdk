"""Agent policy engine -- non-bypassable constraints for AI agent trading.

The policy layer enforces hard separation between "agent proposes" and
"system executes". Every action tool call passes through the PolicyEngine
before reaching the gateway.
"""

from __future__ import annotations

import json
import logging
import math
import statistics
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from almanak.framework.agent_tools.catalog import RiskTier, ToolDefinition
from almanak.framework.agent_tools.errors import RiskBlockedError

logger = logging.getLogger(__name__)

# Vault lifecycle tools that do NOT transfer user funds and thus skip spend
# limit checks. deposit_vault is intentionally excluded because it moves
# real funds and should be subject to spend limits. teardown_vault is also
# excluded because it orchestrates LP closes and swaps that move real funds.
_VAULT_LIFECYCLE_TOOLS = frozenset(
    {
        "deploy_vault",
        "settle_vault",
        "approve_vault_underlying",
    }
)

# Sub-tools that teardown_vault calls internally via self.execute().
# When allowed_tools is configured, all of these must be permitted for
# teardown to succeed. Pre-validated at the start of _execute_teardown_vault
# to give clear errors instead of cryptic mid-teardown failures.
TEARDOWN_REQUIRED_TOOLS = frozenset(
    {
        "close_lp_position",
        "swap_tokens",
        "get_balance",
        "settle_vault",
    }
)

# Tools that should not start the cooldown timer (superset of lifecycle tools).
# deposit_vault moves real funds (so it keeps spend limit checks) but should
# not block the next operation in the vault setup sequence.
_COOLDOWN_EXEMPT_TOOLS = _VAULT_LIFECYCLE_TOOLS | {"deposit_vault"}


@dataclass
class PortfolioSnapshot:
    """A timestamped portfolio value observation for risk metric calculations."""

    timestamp: datetime
    value_usd: Decimal


@dataclass
class AgentPolicy:
    """Safety constraints for an AI agent trading session.

    Every field has a safe default. Loosen constraints explicitly
    via config to move from Pattern III toward Pattern IV autonomy.
    """

    # ── Spend limits ────────────────────────────────────────────────────
    max_single_trade_usd: Decimal = Decimal("10000")
    max_daily_spend_usd: Decimal = Decimal("50000")
    max_position_size_usd: Decimal = Decimal("100000")

    # ── Scope constraints ───────────────────────────────────────────────
    allowed_tools: set[str] | None = None  # None = all tools allowed
    allowed_chains: set[str] = field(default_factory=lambda: {"arbitrum"})
    allowed_protocols: set[str] | None = None  # None = all protocols
    allowed_tokens: set[str] | None = None  # None = all tokens
    allowed_intent_types: set[str] | None = None  # None = all intent types
    allowed_execution_wallets: set[str] | None = None  # None = any wallet allowed

    # ── Approval gates ──────────────────────────────────────────────────
    require_human_approval_above_usd: Decimal = Decimal("10000")
    require_simulation_before_execution: bool = True

    # ── Rate limits ─────────────────────────────────────────────────────
    max_trades_per_hour: int = 10
    max_tool_calls_per_minute: int = 60

    # ── Circuit breakers ────────────────────────────────────────────────
    stop_loss_pct: Decimal = Decimal("5.0")
    max_consecutive_failures: int = 3

    # ── Economic thresholds ─────────────────────────────────────────────
    min_rebalance_benefit_usd: Decimal = Decimal("10")
    cooldown_seconds: int = 300
    require_rebalance_check: bool = True


@dataclass
class PolicyDecision:
    """Result of a policy check."""

    allowed: bool
    violations: list[str] = field(default_factory=list)
    suggestions: list[str] = field(default_factory=list)

    def raise_if_denied(self, tool_name: str) -> None:
        """Raise ``RiskBlockedError`` if the decision is denied."""
        if not self.allowed:
            msg = f"Policy denied '{tool_name}': {'; '.join(self.violations)}"
            suggestion = "; ".join(self.suggestions) if self.suggestions else None
            raise RiskBlockedError(msg, suggestion=suggestion, tool_name=tool_name)


_LP_TOOLS = frozenset({"open_lp_position", "close_lp_position"})


class PolicyStateStore:
    """Simple JSON file-backed persistence for PolicyEngine runtime state.

    Writes are synchronous and happen on every mutation (write-through).
    Reads happen once at initialization. Malformed or missing files are
    handled gracefully by starting with fresh state.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    def save(self, state: dict) -> None:
        """Persist state dict to JSON file."""
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._path.with_suffix(".tmp")
            tmp.write_text(json.dumps(state, default=str), encoding="utf-8")
            tmp.replace(self._path)
        except OSError:
            logger.warning("Failed to persist policy state to %s", self._path, exc_info=True)

    def load(self) -> dict | None:
        """Load state dict from JSON file. Returns None if missing or corrupt."""
        if not self._path.exists():
            return None
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                logger.warning("Policy state file %s is not a dict, ignoring", self._path)
                return None
            return data
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            logger.warning(
                "Failed to load policy state from %s, starting fresh",
                self._path,
                exc_info=True,
            )
            return None


class PolicyEngine:
    """Evaluates tool calls against an ``AgentPolicy``.

    Tracks runtime state (daily spend, trade count, consecutive failures)
    to enforce rate limits and circuit breakers across a session.
    """

    def __init__(
        self,
        policy: AgentPolicy,
        *,
        price_lookup: Callable[[str], Decimal | None] | None = None,
        default_wallet: str = "",
        state_persistence_path: str | Path | None = None,
    ) -> None:
        self.policy = policy
        self._price_lookup = price_lookup
        self._default_wallet = default_wallet

        # Runtime accounting
        self._daily_spend_usd = Decimal("0")
        self._trades_this_hour: list[float] = []  # timestamps
        self._tool_calls_this_minute: list[float] = []
        self._consecutive_failures: int = 0
        self._last_trade_timestamp: float = 0.0
        self._day_start: float = time.time()

        # Stop-loss tracking (high-water mark drawdown)
        self._peak_portfolio_usd: Decimal = Decimal("0")
        self._current_portfolio_usd: Decimal = Decimal("0")

        # Portfolio snapshot history for risk metric calculations.
        # Rolling window of (timestamp, value_usd) observations.
        self._portfolio_snapshots: list[PortfolioSnapshot] = []
        self._max_snapshots: int = 100

        # Rebalance viability gate
        self._rebalance_approved: bool = False

        # State persistence (opt-in)
        self._state_store: PolicyStateStore | None = (
            PolicyStateStore(state_persistence_path) if state_persistence_path else None
        )
        if self._state_store:
            self._restore_state()

    # -- Public API ---------------------------------------------------------

    def check(self, tool_def: ToolDefinition, arguments: dict) -> PolicyDecision:
        """Run all policy checks for a tool call. Returns a decision."""
        violations: list[str] = []
        suggestions: list[str] = []

        # Resolve effective args: for tools like compile_intent that nest
        # token/protocol/chain inside a "params" dict, merge those fields
        # into the effective arguments so scope checks can find them.
        effective_args = self._resolve_effective_args(arguments)

        self._check_tool_allowed(tool_def, violations, suggestions)
        self._check_chain_allowed(effective_args, violations, suggestions)
        self._check_execution_wallet(effective_args, violations, suggestions)
        self._check_protocol_allowed(effective_args, violations, suggestions)
        self._check_token_allowed(effective_args, violations, suggestions)
        self._check_intent_type_allowed(effective_args, violations, suggestions)
        self._check_rate_limits(tool_def, violations, suggestions)
        self._check_circuit_breaker(violations, suggestions)

        if tool_def.risk_tier in (RiskTier.MEDIUM, RiskTier.HIGH):
            self._check_stop_loss(violations, suggestions)
            # Vault lifecycle tools use raw token amounts (e.g. 10000000 = 10 USDC),
            # not USD values. Skip spend limit checks for these tools.
            if tool_def.name not in _VAULT_LIFECYCLE_TOOLS:
                self._check_spend_limits(effective_args, violations, suggestions)
                self._check_position_size(effective_args, violations, suggestions)
                self._check_approval_gate(tool_def, effective_args, violations, suggestions)
            # Cooldown exemption is broader: deposit_vault also skips cooldown
            # so the vault setup sequence (deposit → open_lp) isn't blocked.
            if tool_def.name not in _COOLDOWN_EXEMPT_TOOLS:
                self._check_cooldown(violations, suggestions)
            self._check_rebalance_gate(tool_def, violations, suggestions)

        return PolicyDecision(
            allowed=len(violations) == 0,
            violations=violations,
            suggestions=suggestions,
        )

    def record_trade(self, usd_amount: Decimal, *, success: bool, tool_name: str = "") -> None:
        """Update runtime accounting after a trade attempt."""
        now = time.time()
        self._trades_this_hour.append(now)
        # Vault lifecycle tools (settle, deploy, approve, teardown, deposit)
        # should not start the cooldown timer -- they are part of the vault
        # setup sequence and shouldn't block subsequent operations.
        if tool_name not in _COOLDOWN_EXEMPT_TOOLS:
            self._last_trade_timestamp = now

        if success:
            self._daily_spend_usd += usd_amount
            self._consecutive_failures = 0
        else:
            self._consecutive_failures += 1
        self._save_state()

    def record_tool_call(self) -> None:
        """Track a tool call for rate-limiting."""
        self._tool_calls_this_minute.append(time.time())
        self._save_state()

    def reset_daily(self) -> None:
        """Reset daily accumulators (call at day boundary)."""
        self._daily_spend_usd = Decimal("0")
        self._trades_this_hour.clear()
        self._day_start = time.time()
        self._save_state()

    def update_portfolio_value(self, usd_value: Decimal) -> None:
        """Update portfolio value for stop-loss tracking and risk metrics.

        Records a timestamped snapshot for rolling risk calculations
        (volatility, Sharpe, VaR, drawdown). Maintains a bounded window
        of at most ``_max_snapshots`` observations.
        """
        self._current_portfolio_usd = usd_value
        if usd_value > self._peak_portfolio_usd:
            self._peak_portfolio_usd = usd_value
        self._save_state()

        # Record snapshot for risk metric calculations
        self._portfolio_snapshots.append(PortfolioSnapshot(timestamp=datetime.now(UTC), value_usd=usd_value))
        if len(self._portfolio_snapshots) > self._max_snapshots:
            self._portfolio_snapshots = self._portfolio_snapshots[-self._max_snapshots :]

    def set_rebalance_approved(self, approved: bool) -> None:
        """Set rebalance viability gate (called after compute_rebalance_candidate)."""
        self._rebalance_approved = approved
        self._save_state()

    @property
    def consecutive_failures(self) -> int:
        """Current consecutive failure count (read-only)."""
        return self._consecutive_failures

    @property
    def is_circuit_breaker_tripped(self) -> bool:
        """True if consecutive failures have reached the circuit breaker threshold."""
        return self._consecutive_failures >= self.policy.max_consecutive_failures

    @property
    def portfolio_snapshots(self) -> list[PortfolioSnapshot]:
        """Read-only access to portfolio snapshot history."""
        return list(self._portfolio_snapshots)

    # -- Risk metric calculations -------------------------------------------

    def _compute_returns(self) -> list[float]:
        """Compute period-over-period returns from portfolio snapshots.

        Returns a list of fractional returns (e.g., 0.05 = 5% gain).
        Skips any period where the previous value is zero to avoid division errors.
        """
        returns: list[float] = []
        for i in range(1, len(self._portfolio_snapshots)):
            prev = self._portfolio_snapshots[i - 1].value_usd
            curr = self._portfolio_snapshots[i].value_usd
            if prev > 0:
                returns.append(float((curr - prev) / prev))
        return returns

    def calculate_max_drawdown(self) -> Decimal:
        """Peak-to-trough decline as a decimal fraction (e.g., 0.05 = 5%).

        Uses the full snapshot history to find the worst drawdown, not just
        the current drawdown from the high-water mark.
        """
        if not self._portfolio_snapshots:
            return Decimal("0")

        peak = Decimal("0")
        max_dd = Decimal("0")
        for snap in self._portfolio_snapshots:
            if snap.value_usd > peak:
                peak = snap.value_usd
            if peak > 0:
                dd = (peak - snap.value_usd) / peak
                if dd > max_dd:
                    max_dd = dd
        return max_dd

    def calculate_volatility(self, annualization_factor: int = 252) -> Decimal:
        """Annualized volatility from portfolio return observations.

        Requires at least 3 snapshots (2 returns) to compute a meaningful
        standard deviation. Returns ``Decimal("0")`` when insufficient data.

        Args:
            annualization_factor: Number of periods per year. Default 252
                (trading days). Callers can override for different frequencies.
        """
        returns = self._compute_returns()
        if len(returns) < 2:
            return Decimal("0")

        std_dev = statistics.stdev(returns)
        annualized = std_dev * math.sqrt(annualization_factor)
        return Decimal(str(round(annualized, 6)))

    def calculate_sharpe(self, risk_free_rate: float = 0.04, annualization_factor: int = 252) -> Decimal:
        """Sharpe ratio from portfolio return observations.

        Requires at least 3 snapshots (2 returns). Returns ``Decimal("0")``
        when insufficient data or when volatility is zero.

        Args:
            risk_free_rate: Annual risk-free rate (default 4%).
            annualization_factor: Periods per year (default 252 trading days).
        """
        returns = self._compute_returns()
        if len(returns) < 2:
            return Decimal("0")

        mean_return = statistics.mean(returns)
        std_return = statistics.stdev(returns)
        if std_return == 0:
            return Decimal("0")

        annual_return = mean_return * annualization_factor
        annual_vol = std_return * math.sqrt(annualization_factor)
        sharpe = (annual_return - risk_free_rate) / annual_vol
        return Decimal(str(round(sharpe, 4)))

    def calculate_var_95(self) -> Decimal:
        """Historical 95% Value at Risk as a decimal fraction of portfolio.

        Uses the empirical 5th-percentile of observed returns. Requires at
        least 10 snapshots (9 returns) for a minimally reliable estimate.
        Returns ``Decimal("0")`` with insufficient data.
        """
        returns = self._compute_returns()
        if len(returns) < 9:
            return Decimal("0")

        sorted_returns = sorted(returns)
        # 5th percentile index (floor)
        index = int(len(sorted_returns) * 0.05)
        # Clamp to valid range
        index = max(0, min(index, len(sorted_returns) - 1))
        var_95 = abs(sorted_returns[index])
        return Decimal(str(round(var_95, 6)))

    def get_risk_metrics(self) -> dict:
        """Compute all risk metrics from portfolio snapshot history.

        Returns a dict with calculated metrics and metadata about data quality.
        """
        n = len(self._portfolio_snapshots)
        current_value = self._current_portfolio_usd

        max_drawdown = self.calculate_max_drawdown()
        volatility = self.calculate_volatility()
        sharpe = self.calculate_sharpe()
        var_95 = self.calculate_var_95()

        warnings: list[str] = []
        if n < 3:
            warnings.append("Insufficient data for volatility and Sharpe ratio (need 3+ snapshots)")
        if n < 10:
            warnings.append("Insufficient data for reliable VaR estimate (need 10+ snapshots)")

        return {
            "portfolio_value_usd": str(current_value),
            "max_drawdown_pct": str(max_drawdown),
            "volatility_annualized": str(volatility),
            "sharpe_ratio": str(sharpe),
            "var_95_pct": str(var_95),
            "data_points": n,
            "data_sufficient": n >= 10,
            "warnings": warnings,
        }

    # -- State persistence --------------------------------------------------

    def _get_state_dict(self) -> dict:
        """Serialize mutable runtime state to a JSON-safe dict."""
        return {
            "daily_spend_usd": str(self._daily_spend_usd),
            "day_start": self._day_start,
            "day_start_date": datetime.fromtimestamp(self._day_start, UTC).date().isoformat(),
            "trades_this_hour": self._trades_this_hour,
            "tool_calls_this_minute": self._tool_calls_this_minute,
            "consecutive_failures": self._consecutive_failures,
            "last_trade_timestamp": self._last_trade_timestamp,
            "peak_portfolio_usd": str(self._peak_portfolio_usd),
            "current_portfolio_usd": str(self._current_portfolio_usd),
            "rebalance_approved": self._rebalance_approved,
            "saved_at": datetime.now(UTC).isoformat(),
        }

    def _restore_state(self) -> None:
        """Load persisted state, handling stale data gracefully.

        All deserialization is parsed into local variables first so that a
        corrupt field never leaves the engine in a partially-restored state.
        Fields are only committed to ``self`` after all conversions succeed.
        """
        if not self._state_store:
            return
        state = self._state_store.load()
        if not state:
            return

        try:
            today = datetime.now(UTC).date().isoformat()
            saved_date = state.get("day_start_date", "")
            same_day = saved_date == today

            # Parse non-daily fields first (always restored)
            consecutive_failures = int(state.get("consecutive_failures", 0))
            last_trade_timestamp = float(state.get("last_trade_timestamp", 0.0))
            peak_portfolio_usd = Decimal(state.get("peak_portfolio_usd", "0"))
            current_portfolio_usd = Decimal(state.get("current_portfolio_usd", "0"))
            rebalance_approved = bool(state.get("rebalance_approved", False))

            # Guard against NaN/Infinity in Decimal fields (Decimal('NaN') parses
            # without error but crashes comparisons with InvalidOperation later)
            for val in (peak_portfolio_usd, current_portfolio_usd):
                if val.is_nan() or val.is_infinite():
                    raise ValueError(f"Invalid Decimal value in persisted state: {val}")

            # Parse daily fields (only restored if same day)
            if same_day:
                daily_spend_usd = Decimal(state.get("daily_spend_usd", "0"))
                if daily_spend_usd.is_nan() or daily_spend_usd.is_infinite():
                    raise ValueError(f"Invalid daily_spend_usd: {daily_spend_usd}")
                day_start = float(state.get("day_start", time.time()))
                now = time.time()
                trades_this_hour = [float(t) for t in state.get("trades_this_hour", []) if now - float(t) < 3600]
                tool_calls_this_minute = [
                    float(t) for t in state.get("tool_calls_this_minute", []) if now - float(t) < 60
                ]
            else:
                logger.info(
                    "Policy state is from a previous day (%s), resetting daily counters",
                    saved_date,
                )
        except (InvalidOperation, ValueError, TypeError, KeyError):
            logger.warning(
                "Failed to deserialize policy state from %s, starting fresh",
                self._state_store._path,
                exc_info=True,
            )
            return

        # All conversions succeeded -- commit to self atomically
        self._consecutive_failures = consecutive_failures
        self._last_trade_timestamp = last_trade_timestamp
        self._peak_portfolio_usd = peak_portfolio_usd
        self._current_portfolio_usd = current_portfolio_usd
        self._rebalance_approved = rebalance_approved

        if same_day:
            self._daily_spend_usd = daily_spend_usd
            self._day_start = day_start
            self._trades_this_hour = trades_this_hour
            self._tool_calls_this_minute = tool_calls_this_minute

        logger.info(
            "Restored policy state: failures=%d, peak=$%s, daily_spend=$%s (same_day=%s)",
            self._consecutive_failures,
            self._peak_portfolio_usd,
            self._daily_spend_usd,
            same_day,
        )

    def _save_state(self) -> None:
        """Persist current state if a store is configured."""
        if self._state_store:
            self._state_store.save(self._get_state_dict())

    # -- Argument resolution -------------------------------------------------

    @staticmethod
    def _resolve_effective_args(args: dict) -> dict:
        """Merge nested ``params`` into top-level args for scope checks.

        Tools like ``compile_intent`` nest token/protocol/chain fields inside
        a ``params`` dict. This method surfaces those fields so that scope
        checks (token allowlist, protocol allowlist, etc.) can find them.

        Intent field names are mapped to the canonical names used by policy
        checks: ``from_token`` -> ``token_in``, ``to_token`` -> ``token_out``,
        ``borrow_token`` -> ``token``.
        """
        params = args.get("params")
        if not params or not isinstance(params, dict):
            return args

        # Start with top-level args, then fill in missing fields from params
        effective = dict(args)

        # Direct field mappings: intent vocabulary -> policy vocabulary
        _INTENT_FIELD_MAP = {
            "from_token": "token_in",
            "to_token": "token_out",
            "borrow_token": "token",
            "borrow_amount": "amount",
        }

        # Fields that policy checks look for
        _POLICY_FIELDS = (
            "chain",
            "protocol",
            "intent_type",
            "token",
            "token_in",
            "token_out",
            "token_a",
            "token_b",
            "amount",
            "amount_a",
            "amount_b",
            "amount_usd",
            "collateral_token",
            "collateral_amount",
            "execution_wallet",
            "destination_chain",
            "from_chain",
            "to_chain",
        )

        # Map intent field names to policy field names
        for intent_key, policy_key in _INTENT_FIELD_MAP.items():
            if intent_key in params and policy_key not in effective:
                effective[policy_key] = params[intent_key]

        # Surface params fields that policy checks look for
        for key in _POLICY_FIELDS:
            if key not in effective and key in params:
                effective[key] = params[key]

        return effective

    # -- Private checks -----------------------------------------------------

    def _check_tool_allowed(self, tool_def: ToolDefinition, violations: list[str], suggestions: list[str]) -> None:
        if self.policy.allowed_tools is not None and tool_def.name not in self.policy.allowed_tools:
            violations.append(f"Tool '{tool_def.name}' is not in the allowed set.")
            suggestions.append(f"Allowed tools: {sorted(self.policy.allowed_tools)}")

    def _check_chain_allowed(self, args: dict, violations: list[str], suggestions: list[str]) -> None:
        if self.policy.allowed_chains is None:
            return
        allowed_lower = {c.lower() for c in self.policy.allowed_chains}
        sorted_allowed = sorted(self.policy.allowed_chains)
        # Check all chain-like fields: chain, destination_chain, from_chain, to_chain
        for key, label in [
            ("chain", "Chain"),
            ("destination_chain", "Destination chain"),
            ("from_chain", "Source chain"),
            ("to_chain", "Destination chain"),
        ]:
            value = args.get(key)
            if value and value.lower() not in allowed_lower:
                violations.append(f"{label} '{value}' is not allowed.")
                suggestions.append(f"Allowed chains: {sorted_allowed}")

    def _check_protocol_allowed(self, args: dict, violations: list[str], suggestions: list[str]) -> None:
        protocol = args.get("protocol")
        if (
            protocol
            and self.policy.allowed_protocols is not None
            and protocol.lower() not in {p.lower() for p in self.policy.allowed_protocols}
        ):
            violations.append(f"Protocol '{protocol}' is not allowed.")
            suggestions.append(f"Allowed protocols: {sorted(self.policy.allowed_protocols)}")

    def _check_token_allowed(self, args: dict, violations: list[str], suggestions: list[str]) -> None:
        if self.policy.allowed_tokens is None:
            return
        allowed_lower = {t.lower() for t in self.policy.allowed_tokens}
        chain = args.get("chain", "")
        for key in ("token", "token_in", "token_out", "token_a", "token_b"):
            token = args.get(key)
            if not token:
                continue
            if token.lower() in allowed_lower:
                continue
            # If token looks like an address, try to resolve it to a symbol
            if token.startswith("0x") and len(token) == 42 and chain:
                resolved_symbol = self._resolve_token_symbol(token, chain)
                if resolved_symbol and resolved_symbol.lower() in allowed_lower:
                    continue
            violations.append(f"Token '{token}' is not in the allowed set.")
            suggestions.append(f"Allowed tokens: {sorted(self.policy.allowed_tokens)}")

    @staticmethod
    def _resolve_token_symbol(address: str, chain: str) -> str | None:
        """Try to resolve an address to a token symbol. Returns None on failure."""
        try:
            from almanak.framework.data.tokens import get_token_resolver
            from almanak.framework.data.tokens.resolver import TokenResolutionError

            resolver = get_token_resolver()
            resolved = resolver.resolve(address, chain)
            return resolved.symbol
        except (TokenResolutionError, ImportError, KeyError, ValueError):
            return None

    @staticmethod
    def _resolve_token_decimals(token: str, chain: str) -> int | None:
        """Try to resolve token decimals for raw-amount normalization. Returns None on failure."""
        if not chain:
            return None
        try:
            from almanak.framework.data.tokens import get_token_resolver
            from almanak.framework.data.tokens.resolver import TokenResolutionError

            resolver = get_token_resolver()
            resolved = resolver.resolve(token, chain)
            return resolved.decimals
        except (TokenResolutionError, ImportError, KeyError, ValueError):
            return None

    def _check_intent_type_allowed(self, args: dict, violations: list[str], suggestions: list[str]) -> None:
        intent_type = args.get("intent_type")
        if (
            intent_type
            and self.policy.allowed_intent_types is not None
            and intent_type.lower() not in {t.lower() for t in self.policy.allowed_intent_types}
        ):
            violations.append(f"Intent type '{intent_type}' is not allowed.")
            suggestions.append(f"Allowed intent types: {sorted(self.policy.allowed_intent_types)}")

    def _check_spend_limits(self, args: dict, violations: list[str], suggestions: list[str]) -> None:
        # Auto daily reset: check if 24h has elapsed since last reset
        now = time.time()
        if now - self._day_start >= 86400:
            self.reset_daily()

        total_usd = self._estimate_usd_value(args)

        if total_usd == 0:
            return

        if total_usd > self.policy.max_single_trade_usd:
            violations.append(
                f"Estimated trade value ${total_usd:.2f} exceeds single-trade limit ${self.policy.max_single_trade_usd}."
            )
            suggestions.append(f"Reduce amount to at most ${self.policy.max_single_trade_usd}.")

        projected = self._daily_spend_usd + total_usd
        if projected > self.policy.max_daily_spend_usd:
            violations.append(
                f"Projected daily spend ${projected:.2f} exceeds daily limit ${self.policy.max_daily_spend_usd}."
            )
            suggestions.append("Wait until the daily limit resets or reduce the trade size.")

    def _check_rate_limits(self, tool_def: ToolDefinition, violations: list[str], suggestions: list[str]) -> None:
        now = time.time()

        # Prune old entries
        self._tool_calls_this_minute = [t for t in self._tool_calls_this_minute if now - t < 60]
        if len(self._tool_calls_this_minute) >= self.policy.max_tool_calls_per_minute:
            violations.append(f"Tool call rate limit reached ({self.policy.max_tool_calls_per_minute}/min).")
            suggestions.append("Wait before making more tool calls.")

        if tool_def.risk_tier in (RiskTier.MEDIUM, RiskTier.HIGH):
            self._trades_this_hour = [t for t in self._trades_this_hour if now - t < 3600]
            if len(self._trades_this_hour) >= self.policy.max_trades_per_hour:
                violations.append(f"Trade rate limit reached ({self.policy.max_trades_per_hour}/hour).")
                suggestions.append("Wait before executing more trades.")

    def _check_circuit_breaker(self, violations: list[str], suggestions: list[str]) -> None:
        if self._consecutive_failures >= self.policy.max_consecutive_failures:
            violations.append(f"Circuit breaker: {self._consecutive_failures} consecutive failures.")
            suggestions.append("Investigate recent failures before retrying.")

    def _check_cooldown(self, violations: list[str], suggestions: list[str]) -> None:
        if self._last_trade_timestamp > 0:
            elapsed = time.time() - self._last_trade_timestamp
            if elapsed < self.policy.cooldown_seconds:
                remaining = int(self.policy.cooldown_seconds - elapsed)
                violations.append(f"Cooldown active: {remaining}s remaining.")
                suggestions.append(f"Wait {remaining} seconds before the next trade.")

    def _check_stop_loss(self, violations: list[str], suggestions: list[str]) -> None:
        """Block MEDIUM/HIGH risk tools if portfolio drawdown exceeds stop_loss_pct."""
        if self._peak_portfolio_usd <= 0 or self._current_portfolio_usd <= 0:
            return
        drawdown_pct = (self._peak_portfolio_usd - self._current_portfolio_usd) / self._peak_portfolio_usd * 100
        if drawdown_pct >= self.policy.stop_loss_pct:
            violations.append(
                f"Stop-loss triggered: portfolio down {drawdown_pct:.1f}% from peak "
                f"(limit: {self.policy.stop_loss_pct}%)."
            )
            suggestions.append("Investigate portfolio losses before continuing trading.")

    def _check_position_size(self, args: dict, violations: list[str], suggestions: list[str]) -> None:
        """Block trades that exceed max_position_size_usd."""
        total_usd = self._estimate_usd_value(args)
        if total_usd > 0 and total_usd > self.policy.max_position_size_usd:
            violations.append(
                f"Estimated position value ${total_usd:.2f} exceeds max position size "
                f"${self.policy.max_position_size_usd}."
            )
            suggestions.append(f"Reduce position to at most ${self.policy.max_position_size_usd}.")

    def _check_approval_gate(
        self, tool_def: ToolDefinition, args: dict, violations: list[str], suggestions: list[str]
    ) -> None:
        """Block trades exceeding the approval threshold (hard block for autonomous agents)."""
        policy_threshold = self.policy.require_human_approval_above_usd
        per_tool_threshold = (
            Decimal(str(tool_def.requires_approval_above_usd))
            if tool_def.requires_approval_above_usd is not None
            else None
        )
        # Use the more restrictive threshold
        if per_tool_threshold is not None:
            effective = min(policy_threshold, per_tool_threshold)
        else:
            effective = policy_threshold

        total_usd = self._estimate_usd_value(args)
        if total_usd > 0 and total_usd > effective:
            violations.append(f"Estimated value ${total_usd:.2f} exceeds approval threshold ${effective}.")
            suggestions.append(f"Reduce trade size to at most ${effective}.")

    def _check_rebalance_gate(self, tool_def: ToolDefinition, violations: list[str], suggestions: list[str]) -> None:
        """Check LP open/close has verified economic viability first."""
        if tool_def.name not in _LP_TOOLS:
            return
        if not self._rebalance_approved:
            if self.policy.require_rebalance_check:
                violations.append("LP action requires compute_rebalance_candidate check first.")
                suggestions.append("Call compute_rebalance_candidate before open/close LP.")
            else:
                suggestions.append("Consider calling compute_rebalance_candidate first to verify economic viability.")

    def _check_execution_wallet(self, args: dict, violations: list[str], suggestions: list[str]) -> None:
        """Block execution if wallet is not in the configured allowlist.

        When execution_wallet is omitted, the executor falls back to its default
        wallet. We check that wallet address against the allowlist too.
        """
        if self.policy.allowed_execution_wallets is None:
            return
        wallet = args.get("execution_wallet") or self._default_wallet
        if not wallet:
            return
        allowed_lower = {w.lower() for w in self.policy.allowed_execution_wallets}
        if wallet.lower() not in allowed_lower:
            violations.append(f"Execution wallet '{wallet}' is not in the allowed set.")
            suggestions.append(f"Allowed wallets: {sorted(self.policy.allowed_execution_wallets)}")

    def _estimate_usd_value(self, args: dict) -> Decimal:
        """Shared USD estimation logic for spend/position/approval checks."""
        amount_token_pairs = [
            ("amount", "token_in", "token", "from_token", "underlying_token"),
            ("amount_a", "token_a"),
            ("amount_b", "token_b"),
            ("collateral_amount", "collateral_token"),
        ]

        total_usd = Decimal("0")
        for entry in amount_token_pairs:
            amount_key = entry[0]
            token_keys = entry[1:]
            val = args.get(amount_key)
            if val is None:
                continue
            try:
                raw_amount = Decimal(str(val))
            except (InvalidOperation, TypeError, ValueError):
                continue

            # Find the matching token key
            token = None
            matched_key = None
            for tk in token_keys:
                token = args.get(tk)
                if token:
                    matched_key = tk
                    break

            # Vault deposits use raw token units (e.g. 10000000 = 10 USDC).
            # Normalize by decimals when underlying_token is the matched key.
            amount = raw_amount
            if matched_key == "underlying_token" and token:
                decimals = self._resolve_token_decimals(token, args.get("chain", ""))
                if decimals is not None:
                    amount = raw_amount / Decimal(10**decimals)

            usd_amount = amount  # fallback: treat as USD
            if self._price_lookup and token:
                try:
                    price = self._price_lookup(token)
                    if price is not None and price > 0:
                        usd_amount = amount * price
                except Exception:  # noqa: BLE001
                    logger.debug("Price lookup failed for token %r, using raw amount as USD estimate", token)

            total_usd += usd_amount

        return total_usd
