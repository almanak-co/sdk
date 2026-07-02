"""Preflight support matrix for the PnL backtest engine.

Cross-references a backtest's ``(chain, detected strategy type, declared
protocols)`` against the manifest-derived registries BEFORE the simulation
loop runs, so an unsupported combination aborts at preflight instead of
degrading silently (or dying) mid-run.

Every check derives from an existing registry or connector manifest — the
chain registry (``almanak.core.chains``), the connector manifests
(``CONNECTOR_REGISTRY``), and the strategy-side dispatch registries
(``FeeModelRegistry``, ``DexVolumeRegistry``, ``LendingReadRegistry``,
``FundingHistoryRegistry``). There is deliberately no hardcoded
protocol/chain table here; a chain or protocol becomes "supported" by
declaring the capability on its descriptor/manifest, never by editing this
module.

Semantics (blueprint 31, "Preflight support matrix"):

- **Hard failures** (unresolvable chain; a vendor-platform price provider
  that cannot price any token on the chain) abort the run before the loop.
  The abort is unconditional — ``fail_on_preflight_error=False`` does not
  bypass it; disabling ``preflight_validation`` entirely is the only escape
  hatch.
- **Degraded lanes** (missing fee model, LP volume, lending APY, or perp
  funding data) print a table + WARNING and continue. In institutional /
  strict-reproducibility mode they are additionally recorded as compliance
  violations at boot (:func:`boot_compliance_violations`).
- **Warnings** (protocols not declared; connector-declared intent types
  outside the simulated envelope) never block; the intent-envelope warning
  exists because the run FAILS with ``UnsupportedIntentError`` if such an
  intent reaches the generic lane (PR #3155 refusal semantics).

Not to be confused with ``almanak/framework/cli/support_matrix.py`` — the
``almanak info matrix`` chains x protocols LIVE-trading matrix. This module
answers a different question: "can the PnL engine honestly simulate this
configuration?".
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from almanak.connectors._strategy_base.dex_volume_registry import DexVolumeRegistry
from almanak.connectors._strategy_base.funding_history_registry import FundingHistoryRegistry
from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry
from almanak.core.chains import ChainRegistry
from almanak.core.chains._helpers import external_id_for, vendor_chain_map

if TYPE_CHECKING:
    from almanak.framework.backtesting.config import BacktestDataConfig
    from almanak.framework.backtesting.pnl.config import PnLBacktestConfig

__all__ = [
    "BacktestSupportReport",
    "LaneSupport",
    "boot_compliance_violations",
    "evaluate_backtest_support",
]

#: Lane vocabulary. One entry per data/simulation capability the engine
#: needs; type-specific lanes are only evaluated for matching strategies.
LANE_PRICE = "price"
LANE_FEE_MODEL = "fee_model"
LANE_LP_VOLUME = "lp_volume"
LANE_LENDING_APY = "lending_apy"
LANE_PERP_FUNDING = "perp_funding"
LANE_INTENTS = "intents"

LaneStatus = Literal["supported", "degraded", "unsupported"]

_STATUS_WIDTH = len("UNSUPPORTED")


@dataclass
class LaneSupport:
    """Support verdict for one (lane, protocol) pair.

    Attributes:
        lane: Lane name (one of the ``LANE_*`` constants).
        status: ``supported`` (real data lane), ``degraded`` (simulation
            falls back to defaults/heuristics), or ``unsupported`` (the lane
            cannot function at all — always paired with a hard failure or a
            fail-loud runtime contract named in ``detail``).
        detail: Human-readable explanation, including remediation where one
            exists (CLI flags, supported chains, ...).
        protocol: The protocol this verdict is scoped to, or ``None`` for
            chain-level lanes (price).
    """

    lane: str
    status: LaneStatus
    detail: str
    protocol: str | None = None

    @property
    def label(self) -> str:
        """Render key for tables: ``lane`` or ``lane[protocol]``."""
        return self.lane if self.protocol is None else f"{self.lane}[{self.protocol}]"

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dictionary (``protocol`` emitted only when set)."""
        out: dict[str, Any] = {
            "lane": self.lane,
            "status": self.status,
            "detail": self.detail,
        }
        if self.protocol is not None:
            out["protocol"] = self.protocol
        return out

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LaneSupport:
        """Deserialize from a dictionary."""
        return cls(
            lane=data["lane"],
            status=data["status"],
            detail=data["detail"],
            protocol=data.get("protocol"),
        )


@dataclass
class BacktestSupportReport:
    """Result of the preflight support evaluation for one backtest config.

    Attributes:
        chain: The chain exactly as configured (aliases preserved).
        strategy_type: Detected strategy type (``lp``/``lending``/``perp``/
            ``swap``/...), or ``None`` when detection found nothing.
        protocols: Declared protocols (normalized), possibly empty.
        lanes: Per-lane verdicts (see :class:`LaneSupport`).
        hard_failures: Conditions under which no honest simulation is
            possible; the engine aborts the run before the loop.
        warnings: Non-blocking findings (undeclared protocols, declared
            intent types outside the simulated envelope, ...).
        recommendations: Actionable remediation strings for hard failures.
    """

    chain: str
    strategy_type: str | None = None
    protocols: list[str] = field(default_factory=list)
    lanes: list[LaneSupport] = field(default_factory=list)
    hard_failures: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)

    @property
    def degraded_lanes(self) -> list[LaneSupport]:
        """Lanes that are not fully supported (degraded or unsupported)."""
        return [lane for lane in self.lanes if lane.status != "supported"]

    @property
    def has_signal(self) -> bool:
        """True when the report carries anything beyond all-green lanes.

        Serialization keys on this so a fully supported default run leaves
        result artifacts unchanged (the ``swap:fiat_usd_pin`` discipline:
        new fields are emitted only when non-empty/non-default).
        """
        return bool(self.hard_failures or self.warnings or self.degraded_lanes)

    def render_table(self) -> str:
        """Render the support table as plain text (CLI + engine log)."""
        protocols = ", ".join(self.protocols) if self.protocols else "(not declared)"
        header = f"Backtest support — chain: {self.chain}"
        if self.strategy_type:
            header += f", strategy type: {self.strategy_type}"
        header += f", protocols: {protocols}"
        lines = [header]
        if self.lanes:
            width = max(len(lane.label) for lane in self.lanes)
            for lane in self.lanes:
                lines.append(f"  {lane.label.ljust(width)}  {lane.status.upper().ljust(_STATUS_WIDTH)}  {lane.detail}")
        for failure in self.hard_failures:
            lines.append(f"  HARD FAILURE: {failure}")
        for warning in self.warnings:
            lines.append(f"  Warning: {warning}")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a dictionary."""
        return {
            "chain": self.chain,
            "strategy_type": self.strategy_type,
            "protocols": list(self.protocols),
            "lanes": [lane.to_dict() for lane in self.lanes],
            "hard_failures": list(self.hard_failures),
            "warnings": list(self.warnings),
            "recommendations": list(self.recommendations),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BacktestSupportReport:
        """Deserialize from a dictionary."""
        return cls(
            chain=data["chain"],
            strategy_type=data.get("strategy_type"),
            protocols=list(data.get("protocols", [])),
            lanes=[LaneSupport.from_dict(lane) for lane in data.get("lanes", [])],
            hard_failures=list(data.get("hard_failures", [])),
            warnings=list(data.get("warnings", [])),
            recommendations=list(data.get("recommendations", [])),
        )


# =============================================================================
# Strategy type / protocol discovery (best-effort, never guessing)
# =============================================================================


def _normalize_protocol(protocol: str) -> str:
    return protocol.strip().lower().replace("-", "_")


def _strategy_config_dict(strategy: Any) -> dict[str, Any]:
    """Best-effort plain-dict view of ``strategy.config`` (instances only)."""
    if strategy is None or isinstance(strategy, type):
        return {}
    config = getattr(strategy, "config", None)
    if config is None:
        return {}
    if isinstance(config, dict):
        return config
    return _coerce_config_dict(config)


def _coerce_config_dict(config: Any) -> dict[str, Any]:
    """Coerce a non-dict config object to a plain dict.

    ``to_dict()`` is preferred when it yields a dict; any failure falls
    through to ``dict(config)``; anything unconvertible yields ``{}`` —
    best-effort extraction only, never raising into preflight.
    """
    to_dict = getattr(config, "to_dict", None)
    if callable(to_dict):
        try:
            result = to_dict()
            if isinstance(result, dict):
                return result
        except Exception:  # noqa: BLE001 - best-effort extraction only
            pass
    try:
        return dict(config)
    except (TypeError, ValueError):
        return {}


def _strategy_metadata(strategy: Any) -> Any:
    """Safely resolve ``STRATEGY_METADATA`` from an instance OR a class.

    ``detect_strategy_type``'s own extraction calls ``strategy.get_metadata()``,
    which raises for bare classes (unbound method); the class attribute read
    below works for both shapes.
    """
    if strategy is None:
        return None
    return getattr(strategy, "STRATEGY_METADATA", None)


def _detection_subject(strategy: Any) -> Any:
    """Build the ``detect_strategy_type`` input for an instance, class, or None.

    Classes are converted to the dict form the detector accepts, because its
    instance path calls ``get_metadata()`` which is not class-safe.
    """
    if strategy is not None and not isinstance(strategy, type):
        return strategy
    metadata = _strategy_metadata(strategy)
    if metadata is None:
        return {}
    return {
        "metadata": {
            "tags": list(getattr(metadata, "tags", None) or []),
            "supported_protocols": list(getattr(metadata, "supported_protocols", None) or []),
            "intent_types": list(getattr(metadata, "intent_types", None) or []),
        }
    }


def _detect_type(strategy: Any, explicit_strategy_type: str | None) -> str | None:
    """Detect the strategy type via the adapter registry's detection system."""
    # Deferred import: adapters.registry pulls in the adapter base module;
    # keeping it lazy avoids import-time coupling (and any future cycle
    # through backtesting.models).
    from almanak.framework.backtesting.adapters.registry import detect_strategy_type

    detection_config: dict[str, Any] | None = None
    if explicit_strategy_type is not None and explicit_strategy_type != "auto":
        detection_config = {"strategy_type": explicit_strategy_type}
    hint = detect_strategy_type(_detection_subject(strategy), detection_config)
    return hint.strategy_type


def _declared_protocols(strategy: Any, strategy_config: dict[str, Any]) -> list[str]:
    """Discover declared protocols from config + strategy metadata.

    The strategy config's explicit ``protocol`` field wins outright — it
    pins THIS run's protocol, so the metadata's full
    ``@almanak_strategy(supported_protocols=...)`` list would only add
    noise. Without it, every metadata-declared protocol is evaluated. A
    ``pool`` field (e.g. ``"WETH/USDC/500"``) names tokens, not a protocol,
    so it contributes nothing here — no guessing.
    """
    config_protocol = strategy_config.get("protocol")
    if isinstance(config_protocol, str) and config_protocol.strip():
        return [_normalize_protocol(config_protocol)]
    protocols: list[str] = []
    metadata = _strategy_metadata(strategy)
    for protocol in getattr(metadata, "supported_protocols", None) or []:
        if isinstance(protocol, str) and protocol.strip():
            normalized = _normalize_protocol(protocol)
            if normalized not in protocols:
                protocols.append(normalized)
    return protocols


def _connector_strategy_intents(protocol: str) -> tuple[str, ...] | None:
    """Return the connector manifest's ``strategy_intents``, or None."""
    # Deferred import: connector discovery must never run at module import
    # time (same rule as the sibling _strategy_base registries).
    from almanak.connectors._connector import CONNECTOR_REGISTRY

    manifest = CONNECTOR_REGISTRY.get(protocol)
    if manifest is None:
        return None
    return manifest.strategy_intents


def _protocol_strategy_types(protocol: str) -> set[str]:
    """Manifest-derived strategy types a protocol serves (may be empty)."""
    from almanak.framework.backtesting.adapters.registry import (
        INTENT_TO_STRATEGY_TYPE,
        PROTOCOL_TO_STRATEGY_TYPE,
    )

    types: set[str] = set()
    declared_type = PROTOCOL_TO_STRATEGY_TYPE.get(protocol)
    if declared_type is not None:
        types.add(declared_type)
    for intent in _connector_strategy_intents(protocol) or ():
        intent_type = INTENT_TO_STRATEGY_TYPE.get(intent.upper())
        if intent_type is not None:
            types.add(intent_type)
    return types


def _lane_protocols(lane_type: str, strategy_type: str | None, protocols: list[str]) -> list[str]:
    """Protocols a type-specific lane (lp/lending/perp) must evaluate.

    The lane fires when the detected strategy type matches, or — for
    undetected / multi-protocol strategies — for every declared protocol
    whose manifest serves that type. A protocol with no manifest facts is
    still evaluated when the strategy type matches explicitly (the
    registries themselves then report the missing data honestly).
    """
    if strategy_type not in (lane_type, None, "multi_protocol"):
        return []
    selected: list[str] = []
    for protocol in protocols:
        protocol_types = _protocol_strategy_types(protocol)
        if lane_type in protocol_types or (not protocol_types and strategy_type == lane_type):
            selected.append(protocol)
    return selected


# =============================================================================
# Lane checks
# =============================================================================


def _check_price_lane(
    chain: str,
    configured_chain: str,
    vendor: str | None,
    data_provider: Any,
    report: BacktestSupportReport,
) -> None:
    """Price lane: can the historical price provider price this chain at all?

    Providers that resolve token prices through a vendor chain-platform id
    declare ``price_platform_vendor`` (CoinGecko does); for those, a chain
    without the vendor's platform id in the chain registry cannot price any
    contract-addressed token — a hard failure, because today that dies
    mid-run at the first price lookup. Providers without the attribute
    (custom/synthetic fixtures) are not judged here; the standard per-token
    availability preflight still probes them.
    """
    if vendor is None:
        provider_name = getattr(data_provider, "provider_name", None)
        provider_label = f"provider '{provider_name}'" if provider_name else "the data provider"
        report.lanes.append(
            LaneSupport(
                lane=LANE_PRICE,
                status="supported",
                detail=(
                    f"{provider_label} does not price via a vendor chain platform; "
                    "per-token availability is probed by the standard preflight"
                ),
            )
        )
        return

    platform = external_id_for(chain, vendor)
    if platform is not None:
        report.lanes.append(
            LaneSupport(
                lane=LANE_PRICE,
                status="supported",
                detail=f"{vendor} platform id '{platform}'",
            )
        )
        return

    supported_chains = ", ".join(sorted(vendor_chain_map(vendor)))
    report.lanes.append(
        LaneSupport(
            lane=LANE_PRICE,
            status="unsupported",
            detail=(
                f"chain '{configured_chain}' declares no {vendor} platform id — "
                "the historical price provider cannot resolve token prices on this chain"
            ),
        )
    )
    report.hard_failures.append(
        f"chain '{configured_chain}' declares no {vendor} platform id in the chain registry; "
        f"the historical price provider ('{vendor}') cannot price any token on it"
    )
    report.recommendations.append(
        f"Run the backtest on a {vendor}-indexed chain ({supported_chains}), "
        f"or supply a data provider that can price '{configured_chain}'."
    )


def _check_fee_model_lane(protocols: list[str], report: BacktestSupportReport) -> None:
    """Fee-model lane: is protocol fee math registered for each protocol?"""
    # Deferred import: fee_models.base imports backtesting.models; keep this
    # module import-light so models.py can reference the report types.
    from almanak.framework.backtesting.pnl.fee_models.base import FeeModelRegistry

    for protocol in protocols:
        metadata = FeeModelRegistry.get_metadata(protocol)
        if metadata is not None:
            report.lanes.append(
                LaneSupport(
                    lane=LANE_FEE_MODEL,
                    status="supported",
                    detail=f"protocol fee model '{metadata.name}' registered",
                    protocol=protocol,
                )
            )
        else:
            report.lanes.append(
                LaneSupport(
                    lane=LANE_FEE_MODEL,
                    status="degraded",
                    detail=(
                        f"no protocol fee model registered for '{protocol}' — the engine's flat "
                        "default fee model is used; the protocol's real fee structure is not modeled"
                    ),
                    protocol=protocol,
                )
            )


def _check_lp_volume_lane(
    chain: str,
    protocols: list[str],
    data_config: BacktestDataConfig | None,
    report: BacktestSupportReport,
) -> None:
    """LP volume lane: is there an honest volume source for LP fee accrual?"""
    volume_flag_hint = (
        "pass --pool-volume-usd-daily <usd> (optionally --pool-liquidity-usd <usd>) for an "
        "explicit volume, or opt into --allow-volume-fallback (LOW-confidence heuristic)"
    )
    explicit_volume = data_config is not None and data_config.explicit_pool_volume_usd_daily is not None
    fallback_opt_in = data_config is not None and data_config.allow_volume_fallback

    for protocol in protocols:
        entry = DexVolumeRegistry.entry_for(protocol)
        if explicit_volume:
            status: LaneStatus = "supported"
            detail = "explicit pool volume supplied (--pool-volume-usd-daily)"
        elif data_config is not None and not data_config.use_historical_volume:
            status = "degraded"
            detail = (
                f"historical LP volume disabled (BacktestDataConfig.use_historical_volume=False) — {volume_flag_hint}"
            )
        elif entry is not None and chain in entry.chains:
            status = "supported"
            detail = f"historical volume via '{entry.volume_data_source}' (gateway DEX lane '{entry.dex}')"
        elif fallback_opt_in:
            status = "degraded"
            detail = (
                "LOW-confidence volume_multiplier heuristic opted in via --allow-volume-fallback — "
                "LP fee estimates can be off by an order of magnitude"
            )
        elif entry is not None:
            declared = ", ".join(entry.chains)
            status = "degraded"
            detail = (
                f"'{protocol}' declares volume data for [{declared}], not '{chain}' — the engine "
                f"refuses to fabricate LP volume (VIB-4849); {volume_flag_hint}"
            )
        else:
            status = "degraded"
            detail = (
                f"no DEX volume declaration for '{protocol}' — the engine refuses to fabricate "
                f"LP volume (VIB-4849); {volume_flag_hint}"
            )
        report.lanes.append(LaneSupport(lane=LANE_LP_VOLUME, status=status, detail=detail, protocol=protocol))


def _check_lending_apy_lane(
    chain: str,
    protocols: list[str],
    data_config: BacktestDataConfig | None,
    report: BacktestSupportReport,
) -> None:
    """Lending APY lane: historical rates, or manifest/framework defaults?"""
    for protocol in protocols:
        if data_config is not None and not data_config.use_historical_apy:
            report.lanes.append(
                LaneSupport(
                    lane=LANE_LENDING_APY,
                    status="degraded",
                    detail=(
                        "historical APY disabled (BacktestDataConfig.use_historical_apy=False) — "
                        "static default/fallback APYs are used instead of historical rates"
                    ),
                    protocol=protocol,
                )
            )
            continue
        provider_key = LendingReadRegistry.backtest_provider_key(protocol)
        if provider_key is not None:
            rate_chains = LendingReadRegistry.rate_history_chains(protocol)
            if not rate_chains or chain in rate_chains:
                report.lanes.append(
                    LaneSupport(
                        lane=LANE_LENDING_APY,
                        status="supported",
                        detail=f"historical APY provider '{provider_key}'",
                        protocol=protocol,
                    )
                )
                continue
            declared = ", ".join(rate_chains)
            report.lanes.append(
                LaneSupport(
                    lane=LANE_LENDING_APY,
                    status="degraded",
                    detail=(
                        f"'{protocol}' declares APY history for [{declared}], not '{chain}' — "
                        "static default APYs are used instead of historical rates"
                    ),
                    protocol=protocol,
                )
            )
            continue

        default_supply, default_borrow = LendingReadRegistry.backtest_default_apys(protocol)
        if default_supply is not None or default_borrow is not None:
            defaults = f"manifest default APYs used (supply {default_supply}, borrow {default_borrow})"
        else:
            defaults = "framework fallback APYs used (BacktestDataConfig.supply_apy_fallback / borrow_apy_fallback)"
        report.lanes.append(
            LaneSupport(
                lane=LANE_LENDING_APY,
                status="degraded",
                detail=f"no historical APY provider for '{protocol}' — {defaults}",
                protocol=protocol,
            )
        )


def _check_perp_funding_lane(
    chain: str,
    protocols: list[str],
    data_config: BacktestDataConfig | None,
    report: BacktestSupportReport,
) -> None:
    """Perp funding lane: historical funding rates, or the flat fallback?"""
    for protocol in protocols:
        if data_config is not None and not data_config.use_historical_funding:
            report.lanes.append(
                LaneSupport(
                    lane=LANE_PERP_FUNDING,
                    status="degraded",
                    detail=(
                        "historical funding disabled (BacktestDataConfig.use_historical_funding=False) — "
                        "a flat fallback funding rate is used"
                    ),
                    protocol=protocol,
                )
            )
            continue
        if FundingHistoryRegistry.has(protocol):
            declared_chains = FundingHistoryRegistry.declared_chains(protocol)
            if not declared_chains or chain in declared_chains:
                venue = FundingHistoryRegistry.venue_for(protocol)
                report.lanes.append(
                    LaneSupport(
                        lane=LANE_PERP_FUNDING,
                        status="supported",
                        detail=f"funding history venue '{venue}'",
                        protocol=protocol,
                    )
                )
                continue
            declared = ", ".join(declared_chains)
            report.lanes.append(
                LaneSupport(
                    lane=LANE_PERP_FUNDING,
                    status="degraded",
                    detail=(
                        f"'{protocol}' declares funding history for [{declared}], not '{chain}' — "
                        "a flat fallback funding rate is used"
                    ),
                    protocol=protocol,
                )
            )
            continue
        report.lanes.append(
            LaneSupport(
                lane=LANE_PERP_FUNDING,
                status="degraded",
                detail=f"no funding-history declaration for '{protocol}' — a flat fallback funding rate is used",
                protocol=protocol,
            )
        )


def _check_intents_lane(protocols: list[str], report: BacktestSupportReport) -> None:
    """Intents lane: connector-declared intents vs the simulated envelope.

    Declared-but-unsimulated intent types are surfaced as warnings because
    the run FAILS with ``UnsupportedIntentError`` the moment such an intent
    reaches the generic lane (PR #3155 refusal semantics — never a costed
    no-op). An adapter that genuinely handles a type outside the envelope is
    unaffected; the warning states the contract precisely.
    """
    from almanak.framework.backtesting.pnl._engine_helpers import GENERIC_SIMULATED_INTENT_TYPES

    simulated = {intent_type.value for intent_type in GENERIC_SIMULATED_INTENT_TYPES}
    for protocol in protocols:
        declared = _connector_strategy_intents(protocol)
        if declared is None:
            report.warnings.append(
                f"protocol '{protocol}' has no connector strategy manifest — "
                "intent support is unknown; runtime checks still apply"
            )
            continue
        unsimulated = [intent for intent in declared if intent.upper() not in simulated]
        if not unsimulated:
            report.lanes.append(
                LaneSupport(
                    lane=LANE_INTENTS,
                    status="supported",
                    detail="all connector-declared intents are inside the simulated envelope",
                    protocol=protocol,
                )
            )
            continue
        unsimulated_label = ", ".join(unsimulated)
        if len(unsimulated) == len(declared):
            report.lanes.append(
                LaneSupport(
                    lane=LANE_INTENTS,
                    status="degraded",
                    detail=(
                        f"none of '{protocol}''s declared intents ({unsimulated_label}) are inside "
                        "the simulated envelope — the run FAILS (UnsupportedIntentError) on the "
                        "first such intent unless an adapter lane simulates it"
                    ),
                    protocol=protocol,
                )
            )
        else:
            report.lanes.append(
                LaneSupport(
                    lane=LANE_INTENTS,
                    status="supported",
                    detail=f"declared intents outside the simulated envelope: {unsimulated_label}",
                    protocol=protocol,
                )
            )
        report.warnings.append(
            f"connector '{protocol}' declares intent(s) outside the simulated envelope: "
            f"{unsimulated_label} — the backtest FAILS (UnsupportedIntentError) if the strategy "
            "emits one and no adapter lane simulates it"
        )


# =============================================================================
# Public API
# =============================================================================


def evaluate_backtest_support(
    config: PnLBacktestConfig,
    strategy: Any | None = None,
    strategy_config: dict[str, Any] | None = None,
    *,
    data_provider: Any | None = None,
    data_config: BacktestDataConfig | None = None,
    explicit_strategy_type: str | None = None,
    price_vendor: str | None = None,
) -> BacktestSupportReport:
    """Evaluate chain/protocol support for a PnL backtest configuration.

    Pure and network-free: every verdict derives from the chain registry,
    the connector manifests, and the strategy-side dispatch registries.

    Args:
        config: The backtest configuration (``chain`` is the pivot).
        strategy: Strategy instance or class (best-effort type/protocol
            detection; ``None`` skips strategy-derived facts).
        strategy_config: Plain strategy config dict (``protocol`` field is
            read); defaults to a best-effort extraction from ``strategy``.
        data_provider: The historical data provider; consulted only for the
            duck-typed ``price_platform_vendor`` / ``provider_name`` hints.
        data_config: Adapter data config (explicit LP volume / fallback
            opt-ins change the LP volume verdict).
        explicit_strategy_type: Explicit strategy-type override (the
            backtester's ``strategy_type``; ``"auto"`` means detect).
        price_vendor: Vendor override for callers that know the price lane
            before constructing a provider (the CLI passes CoinGecko's);
            defaults to ``data_provider.price_platform_vendor``.

    Returns:
        A :class:`BacktestSupportReport`; ``hard_failures`` non-empty means
        the run must abort before the simulation loop.
    """
    report = BacktestSupportReport(chain=config.chain)

    if strategy_config is None:
        strategy_config = _strategy_config_dict(strategy)

    report.strategy_type = _detect_type(strategy, explicit_strategy_type)
    report.protocols = _declared_protocols(strategy, strategy_config)

    descriptor = ChainRegistry.try_resolve(config.chain)
    if descriptor is None:
        report.lanes.append(
            LaneSupport(
                lane=LANE_PRICE,
                status="unsupported",
                detail=f"chain '{config.chain}' is not a registered chain — no data lane can resolve it",
            )
        )
        report.hard_failures.append(f"chain '{config.chain}' is not a registered chain")
        report.recommendations.append(
            "Choose a registered chain: " + ", ".join(sorted(d.name for d in ChainRegistry.all())) + "."
        )
        return report
    chain = descriptor.name

    vendor = price_vendor or getattr(data_provider, "price_platform_vendor", None)
    if not isinstance(vendor, str) or not vendor.strip():
        # The duck-typed signal must be a real vendor string. Anything else
        # (absent, empty, or a test double standing in for the provider
        # class) means "does not price via a vendor chain platform" — the
        # per-token availability preflight still applies.
        vendor = None
    _check_price_lane(chain, config.chain, vendor, data_provider, report)

    if not report.protocols:
        report.warnings.append(
            "protocols not declared — protocol-specific support checks skipped; runtime checks still apply"
        )
        return report

    _check_fee_model_lane(report.protocols, report)
    _check_lp_volume_lane(chain, _lane_protocols("lp", report.strategy_type, report.protocols), data_config, report)
    _check_lending_apy_lane(
        chain, _lane_protocols("lending", report.strategy_type, report.protocols), data_config, report
    )
    _check_perp_funding_lane(
        chain, _lane_protocols("perp", report.strategy_type, report.protocols), data_config, report
    )
    _check_intents_lane(report.protocols, report)

    return report


def boot_compliance_violations(
    support: BacktestSupportReport | None,
    config: PnLBacktestConfig,
) -> list[str]:
    """Compliance violations for degraded lanes in institutional/strict mode.

    Default mode records nothing (degraded lanes only warn); institutional
    mode and strict reproducibility record each degraded lane as a
    compliance violation at boot so ``result.institutional_compliance``
    reflects the fidelity gap even if the simulation completes cleanly.
    """
    if support is None:
        return []
    if not (config.institutional_mode or config.strict_reproducibility):
        return []
    return [
        f"Support matrix: lane '{lane.label}' is {lane.status} on chain '{support.chain}' — {lane.detail}"
        for lane in support.degraded_lanes
    ]
