"""Liquidation parameter registry for per-asset, per-protocol thresholds.

This module provides a registry for liquidation parameters that supports:
- Per-protocol, per-asset specific thresholds
- Protocol-level default fallbacks
- Source tracking (default vs asset-specific)

Key Concepts:
    - Liquidation Threshold (Lending): The LTV at which liquidation can occur
      (e.g., 0.825 means 82.5% LTV)
    - Maintenance Margin (Perps): Minimum collateral ratio to keep position open
      (e.g., 0.05 means 5%)
    - Liquidation Penalty: Fee charged during liquidation (e.g., 0.05 = 5%)

Example:
    from almanak.framework.backtesting.pnl.calculators.liquidation_params import (
        LiquidationParamRegistry,
        LiquidationParams,
        LiquidationParamSource,
    )

    registry = LiquidationParamRegistry()

    # Look up parameters for ETH on Aave V3
    params = registry.get_params(
        protocol="aave_v3",
        asset="ETH",
    )
    print(f"Liquidation threshold: {params.liquidation_threshold}")
    print(f"Source: {params.source}")  # ASSET_SPECIFIC or PROTOCOL_DEFAULT

References:
    - Aave V3 Risk Parameters: https://docs.aave.com/developers/v/2.0/the-core-protocol/lendingpool
    - GMX V2 Position Parameters: https://docs.gmx.io/docs/trading/v2
"""

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.connectors._connector_descriptor import BacktestRiskDecl

logger = logging.getLogger(__name__)


class LiquidationParamSource(StrEnum):
    """Source of liquidation parameters.

    Tracks where the parameters came from for audit/compliance purposes.

    Values:
        ASSET_SPECIFIC: Parameters specific to the asset (highest confidence)
        PROTOCOL_DEFAULT: Protocol-level default (medium confidence)
        GLOBAL_DEFAULT: Fallback default when protocol not found (lowest confidence)
        HISTORICAL: Parameters from historical data source
    """

    ASSET_SPECIFIC = "asset_specific"
    PROTOCOL_DEFAULT = "protocol_default"
    GLOBAL_DEFAULT = "global_default"
    HISTORICAL = "historical"


@dataclass(frozen=True)
class LiquidationParams:
    """Liquidation parameters for a specific protocol/asset combination.

    This dataclass captures all relevant liquidation parameters for both
    lending (liquidation threshold) and perpetual (maintenance margin) positions.

    Attributes:
        protocol: Protocol name (e.g., "aave_v3", "gmx_v2")
        asset: Asset symbol (e.g., "ETH", "BTC") or None for protocol-level
        liquidation_threshold: For lending - LTV at liquidation (e.g., 0.825)
        maintenance_margin: For perps - minimum margin ratio (e.g., 0.05)
        liquidation_penalty: Fee charged during liquidation (e.g., 0.05)
        source: Where these parameters came from
        source_timestamp: When the parameters were fetched (for historical data)
    """

    protocol: str
    asset: str | None
    liquidation_threshold: Decimal
    maintenance_margin: Decimal
    liquidation_penalty: Decimal
    source: LiquidationParamSource
    source_timestamp: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "protocol": self.protocol,
            "asset": self.asset,
            "liquidation_threshold": str(self.liquidation_threshold),
            "maintenance_margin": str(self.maintenance_margin),
            "liquidation_penalty": str(self.liquidation_penalty),
            "source": self.source.value,
            "source_timestamp": self.source_timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LiquidationParams":
        """Deserialize from dictionary."""
        return cls(
            protocol=data["protocol"],
            asset=data.get("asset"),
            liquidation_threshold=Decimal(str(data["liquidation_threshold"])),
            maintenance_margin=Decimal(str(data["maintenance_margin"])),
            liquidation_penalty=Decimal(str(data["liquidation_penalty"])),
            source=LiquidationParamSource(data["source"]),
            source_timestamp=data.get("source_timestamp"),
        )


def _legacy_keys_for_manifest(manifest_name: str, decl: "BacktestRiskDecl") -> tuple[str, ...]:
    """Return the ``LiquidationParamRegistry`` protocol key(s) for ``manifest_name``.

    Most connectors use their own name as the sole key.  Two connectors carry
    historical aliases declared on their ``BacktestRiskDecl.legacy_param_keys``:
    - morpho_blue -> ``("morpho",)``
    - gmx_v2     -> ``("gmx", "gmx_v2")``

    The alias data lives on the connector decl so this helper is generic
    iteration with zero protocol literals.
    """
    return decl.legacy_param_keys if decl.legacy_param_keys else (manifest_name,)


## Sanctioned residual: off-platform venues with no connector
#
# These venues have no Almanak connector and therefore no manifest where
# liquidation defaults can be declared. This table is the ONLY permitted
# location for such entries. Any venue that gains a connector MUST move its
# row to that connector's ``BacktestRiskDecl`` and remove it from here.
# The literal dict keys intentionally remain in the literal-dispatch-scan
# baseline as a sanctioned entry (see plan-022 commit message).
OFF_PLATFORM_VENUE_DEFAULTS: dict[str, "LiquidationParams"] = {
    "binance_perp": LiquidationParams(
        protocol="binance_perp",
        asset=None,
        liquidation_threshold=Decimal("0"),
        maintenance_margin=Decimal("0.04"),  # 4% maintenance margin
        liquidation_penalty=Decimal("0.05"),
        source=LiquidationParamSource.PROTOCOL_DEFAULT,
    ),
    "bybit": LiquidationParams(
        protocol="bybit",
        asset=None,
        liquidation_threshold=Decimal("0"),
        maintenance_margin=Decimal("0.05"),  # 5% maintenance margin
        liquidation_penalty=Decimal("0.05"),
        source=LiquidationParamSource.PROTOCOL_DEFAULT,
    ),
    "dydx": LiquidationParams(
        protocol="dydx",
        asset=None,
        liquidation_threshold=Decimal("0"),
        maintenance_margin=Decimal("0.03"),  # 3% maintenance margin
        liquidation_penalty=Decimal("0.05"),
        source=LiquidationParamSource.PROTOCOL_DEFAULT,
    ),
}


@dataclass
class LiquidationParamRegistry:
    """Registry for liquidation parameters with per-asset lookup and fallback.

    This registry maintains liquidation parameters organized by protocol and asset.
    It provides fallback logic from asset-specific to protocol-default to global-default.

    Lookup priority:
        1. Asset-specific parameters (protocol + asset)
        2. Protocol-level defaults (protocol only)
        3. Global defaults (hardcoded fallback)

    The source field in returned LiquidationParams indicates which level was used.

    Attributes:
        protocol_defaults: Default parameters per protocol
        asset_params: Asset-specific parameters keyed by (protocol, asset)
        global_default_threshold: Global fallback liquidation threshold
        global_default_margin: Global fallback maintenance margin
        global_default_penalty: Global fallback liquidation penalty

    Example:
        registry = LiquidationParamRegistry()

        # Add custom asset-specific params
        registry.register_asset_params(
            protocol="aave_v3",
            asset="WBTC",
            liquidation_threshold=Decimal("0.80"),
            maintenance_margin=Decimal("0.05"),
            liquidation_penalty=Decimal("0.05"),
        )

        # Look up params (will use asset-specific if registered)
        params = registry.get_params("aave_v3", "WBTC")
        assert params.source == LiquidationParamSource.ASSET_SPECIFIC
    """

    # Protocol-level default parameters
    protocol_defaults: dict[str, LiquidationParams] = field(default_factory=dict)

    # Asset-specific parameters: (protocol, asset) -> LiquidationParams
    asset_params: dict[tuple[str, str], LiquidationParams] = field(default_factory=dict)

    # Global fallback values
    global_default_threshold: Decimal = Decimal("0.825")
    global_default_margin: Decimal = Decimal("0.05")
    global_default_penalty: Decimal = Decimal("0.05")

    def __post_init__(self) -> None:
        """Initialize default protocol parameters."""
        if not self.protocol_defaults:
            self._initialize_defaults()

    def _initialize_defaults(self) -> None:
        """Initialize protocol-level default parameters from connector manifests.

        Protocol-owned connectors are queried via ``BacktestRiskRegistry`` (plan 022).
        Off-platform venues (no connector) are populated from ``OFF_PLATFORM_VENUE_DEFAULTS``.

        Legacy keys are preserved verbatim so that callers using historical strings
        (``"morpho"``, ``"gmx"``) continue to resolve.  The alias rules live on each
        connector's ``BacktestRiskDecl.legacy_param_keys``; this helper is generic
        iteration with zero protocol literals.
        """
        # Lazy import avoids pulling the connector registry walk onto the hot path.
        from almanak.connectors._strategy_backtest_risk_registry import BACKTEST_RISK_REGISTRY

        for manifest_name, decl in BACKTEST_RISK_REGISTRY.liquidation_params().items():
            if decl.liquidation_default is None:
                continue
            ld = decl.liquidation_default
            for key in _legacy_keys_for_manifest(manifest_name, decl):
                self.protocol_defaults[key] = LiquidationParams(
                    protocol=key,
                    asset=None,
                    liquidation_threshold=ld.liquidation_threshold,
                    maintenance_margin=ld.maintenance_margin,
                    liquidation_penalty=ld.liquidation_penalty,
                    source=LiquidationParamSource.PROTOCOL_DEFAULT,
                )

        # Off-platform venues (no connector) — sanctioned residual table.
        # Any venue that gains a connector must move its row to the manifest.
        for key, params in OFF_PLATFORM_VENUE_DEFAULTS.items():
            self.protocol_defaults[key] = params

        # Initialize common asset-specific parameters for lending protocols
        self._initialize_asset_defaults()

    def _initialize_asset_defaults(self) -> None:
        """Initialize common asset-specific parameters from connector manifests.

        Asset keys are UPPER-cased to match the historical convention.
        The protocol key for asset rows equals the connector name directly
        (aave_v3, compound_v3, gmx_v2); every historical asset_params protocol
        key equals the connector name, so no alias translation is needed here.
        """
        # Lazy import avoids pulling the connector registry walk onto the hot path.
        from almanak.connectors._strategy_backtest_risk_registry import BACKTEST_RISK_REGISTRY

        for manifest_name, decl in BACKTEST_RISK_REGISTRY.liquidation_params().items():
            if not decl.liquidation_asset_params:
                continue
            for asset, (threshold, margin, penalty) in decl.liquidation_asset_params.items():
                # Asset keys are already UPPER-cased on the manifest (validated by BacktestRiskDecl).
                self.asset_params[(manifest_name, asset)] = LiquidationParams(
                    protocol=manifest_name,
                    asset=asset,
                    liquidation_threshold=threshold,
                    maintenance_margin=margin,
                    liquidation_penalty=penalty,
                    source=LiquidationParamSource.ASSET_SPECIFIC,
                )

    def get_params(
        self,
        protocol: str,
        asset: str | None = None,
    ) -> LiquidationParams:
        """Get liquidation parameters for a protocol/asset combination.

        Lookup priority:
            1. Asset-specific parameters (if asset provided)
            2. Protocol-level defaults
            3. Global defaults

        Args:
            protocol: Protocol name (e.g., "aave_v3", "gmx_v2")
            asset: Asset symbol (e.g., "ETH", "BTC") or None for protocol default

        Returns:
            LiquidationParams with source indicating where values came from

        Example:
            params = registry.get_params("aave_v3", "ETH")
            print(f"Threshold: {params.liquidation_threshold}")  # 0.86
            print(f"Source: {params.source}")  # ASSET_SPECIFIC
        """
        protocol_key = protocol.lower()
        asset_key = asset.upper() if asset else None

        # 1. Try asset-specific lookup
        if asset_key:
            key = (protocol_key, asset_key)
            if key in self.asset_params:
                logger.debug(
                    f"Using asset-specific params for {protocol}/{asset}: "
                    f"threshold={self.asset_params[key].liquidation_threshold}"
                )
                return self.asset_params[key]

        # 2. Try protocol-level default
        if protocol_key in self.protocol_defaults:
            params = self.protocol_defaults[protocol_key]
            logger.debug(
                f"Using protocol default for {protocol}: "
                f"threshold={params.liquidation_threshold}, margin={params.maintenance_margin}"
            )
            return params

        # 3. Return global default
        logger.warning(f"No liquidation params found for {protocol}/{asset}, using global defaults")
        return LiquidationParams(
            protocol=protocol,
            asset=asset,
            liquidation_threshold=self.global_default_threshold,
            maintenance_margin=self.global_default_margin,
            liquidation_penalty=self.global_default_penalty,
            source=LiquidationParamSource.GLOBAL_DEFAULT,
        )

    def get_liquidation_threshold(
        self,
        protocol: str,
        asset: str | None = None,
    ) -> Decimal:
        """Get liquidation threshold for a protocol/asset (convenience method).

        Args:
            protocol: Protocol name
            asset: Asset symbol or None

        Returns:
            Liquidation threshold as Decimal
        """
        return self.get_params(protocol, asset).liquidation_threshold

    def get_maintenance_margin(
        self,
        protocol: str,
        asset: str | None = None,
    ) -> Decimal:
        """Get maintenance margin for a protocol/asset (convenience method).

        Args:
            protocol: Protocol name
            asset: Asset symbol or None

        Returns:
            Maintenance margin as Decimal
        """
        return self.get_params(protocol, asset).maintenance_margin

    def get_liquidation_penalty(
        self,
        protocol: str,
        asset: str | None = None,
    ) -> Decimal:
        """Get liquidation penalty for a protocol/asset (convenience method).

        Args:
            protocol: Protocol name
            asset: Asset symbol or None

        Returns:
            Liquidation penalty as Decimal
        """
        return self.get_params(protocol, asset).liquidation_penalty

    def register_asset_params(
        self,
        protocol: str,
        asset: str,
        liquidation_threshold: Decimal | None = None,
        maintenance_margin: Decimal | None = None,
        liquidation_penalty: Decimal | None = None,
        source: LiquidationParamSource = LiquidationParamSource.ASSET_SPECIFIC,
        source_timestamp: str | None = None,
    ) -> LiquidationParams:
        """Register asset-specific liquidation parameters.

        Use this to add custom parameters for assets not in the defaults,
        or to override defaults with more accurate values (e.g., from on-chain data).

        Args:
            protocol: Protocol name
            asset: Asset symbol
            liquidation_threshold: Liquidation threshold (uses existing or default if None)
            maintenance_margin: Maintenance margin (uses existing or default if None)
            liquidation_penalty: Liquidation penalty (uses existing or default if None)
            source: Source of these parameters
            source_timestamp: When parameters were fetched (for historical)

        Returns:
            The registered LiquidationParams

        Example:
            # Register on-chain fetched params
            registry.register_asset_params(
                protocol="aave_v3",
                asset="NEW_TOKEN",
                liquidation_threshold=Decimal("0.70"),
                maintenance_margin=Decimal("0"),
                liquidation_penalty=Decimal("0.10"),
                source=LiquidationParamSource.HISTORICAL,
                source_timestamp="2024-01-15T00:00:00Z",
            )
        """
        protocol_key = protocol.lower()
        asset_key = asset.upper()
        key = (protocol_key, asset_key)

        # Get existing or default values for any unspecified params
        existing = self.get_params(protocol_key, asset_key)

        params = LiquidationParams(
            protocol=protocol_key,
            asset=asset_key,
            liquidation_threshold=liquidation_threshold
            if liquidation_threshold is not None
            else existing.liquidation_threshold,
            maintenance_margin=maintenance_margin if maintenance_margin is not None else existing.maintenance_margin,
            liquidation_penalty=liquidation_penalty
            if liquidation_penalty is not None
            else existing.liquidation_penalty,
            source=source,
            source_timestamp=source_timestamp,
        )

        self.asset_params[key] = params
        logger.info(
            f"Registered asset params for {protocol}/{asset}: "
            f"threshold={params.liquidation_threshold}, margin={params.maintenance_margin}, "
            f"source={source.value}"
        )
        return params

    def register_protocol_default(
        self,
        protocol: str,
        liquidation_threshold: Decimal | None = None,
        maintenance_margin: Decimal | None = None,
        liquidation_penalty: Decimal | None = None,
    ) -> LiquidationParams:
        """Register or update protocol-level default parameters.

        Args:
            protocol: Protocol name
            liquidation_threshold: Default liquidation threshold
            maintenance_margin: Default maintenance margin
            liquidation_penalty: Default liquidation penalty

        Returns:
            The registered LiquidationParams
        """
        protocol_key = protocol.lower()

        # Get existing or global defaults
        existing = self.protocol_defaults.get(protocol_key)
        if existing:
            threshold = liquidation_threshold if liquidation_threshold is not None else existing.liquidation_threshold
            margin = maintenance_margin if maintenance_margin is not None else existing.maintenance_margin
            penalty = liquidation_penalty if liquidation_penalty is not None else existing.liquidation_penalty
        else:
            threshold = liquidation_threshold if liquidation_threshold is not None else self.global_default_threshold
            margin = maintenance_margin if maintenance_margin is not None else self.global_default_margin
            penalty = liquidation_penalty if liquidation_penalty is not None else self.global_default_penalty

        params = LiquidationParams(
            protocol=protocol_key,
            asset=None,
            liquidation_threshold=threshold,
            maintenance_margin=margin,
            liquidation_penalty=penalty,
            source=LiquidationParamSource.PROTOCOL_DEFAULT,
        )

        self.protocol_defaults[protocol_key] = params
        return params

    def get_all_registered_assets(self, protocol: str) -> list[str]:
        """Get list of all assets with registered parameters for a protocol.

        Args:
            protocol: Protocol name

        Returns:
            List of asset symbols with registered parameters
        """
        protocol_key = protocol.lower()
        return [asset for (proto, asset) in self.asset_params if proto == protocol_key]

    def to_dict(self) -> dict[str, Any]:
        """Serialize registry to dictionary."""
        return {
            "protocol_defaults": {k: v.to_dict() for k, v in self.protocol_defaults.items()},
            "asset_params": {f"{k[0]}:{k[1]}": v.to_dict() for k, v in self.asset_params.items()},
            "global_default_threshold": str(self.global_default_threshold),
            "global_default_margin": str(self.global_default_margin),
            "global_default_penalty": str(self.global_default_penalty),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LiquidationParamRegistry":
        """Deserialize registry from dictionary."""
        registry = cls(
            protocol_defaults={},
            asset_params={},
            global_default_threshold=Decimal(str(data.get("global_default_threshold", "0.825"))),
            global_default_margin=Decimal(str(data.get("global_default_margin", "0.05"))),
            global_default_penalty=Decimal(str(data.get("global_default_penalty", "0.05"))),
        )

        # Load protocol defaults
        for protocol, params_dict in data.get("protocol_defaults", {}).items():
            registry.protocol_defaults[protocol] = LiquidationParams.from_dict(params_dict)

        # Load asset params
        for key_str, params_dict in data.get("asset_params", {}).items():
            protocol, asset = key_str.split(":", 1)
            registry.asset_params[(protocol, asset)] = LiquidationParams.from_dict(params_dict)

        return registry


__all__ = [
    "LiquidationParamRegistry",
    "LiquidationParams",
    "LiquidationParamSource",
    "OFF_PLATFORM_VENUE_DEFAULTS",
]
