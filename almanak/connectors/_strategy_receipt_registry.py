"""Strategy-side receipt-parser connector registration site (VIB-4854 / W2).

Sibling of :mod:`almanak.connectors._gateway_registry`, scoped to the
receipt-parser concern.

Lives one level up from ``_strategy_base/`` because it imports every
connector's ``receipt_parser_provider`` module — and ``_strategy_base/``
must stay protocol-clean (no concrete connector imports). Adding a new
strategy-side connector with a receipt parser means one import + one
``STRATEGY_RECEIPT_PARSER_REGISTRY.register`` line below.

Each provider is imported from
``almanak.connectors.<protocol>.receipt_parser_provider``::

    from almanak.connectors.<protocol>.receipt_parser_provider import (
        <Protocol>ReceiptParserConnector,
    )
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(<Protocol>ReceiptParserConnector())

The completeness invariant — every connector that ships a
``receipt_parser.py`` file MUST register here — is enforced statically
by ``tests/unit/core/test_receipt_parser_registry_completeness.py``.

Why a strategy-side registry (vs. reading from ``GATEWAY_REGISTRY``)
====================================================================

Receipt parsing runs inside the strategy container — the framework's
``ResultEnricher`` and migration backfill construct parser instances
at runtime and feed them already-fetched transaction receipts.
Strategy-side modules are forbidden from importing the gateway-side
registry (``almanak.connectors._gateway_registry``) per
``tests/static/test_strategy_import_boundary.py``, so the receipt
parser dispatch cannot consume ``GATEWAY_REGISTRY``. This file is the
strategy-side mirror.

This file is allow-listed in the strategy-side import boundary scan
(``_STRATEGY_SCAN_SKIP_PARTS`` in
``tests/static/test_strategy_import_boundary.py``) the same way
``_gateway_registry.py`` is allow-listed on the gateway side: it is
the boot-time discovery entry point that legitimately knows every
connector by name.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.receipt_parser_registry import (
    STRATEGY_RECEIPT_PARSER_REGISTRY,
)

__all__ = ["STRATEGY_RECEIPT_PARSER_REGISTRY"]


def _register_all() -> None:
    """Register every strategy-side receipt-parser connector.

    Imports are local to the function so that loading this module does
    not transitively import every parser module's class until the
    registry resolves a key (parser modules are themselves large and
    pull in connector-side dependencies — chain enums, ABI loaders, …
    — that we don't want loaded just to know "this connector exists").
    """
    # DEX / AMM
    # Lending
    from almanak.connectors.aave_v3.receipt_parser_provider import (
        AaveV3ReceiptParserConnector,
    )

    # Bridges
    from almanak.connectors.across.receipt_parser_provider import (
        AcrossReceiptParserConnector,
    )
    from almanak.connectors.aerodrome.receipt_parser_provider import (
        AerodromeReceiptParserConnector,
    )

    # Perps
    from almanak.connectors.aster_perps.receipt_parser_provider import (
        AsterPerpsReceiptParserConnector,
    )
    from almanak.connectors.benqi.receipt_parser_provider import (
        BenqiReceiptParserConnector,
    )
    from almanak.connectors.compound_v3.receipt_parser_provider import (
        CompoundV3ReceiptParserConnector,
    )
    from almanak.connectors.curvance.receipt_parser_provider import (
        CurvanceReceiptParserConnector,
    )
    from almanak.connectors.curve.receipt_parser_provider import (
        CurveReceiptParserConnector,
    )
    from almanak.connectors.drift.receipt_parser_provider import (
        DriftReceiptParserConnector,
    )

    # Aggregators / cross-chain swap
    from almanak.connectors.enso.receipt_parser_provider import (
        EnsoReceiptParserConnector,
    )

    # Staking / pegged-asset issuers
    from almanak.connectors.ethena.receipt_parser_provider import (
        EthenaReceiptParserConnector,
    )
    from almanak.connectors.euler_v2.receipt_parser_provider import (
        EulerV2ReceiptParserConnector,
    )
    from almanak.connectors.fluid.receipt_parser_provider import (
        FluidReceiptParserConnector,
    )
    from almanak.connectors.gimo.receipt_parser_provider import (
        GimoReceiptParserConnector,
    )
    from almanak.connectors.gmx_v2.receipt_parser_provider import (
        GmxV2ReceiptParserConnector,
    )
    from almanak.connectors.joelend.receipt_parser_provider import (
        JoeLendReceiptParserConnector,
    )

    # Solana — swap / lending / LP
    from almanak.connectors.jupiter.receipt_parser_provider import (
        JupiterReceiptParserConnector,
    )
    from almanak.connectors.jupiter_lend.receipt_parser_provider import (
        JupiterLendReceiptParserConnector,
    )
    from almanak.connectors.kamino.receipt_parser_provider import (
        KaminoReceiptParserConnector,
    )

    # Vaults
    from almanak.connectors.lagoon.receipt_parser_provider import (
        LagoonReceiptParserConnector,
    )
    from almanak.connectors.lido.receipt_parser_provider import (
        LidoReceiptParserConnector,
    )
    from almanak.connectors.lifi.receipt_parser_provider import (
        LiFiReceiptParserConnector,
    )
    from almanak.connectors.meteora.receipt_parser_provider import (
        MeteoraReceiptParserConnector,
    )
    from almanak.connectors.morpho_blue.receipt_parser_provider import (
        MorphoBlueReceiptParserConnector,
    )
    from almanak.connectors.morpho_vault.receipt_parser_provider import (
        MetaMorphoReceiptParserConnector,
    )
    from almanak.connectors.orca.receipt_parser_provider import (
        OrcaReceiptParserConnector,
    )
    from almanak.connectors.pancakeswap_perps.receipt_parser_provider import (
        PancakeSwapPerpsReceiptParserConnector,
    )
    from almanak.connectors.pancakeswap_v3.receipt_parser_provider import (
        PancakeSwapV3ReceiptParserConnector,
    )

    # Yield trading
    from almanak.connectors.pendle.receipt_parser_provider import (
        PendleReceiptParserConnector,
    )

    # Prediction markets
    from almanak.connectors.polymarket.receipt_parser_provider import (
        PolymarketReceiptParserConnector,
    )
    from almanak.connectors.raydium.receipt_parser_provider import (
        RaydiumReceiptParserConnector,
    )
    from almanak.connectors.silo_v2.receipt_parser_provider import (
        SiloV2ReceiptParserConnector,
    )
    from almanak.connectors.spark.receipt_parser_provider import (
        SparkReceiptParserConnector,
    )
    from almanak.connectors.stargate.receipt_parser_provider import (
        StargateReceiptParserConnector,
    )
    from almanak.connectors.sushiswap_v3.receipt_parser_provider import (
        SushiSwapV3ReceiptParserConnector,
    )
    from almanak.connectors.traderjoe_v2.receipt_parser_provider import (
        TraderJoeV2ReceiptParserConnector,
    )
    from almanak.connectors.uniswap_v3.receipt_parser_provider import (
        UniswapV3ReceiptParserConnector,
    )
    from almanak.connectors.uniswap_v4.receipt_parser_provider import (
        UniswapV4ReceiptParserConnector,
    )

    # DEX / AMM
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(UniswapV3ReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(UniswapV4ReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(PancakeSwapV3ReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(AerodromeReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(TraderJoeV2ReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(FluidReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(SushiSwapV3ReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(CurveReceiptParserConnector())

    # Lending
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(AaveV3ReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(SparkReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(MorphoBlueReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(CurvanceReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(CompoundV3ReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(BenqiReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(JoeLendReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(EulerV2ReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(SiloV2ReceiptParserConnector())

    # Perps
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(DriftReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(GmxV2ReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(AsterPerpsReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(PancakeSwapPerpsReceiptParserConnector())

    # Staking / pegged-asset issuers
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(LidoReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(EthenaReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(GimoReceiptParserConnector())

    # Aggregators / cross-chain swap
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(EnsoReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(LiFiReceiptParserConnector())

    # Bridges
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(AcrossReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(StargateReceiptParserConnector())

    # Yield trading
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(PendleReceiptParserConnector())

    # Prediction markets
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(PolymarketReceiptParserConnector())

    # Vaults
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(LagoonReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(MetaMorphoReceiptParserConnector())

    # Solana — swap / lending / LP
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(JupiterReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(JupiterLendReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(KaminoReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(RaydiumReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(MeteoraReceiptParserConnector())
    STRATEGY_RECEIPT_PARSER_REGISTRY.register(OrcaReceiptParserConnector())


_register_all()
