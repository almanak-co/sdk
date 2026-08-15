"""Aave V3 connector manifest."""

from __future__ import annotations

from datetime import date

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    BacktestStrategyTypeDecl,
    Connector,
    FeeModelDecl,
    ImportRef,
    LendingReadDecl,
    LifecycleObligationDecl,
    MetadataAmountEncoding,
    SupportedChainsSpec,
)
from almanak.connectors._lifecycle_declaration_bundle import LifecycleClaimCell, LifecycleDeclarationBundle
from almanak.connectors._strategy_base.address_table import AddressTableSpec
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec
from almanak.connectors.aave_v3.backtest_risk import BACKTEST_RISK as _BACKTEST_RISK
from almanak.core.capability_obligations import (
    EvidenceKind,
    EvidenceRef,
    ObligationDeclaration,
    ObligationDisposition,
    ObligationId,
    Satisfied,
    Unsupported,
)
from almanak.core.chains.arbitrum import DESCRIPTOR as ARBITRUM
from almanak.core.chains.avalanche import DESCRIPTOR as AVALANCHE
from almanak.core.chains.base import DESCRIPTOR as BASE
from almanak.core.chains.bsc import DESCRIPTOR as BSC
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM
from almanak.core.chains.linea import DESCRIPTOR as LINEA
from almanak.core.chains.mantle import DESCRIPTOR as MANTLE
from almanak.core.chains.optimism import DESCRIPTOR as OPTIMISM
from almanak.core.chains.polygon import DESCRIPTOR as POLYGON
from almanak.core.chains.xlayer import DESCRIPTOR as XLAYER
from almanak.core.intent_types import IntentType

_SUPPORTED_CHAINS = (
    ETHEREUM,
    ARBITRUM,
    OPTIMISM,
    POLYGON,
    BASE,
    AVALANCHE,
    BSC,
    MANTLE,
    XLAYER,
    LINEA,
)
_BORROW_CHAINS = tuple(chain for chain in _SUPPORTED_CHAINS if chain is not MANTLE)

_PRODUCTION_CHAINS = (ARBITRUM, AVALANCHE, BASE, BSC, ETHEREUM, OPTIMISM, POLYGON)
_TEST_CHAIN_PATHS = {chain.name: ("bnb" if chain is BSC else chain.name) for chain in _PRODUCTION_CHAINS}
_PRODUCTION_INTENTS = tuple(
    sorted(
        (IntentType.SUPPLY, IntentType.WITHDRAW, IntentType.BORROW, IntentType.REPAY),
        key=lambda intent: intent.value,
    )
)
_PROVIDER_REFS = {
    ObligationId.ASSET_RESOLUTION: "almanak.connectors.aave_v3.compiler:AaveV3Compiler",
    ObligationId.VENUE_RESOLUTION: "almanak.connectors.aave_v3.adapter:AaveV3Adapter",
    ObligationId.AMOUNT_PROTECTION: "almanak.connectors.aave_v3.adapter:AaveV3Adapter",
    ObligationId.COMPILER: "almanak.connectors.aave_v3.compiler:AaveV3Compiler",
    ObligationId.RECEIPT_EVIDENCE: "almanak.connectors.aave_v3.receipt_parser:AaveV3ReceiptParser",
    ObligationId.PERMISSION_PLAN: "almanak.connectors.aave_v3.permission_hints:PERMISSION_HINTS",
}


def _core_dispositions(
    evidence: tuple[EvidenceRef, ...],
    *,
    receipt_evidence_supported: bool,
) -> tuple[ObligationDeclaration, ...]:
    dispositions = []
    for obligation in (
        ObligationId.ASSET_RESOLUTION,
        ObligationId.VENUE_RESOLUTION,
        ObligationId.AMOUNT_PROTECTION,
        ObligationId.COMPILER,
        ObligationId.RECEIPT_EVIDENCE,
        ObligationId.MONEY_LEGS,
        ObligationId.PERMISSION_PLAN,
    ):
        disposition: ObligationDisposition
        if obligation is ObligationId.MONEY_LEGS:
            disposition = Unsupported(
                reason="Aave V3 does not emit canonical typed PrimitiveMoneyLegs for lending receipts.",
                tracking_ref="VIB-6660",
                owner="SDK Accounting",
                review_by=date(2026, 10, 15),
            )
        elif obligation is ObligationId.RECEIPT_EVIDENCE and not receipt_evidence_supported:
            disposition = Unsupported(
                reason="The exact real-fork path lacks an intent-specific receipt parser assertion.",
                tracking_ref="VIB-6661",
                owner="SDK Capability Audit",
                review_by=date(2026, 10, 15),
            )
        else:
            disposition = Satisfied(
                provider_ref=_PROVIDER_REFS[obligation],
                contract_version="aave_v3.core_execution.v1",
                test_evidence=evidence,
            )
        dispositions.append(ObligationDeclaration(obligation, disposition))
    return tuple(dispositions)


def _production_lifecycle_declarations():
    declarations: list[LifecycleObligationDecl] = []
    for chain in _PRODUCTION_CHAINS:
        evidence = (
            EvidenceRef(
                EvidenceKind.REAL_FORK,
                f"tests/intents/{_TEST_CHAIN_PATHS[chain.name]}/test_aave_v3_lending.py",
            ),
        )
        receipt_gaps = {
            "avalanche": frozenset({IntentType.WITHDRAW, IntentType.REPAY}),
            "polygon": frozenset({IntentType.REPAY}),
        }.get(chain.name, frozenset())
        for receipt_evidence_supported, intents in (
            (True, tuple(intent for intent in _PRODUCTION_INTENTS if intent not in receipt_gaps)),
            (False, tuple(intent for intent in _PRODUCTION_INTENTS if intent in receipt_gaps)),
        ):
            if not intents:
                continue
            suffix = "core_execution" if receipt_evidence_supported else "receipt_gap"
            cells = tuple(LifecycleClaimCell(protocol="aave_v3", chain=chain, intent=intent) for intent in intents)
            declarations.extend(
                LifecycleDeclarationBundle(
                    bundle_id=f"aave_v3.{chain.name}.{suffix}",
                    cells=cells,
                    declarations=_core_dispositions(
                        evidence,
                        receipt_evidence_supported=receipt_evidence_supported,
                    ),
                    source_ref=f"Connector.lifecycle_declarations[aave_v3.{chain.name}.{suffix}]",
                    source_detail="Exact-chain lending evidence with explicit parser and typed money-leg gaps.",
                ).expand()
            )
    return tuple(declarations)


CONNECTOR = Connector(
    name="aave_v3",
    kind=ProtocolKind.LENDING,
    external_ids={"defillama": "aave-v3"},
    teardown_post_condition=ImportRef(
        module="almanak.connectors.aave_v3.teardown_post_condition",
        attribute="aave_v3_teardown_post_condition",
    ),
    fee_model=FeeModelDecl(
        model=ImportRef(module="almanak.connectors.aave_v3.fee_model", attribute="AaveV3FeeModel"),
        description="Aave V3 lending protocol fee model",
        aliases=("aave", "aave_v2"),
    ),
    backtest_strategy_type=BacktestStrategyTypeDecl(strategy_type="lending", aliases=("aave",)),
    address_tables=(
        AddressTableSpec(
            protocol="aave_v3",
            module="almanak.connectors.aave_v3.addresses",
            attribute="AAVE_V3",
        ),
        AddressTableSpec(
            protocol="aave_v3_tokens",
            module="almanak.connectors.aave_v3.addresses",
            attribute="AAVE_V3_TOKENS",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.aave_v3.gateway.provider",
        attribute="AaveV3GatewayConnector",
        order=2,
    ),
    gas_estimate_connector=ImportRef(
        module="almanak.connectors.aave_v3.gas_estimate_provider",
        attribute="AaveV3GasEstimateConnector",
    ),
    agent_read_connector=ImportRef(
        module="almanak.connectors.aave_v3.agent_read_provider",
        attribute="AaveV3AgentReadConnector",
        order=6,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.aave_v3.receipt_parser_provider",
        attribute="AaveV3ReceiptParserConnector",
    ),
    contract_monitoring=ImportRef(
        module="almanak.connectors.aave_v3.contract_monitoring",
        attribute="AAVE_V3_CONTRACT_MONITORING_SPECS",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.aave_v3.contract_roles",
        attribute="CONTRACT_ROLES",
        order=9,
    ),
    protocol_family=ImportRef(
        module="almanak.connectors.aave_v3.protocol_family",
        attribute="PROTOCOL_FAMILY",
    ),
    compiler=ImportRef(
        module="almanak.connectors.aave_v3.compiler",
        attribute="AaveV3Compiler",
    ),
    flash_loan_provider_name="aave",
    flash_loan_provider=ImportRef(
        module="almanak.connectors.aave_v3.flash_loan_provider",
        attribute="AaveFlashLoanProvider",
        order=1,
    ),
    flash_loan_builder=ImportRef(
        module="almanak.connectors.aave_v3.flash_loan",
        attribute="build_aave_flash_loan",
    ),
    flash_loan_synthetic_discovery=True,
    capabilities=CapabilitiesSpec(
        keys=("aave_v3",),
        module="almanak.connectors.aave_v3.capabilities",
    ),
    primitive=ImportRef(
        module="almanak.connectors.aave_v3.primitive",
        attribute="PRIMITIVE",
    ),
    # Aave-family reads (VIB-4929): whole-wallet account state; 'aave' alias is lending-scoped.
    lending_read=LendingReadDecl(
        # bsc included: position_health already worked there and
        # addresses.py ships the bsc pool + pool_data_provider.
        rate_history_chains=("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bsc"),
        backtest_default_supply_apy="0.03",
        backtest_default_borrow_apy="0.05",
        backtest_provider=ImportRef(
            module="almanak.connectors.aave_v3.backtest_apy",
            attribute="AaveV3APYProvider",
        ),
        spec=ImportRef(module="almanak.connectors.aave_v3.lending_read", attribute="LENDING_READ_SPEC"),
        account_state=ImportRef(module="almanak.connectors.aave_v3.lending_read", attribute="ACCOUNT_STATE_READ_SPEC"),
        aliases=("aave", "aavev3"),
    ),
    # Aave-family compilers ship lending metadata amounts wei-encoded (VIB-3747).
    metadata_amount_encoding=MetadataAmountEncoding(lending="wei"),
    # No yield_poke: AToken.balanceOf projects the liquidity index lazily, so
    # evm_increaseTime alone surfaces accrued interest on a persistent fork.
    # The old supply(USDC, 0, wallet, 0) poke reverted with InvalidAmount()
    # on every tick (VIB-2630 spike). If a storage-writing poke is ever
    # needed, use a 1-wei supply with prior approval.
    backtest_risk=_BACKTEST_RISK,
    # Strategy support declares ONLY the four executable lending intents.
    # FLASH_LOAN is intentionally absent (VIB-5916): the flash-loan lane
    # compiles but has no receiver/accounting execution support, so it must
    # not be advertised as strategy support. The flash-loan PROVIDER stays
    # registered/discoverable through the dedicated flash_loan_* descriptor
    # fields below (with_flash_loan / FLASH_LOAN_PROVIDER_REGISTRY), which are
    # keyed independently of strategy_intents — see
    # tests/unit/connectors/aave_v3/test_manifest_truthfulness_vib5916.py.
    strategy_intents=(IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY, IntentType.WITHDRAW),
    # linea added (VIB-5916) after Phase-0 live-reserve verification. Plasma
    # is deliberately NOT declared because its token catalogue is incomplete.
    # Sonic is deliberately NOT declared because Aave governance proposed a
    # whole-market wind-down on 2026-07-29 (ARFC 25401); its retained address
    # entries support historical-position exits, not new strategy activity.
    # The support matrix now DERIVES from (intents, chains) — no manual matrix
    # override — so the displayed lending row equals supported_chains exactly
    # and cannot outrun this declaration.
    supported_chains=SupportedChainsSpec(
        chains=_SUPPORTED_CHAINS,
        # Mantle omits BORROW because THIS CONNECTOR cannot borrow there — not
        # because the market cannot be borrowed against (ALM-3075).
        #
        # Aave V3 Mantle runs v3.2 "liquid eModes": governance zeroed base LTV
        # on all 10 reserves (VIB-6111) and moved the collateral power into six
        # eMode categories, measured live at block 98645311 with LTV 40-93%
        # ('WMNT__Stablecoins' 40%, 'sUSDe Stablecoins' 90%, 'wrsETH
        # Correlated' 93%, ...). The market is active — ~$78M outstanding
        # borrows at ~33% utilisation — so borrowing there is entirely
        # possible, via eMode.
        #
        # We cannot reach it: `adapter.set_user_emode()` exists but no compiler
        # or intent path calls it, so the user is never enrolled in a category
        # and a compiled borrow sees base LTV 0 -> zero borrowing power ->
        # revert. `EMODE_CATEGORIES` in adapter.py is also a hardcoded mainnet
        # map that cannot describe Mantle's categories.
        #
        # Do NOT read this as "Mantle is offboarded". Zeroed base LTV alongside
        # non-zero liquidation thresholds looks identical to a wind-down, and
        # that misreading is what ALM-3075 exists to correct. Restore mantle
        # here once BORROW compiles an eMode enrolment.
        intent_overrides={IntentType.BORROW: _BORROW_CHAINS},
    ),
    lifecycle_declarations=_production_lifecycle_declarations(),
)

__all__ = ["CONNECTOR"]
