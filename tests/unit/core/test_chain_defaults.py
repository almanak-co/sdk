"""Freeze the default-chain POLICY constants (VIB-4851 Phase E, CS-1).

CS-1 replaced ~140 scattered ``"arbitrum"`` / ``"base"`` default literals
with the three constants in ``almanak/core/chains/defaults.py``. The agent
tool surface among those call sites is SPEND-CONTROL (``AgentPolicy``
defaults, tool-schema chain defaults), so this module pins:

1. The constant values themselves — changing ``DEFAULT_CHAIN`` is a product
   decision and must fail a test, not slip through a refactor.
   ``LEGACY_SERIALIZED_CHAIN`` must NEVER change: it encodes the chain
   implied by serialized records written before the ``chain`` field
   existed (a fact about old data, not a preference).
2. The exact per-schema chain defaults in
   ``almanak.framework.agent_tools.schemas`` — frozen verbatim from the
   pre-CS-1 literals so the sweep (and any future edit) is provably
   behaviour-preserving. A new schema with a chain default must be added
   here deliberately.
3. ``AgentPolicy.allowed_chains`` — the default spend-control chain set.
"""

from __future__ import annotations

import inspect

from pydantic import BaseModel

from almanak.core.chains import (
    DEFAULT_CHAIN,
    DEFAULT_VAULT_CHAIN,
    LEGACY_SERIALIZED_CHAIN,
)

# Frozen verbatim — the pre-CS-1 literal default of every schema in
# almanak.framework.agent_tools.schemas that declares a string default for
# its ``chain`` field. Response models with ``chain: str = ""`` are
# included so the inventory is complete.
FROZEN_SCHEMA_CHAIN_DEFAULTS: dict[str, str] = {
    "ApproveVaultUnderlyingRequest": "base",
    "BatchGetBalancesRequest": "arbitrum",
    "BorrowLendingRequest": "arbitrum",
    "CheckProtocolSupportRequest": "",
    "CheckProtocolSupportResponse": "",
    "CloseLPPositionRequest": "arbitrum",
    "CompileIntentRequest": "arbitrum",
    "ComputeRebalanceCandidateRequest": "base",
    "DepositVaultRequest": "base",
    "EstimateGasRequest": "arbitrum",
    "ExecuteCompiledBundleRequest": "arbitrum",
    "GetBalanceRequest": "arbitrum",
    "GetIndicatorRequest": "arbitrum",
    "GetLPPositionRequest": "arbitrum",
    "GetPoolStateRequest": "arbitrum",
    "GetPortfolioRequest": "arbitrum",
    "GetPriceRequest": "arbitrum",
    "GetRiskMetricsRequest": "arbitrum",
    "GetVaultStateRequest": "base",
    "GetWalletOverviewRequest": "arbitrum",
    "GetWalletOverviewResponse": "",
    "ListLPPositionsRequest": "arbitrum",
    "ListLendingPositionsRequest": "arbitrum",
    "ListLendingReservesRequest": "arbitrum",
    "OpenLPPositionRequest": "arbitrum",
    "RepayLendingRequest": "arbitrum",
    "ResolveTokenRequest": "arbitrum",
    "SettleVaultRequest": "base",
    "SimulateIntentRequest": "arbitrum",
    "SupplyLendingRequest": "arbitrum",
    "SwapTokensRequest": "arbitrum",
    "TeardownVaultRequest": "base",
    "UnwrapNativeRequest": "arbitrum",
    "UnwrapNativeResponse": "",
    "ValidateRiskRequest": "arbitrum",
    "WithdrawLendingRequest": "arbitrum",
    "WrapNativeRequest": "arbitrum",
    "WrapNativeResponse": "",
}


class TestConstantValues:
    def test_default_chain(self) -> None:
        assert DEFAULT_CHAIN == "arbitrum"

    def test_legacy_serialized_chain_is_frozen_history(self) -> None:
        # This value encodes what serialized records without a ``chain``
        # field meant when they were written. It must never change, even
        # if DEFAULT_CHAIN someday does.
        assert LEGACY_SERIALIZED_CHAIN == "arbitrum"

    def test_default_vault_chain(self) -> None:
        assert DEFAULT_VAULT_CHAIN == "base"

    def test_constants_are_registered_canonical_names(self) -> None:
        from almanak.core.chains import ChainRegistry

        names = {d.name for d in ChainRegistry.all()}
        assert {DEFAULT_CHAIN, LEGACY_SERIALIZED_CHAIN, DEFAULT_VAULT_CHAIN} <= names


class TestAgentToolSpendControlDefaults:
    """Byte-equality of the LLM-facing spend-control defaults."""

    def _live_defaults(self) -> dict[str, str]:
        import almanak.framework.agent_tools.schemas as schemas

        live: dict[str, str] = {}
        for name, obj in vars(schemas).items():
            if (
                inspect.isclass(obj)
                and issubclass(obj, BaseModel)
                and obj.__module__ == schemas.__name__
            ):
                field = obj.model_fields.get("chain")
                if field is not None and isinstance(field.default, str):
                    live[name] = field.default
        return live

    def test_schema_chain_defaults_frozen(self) -> None:
        live = self._live_defaults()
        assert live == FROZEN_SCHEMA_CHAIN_DEFAULTS, (
            "Agent-tool schema chain defaults drifted from the frozen "
            "inventory. If this is a deliberate product decision, update "
            "FROZEN_SCHEMA_CHAIN_DEFAULTS with reviewer sign-off; these "
            "defaults are spend-control surface."
        )

    def test_agent_policy_allowed_chains_default(self) -> None:
        from almanak.framework.agent_tools.policy import AgentPolicy

        assert AgentPolicy().allowed_chains == {"arbitrum"}
