"""Strategy-runner hooks for the Fluid vault (NFT-CDP) connector (VIB-5031).

One hook: best-effort post-receipt result enrichment that stamps the
``FluidVaultOperateData`` dict (``nft_id`` / ``vault`` / ``col_delta`` /
``debt_delta``, string-encoded ints) into ``result.extracted_data`` BEFORE
the runner serializes it into ``transaction_ledger.extracted_data_json``
(the ``_maybe_enrich_result_with_runner_hooks`` seam, uniswap_v3 slot0
precedent). This is the ONLY persisted home of the nftId at v1 (ADR §6.3
r2): no Postgres DDL, no typed accounting-event field — dashboards/audit
SQL join ledger -> events on tx hash.

Capability-gated, not protocol-string-gated: the hook no-ops unless a
receipt in the result actually carries the byte-verified VaultT1
``LogOperate`` topic, so running it unconditionally for every protocol's
results is safe (the registry runs every registered enrichment hook).
Persisted nftIds are audit/dashboard data ONLY — execution always
re-resolves from chain (VIB-5010; ADR §1.4).
"""

from __future__ import annotations

import logging
from typing import Any, ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.runner_hook_registry import (
    RunnerHookConnector,
    RunnerResultEnrichmentCapability,
)

logger = logging.getLogger(__name__)

#: extracted_data key carrying the FluidVaultOperateData dict.
FLUID_VAULT_OPERATE_KEY = "fluid_vault_operate"


def _receipt_dicts(result: Any) -> list[dict[str, Any]]:
    """Receipts from successful transaction results, as plain dicts."""
    receipts: list[dict[str, Any]] = []
    for tx_result in getattr(result, "transaction_results", None) or []:
        if not getattr(tx_result, "success", False):
            continue
        receipt = getattr(tx_result, "receipt", None)
        if receipt is None:
            continue
        if isinstance(receipt, dict):
            receipts.append(receipt)
        elif hasattr(receipt, "to_dict"):
            receipts.append(receipt.to_dict())
    return receipts


class FluidVaultRunnerHookConnector(
    RunnerHookConnector,
    RunnerResultEnrichmentCapability,
):
    """Runner hooks for Fluid vault receipt-truth nftId/delta persistence."""

    protocol: ClassVar[ProtocolName] = ProtocolName("fluid_vault")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def enrich_result(self, result: Any, *, gateway_client: Any, chain: str, wallet_address: str = "") -> None:
        """Stamp ``FluidVaultOperateData`` onto ``result.extracted_data``.

        Best-effort (the registry already wraps hooks fail-open): never
        overwrites an existing value, never fabricates one — a receipt with
        no single attributable vault LogOperate stamps nothing (the parser's
        fail-closed contract).

        ``wallet_address`` is part of the VIB-5595 hook contract; the Fluid
        vault-operate stamp does not use it.
        """
        _ = wallet_address
        extracted = getattr(result, "extracted_data", None)
        if not isinstance(extracted, dict) or FLUID_VAULT_OPERATE_KEY in extracted:
            return

        from almanak.connectors._fluid_core.receipt_parser import (
            VAULT_LOG_OPERATE_TOPIC,
            FluidVaultReceiptParser,
        )

        receipts = _receipt_dicts(result)
        if not receipts:
            return

        def _has_vault_topic(receipt: dict[str, Any]) -> bool:
            logs = receipt.get("logs")
            if not isinstance(logs, list):
                return False  # malformed receipt — never raise from a best-effort hook
            for log in logs:
                if not isinstance(log, dict):
                    continue
                topics = log.get("topics")
                if (
                    isinstance(topics, list | tuple)
                    and topics
                    and FluidVaultReceiptParser._normalize_topic(topics[0]) == VAULT_LOG_OPERATE_TOPIC
                ):
                    return True
            return False

        vault_receipts = [receipt for receipt in receipts if _has_vault_topic(receipt)]
        if len(vault_receipts) != 1:
            if len(vault_receipts) > 1:
                logger.warning(
                    "Fluid vault runner hook: %d receipts carry vault LogOperate — ambiguous, stamping nothing",
                    len(vault_receipts),
                )
            return

        parser = FluidVaultReceiptParser(chain=chain)
        try:
            data = parser.extract_lending_data(vault_receipts[0])
        except Exception:
            # Malformed log shapes must never break the commit pipeline — the
            # registry wraps hooks fail-open, but this hook also honours that
            # contract directly: no raise, no stamp.
            logger.warning("Fluid vault runner hook: receipt parse failed — stamping nothing", exc_info=True)
            return
        if data is None:
            return
        extracted[FLUID_VAULT_OPERATE_KEY] = data
        # The LP-tokenId precedent: surface the nftId under the generic
        # position_id key too when nothing else claimed it. The parser OMITS
        # nft_id when the receipt could not resolve a real id (0 is the mint
        # sentinel, never a position id) — never fabricate one here.
        nft_id = data.get("nft_id")
        if nft_id is not None:
            extracted.setdefault("nft_id", nft_id)
        logger.debug("Fluid vault runner hook stamped operate data: %s", data)


__all__ = ["FLUID_VAULT_OPERATE_KEY", "FluidVaultRunnerHookConnector"]
