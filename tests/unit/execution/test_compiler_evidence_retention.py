"""Compiler evidence is retained verbatim, separately from execution attestations."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from almanak.connectors.uniswap_v4.connector import CONNECTOR
from almanak.framework.execution.orchestrator import ExecutionPhase, ExecutionResult
from almanak.framework.execution.result_enricher import ResultEnricher
from almanak.framework.observability.ledger import deserialize_extracted_data, serialize_extracted_data


def enrich(protocol, metadata, result=None):
    result = result or SimpleNamespace(success=True, extracted_data={}, transaction_results=[], extraction_warnings=[])
    return ResultEnricher().enrich(
        result,
        SimpleNamespace(intent_type="SWAP", protocol=protocol),
        SimpleNamespace(chain="base", protocol=protocol),
        bundle_metadata=metadata,
    )


def artifact():
    return {
        "schema_version": 1,
        "pool_key": {"fee": 31100, "hooks": "0x" + "00" * 20},
        "block": {"number": 123, "hash": "0x" + "11" * 32},
        "quote": {"amount_out": "200", "lp_fee": 31100},
        "transaction_digests": ["0x" + "22" * 32],
    }


def test_v4_compiler_artifact_is_deeply_copied_and_serializes_exactly():
    original = artifact()
    result = enrich("uniswap_v4", {"v4_operation": original, "unrelated": "not retained"})
    assert result.extracted_data == {"compiler_evidence": {"v4_operation": original}}
    retained = result.extracted_data["compiler_evidence"]["v4_operation"]
    original["pool_key"]["fee"] = 500
    original["transaction_digests"].append("changed")
    assert retained == artifact()
    assert deserialize_extracted_data(serialize_extracted_data(result.extracted_data)) == result.extracted_data


@pytest.mark.parametrize("success", [True, False])
@pytest.mark.parametrize("status", ["passed", "skipped", "refused"])
def test_v4_price_impact_decision_survives_execution_enrichment_and_ledger_roundtrip(success, status):
    decision = {
        "schema_version": 1,
        "status": status,
        "amount_in_raw": str(2**128 + 1),
        "quote_amount_raw": str(2**128 - 1),
        "pool_id": "0x" + "ab" * 32,
        "chain": "robinhood",
        "quote_block": 58956617,
    }
    original = dict(decision)
    result = ExecutionResult(success=success, phase=ExecutionPhase.CONFIRMATION)
    enrich("uniswap_v4", {"price_impact_check": decision}, result)
    decision["status"] = "changed"
    restored = deserialize_extracted_data(serialize_extracted_data(result.extracted_data))
    assert restored["compiler_evidence"]["price_impact_check"] == original
    enrich("uniswap_v4", {"price_impact_check": original}, result)
    with pytest.raises(ValueError, match="Compiler evidence changed"):
        enrich("uniswap_v4", {"price_impact_check": decision}, result)


@pytest.mark.parametrize(
    "protocol,metadata",
    [
        ("uniswap_v3", {"v4_operation": artifact()}),
        ("uniswap_v4", {}),
        ("uniswap_v4", {"unrelated": artifact()}),
        ("unknown_protocol", {"v4_operation": artifact()}),
    ],
)
def test_absent_or_undeclared_evidence_is_not_invented(protocol, metadata):
    assert "compiler_evidence" not in enrich(protocol, metadata).extracted_data


def test_repeated_enrichment_is_idempotent_but_replacement_is_refused():
    result = enrich("uniswap_v4", {"v4_operation": artifact()})
    enrich("uniswap_v4", {"v4_operation": artifact()}, result)
    replacement = artifact()
    replacement["quote"]["amount_out"] = "201"
    with pytest.raises(ValueError, match="Compiler evidence changed"):
        enrich("uniswap_v4", {"v4_operation": replacement}, result)


@pytest.mark.parametrize("protocol", ["uniswap_v4", "curve"])
def test_reverted_attempt_retains_evidence_without_fill_parser_or_lookups(protocol):
    registry = Mock()
    registry.get.side_effect = AssertionError("A reverted attempt must not request a fill parser")
    key_lookup, meta_lookup = Mock(), Mock()
    enricher = ResultEnricher(parser_registry=registry, pool_key_lookup=key_lookup, pool_meta_lookup=meta_lookup)
    result = ExecutionResult(success=False, phase=ExecutionPhase.CONFIRMATION)
    actual = enricher.enrich(
        result,
        SimpleNamespace(intent_type="LP_CLOSE", protocol=protocol),
        SimpleNamespace(chain="base", protocol=protocol),
        bundle_metadata={"v4_operation": artifact()},
    )
    assert actual is result and not actual.success
    assert result.extracted_data == (
        {"compiler_evidence": {"v4_operation": artifact()}} if protocol == "uniswap_v4" else {}
    )
    registry.get.assert_not_called()
    key_lookup.assert_not_called()
    meta_lookup.assert_not_called()


@pytest.mark.parametrize("keys", [["key"], ("",), ("key", "key"), None])
def test_manifest_refuses_invalid_evidence_declarations(keys):
    with pytest.raises(ValueError, match="execution_evidence_keys"):
        replace(CONNECTOR, execution_evidence_keys=keys)
