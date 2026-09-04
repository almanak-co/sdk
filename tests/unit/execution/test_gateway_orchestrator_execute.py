import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.execution.gateway_orchestrator import (
    GatewayExecutionOrchestrator,
    _encode_action_bundle,
    _execution_error_result,
    _execution_result_from_proto,
    _resolve_execution_options,
    _submission_transactions_from_proto,
)
from almanak.framework.execution.submission import ReplayPolicy, SubmissionProvenance, TransactionRole
from almanak.gateway.proto import gateway_pb2

WALLET = "0x" + "ab" * 20
EVM_HASH = "12" * 32
SOLANA_SIGNATURE = "5VERv8NMHKRYsGeYfVb9oKzvoHvU9vE3yo9Xq2Gj8j3B8VeqiZLzQQDCbPVmXNgTjEFGdYkhNmj1PYqC7GsQzXvA"


def _orchestrator(*, chain: str = "arbitrum", wallet_address: str | None = WALLET):
    return GatewayExecutionOrchestrator(MagicMock(), chain=chain, wallet_address=wallet_address)


def _response(**overrides):
    values = {
        "success": False,
        "tx_hashes": [],
        "total_gas_used": 0,
        "receipts": b"",
        "execution_id": "execution-1",
        "error": "",
        "error_code": "",
        "submission_provenance": gateway_pb2.SUBMISSION_PROVENANCE_UNSPECIFIED,
        "execution_plan_hash": "",
        "submission_transactions": [],
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _request_options(**overrides):
    values = {
        "context": None,
        "deployment_id": "deployment:explicit",
        "intent_id": "intent-explicit",
        "dry_run": False,
        "simulation_enabled": True,
        "wallet_address": None,
        "default_wallet_address": WALLET,
    }
    values.update(overrides)
    return _resolve_execution_options(**values)


def test_resolve_execution_options_uses_explicit_values_and_default_wallet():
    options = _request_options()

    assert options.deployment_id == "deployment:explicit"
    assert options.intent_id == "intent-explicit"
    assert options.dry_run is False
    assert options.simulation_enabled is True
    assert options.wallet_address == WALLET


def test_resolve_execution_options_preserves_context_precedence_pending_alm_3523():
    context = SimpleNamespace(
        deployment_id="deployment:context",
        intent_id="",
        dry_run=True,
        simulation_enabled=False,
        wallet_address="",
    )

    options = _request_options(
        context=context,
        intent_id="intent-explicit",
        dry_run=False,
        simulation_enabled=True,
        wallet_address="0xexplicit",
    )

    assert options.deployment_id == "deployment:context"
    assert options.intent_id == "intent-explicit"
    assert options.dry_run is True
    assert options.simulation_enabled is False
    assert options.wallet_address == "0xexplicit"


def test_resolve_execution_options_requires_wallet_with_exact_error():
    with pytest.raises(ValueError, match="^wallet_address is required$"):
        _request_options(default_wallet_address=None)


class _DictBundle:
    def __init__(self):
        self.serialized = {"transactions": [{"data": "first"}, {"data": "second"}]}
        self.sensitive_data = {"mint_secret": "secret"}

    def to_dict(self):
        return self.serialized


class _ModelBundle:
    sensitive_data = None

    def model_dump(self):
        return {"transactions": [{"data": "model"}]}


def test_encode_action_bundle_includes_sensitive_data_without_mutating_public_dict():
    bundle = _DictBundle()

    payload, plan_hash = _encode_action_bundle(bundle)

    decoded = json.loads(payload)
    assert [item["data"] for item in decoded["transactions"]] == ["first", "second"]
    assert decoded["_sensitive_data"] == bundle.sensitive_data
    assert bundle.serialized == {"transactions": [{"data": "first"}, {"data": "second"}]}
    assert len(plan_hash) == 64


@pytest.mark.parametrize(
    ("bundle", "expected_data"),
    [
        (_ModelBundle(), "model"),
        ({"transactions": [{"data": "mapping"}]}, "mapping"),
    ],
)
def test_encode_action_bundle_supports_model_and_mapping_shapes(bundle, expected_data):
    payload, plan_hash = _encode_action_bundle(bundle)

    assert json.loads(payload)["transactions"][0]["data"] == expected_data
    assert len(plan_hash) == 64


def test_build_execute_request_preserves_all_wire_fields():
    orchestrator = GatewayExecutionOrchestrator(
        MagicMock(),
        chain="base",
        wallet_address=WALLET,
        max_gas_price_gwei=42,
    )
    options = _request_options(
        deployment_id="deployment:1",
        intent_id="intent-1",
        dry_run=True,
        simulation_enabled=False,
    )

    request = orchestrator._build_execute_request(b'{"transactions":[]}', options)

    assert request.action_bundle == b'{"transactions":[]}'
    assert request.dry_run is True
    assert request.simulation_enabled is False
    assert request.deployment_id == "deployment:1"
    assert request.intent_id == "intent-1"
    assert request.chain == "base"
    assert request.wallet_address == WALLET
    assert request.max_gas_price_gwei == 42


def test_dispatch_execute_uses_execute_timeout_and_returns_identity():
    orchestrator = _orchestrator()
    orchestrator._execute_timeout = 321
    response = object()
    orchestrator._client.execution.Execute.return_value = response
    request = gateway_pb2.ExecuteRequest()

    assert orchestrator._dispatch_execute(request) is response
    orchestrator._client.execution.Execute.assert_called_once_with(request, timeout=321)


def test_submission_transactions_decode_all_role_and_replay_branches():
    response = _response(
        submission_transactions=[
            SimpleNamespace(
                tx_id="approval",
                role=gateway_pb2.EXECUTION_TRANSACTION_ROLE_SETUP_APPROVAL,
                replay_policy=gateway_pb2.REPLAY_POLICY_RECOMPILE_ONLY,
            ),
            SimpleNamespace(
                tx_id="action",
                role=gateway_pb2.EXECUTION_TRANSACTION_ROLE_ACTION,
                replay_policy=gateway_pb2.REPLAY_POLICY_NEVER,
            ),
            SimpleNamespace(tx_id="unknown", role=999, replay_policy=999),
        ]
    )

    evidence = _submission_transactions_from_proto(response, plan_hash_matches=True)

    assert [(item.role, item.replay_policy) for item in evidence] == [
        (TransactionRole.SETUP_APPROVAL, ReplayPolicy.RECOMPILE_ONLY),
        (TransactionRole.ACTION, ReplayPolicy.NEVER),
        (TransactionRole.UNKNOWN, ReplayPolicy.NEVER),
    ]
    assert _submission_transactions_from_proto(response, plan_hash_matches=False) == []


def test_execution_result_decodes_evm_response_and_matching_plan_evidence():
    plan_hash = "a" * 64
    receipts = [{"tx_hash": f"0x{EVM_HASH}", "status": 0}]
    response = _response(
        tx_hashes=[EVM_HASH],
        total_gas_used=21_000,
        receipts=json.dumps(receipts).encode(),
        error="reverted",
        error_code="REVERTED",
        extraction_warnings=["warning"],
        submission_provenance=gateway_pb2.SUBMISSION_PROVENANCE_ATTEMPTED,
        execution_plan_hash=plan_hash,
        submission_transactions=[
            SimpleNamespace(
                tx_id=f"0x{EVM_HASH}",
                role=gateway_pb2.EXECUTION_TRANSACTION_ROLE_ACTION,
                replay_policy=gateway_pb2.REPLAY_POLICY_NEVER,
            )
        ],
    )

    result = _execution_result_from_proto(response, chain="arbitrum", expected_plan_hash=plan_hash)

    assert result.tx_hashes == [f"0x{EVM_HASH}"]
    assert result.receipts == receipts
    assert result.total_gas_used == 21_000
    assert result.execution_id == "execution-1"
    assert result.chain_family == "EVM"
    assert result.error == "reverted"
    assert result.error_code == "REVERTED"
    assert result.extraction_warnings == ["warning"]
    assert result.submission_provenance is SubmissionProvenance.ATTEMPTED
    assert result.execution_plan_hash == plan_hash
    assert result.submission_transactions[0].role is TransactionRole.ACTION


def test_execution_result_preserves_solana_signature_and_empty_optional_fields():
    response = _response(tx_hashes=[SOLANA_SIGNATURE])

    result = _execution_result_from_proto(response, chain="solana", expected_plan_hash="expected")

    assert result.tx_hashes == [SOLANA_SIGNATURE]
    assert result.receipts == []
    assert result.chain_family == "SOLANA"
    assert result.error is None
    assert result.error_code is None
    assert result.extraction_warnings == []
    assert result.execution_plan_hash == ""
    assert result.submission_transactions == []


def test_execution_error_result_preserves_exact_legacy_failure_shape():
    result = _execution_error_result(TypeError("not serializable"))

    assert result.success is False
    assert result.tx_hashes == []
    assert result.total_gas_used == 0
    assert result.receipts == []
    assert result.execution_id == ""
    assert result.error == "not serializable"
    assert result.error_code is None
    assert result.submission_provenance is SubmissionProvenance.UNSPECIFIED


@pytest.mark.asyncio
async def test_execute_resolves_context_builds_request_and_decodes_response():
    orchestrator = _orchestrator()
    orchestrator._client.execution.Execute.return_value = _response()
    context = SimpleNamespace(
        deployment_id="deployment:context",
        intent_id="intent-context",
        dry_run=True,
        simulation_enabled=False,
        wallet_address=WALLET,
    )

    result = await orchestrator.execute({"transactions": []}, context=context)

    request = orchestrator._client.execution.Execute.call_args.args[0]
    assert request.deployment_id == "deployment:context"
    assert request.intent_id == "intent-context"
    assert request.dry_run is True
    assert request.simulation_enabled is False
    assert result.success is False
    assert result.execution_id == "execution-1"


@pytest.mark.asyncio
async def test_execute_returns_exact_error_result_when_encoding_fails():
    orchestrator = _orchestrator()

    result = await orchestrator.execute({"not_json": object()})

    assert result.success is False
    assert result.tx_hashes == []
    assert result.execution_id == ""
    assert result.error == "Object of type object is not JSON serializable"
    orchestrator._client.execution.Execute.assert_not_called()


@pytest.mark.asyncio
async def test_execute_returns_exact_error_result_when_rpc_fails():
    orchestrator = _orchestrator()
    orchestrator._client.execution.Execute.side_effect = RuntimeError("gateway unavailable")

    result = await orchestrator.execute({"transactions": []})

    assert result.success is False
    assert result.error == "gateway unavailable"


@pytest.mark.asyncio
async def test_execute_returns_exact_error_result_when_response_decode_fails():
    orchestrator = _orchestrator()
    orchestrator._client.execution.Execute.return_value = _response(receipts=b"not-json")

    result = await orchestrator.execute({"transactions": []})

    assert result.success is False
    assert result.tx_hashes == []
    assert result.execution_id == ""
    assert result.error == "Expecting value: line 1 column 1 (char 0)"


@pytest.mark.asyncio
async def test_execute_missing_wallet_still_raises_instead_of_returning_failure():
    orchestrator = _orchestrator(wallet_address=None)

    with pytest.raises(ValueError, match="^wallet_address is required$"):
        await orchestrator.execute({"transactions": []})
