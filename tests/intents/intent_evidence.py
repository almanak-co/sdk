"""Opt-in, test-only receipt evidence for the Intent QA Lab.

This module deliberately has no connector-parser or network dependencies.  A
test supplies the parser callable it already uses; the recorder invokes it
once, preserves its result, and independently walks the raw receipt logs.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

from scripts.qa.permission_attestation import validate_permission_attestation

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
APPROVAL_TOPIC = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"
EVENTS = {
    TRANSFER_TOPIC: ("Transfer", "Transfer(address,address,uint256)", ("from", "to")),
    APPROVAL_TOPIC: ("Approval", "Approval(address,address,uint256)", ("owner", "spender")),
}


def _receipt_status_ok(status: Any) -> bool | None:
    """Return the receipt's success bit, or ``None`` when it is unreadable.

    ``None`` is the unmeasured answer, never a failure: a receipt shape this
    function cannot decode says nothing about whether the transaction reverted.
    """
    if isinstance(status, bool):
        return status
    if isinstance(status, int):
        return status == 1
    if isinstance(status, str):
        try:
            return int(status, 16 if status.lower().startswith("0x") else 10) == 1
        except ValueError:
            return None
    return None


def _execute_state(transaction_result: Any, receipt: Mapping[str, Any]) -> str:
    """Derive Layer 2 from the observed execution.

    The recorder used to stamp ``execute: PASS`` for every captured receipt,
    which made the claim a statement about having been called rather than about
    what happened.  A test that bound a reverted receipt still published a
    passing execution layer.  Both observable signals are consulted and both
    must agree; ``Empty != Zero`` applies, so an execution neither signal can
    describe is ``UNMEASURED`` rather than ``PASS``.
    """
    success = getattr(transaction_result, "success", None)
    observations = [
        observed
        for observed in (success if isinstance(success, bool) else None, _receipt_status_ok(receipt.get("status")))
        if observed is not None
    ]
    if not observations:
        return "UNMEASURED"
    return "PASS" if all(observations) else "FAIL"


def json_safe(value: Any) -> Any:  # noqa: C901 - explicit type order is the evidence contract
    """Return a deterministic JSON value without inventing missing data."""
    if value is None or isinstance(value, str | int | bool):
        return value
    if isinstance(value, float):
        if value != value or value in {float("inf"), float("-inf")}:
            raise ValueError("Non-finite floats are not valid receipt evidence")
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return json_safe(value.value)
    if isinstance(value, bytes | bytearray | memoryview):
        return "0x" + bytes(value).hex()
    if hasattr(value, "hex") and type(value).__module__.startswith("hexbytes"):
        rendered = value.hex()
        return rendered if str(rendered).startswith("0x") else f"0x{rendered}"
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return json_safe(value.to_dict())
    if is_dataclass(value) and not isinstance(value, type):
        return json_safe(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, tuple | list | set | frozenset):
        items = list(value)
        if isinstance(value, set | frozenset):
            items.sort(key=repr)
        return [json_safe(item) for item in items]
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if hasattr(value, "model_dump") and callable(value.model_dump):
        return json_safe(value.model_dump(mode="python"))
    if hasattr(value, "__dict__"):
        return json_safe(vars(value))
    raise TypeError(f"Unsupported receipt evidence value: {type(value).__module__}.{type(value).__qualname__}")


def _hex(value: Any) -> str:
    normalized = json_safe(value)
    if not isinstance(normalized, str):
        raise ValueError(f"Expected hex string, got {normalized!r}")
    return normalized.lower()


def _topic_address(topic: str) -> str:
    body = topic.removeprefix("0x")
    if len(body) != 64:
        raise ValueError(f"address topic must be 32 bytes, got {len(body) // 2}")
    return "0x" + body[-40:]


def decode_explorer_view(raw_receipt: Mapping[str, Any]) -> dict[str, Any]:
    """Decode standard ERC-20 logs without reusing connector parser code."""
    receipt = json_safe(dict(raw_receipt))
    raw_logs = receipt.get("logs") or []
    if not isinstance(raw_logs, list):
        raise ValueError("Receipt logs must be a list")
    decoded: list[dict[str, Any]] = []
    for fallback_index, raw_log in enumerate(raw_logs):
        if not isinstance(raw_log, dict):
            decoded.append({"index": fallback_index, "raw": raw_log, "decode_error": "log is not an object"})
            continue
        topics = raw_log.get("topics") or []
        entry: dict[str, Any] = {
            "index": raw_log.get("log_index", raw_log.get("logIndex", fallback_index)),
            "address": raw_log.get("address"),
            "topic0": _hex(topics[0]) if topics else None,
            "topics": [_hex(topic) for topic in topics],
            "data": _hex(raw_log.get("data", "0x")),
            "name": None,
            "signature": None,
            "args": None,
        }
        event = EVENTS.get(entry["topic0"])
        if event:
            name, signature, indexed_names = event
            entry["name"], entry["signature"] = name, signature
            try:
                if len(entry["topics"]) not in {3, 4}:
                    raise ValueError(f"expected 3 or 4 topics, got {len(entry['topics'])}")
                if len(entry["topics"]) == 4:
                    value = int(entry["topics"][3], 16)
                    entry["standard"] = "ERC721"
                else:
                    data_body = entry["data"].removeprefix("0x")
                    if len(data_body) != 64:
                        raise ValueError(f"expected 32-byte value, got {len(data_body) // 2}")
                    value = int(data_body, 16)
                    entry["standard"] = "ERC20"
                entry["args"] = {
                    indexed_names[0]: _topic_address(entry["topics"][1]),
                    indexed_names[1]: _topic_address(entry["topics"][2]),
                    "value": str(value),
                }
            except (TypeError, ValueError) as exc:
                entry["decode_error"] = str(exc)
        decoded.append(entry)
    tx_hash = receipt.get("tx_hash", receipt.get("transactionHash"))
    return {
        "decoder": "almanak-qa-explorer/v1",
        "label": "Explorer-style",
        "overview": {
            "transaction_hash": tx_hash,
            "block_number": receipt.get("block_number", receipt.get("blockNumber")),
            "status": receipt.get("status"),
        },
        "logs": decoded,
    }


def _axis_value(intent: Any, name: str) -> str:
    value = getattr(intent, name, None)
    if name == "intent":
        value = getattr(intent, "intent_type", None)
    value = getattr(value, "value", value)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Intent evidence requires {name} on {type(intent).__name__}")
    return value.strip().lower() if name in {"protocol", "chain"} else value.strip().upper()


def _receipt_dict(transaction_result: Any) -> dict[str, Any]:
    """Return the production-shaped receipt passed to connector parsers.

    Serialization belongs at the evidence boundary, after parser invocation.
    Normalizing HexBytes and other web3 values before invoking the parser would
    make evidence mode exercise a different input contract than production.
    """
    receipt = getattr(transaction_result, "receipt", transaction_result)
    if receipt is None:
        raise ValueError("Intent receipt evidence requires a transaction receipt")
    value = receipt.to_dict() if hasattr(receipt, "to_dict") else receipt
    if not isinstance(value, Mapping):
        raise ValueError("Transaction receipt must be an object")
    return dict(value)


def _fidelity(
    *,
    declared_hard: bool,
    flags: Mapping[str, bool] | None,
    witnesses: list[Any] | None,
    notes: list[str] | None,
) -> dict[str, Any]:
    """Derive hard fidelity from named, discriminating boolean predicates.

    ``declared_hard`` remains in the artifact as provenance during the schema
    transition, but cannot confer a HARD grade. An empty predicate set, a
    non-boolean value, or any false predicate is fail-closed to SOFT.
    """
    normalized_flags = json_safe(flags or {})
    if not isinstance(normalized_flags, dict):
        raise ValueError("Intent fidelity flags must be an object")
    predicates_are_boolean = all(type(value) is bool for value in normalized_flags.values())
    derived_hard = bool(normalized_flags) and predicates_are_boolean and all(normalized_flags.values())
    fidelity_notes = list(notes or [])
    if declared_hard and not derived_hard:
        fidelity_notes.append("HARD was requested but not earned by a non-empty all-true boolean predicate set.")
    return {
        "hard": derived_hard,
        "declared_hard": bool(declared_hard),
        "flags": normalized_flags,
        "witnesses": json_safe(witnesses or []),
        "notes": fidelity_notes,
    }


@dataclass
class _BoundIntent:
    payload: dict[str, Any]
    source_intent: Any = field(repr=False)
    receipt_artifacts: list[str] = field(default_factory=list)


def _source_request(intent: Any) -> dict[str, Any] | None:
    intent_name = _axis_value(intent, "intent")
    if intent_name == "LP_OPEN":
        values = {
            "amount0": getattr(getattr(intent, "amount0", None), "root", getattr(intent, "amount0", None)),
            "amount1": getattr(getattr(intent, "amount1", None), "root", getattr(intent, "amount1", None)),
            "range_lower": getattr(getattr(intent, "range_lower", None), "root", getattr(intent, "range_lower", None)),
            "range_upper": getattr(getattr(intent, "range_upper", None), "root", getattr(intent, "range_upper", None)),
        }
        pool = getattr(intent, "pool", None)
        if not isinstance(pool, str) or not pool.strip() or any(value is None for value in values.values()):
            return None
        return {
            "schema_version": 1,
            "captured_by": "compiler_observer",
            "intent": intent_name,
            "pool_reference": pool,
            **{key: str(json_safe(value)) for key, value in values.items()},
            "fee_tier_units": getattr(intent, "fee_tier_units", None),
        }
    if intent_name in {"LP_CLOSE", "LP_COLLECT_FEES"}:
        pool = getattr(intent, "pool", None)
        if not isinstance(pool, str) or not pool.strip():
            return None
        request = {
            "schema_version": 1,
            "captured_by": "compiler_observer",
            "intent": intent_name,
            "pool_reference": pool,
            "position_id": str((getattr(intent, "protocol_params", None) or {}).get("position_id") or ""),
        }
        if intent_name == "LP_CLOSE":
            request["position_id"] = str(getattr(intent, "position_id", ""))
            request["collect_fees"] = bool(getattr(intent, "collect_fees", False))
        return request
    if intent_name == "SWAP":
        amount_field = "amount"
        asset_field = "from_token"
        target_asset = getattr(intent, "to_token", None)
    else:
        amount_field = "borrow_amount" if intent_name == "BORROW" else "amount"
        asset_field = "borrow_token" if intent_name == "BORROW" else "token"
        target_asset = None
    amount = getattr(intent, amount_field, None)
    amount = getattr(amount, "root", amount)
    asset = getattr(intent, asset_field, None)
    if not isinstance(asset, str) or not asset.strip() or amount is None:
        return None
    normalized_amount = json_safe(amount)
    if not isinstance(normalized_amount, str | int):
        raise ValueError(f"Intent evidence requires one exact numeric {amount_field}, got {normalized_amount!r}")
    request = {
        "schema_version": 1,
        "captured_by": "compiler_observer",
        "intent": intent_name,
        "asset_reference": asset,
        "amount": str(normalized_amount),
    }
    if intent_name == "SWAP":
        if not isinstance(target_asset, str) or not target_asset.strip():
            return None
        request["target_asset_reference"] = target_asset
    return request


class IntentEvidenceRecorder:
    """Per-pytest-node invocation recorder."""

    def __init__(
        self,
        *,
        output_dir: Path,
        nodeid: str,
        network: str,
        exec_path: str,
        git_sha: str = "unknown",
        chain_id: int | None = None,
        declared_intents: set[str] | None = None,
        observed_intents: list[Any] | None = None,
        source_provenance: Mapping[str, Any] | None = None,
    ) -> None:
        if network not in {"anvil", "mainnet"} or exec_path not in {"safe", "eoa"}:
            raise ValueError("Intent evidence axes must be anvil|mainnet and safe|eoa")
        self.output_dir = output_dir
        self.nodeid = nodeid
        self.network = network
        self.exec_path = exec_path
        self.git_sha = git_sha
        self.chain_id = chain_id
        self.declared_intents = declared_intents
        self.observed_intents = observed_intents
        self.source_provenance = json_safe(source_provenance or {})
        self._intents: dict[str, _BoundIntent] = {}
        self._invocations = 0
        self._fidelity: dict[str, Any] = _fidelity(declared_hard=False, flags=None, witnesses=None, notes=None)
        self._balance_deltas: Any = None
        self._balance_checks: dict[str, bool] = {}
        self._fidelity_by_receipt_role: dict[str, dict[str, Any]] = {}
        self._balance_deltas_by_receipt_role: dict[str, Any] = {}
        self._balance_checks_by_receipt_role: dict[str, dict[str, bool]] = {}
        self._semantic_contract_by_receipt_role: dict[str, dict[str, Any]] = {}
        self._artifact_payloads: list[tuple[Path, dict[str, Any]]] = []

    @staticmethod
    def _receipt_role(value: str) -> str:
        role = str(value).strip().lower()
        if re.fullmatch(r"[a-z][a-z0-9_-]*", role) is None:
            raise ValueError(
                f"Intent receipt_role must be a non-empty lowercase slug ([a-z][a-z0-9_-]*), got {value!r}"
            )
        return role

    def bind(self, intent: Any, *, outcome_class: str = "happy", receipt_expected: bool = True) -> str:
        protocol = _axis_value(intent, "protocol")
        chain = _axis_value(intent, "chain")
        intent_name = _axis_value(intent, "intent")
        if self.declared_intents is not None and intent_name not in self.declared_intents:
            raise ValueError(
                f"Compiled/bound {intent_name} is absent from {self.nodeid}'s "
                f"@pytest.mark.intent declarations {sorted(self.declared_intents)!r}"
            )
        cell_id = f"intent.{protocol}.{chain}.{intent_name}.{self.network}.{self.exec_path}"
        payload = {
            "intent_cell_id": cell_id,
            "protocol": protocol,
            "intent": intent_name,
            "chain": chain,
            "network": self.network,
            "exec_path": self.exec_path,
            "outcome_class": outcome_class,
            "receipt_expected": receipt_expected,
            "source_request": _source_request(intent),
        }
        existing = self._intents.get(cell_id)
        if existing and existing.payload != payload:
            raise ValueError(f"Conflicting Intent evidence binding for {cell_id}")
        self._intents.setdefault(cell_id, _BoundIntent(payload, source_intent=intent))
        return cell_id

    def record_fidelity(
        self,
        *,
        hard: bool,
        flags: Mapping[str, bool] | None = None,
        witnesses: list[Any] | None = None,
        notes: list[str] | None = None,
        receipt_role: str | None = None,
    ) -> None:
        fidelity = _fidelity(
            declared_hard=hard,
            flags=flags,
            witnesses=witnesses,
            notes=notes,
        )
        if receipt_role is None:
            self._fidelity = fidelity
        else:
            self._fidelity_by_receipt_role[self._receipt_role(receipt_role)] = fidelity

    def record_balance_deltas(
        self,
        *,
        receipt_role: str | None = None,
        checks: Mapping[str, bool] | None = None,
        **deltas: Any,
    ) -> None:
        normalized = json_safe(deltas)
        normalized_checks = json_safe(checks or {})
        if not isinstance(normalized_checks, dict) or not all(
            type(value) is bool for value in normalized_checks.values()
        ):
            raise ValueError("Balance checks must be named booleans")
        if receipt_role is None:
            self._balance_deltas = normalized
            self._balance_checks = normalized_checks
        else:
            role = self._receipt_role(receipt_role)
            self._balance_deltas_by_receipt_role[role] = normalized
            self._balance_checks_by_receipt_role[role] = normalized_checks

    def record_semantic_contract(self, *, receipt_role: str = "execution", **contract: Any) -> None:
        """Record raw scientific measurements for seal-time re-derivation.

        The recorder deliberately does not compute a verdict.  The independent
        sealer validates the versioned profile from these measurements and the
        raw receipt; producer-side booleans cannot create contract trust.
        """
        role = self._receipt_role(receipt_role)
        normalized = json_safe(contract)
        if not isinstance(normalized, dict):
            raise ValueError("Intent semantic contract must be an object")
        if role in self._semantic_contract_by_receipt_role:
            raise ValueError(f"Intent semantic contract already recorded for receipt role {role!r}")
        self._semantic_contract_by_receipt_role[role] = normalized

    def capture_parse(
        self,
        *,
        intent: Any,
        transaction_result: Any,
        parser: Callable[[dict[str, Any]], Any],
        parser_method: str = "parse_receipt",
        outcome_class: str = "happy",
        explorer_url: str | None = None,
        receipt_role: str = "execution",
    ) -> Any:
        cell_id = self.bind(intent, outcome_class=outcome_class, receipt_expected=True)
        normalized_receipt_role = self._receipt_role(receipt_role)
        parser_receipt = _receipt_dict(transaction_result)
        receipt = json_safe(parser_receipt)
        if not isinstance(receipt, dict):  # defensive: _receipt_dict already enforces Mapping
            raise ValueError("Transaction receipt must serialize to an object")
        self._invocations += 1
        invocation = self._invocations
        parser_error: dict[str, str] | None = None
        parser_exception: Exception | None = None
        permission_error: dict[str, str] | None = None
        permission_exception: Exception | None = None
        try:
            result = parser(dict(parser_receipt))
        except Exception as exc:
            result = None
            parser_exception = exc
            parser_error = {"type": f"{type(exc).__module__}.{type(exc).__qualname__}", "message": str(exc)}
        tx_hash = receipt.get("tx_hash", receipt.get("transactionHash", getattr(transaction_result, "tx_hash", None)))
        node_hash = hashlib.sha256(self.nodeid.encode()).hexdigest()[:16]
        cell_hash = hashlib.sha256(cell_id.encode()).hexdigest()[:12]
        relpath = Path("receipts") / node_hash / f"{cell_hash}-parse{invocation:02d}.json"
        tx = {
            "hash": json_safe(tx_hash),
            "block_number": receipt.get("block_number", receipt.get("blockNumber")),
            "status": receipt.get("status"),
            "from": receipt.get("from_address", receipt.get("from")),
            "to": receipt.get("to_address", receipt.get("to")),
            "gas_used": receipt.get("gas_used", receipt.get("gasUsed", getattr(transaction_result, "gas_used", None))),
            "explorer_url": explorer_url,
        }
        permission_attestation = json_safe(getattr(transaction_result, "qa_permission_attestation", None))
        permission_status = "NOT_APPLICABLE"
        if self.exec_path == "safe":
            if permission_attestation is None:
                permission_status = "MISSING"
            else:
                try:
                    validate_permission_attestation(permission_attestation)
                    permission_status = "PASS"
                except Exception as exc:  # noqa: BLE001 - persist the refusal before propagating it
                    permission_status = "FAIL"
                    permission_exception = exc
                    permission_error = {
                        "type": f"{type(exc).__module__}.{type(exc).__qualname__}",
                        "message": str(exc),
                    }
        payload = {
            "schema_version": 1,
            "artifact_kind": "almanak.intent_receipt_evidence",
            "artifact_id": f"{node_hash}:{cell_hash}:{invocation}",
            "intent_cell_id": cell_id,
            "network": self.network,
            "exec_path": self.exec_path,
            "protocol": self._intents[cell_id].payload["protocol"],
            "chain": self._intents[cell_id].payload["chain"],
            "chain_id": self.chain_id,
            "intent": self._intents[cell_id].payload["intent"],
            "outcome_class": outcome_class,
            "source_request": self._intents[cell_id].payload["source_request"],
            "receipt_role": normalized_receipt_role,
            "test": {
                "nodeid": self.nodeid,
                "file": self.nodeid.split("::", 1)[0],
                "name": self.nodeid.rsplit("::", 1)[-1],
            },
            "layers": {
                # Layer 1 is resolved in finalize(), where the compiler
                # observation for the whole node is known. Until then the
                # honest value is the unmeasured one.
                "compile": "UNMEASURED",
                "execute": _execute_state(transaction_result, receipt),
                "receipt": "FAIL" if parser_error else "SOFT",
                "balances": "MISSING",
                "permissions": permission_status,
            },
            "tx": tx,
            "raw_receipt": receipt,
            "permission_attestation": permission_attestation,
            "permission_validation_error": permission_error,
            "almanak": {
                "parser_method": parser_method,
                "result_type": None if result is None else f"{type(result).__module__}.{type(result).__qualname__}",
                "result": json_safe(result),
                "error": parser_error,
            },
            "explorer_view": decode_explorer_view(receipt),
            "fidelity": dict(self._fidelity),
            "balance_deltas": self._balance_deltas,
            "git_sha": self.git_sha,
            "sealed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "source_provenance": self.source_provenance,
        }
        self._intents[cell_id].receipt_artifacts.append(relpath.as_posix())
        self._artifact_payloads.append((relpath, payload))
        if parser_exception is not None and permission_exception is not None:
            raise ExceptionGroup(
                "Receipt parsing and permission-attestation validation both failed",
                [parser_exception, permission_exception],
            )
        if parser_exception is not None:
            raise parser_exception
        if permission_exception is not None:
            raise permission_exception
        return result

    def finalize(self, *, outcome: str, duration_seconds: float) -> Path:
        if not self._intents:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            node = {
                "nodeid": self.nodeid,
                "outcome": outcome,
                "duration_seconds": duration_seconds,
                "intents": [],
                "evidence_error": "Evidence-enabled Intent test compiled/bound no intents",
            }
            fragment = self.output_dir / "nodes" / f"{hashlib.sha256(self.nodeid.encode()).hexdigest()}.json"
            fragment.parent.mkdir(parents=True, exist_ok=True)
            _atomic_json(fragment, node)
            return fragment
        if self.observed_intents is not None:
            uncompiled = sorted(
                cell_id
                for cell_id, bound in self._intents.items()
                if not any(observed is bound.source_intent for observed in self.observed_intents)
            )
            if uncompiled:
                raise ValueError(
                    f"Intent evidence bound source requests IntentCompiler.compile did not compile: {uncompiled!r}"
                )
            observed_cells = {
                f"intent.{_axis_value(intent, 'protocol')}.{_axis_value(intent, 'chain')}."
                f"{_axis_value(intent, 'intent')}.{self.network}.{self.exec_path}"
                for intent in self.observed_intents
            }
            unobserved = sorted(set(self._intents) - observed_cells)
            if unobserved:
                raise ValueError(f"Intent evidence claims cells IntentCompiler.compile did not compile: {unobserved!r}")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # Layer 1 is earned by the two gates above. ``observed_intents`` is the
        # set of intents whose compile RETURNED CompilationStatus.SUCCESS -- see
        # conftest's _record_compilation -- so reaching here means every bound
        # intent object and every claimed cell produced a compiled bundle.
        # Without that instrument wired there is no observation to report, so
        # the layer stays unmeasured.
        compile_state = "PASS" if self.observed_intents is not None else "UNMEASURED"
        for relpath, payload in self._artifact_payloads:
            payload["test"]["outcome"] = outcome
            payload["layers"]["compile"] = compile_state
            receipt_role = payload["receipt_role"]
            payload["fidelity"] = dict(self._fidelity_by_receipt_role.get(receipt_role, self._fidelity))
            payload["semantic_contract"] = self._semantic_contract_by_receipt_role.get(receipt_role)
            parser_failed = payload["almanak"]["error"] is not None or payload["layers"]["receipt"] == "FAIL"
            if payload["fidelity"]["hard"] and not parser_failed:
                payload["layers"]["receipt"] = "PASS"
            balance_deltas = self._balance_deltas_by_receipt_role.get(receipt_role, self._balance_deltas)
            if balance_deltas is not None:
                payload["balance_deltas"] = balance_deltas
                balance_checks = self._balance_checks_by_receipt_role.get(receipt_role, self._balance_checks)
                payload["balance_checks"] = balance_checks
                if balance_checks and all(balance_checks.values()):
                    payload["layers"]["balances"] = "PASS"
                else:
                    payload["layers"]["balances"] = "SOFT"
            target = self.output_dir / relpath
            target.parent.mkdir(parents=True, exist_ok=True)
            _atomic_json(target, payload)
        node = {
            "nodeid": self.nodeid,
            "outcome": outcome,
            "duration_seconds": duration_seconds,
            "intents": [
                {**bound.payload, "receipt_artifacts": list(bound.receipt_artifacts)}
                for _, bound in sorted(self._intents.items())
            ],
        }
        fragment = self.output_dir / "nodes" / f"{hashlib.sha256(self.nodeid.encode()).hexdigest()}.json"
        fragment.parent.mkdir(parents=True, exist_ok=True)
        _atomic_json(fragment, node)
        return fragment


class DisabledIntentEvidenceRecorder:
    """Side-effect-free adapter so ordinary Intent runs need no QA flags."""

    def bind(self, intent: Any, *, outcome_class: str = "happy", receipt_expected: bool = True) -> None:
        return None

    def capture_parse(
        self,
        *,
        intent: Any,
        transaction_result: Any,
        parser: Callable[[dict[str, Any]], Any],
        parser_method: str = "parse_receipt",
        outcome_class: str = "happy",
        explorer_url: str | None = None,
        receipt_role: str = "execution",
    ) -> Any:
        del receipt_role
        return parser(_receipt_dict(transaction_result))

    def record_fidelity(self, **_: Any) -> None:
        return None

    def record_balance_deltas(self, **_: Any) -> None:
        return None

    def record_semantic_contract(self, **_: Any) -> None:
        return None


def build_evidence_manifest(output_dir: Path) -> Path:
    """Assemble per-node fragments after pytest completes."""
    nodes = []
    for path in sorted((output_dir / "nodes").glob("*.json")):
        nodes.append(json.loads(path.read_text(encoding="utf-8")))
    target = output_dir / "evidence-manifest.json"
    _atomic_json(target, {"schema_version": 1, "nodes": nodes})
    return target


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(json.dumps(json_safe(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temp, path)
