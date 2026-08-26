"""Contracts for fail-closed QA Lab re-check routing."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest


@pytest.fixture(scope="module")
def recheck_module() -> ModuleType:
    path = Path(__file__).resolve().parents[3] / "scripts" / "quant-test" / "qa_recheck.py"
    spec = importlib.util.spec_from_file_location("qa_recheck_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _catalogs() -> tuple[dict, dict, dict]:
    def proof_recipe(protocol: str, intent: str) -> dict:
        return {
            "roles": [
                {
                    "role": "positive_runtime",
                    "nodes": [
                        {
                            "nodeid": f"tests/intents/test_{protocol}.py::test_{intent.lower()}_safe",
                            "exec_path": "safe",
                        },
                        {"nodeid": f"tests/intents/test_{protocol}.py::test_{intent.lower()}_eoa", "exec_path": "eoa"},
                    ],
                }
            ]
        }

    qa = {
        "defaults": {"networks": ["anvil", "mainnet"], "exec_paths": ["eoa", "safe"]},
        "cells": [
            {
                "id": "perp.gmx_v2.arbitrum.simple",
                "protocol": "gmx_v2",
                "chain": "arbitrum",
            },
            {"id": "swap.other.arbitrum.simple", "protocol": "other", "chain": "arbitrum"},
        ],
    }
    intent = {
        "cells": [
            {
                "id": "intent.gmx_v2.arbitrum.PERP_OPEN",
                "protocol": "gmx_v2",
                "intent": "PERP_OPEN",
                "chain": "arbitrum",
                "proof_recipe": proof_recipe("gmx_v2", "PERP_OPEN"),
            },
            {
                "id": "intent.gmx_v2.arbitrum.PERP_CANCEL_ORDER",
                "protocol": "gmx_v2",
                "intent": "PERP_CANCEL_ORDER",
                "chain": "arbitrum",
                "proof_recipe": proof_recipe("gmx_v2", "PERP_CANCEL_ORDER"),
            },
            {
                "id": "intent.aave_v3.arbitrum.SUPPLY",
                "protocol": "aave_v3",
                "intent": "SUPPLY",
                "chain": "arbitrum",
                "proof_recipe": proof_recipe("aave_v3", "SUPPLY"),
                "test_paths": {
                    "eoa": ["tests/intents/arbitrum/test_aave_v3_lending.py"],
                    "safe": ["tests/intents/arbitrum/test_aave_v3_lending.py"],
                },
                "mainnet_recipes": [
                    {
                        "recipe_id": "aave_v3.supply.arbitrum.eoa",
                        "recipe_sha256": "a" * 64,
                        "exec_path": "eoa",
                    }
                ],
            },
        ]
    }
    readiness = {
        "subjects": [
            {
                "kind": "protocol",
                "id": "gmx_v2",
                "gates": [{"id": "P3"}, {"id": "P8"}, {"id": "P9"}],
            }
        ]
    }
    return qa, intent, readiness


def test_gmx_mainnet_routes_bind_existing_owners_and_approval(recheck_module: ModuleType, tmp_path: Path) -> None:
    qa, intent, readiness = _catalogs()
    routes = recheck_module.build_recheck_catalog(
        repo_root=tmp_path,
        qa_catalog=qa,
        intent_catalog=intent,
        readiness_catalog=readiness,
    )

    quant = routes["quant"]["perp.gmx_v2.arbitrum.simple.mainnet.eoa"]
    intent_open = routes["intent"]["intent.gmx_v2.arbitrum.PERP_OPEN.mainnet.eoa"]
    lifecycle = routes["readiness"]["protocol:gmx_v2:P9"]

    assert routes["mode"] == "render_only_no_dispatch"
    assert quant["route_status"] == "ROUTABLE"
    assert quant["owner"] == "/quant-test"
    assert quant["approval"]["required"] is True
    assert quant["budget"]["total_wallet_cap_usd"] == "11.20"
    assert "durable pool wallet only" in quant["command"]
    assert "Do not fund or launch" in quant["command"]
    assert intent_open["owner"] == "/quant-test → GMX Mainnet Intent certifier"
    assert "real keeper" in intent_open["reason"]
    assert lifecycle["route_status"] == "ROUTABLE"
    assert "readiness itself remains separately sealed" in quant["expected_updates"][-1]


def test_missing_gmx_live_owners_are_unroutable(recheck_module: ModuleType, tmp_path: Path) -> None:
    qa, intent, readiness = _catalogs()
    routes = recheck_module.build_recheck_catalog(
        repo_root=tmp_path,
        qa_catalog=qa,
        intent_catalog=intent,
        readiness_catalog=readiness,
    )

    cancel = routes["intent"]["intent.gmx_v2.arbitrum.PERP_CANCEL_ORDER.mainnet.eoa"]
    safe = routes["intent"]["intent.gmx_v2.arbitrum.PERP_OPEN.mainnet.safe"]
    async_gate = routes["readiness"]["protocol:gmx_v2:P8"]
    unrelated = routes["quant"]["swap.other.arbitrum.simple.mainnet.eoa"]

    for route in (cancel, safe, async_gate, unrelated):
        assert route["route_status"] == "UNROUTABLE"
        assert route["command"] is None
        assert route["preflight_state"] == "BLOCKED"
    assert "cannot create that transaction" in cancel["reason"]
    assert "Mainnet Safe" in safe["reason"]
    assert "PERP_CANCEL_ORDER" in async_gate["reason"]


def test_read_only_and_anvil_routes_do_not_request_money_approval(recheck_module: ModuleType, tmp_path: Path) -> None:
    qa, intent, readiness = _catalogs()
    routes = recheck_module.build_recheck_catalog(
        repo_root=tmp_path,
        qa_catalog=qa,
        intent_catalog=intent,
        readiness_catalog=readiness,
    )

    protocol = routes["readiness"]["protocol:gmx_v2:P3"]
    anvil = routes["intent"]["intent.gmx_v2.arbitrum.PERP_OPEN.anvil.eoa"]
    aave = routes["intent"]["intent.aave_v3.arbitrum.SUPPLY.anvil.eoa"]
    aave_mainnet = routes["intent"]["intent.aave_v3.arbitrum.SUPPLY.mainnet.eoa"]

    assert protocol["route_status"] == "ROUTABLE"
    assert protocol["owner"] == "Protocol Tests"
    assert protocol["budget"]["trading_cap_usd"] == "0"
    assert protocol["approval"]["required"] is False
    assert anvil["command"] == "/test-intent intent.gmx_v2.arbitrum.PERP_OPEN.anvil.eoa"
    assert anvil["approval"]["required"] is False
    assert "JUnit alone cannot paint DECODE" in anvil["prerequisites"][-1]
    assert aave["route_status"] == "ROUTABLE"
    assert aave["owner"] == "/test-intent"
    assert aave["command"] == "/test-intent intent.aave_v3.arbitrum.SUPPLY.anvil.eoa"
    assert aave["approval"]["required"] is False
    assert aave_mainnet["route_status"] == "ROUTABLE"
    assert aave_mainnet["approval"]["required"] is True
    assert "run_mainnet_intent.py plan" in aave_mainnet["command"]
