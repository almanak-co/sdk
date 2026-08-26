"""Contracts for the Protocol utility QA catalog, exact seals, and Lab board."""

from __future__ import annotations

import ast
import importlib.util
import json
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

TEST_COMMIT = "a" * 40
TEST_SDK = {
    "commit": TEST_COMMIT,
    "branch": "test",
    "dirty": False,
    "sdk_version": "0.0-test",
    "source": "executing-worktree",
}


@pytest.fixture(scope="module")
def protocol_module() -> ModuleType:
    path = Path(__file__).resolve().parents[3] / "scripts" / "quant-test" / "qa_protocol.py"
    spec = importlib.util.spec_from_file_location("qa_protocol_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def authority_module() -> ModuleType:
    path = Path(__file__).resolve().parents[3] / "scripts" / "quant-test" / "qa_gmx_authority.py"
    spec = importlib.util.spec_from_file_location("qa_gmx_authority_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _resource(symbol: str, digit: str) -> dict[str, object]:
    address = lambda suffix: "0x" + (digit * 39) + suffix  # noqa: E731
    return {
        "resource_id": address("1"),
        "name": f"{symbol}/USD [{symbol}/USDC]",
        "label": f"{symbol}/USD",
        "market_token": address("1"),
        "index_token": address("2"),
        "index_symbol": symbol,
        "index_token_decimals": 18,
        "long_token": address("3"),
        "long_token_symbol": symbol,
        "short_token": address("4"),
        "short_token_symbol": "USDC",
    }


class _FakeMarketStub:
    def __init__(self, resources: list[dict[str, object]], *, transport_error: bool = False) -> None:
        self.resources = {str(row["resource_id"]): row for row in resources}
        self.transport_error = transport_error

    def GetPerpMarket(self, request: object, *, timeout: float) -> object:  # noqa: N802, ARG002
        if self.transport_error:
            raise RuntimeError("gateway unavailable")
        row = self.resources[str(request.market)]
        market = SimpleNamespace(
            verified=True,
            market_token=row["market_token"],
            index_token=row["index_token"],
            index_symbol=row["index_symbol"],
            index_token_decimals=row["index_token_decimals"],
            long_token=row["long_token"],
            short_token=row["short_token"],
        )
        return SimpleNamespace(success=True, error="", market=market)

    def GetPrice(self, request: object, *, timeout: float) -> object:  # noqa: N802, ARG002
        return SimpleNamespace(
            price="100",
            timestamp=1,
            source="test-authority",
            confidence=1.0,
            stale=False,
            sources_ok=["test-authority"],
            sources_failed={},
        )


#: The market id ``_resource("XMR", "2")`` actually produces. ``_resource``
#: builds ``"0x" + digit * 39 + suffix`` and keys ``resource_id`` on
#: ``suffix="1"``, so the id ends in ``1``, not ``2``. A sentinel of
#: ``"0x" + "2" * 40`` differs in that last character and never matched, which
#: left the modelled PRICE_UNAVAILABLE rejection dead and let PRODUCT_GAP be
#: proved by an accidental decoded-market mismatch instead.
_PRICE_GAP_MARKET = "0x" + ("2" * 39) + "1"

#: A market no resource owns, for exercising PROTECTION_DECODE identity.
_FOREIGN_MARKET = "0x" + ("9" * 40)


class _FakeExecutionStub:
    def __init__(self, *, mismatch_market: bool = False) -> None:
        self.seen_markets: list[str] = []
        #: Return a market that is not the one requested, so the compile
        #: succeeds and PROTECTION_DECODE is what rejects.
        self.mismatch_market = mismatch_market

    def CompileIntent(self, request: object, *, timeout: float) -> object:  # noqa: N802, ARG002
        intent = json.loads(request.intent_data)
        self.seen_markets.append(str(intent["market"]))
        if str(intent["market"]).lower() == _PRICE_GAP_MARKET:
            return SimpleNamespace(success=False, error="no dynamic price route", error_code="PRICE_UNAVAILABLE")
        bundle = {
            "metadata": {
                "acceptable_price_usd": "101",
                "acceptable_price_30dec": "101000000000000000000000000000000",
                "market_address": _FOREIGN_MARKET if self.mismatch_market else str(intent["market"]),
                "index_token_decimals": 18,
            },
            "transactions": [],
        }
        return SimpleNamespace(success=True, error="", error_code="", action_bundle=json.dumps(bundle).encode())


def _fake_client(
    resources: list[dict[str, object]], *, transport_error: bool = False, mismatch_market: bool = False
) -> object:
    return SimpleNamespace(
        market=_FakeMarketStub(resources, transport_error=transport_error),
        execution=_FakeExecutionStub(mismatch_market=mismatch_market),
    )


def _lending_owner_catalog(protocol: str, chains: list[str]) -> dict[str, object]:
    cells = []
    for chain in chains:
        for intent in ("SUPPLY", "BORROW", "REPAY", "WITHDRAW"):
            cells.append(
                {
                    "id": f"intent.{protocol}.{chain}.{intent}",
                    "protocol": protocol,
                    "chain": chain,
                    "intent": intent,
                    "proof_recipe": {
                        "roles": [
                            {
                                "role": "positive_runtime",
                                "nodes": [
                                    {
                                        "exec_path": exec_path,
                                        "contract_profile": "lending.v1",
                                        "proof_id": (
                                            f"intent.{protocol}.{chain}.{intent}.{exec_path}.positive_runtime.v1"
                                        ),
                                    }
                                ],
                            }
                            for exec_path in ("safe", "eoa")
                        ]
                    },
                }
            )
    return {"schema_version": 1, "cells": cells}


def test_catalog_is_registry_owned_and_fail_closed(protocol_module: ModuleType) -> None:
    from almanak.connectors._connector_descriptor import CONNECTOR_REGISTRY

    catalog = protocol_module.build_protocol_catalog()

    assert catalog["source"] == "almanak.connectors._connector_descriptor.CONNECTOR_REGISTRY"
    assert catalog["summary"]["protocols"] == len(CONNECTOR_REGISTRY.all())
    supported = [connector for connector in CONNECTOR_REGISTRY.all() if connector.has_strategy_support]
    assert catalog["summary"]["supported_protocols"] == len(supported)
    assert catalog["summary"]["family_contracts"] == len(supported)
    assert catalog["summary"]["custom_contracts"] > 1
    assert catalog["summary"]["custom_pipeline_gaps"] > 0
    assert catalog["summary"]["pilot_cells"] == len(catalog["cells"])
    assert catalog["summary"]["utility_surfaces"] > 0
    gmx = next(row for row in catalog["protocols"] if row["name"] == "gmx_v2")
    assert gmx["qa_contract"] == "defined"
    assert gmx["chains"] == ["arbitrum", "avalanche"]
    assert gmx["contract"]["custom_required"] is True
    assert gmx["contract"]["pipeline_ready"] is False
    assert {row["support"] for row in catalog["cells"]} == {"runnable", "gap"}
    assert all(row["qa_contract"] == "defined" for row in catalog["protocols"] if row["strategy_supported"])
    assert all(row["qa_contract"] == "not_supported" for row in catalog["protocols"] if not row["strategy_supported"])
    offline = [row for row in catalog["cells"] if row["environment"] == "offline"]
    assert offline
    assert {row["evidence_role"] for row in offline} == {"reference"}
    # An offline cell must SAY it is offline. Paired with evidence_role
    # "reference" above, that is what stops a Lab reader from taking an SDK
    # self-check for live evidence. (VIB-6715 retitled the GMX row from
    # "Static collateral registry consistency" when its authority stopped being
    # a static hand-maintained mirror; the naming contract is unchanged.)
    assert all("Offline" in row["label"] for row in offline)
    mainnet = [row for row in catalog["cells"] if row["environment"] == "mainnet"]
    assert mainnet
    assert {row["evidence_role"] for row in mainnet} == {"authoritative"}


def test_empty_board_distinguishes_shared_contract_ready_and_missing_test_pipe(
    protocol_module: ModuleType,
    tmp_path: Path,
) -> None:
    store = tmp_path / "qa"
    page = protocol_module.render_protocol_lab(store=store, lab_css="")
    rendered = page.read_text(encoding="utf-8")

    assert (store / "index" / "protocol_latest.json").read_text(encoding="utf-8").strip() == "{}"
    assert "Supported protocol × chain matrix" in rendered
    assert "TEST PIPE MISSING · unique behavior has no runner" in rendered
    assert "SHARED CONTRACT · generic QA owns the proof" in rendered
    assert "READY · protocol-only pipeline exists; no seal" in rendered
    assert "— · environment/chain unsupported" in rendered
    assert 'class="btn active" href="protocol.html">Protocol</a>' in rendered
    assert 'href="readiness.html">Checklists</a>' in rendered
    assert "How to read this page" in rendered
    assert "Protocol QA tests the exception" in rendered
    assert "Static registry diagnostics" in rendered
    assert '<option value="mainnet" selected>Mainnet live authority</option>' in rendered
    assert "SHARED CONTRACT · generic QA owns the proof" in rendered
    assert "Supported protocol × chain matrix" in rendered


def test_every_supported_protocol_gets_a_generated_detail_page(
    protocol_module: ModuleType,
    tmp_path: Path,
) -> None:
    store = tmp_path / "qa"
    protocol_module.render_protocol_lab(store=store, lab_css="")
    catalog = protocol_module.build_protocol_catalog()

    supported = [row for row in catalog["protocols"] if row["strategy_supported"]]
    assert supported
    for row in supported:
        page = store / "lab" / row["detail_path"]
        assert page.is_file(), row["name"]
        rendered = page.read_text(encoding="utf-8")
        assert row["contract"]["family_label"] in rendered
        assert "Pipeline readiness is not a PASS" in rendered


def test_focus_protocol_pages_expose_reviewed_scope_exact_recipes_and_honest_gaps(
    protocol_module: ModuleType,
    tmp_path: Path,
) -> None:
    store = tmp_path / "qa"
    owner_catalog = {
        "schema_version": 1,
        "cells": [
            *_lending_owner_catalog("aave_v3", ["ethereum", "arbitrum", "base"])["cells"],
            *_lending_owner_catalog("euler_v2", ["ethereum", "arbitrum", "base"])["cells"],
            {
                "id": "intent.uniswap_v3.arbitrum.SWAP",
                "protocol": "uniswap_v3",
                "chain": "arbitrum",
                "intent": "SWAP",
                "proof_recipe": {
                    "roles": [
                        {
                            "role": "positive_runtime",
                            "nodes": [
                                {
                                    "exec_path": "safe",
                                    "contract_profile": "swap.v1",
                                    "proof_id": "intent.uniswap_v3.arbitrum.SWAP.safe.positive_runtime.v1",
                                    "nodeid": "tests/intents/arbitrum/test_uniswap.py::test_safe",
                                }
                            ],
                        }
                    ]
                },
            },
        ],
    }
    protocol_module.render_protocol_lab(
        store=store,
        lab_css="",
        owner_catalogs={"intent": owner_catalog},
    )

    aave = (store / "lab" / "protocols" / "aave_v3.html").read_text(encoding="utf-8")
    assert "protocol-scope.aave_v3.v1" in aave
    assert "Canonical Pool and reserve execution" in aave
    assert "intent.aave_v3.arbitrum.BORROW.anvil.safe" in aave
    assert "READY TO RUN" in aave
    assert "NO SEALED RUN" in aave

    uniswap = (store / "lab" / "protocols" / "uniswap_v3.html").read_text(encoding="utf-8")
    assert "protocol-scope.uniswap_v3.v1" in uniswap
    assert "Factory-authenticated exact-pool swap" in uniswap
    assert "NPM position open and full close" in uniswap
    assert "TEST PIPE MISSING" in uniswap

    euler = (store / "lab" / "protocols" / "euler_v2.html").read_text(encoding="utf-8")
    assert "protocol-scope.euler_v2.v1" in euler
    assert "EVault asset/share/debt value flow" in euler
    assert "EVC collateral/controller batch binding" in euler
    assert "TEST PIPE MISSING" in euler

    gmx = (store / "lab" / "protocols" / "gmx_v2.html").read_text(encoding="utf-8")
    assert "protocol-scope.gmx_v2.v1" in gmx
    assert "Synthetic index decimal authority" in gmx
    assert "protocol.gmx_v2.synthetic_decimal_authority.arbitrum.mainnet" in gmx
    assert "Order and position lifecycle" in gmx
    assert "TEST PIPE MISSING" in gmx
    assert "Pipeline readiness is not a PASS" in gmx


def test_protocol_contracts_add_custom_tests_only_for_protocol_owned_exceptions(
    protocol_module: ModuleType,
) -> None:
    catalog = protocol_module.build_protocol_catalog()
    rows = {row["name"]: row for row in catalog["protocols"]}

    assert rows["aave_v3"]["contract"]["custom_obligations"][0]["owner_surface"] == "intent"
    assert rows["aave_v3"]["contract"]["protocol_custom_required"] is True
    assert rows["aave_v3"]["contract"]["pipeline_ready"] is False
    assert rows["aave_v3"]["contract"]["protocol_pipeline_state"] == "missing"
    assert rows["curve"]["contract"]["pipeline_ready"] is False
    assert rows["curve"]["contract"]["protocol_custom_required"] is True
    assert rows["curve"]["contract"]["protocol_pipeline_state"] == "missing"
    assert {row["id"] for row in rows["curve"]["contract"]["custom_obligations"]} == {
        "dynamic_pool_identity",
        "exact_permission_target",
    }
    assert rows["pancakeswap_v3"]["contract"]["custom_required"] is False
    assert rows["pancakeswap_v3"]["contract"]["family"] == "concentrated_liquidity_v3"
    assert rows["pancakeswap_v3"]["contract"]["pipeline_ready"] is False
    assert rows["pancakeswap_v3"]["contract"]["protocol_pipeline_state"] == "shared"


def test_delegation_requires_exact_owner_catalog_recipes(protocol_module: ModuleType) -> None:
    from almanak.connectors._connector_descriptor import CONNECTOR_REGISTRY

    aave = next(connector for connector in CONNECTOR_REGISTRY.all() if connector.name == "aave_v3")
    intent_catalog = _lending_owner_catalog("aave_v3", list(aave.all_supported_chains))
    catalog = protocol_module.build_protocol_catalog(owner_catalogs={"intent": intent_catalog})
    row = next(row for row in catalog["protocols"] if row["name"] == "aave_v3")
    obligation = row["contract"]["custom_obligations"][0]

    assert row["contract"]["protocol_pipeline_state"] == "delegated"
    assert row["contract"]["protocol_custom_required"] is False
    assert obligation["pipeline"] == "ready"
    assert all(value == "intent" for value in obligation["effective_owner_by_chain"].values())
    assert all(binding["cell_ids"] for binding in obligation["delegation_by_chain"].values())
    assert all(binding["proof_ids"] for binding in obligation["delegation_by_chain"].values())

    intent_catalog["cells"][0]["proof_recipe"]["roles"][0]["nodes"] = []
    catalog = protocol_module.build_protocol_catalog(owner_catalogs={"intent": intent_catalog})
    row = next(row for row in catalog["protocols"] if row["name"] == "aave_v3")
    assert row["contract"]["protocol_pipeline_state"] == "missing"
    assert row["contract"]["custom_obligations"][0]["pipeline"] == "missing"


def test_focus_protocol_plans_are_exact_axis_and_fail_closed(protocol_module: ModuleType) -> None:
    intent_catalog = {
        "schema_version": 1,
        "cells": [
            *_lending_owner_catalog("aave_v3", ["arbitrum"])["cells"],
            *_lending_owner_catalog("euler_v2", ["arbitrum"])["cells"],
            {
                "id": "intent.uniswap_v3.arbitrum.SWAP",
                "protocol": "uniswap_v3",
                "chain": "arbitrum",
                "intent": "SWAP",
                "proof_recipe": {
                    "roles": [
                        {
                            "role": "positive_runtime",
                            "nodes": [
                                {
                                    "exec_path": path,
                                    "contract_profile": "swap.v1",
                                    "proof_id": f"intent.uniswap_v3.arbitrum.SWAP.{path}.positive_runtime.v1",
                                    "nodeid": f"tests/intents/arbitrum/test_uniswap.py::test_{path}",
                                }
                            ],
                        }
                        for path in ("safe", "eoa")
                    ]
                },
            },
        ],
    }
    catalog = protocol_module.build_protocol_catalog(owner_catalogs={"intent": intent_catalog})

    aave = protocol_module.protocol_capability_plan(
        catalog=catalog,
        protocol="aave_v3",
        chain="arbitrum",
        network="anvil",
        exec_path="safe",
    )
    assert aave["status"] == "ready"
    assert all(capability["status"] == "ready" for capability in aave["capabilities"])
    assert {recipe["cell_id"] for recipe in aave["capabilities"][0]["recipes"]} == {
        f"intent.aave_v3.arbitrum.{intent}.anvil.safe" for intent in ("SUPPLY", "BORROW", "REPAY", "WITHDRAW")
    }
    assert all("intent-run --cell-id" in recipe["command"] for recipe in aave["capabilities"][0]["recipes"])

    aave_mainnet = protocol_module.protocol_capability_plan(
        catalog=catalog,
        protocol="aave_v3",
        chain="arbitrum",
        network="mainnet",
        exec_path="safe",
    )
    assert aave_mainnet["status"] == "missing"
    assert all("test-wallet lane" in capability["blocker"] for capability in aave_mainnet["capabilities"])

    uniswap = protocol_module.protocol_capability_plan(
        catalog=catalog,
        protocol="uniswap_v3",
        chain="arbitrum",
        network="anvil",
        exec_path="eoa",
    )
    states = {capability["id"]: capability["status"] for capability in uniswap["capabilities"]}
    assert states == {"exact_pool_swap": "ready", "npm_position_lifecycle": "missing", "fee_collection": "missing"}
    assert uniswap["status"] == "missing"

    euler = protocol_module.protocol_capability_plan(
        catalog=catalog,
        protocol="euler_v2",
        chain="arbitrum",
        network="anvil",
        exec_path="eoa",
    )
    assert {capability["id"]: capability["status"] for capability in euler["capabilities"]} == {
        "vault_value_flow": "ready",
        "evc_collateral_controller": "missing",
        "safe_permission_closure": "ready",
    }


def test_gmx_live_plan_routes_decimal_authority_but_not_unimplemented_lifecycle(
    protocol_module: ModuleType,
) -> None:
    catalog = protocol_module.build_protocol_catalog()
    plan = protocol_module.protocol_capability_plan(
        catalog=catalog,
        protocol="gmx_v2",
        chain="arbitrum",
        network="mainnet",
        exec_path="eoa",
    )
    states = {capability["id"]: capability for capability in plan["capabilities"]}
    assert states["synthetic_decimal_authority"]["status"] == "ready"
    assert states["synthetic_decimal_authority"]["recipes"][0]["cell_id"] == (
        "protocol.gmx_v2.synthetic_decimal_authority.arbitrum.mainnet"
    )
    assert states["order_lifecycle"]["status"] == "missing"
    assert plan["status"] == "missing"


def test_protocol_matrix_never_promotes_anvil_recipe_into_mainnet(
    protocol_module: ModuleType,
) -> None:
    intent_catalog = _lending_owner_catalog("aave_v3", ["arbitrum"])
    catalog = protocol_module.build_protocol_catalog(owner_catalogs={"intent": intent_catalog})
    protocol = next(row for row in catalog["protocols"] if row["name"] == "aave_v3")

    def plans(network: str) -> list[dict[str, object]]:
        return [
            protocol_module.protocol_capability_plan(
                catalog=catalog,
                protocol="aave_v3",
                chain="arbitrum",
                network=network,
                exec_path=path,
            )
            for path in ("safe", "eoa")
        ]

    anvil = protocol_module.protocol_matrix_state(
        protocol=protocol,
        chain="arbitrum",
        environment="fork",
        cells=catalog["cells"],
        index={},
        axis_plans=plans("anvil"),
    )
    mainnet = protocol_module.protocol_matrix_state(
        protocol=protocol,
        chain="arbitrum",
        environment="mainnet",
        cells=catalog["cells"],
        index={},
        axis_plans=plans("mainnet"),
    )

    assert anvil == {
        "key": "ready",
        "label": "READY",
        "detail": "reviewed Safe and EOA pipes exist; run evidence missing",
    }
    assert mainnet == {
        "key": "gap",
        "label": "TEST PIPE MISSING",
        "detail": "0/2 execution path(s) runnable",
    }
    rendered_states = protocol_module._protocol_matrix_states(catalog, {})
    assert rendered_states["aave_v3|arbitrum|fork"] == anvil
    assert rendered_states["aave_v3|arbitrum|mainnet"] == mainnet


def test_protocol_pipeline_state_is_fail_closed_and_structurally_consistent(
    protocol_module: ModuleType,
) -> None:
    catalog = protocol_module.build_protocol_catalog()
    for protocol in catalog["protocols"]:
        contract = protocol["contract"]
        obligations = contract["custom_obligations"]
        assert len({row["id"] for row in obligations}) == len(obligations), protocol["name"]
        for row in obligations:
            assert row["owner_surface"] in {"intent", "data", "lifecycle", "protocol"}
            if row["owner_surface"] != "protocol":
                assert row["owner_contract"]
            for chain in protocol["chains"]:
                effective_owner = row["effective_owner_by_chain"][chain]
                pipeline = row["pipeline_by_chain"][chain]
                if effective_owner != "protocol":
                    assert pipeline == "ready"
                    assert row["delegation_by_chain"][chain]["status"] == "ready"
                elif row["owner_surface"] != "protocol":
                    assert pipeline == "missing"
                    assert row["delegation_by_chain"][chain]["status"] == "missing"
        for chain in protocol["chains"]:
            state = contract["protocol_pipeline_state_by_chain"][chain]
            unresolved = [
                row
                for row in obligations
                if row["effective_owner_by_chain"][chain] == "protocol" and row["pipeline_by_chain"][chain] == "missing"
            ]
            assert (state == "missing") is bool(unresolved)


def test_family_contract_is_composed_from_every_declared_intent(protocol_module: ModuleType) -> None:
    catalog = protocol_module.build_protocol_catalog()
    rows = {row["name"]: row for row in catalog["protocols"]}
    for row in rows.values():
        expected = {protocol_module.INTENT_FAMILIES[intent] for intent in row["intents"]}
        if row["name"] in protocol_module.V3_CONNECTORS and "lp" in expected:
            expected.remove("lp")
            expected.add("concentrated_liquidity_v3")
        assert expected <= set(row["contract"]["families"]), row["name"]
    assert {"swap", "lending"} <= set(rows["fluid"]["contract"]["families"])
    assert "canonical_pool_reserve_binding" in {
        obligation["id"] for obligation in rows["spark"]["contract"]["custom_obligations"]
    }


def test_declared_protocol_test_sources_exist(protocol_module: ModuleType) -> None:
    catalog = protocol_module.build_protocol_catalog()
    for protocol in catalog["protocols"]:
        for obligation in protocol["contract"]["custom_obligations"]:
            source = obligation.get("source")
            if source:
                assert (protocol_module.REPO_ROOT / source).is_file(), (protocol["name"], source)


def test_gmx_offline_probe_seals_exact_resource_evidence(
    protocol_module: ModuleType,
    tmp_path: Path,
) -> None:
    store = tmp_path / "qa"
    protocol_module.bootstrap_protocol(store)
    catalog = protocol_module.build_protocol_catalog(output=store / "catalog" / "protocol_cells.json")
    bundle = tmp_path / "bundle"
    protocol_module.run_gmx_offline_probe(output=bundle, catalog=catalog, sdk_provenance=TEST_SDK)

    target = protocol_module.seal_protocol_bundle(bundle=bundle, store=store, catalog=catalog)
    manifest = json.loads((target / "manifest.json").read_text(encoding="utf-8"))
    latest = json.loads((store / "index" / "protocol_latest.json").read_text(encoding="utf-8"))

    assert manifest["attribution_mode"] == "static-reference"
    assert manifest["evidence_status"] == "REFERENCE"
    assert manifest["git_sha"] == TEST_COMMIT
    assert len(manifest["results"]) == 2
    assert all(row["resources"] for row in manifest["results"])
    assert all(artifact["label"] != "manifest.json" for artifact in manifest["artifacts"])
    assert (target / "report.html").is_file()

    # VIB-6715: the probe scores the generated permission seed against the SDK
    # token registry and the framework's offline discovery vectors. The seed is
    # bounded to ONE representative market per chain, so the honest denominator
    # is its two verified collateral legs -- recorded in the evidence, never
    # implied.
    collateral = latest["protocol.gmx_v2.collateral_token_closure.arbitrum.offline"]
    checks = collateral["checks"]
    assert checks["denominator"] == 2
    assert checks["discovery_intents"] == ["PERP_OPEN", "PERP_CLOSE"]
    assert checks["findings"] == []
    assert collateral["verdict"] == "PASS"

    result = next(row for row in manifest["results"] if row["chain"] == "arbitrum")
    legs = {row["leg"]: row for row in result["resources"]}
    assert set(legs) == {"long", "short"}
    # Decimals are compared against the verified market record, not assumed:
    # both sources must state them, and both must agree.
    assert all(row["registry_decimals"] == row["seed_decimals"] for row in legs.values())
    assert all(row["registry_symbol"] for row in legs.values())
    # The offline permission lane really does select a leg this record accepts;
    # without this the closure check could pass while measuring nothing.
    assert legs["short"]["selected_by_discovery"] == ["PERP_OPEN", "PERP_CLOSE"]

    assert collateral["last_pass_at"] is not None
    with pytest.raises(FileExistsError):
        protocol_module.seal_protocol_bundle(bundle=bundle, store=store, catalog=catalog)


def test_gmx_offline_probe_fails_when_a_collateral_leaves_the_token_registry(
    protocol_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Negative control for the registry-closure invariant.

    ``test_gmx_offline_probe_seals_exact_resource_evidence`` above is the
    POSITIVE control: with the real registry every leg closes and the probe
    publishes PASS. If that were the only test, a probe that could never fail
    would look identical to a correct one. Here the registry loses exactly one
    collateral address and the verdict must flip, with the finding naming the
    leg -- Empty != Zero, an unresolvable address is unmeasured, never a pass.
    """
    catalog = protocol_module.build_protocol_catalog()
    baseline = protocol_module._gmx_offline_results(catalog)
    assert {row["verdict"] for row in baseline} == {"PASS"}

    arbitrum = next(row for row in baseline if row["chain"] == "arbitrum")
    dropped = next(row for row in arbitrum["resources"] if row["leg"] == "short")["collateral_address"]

    real_resolve = protocol_module._resolve_offline_token

    def _registry_without(resolver: object, reference: str, chain: str) -> object | None:
        if str(reference).lower() == dropped.lower():
            return None
        return real_resolve(resolver, reference, chain)

    monkeypatch.setattr(protocol_module, "_resolve_offline_token", _registry_without)
    degraded = protocol_module._gmx_offline_results(catalog)

    failed = next(row for row in degraded if row["chain"] == "arbitrum")
    assert failed["verdict"] == "FAIL"
    reasons = {finding["reason"] for finding in failed["checks"]["findings"]}
    assert "collateral_address_absent_from_token_registry" in reasons
    assert protocol_module._aggregate_result_verdict(degraded) == "FAIL"
    # The measured row still reports what it could not measure, rather than
    # dropping out of the denominator.
    assert failed["checks"]["denominator"] == 2
    short = next(row for row in failed["resources"] if row["leg"] == "short")
    assert short["registry_symbol"] is None and short["registry_decimals"] is None


def test_gmx_offline_probe_fails_when_discovery_proposes_an_unaccepted_collateral(
    protocol_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Negative control for the offline discovery-closure invariant.

    A synthetic vector whose collateral the verified market record rejects
    compiles to nothing, so a Safe/Zodiac deployment ships with no GMX grant.
    That must be a FAIL, not a silent green -- and an EMPTY vector list must be
    a finding too, because a lane that produced nothing measured nothing.
    """
    catalog = protocol_module.build_protocol_catalog()
    rows = [{"collateral_address": "0x" + "1" * 40, "selected_by_discovery": []}]

    # POSITIVE CONTROL: the real vectors close against the real seed.
    assert protocol_module._gmx_offline_discovery_findings("arbitrum", []) != []
    real_rows = next(row for row in protocol_module._gmx_offline_results(catalog) if row["chain"] == "arbitrum")[
        "resources"
    ]
    assert protocol_module._gmx_offline_discovery_findings("arbitrum", real_rows) == []

    # A foreign collateral is rejected by the record, never selected.
    reasons = {row["reason"] for row in protocol_module._gmx_offline_discovery_findings("arbitrum", rows)}
    assert reasons == {"discovery_collateral_rejected_by_verified_market_record"}
    assert rows[0]["selected_by_discovery"] == []

    # An empty vector list is a finding, not a vacuous pass.
    import almanak.framework.permissions.synthetic_intents as synthetic

    monkeypatch.setattr(synthetic, "build_synthetic_intents", lambda *a, **k: [])
    empty = protocol_module._gmx_offline_discovery_findings("arbitrum", real_rows)
    assert {row["reason"] for row in empty} == {"no_offline_discovery_vector_produced"}
    assert len(empty) == 2


def test_reference_seal_never_paints_protocol_matrix_pass(
    protocol_module: ModuleType,
    tmp_path: Path,
) -> None:
    catalog = protocol_module.build_protocol_catalog()
    protocol = next(row for row in catalog["protocols"] if row["name"] == "gmx_v2")
    cell_id = "protocol.gmx_v2.collateral_token_closure.arbitrum.offline"
    index = {cell_id: {"cell_id": cell_id, "verdict": "PASS", "summary": "static registry closed"}}

    state = protocol_module.protocol_matrix_state(
        protocol=protocol,
        chain="arbitrum",
        environment="offline",
        cells=catalog["cells"],
        index=index,
    )
    assert state == {"key": "reference", "label": "REFERENCE PASS", "detail": "SDK self-check only"}

    store = tmp_path / "qa"
    protocol_module.bootstrap_protocol(store)
    (store / "index" / "protocol_latest.json").write_text(json.dumps(index), encoding="utf-8")
    page = protocol_module.render_protocol_lab(store=store, lab_css="")
    rendered = page.read_text(encoding="utf-8")
    assert (
        '"gmx_v2|arbitrum|offline":{"key":"reference","label":"REFERENCE PASS",'
        '"detail":"SDK self-check only"}' in rendered
    )

    index[cell_id]["verdict"] = "FAIL"
    state = protocol_module.protocol_matrix_state(
        protocol=protocol,
        chain="arbitrum",
        environment="offline",
        cells=catalog["cells"],
        index=index,
    )
    assert state["key"] == "reference"
    assert state["label"] == "REFERENCE FINDING"


def test_authoritative_passes_cannot_hide_unresolved_protocol_obligation(protocol_module: ModuleType) -> None:
    catalog = protocol_module.build_protocol_catalog()
    protocol = next(row for row in catalog["protocols"] if row["name"] == "gmx_v2")
    runnable = [
        row
        for row in catalog["cells"]
        if row["protocol"] == "gmx_v2"
        and row["chain"] == "arbitrum"
        and row["environment"] == "mainnet"
        and row["support"] == "runnable"
    ]
    index = {row["cell_id"]: {"cell_id": row["cell_id"], "verdict": "PASS", "summary": "measured"} for row in runnable}

    state = protocol_module.protocol_matrix_state(
        protocol=protocol,
        chain="arbitrum",
        environment="mainnet",
        cells=catalog["cells"],
        index=index,
    )
    assert state["key"] == "gap"
    assert state["label"] == "TEST PIPE MISSING"


def test_protocol_seal_rejects_symlinked_evidence(protocol_module: ModuleType, tmp_path: Path) -> None:
    store = tmp_path / "qa"
    protocol_module.bootstrap_protocol(store)
    catalog = protocol_module.build_protocol_catalog()
    bundle = tmp_path / "bundle"
    protocol_module.run_gmx_offline_probe(output=bundle, catalog=catalog, sdk_provenance=TEST_SDK)
    (bundle / "escape").symlink_to(tmp_path / "outside")

    with pytest.raises(ValueError, match="symlinks"):
        protocol_module.seal_protocol_bundle(bundle=bundle, store=store, catalog=catalog)


def test_protocol_seal_rejects_self_promoted_aggregate_verdict(
    protocol_module: ModuleType,
    tmp_path: Path,
) -> None:
    store = tmp_path / "qa"
    protocol_module.bootstrap_protocol(store)
    catalog = protocol_module.build_protocol_catalog()
    bundle = tmp_path / "bundle"
    protocol_module.run_gmx_offline_probe(output=bundle, catalog=catalog, sdk_provenance=TEST_SDK)
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    # The produced manifest is self-consistent -- that is the positive control
    # for this gate: an untouched bundle seals.
    assert manifest["verdict"] == protocol_module._aggregate_result_verdict(manifest["results"])
    # Self-promotion: one result row is red while the manifest keeps claiming
    # the aggregate it published before the row changed. Written this way the
    # test does not depend on which verdict the probe naturally produces.
    manifest["results"][0]["verdict"] = "FAIL"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="fail-closed aggregate FAIL"):
        protocol_module.seal_protocol_bundle(bundle=bundle, store=store, catalog=catalog)


def test_protocol_aggregate_verdict_is_a_failloud_allowlist(protocol_module: ModuleType) -> None:
    """A denylist fails open; an allowlist fails loud.

    The old aggregate returned PASS for anything that was neither INVALID nor
    FAIL, which certified three distinct unmeasured shapes as green: a verdict
    outside the vocabulary, a row missing the key, and an EMPTY result list --
    a run that measured nothing.
    """
    aggregate = protocol_module._aggregate_result_verdict

    # POSITIVE CONTROLS: the whole measured vocabulary still aggregates, and the
    # fail-closed precedence (INVALID > FAIL > PASS) is unchanged. Without these
    # a guard that rejected everything would look identical to a correct one.
    assert aggregate([{"verdict": "PASS"}]) == "PASS"
    assert aggregate([{"verdict": "PASS"}, {"verdict": "FAIL"}]) == "FAIL"
    assert aggregate([{"verdict": "FAIL"}, {"verdict": "INVALID"}]) == "INVALID"
    assert aggregate([{"verdict": "PASS"}, {"verdict": "INVALID"}]) == "INVALID"
    assert protocol_module.PROTOCOL_RESULT_VERDICTS == {"PASS", "FAIL", "INVALID"}

    # 1. A verdict outside the measured vocabulary must raise, not fall through.
    for unknown in ("SKIP", "UNMEASURED", "ERROR", "FAILED", "pass", ""):
        with pytest.raises(ValueError, match="unrecognised verdict"):
            aggregate([{"verdict": unknown}])
    # A single bad row poisons the batch even when every other row is clean.
    with pytest.raises(ValueError, match="unrecognised verdict"):
        aggregate([{"verdict": "PASS"}, {"verdict": "SKIP"}])

    # 2. A row missing the key entirely is UNMEASURED, never PASS.
    with pytest.raises(ValueError, match="unrecognised verdict"):
        aggregate([{}])
    with pytest.raises(ValueError, match="unrecognised verdict"):
        aggregate([{"verdict": None}])
    with pytest.raises(ValueError, match="unrecognised verdict"):
        aggregate(["not-a-mapping"])

    # 3. Zero results is the same vacuity class as a zero-row receipt set.
    with pytest.raises(ValueError, match="measured nothing is not a PASS"):
        aggregate([])


def test_protocol_verdict_vocabulary_is_owned_by_one_constant(protocol_module: ModuleType) -> None:
    """The aggregate and the per-result seal check must not drift apart.

    They were two independent literals; a value added to one and missed by the
    other reopens exactly the fail-open hole the allowlist closes.
    """
    source = Path(protocol_module.__file__).read_text(encoding="utf-8")
    assert source.count('{"PASS", "FAIL", "INVALID"}') == 1, (
        "the PASS/FAIL/INVALID vocabulary must be declared once, as "
        "PROTOCOL_RESULT_VERDICTS, and referenced everywhere else"
    )


def test_gmx_authority_is_product_registry_independent_and_keeps_dynamic_canary(
    authority_module: ModuleType,
) -> None:
    source_path = Path(authority_module.__file__)
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    imported = {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names} | {
        node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
    }
    assert not any(name == "almanak" or name.startswith("almanak.") for name in imported)

    xmr = _resource("XMR", "2")
    payloads = {
        "/markets": {
            "markets": [
                {
                    **xmr,
                    "marketToken": xmr["market_token"],
                    "indexToken": xmr["index_token"],
                    "longToken": xmr["long_token"],
                    "shortToken": xmr["short_token"],
                    "isListed": True,
                }
            ]
        },
        "/tokens": {
            "tokens": [
                {"address": xmr["index_token"], "symbol": "XMR", "decimals": 18},
                {"address": xmr["long_token"], "symbol": "XMR", "decimals": 18},
                {"address": xmr["short_token"], "symbol": "USDC", "decimals": 6},
            ]
        },
    }
    snapshot = authority_module.snapshot_gmx_catalogue(
        "arbitrum",
        fetch_json=lambda url: payloads["/markets" if url.endswith("/markets") else "/tokens"],
    )

    assert snapshot["entry_count"] == 1
    assert snapshot["resources"][0]["index_symbol"] == "XMR"
    assert snapshot["independence"] == "venue API; no Almanak GMX connector imports"


def test_gmx_resource_closure_exposes_exact_denominator_and_product_gap(
    protocol_module: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = tmp_path / "qa"
    protocol_module.bootstrap_protocol(store)
    catalog = protocol_module.build_protocol_catalog()
    hype, xmr = _resource("HYPE", "1"), _resource("XMR", "2")
    resources = [hype, xmr]
    snapshot = {
        "schema_version": 1,
        "source_id": "gmxinfra.markets+tokens",
        "base_url": "https://example.invalid",
        "chain": "arbitrum",
        "fetched_at": "2026-08-07T00:00:00+00:00",
        "catalogue_sha256": "a" * 64,
        "entry_count": 2,
        "selection": "fixture",
        "independence": "venue fixture; no product registry",
        "resources": resources,
    }
    monkeypatch.setattr(
        protocol_module,
        "_decode_gmx_open_protection",
        lambda bundle, chain: {
            "acceptable_price_30dec": bundle["metadata"]["acceptable_price_30dec"],
            "market_address": bundle["metadata"]["market_address"],
            "index_token_decimals": bundle["metadata"]["index_token_decimals"],
        },
    )
    monkeypatch.setattr(protocol_module, "_clean_git_provenance", lambda _requested: "d" * 40)
    bundle = tmp_path / "bundle"
    client = _fake_client(resources)
    protocol_module.run_gmx_resource_closure(
        output=bundle,
        catalog=catalog,
        chain="arbitrum",
        sdk_provenance=TEST_SDK,
        gateway_client=client,
        authority_snapshot=snapshot,
    )
    assert set(client.execution.seen_markets) == {str(row["resource_id"]) for row in resources}
    target = protocol_module.seal_protocol_bundle(bundle=bundle, store=store, catalog=catalog)
    manifest = json.loads((target / "manifest.json").read_text(encoding="utf-8"))
    latest = json.loads((store / "index" / "protocol_latest.json").read_text(encoding="utf-8"))
    consumer = latest["protocol.gmx_v2.consumer_closure.arbitrum.mainnet"]

    assert manifest["evidence_kind"] == "almanak.protocol.resource-closure"
    assert manifest["support_surface"]["denominator"] == 2
    assert manifest["support_surface"]["absent_from_both_dynamic_canaries"] >= 1
    assert manifest["harness"]["money_moved"] is False
    assert consumer["verdict"] == "FAIL"
    assert consumer["checks"]["denominator"] == 2
    assert consumer["checks"]["outcomes"] == {"COMPILE_CLOSED": 1, "PRODUCT_GAP": 1}
    xmr_row = next(row for row in consumer["resources"] if row["label"] == "XMR/USD")
    assert xmr_row["outcome"] == "PRODUCT_GAP"
    # Pin WHICH mechanism produced the gap. Without this the assertion passes on
    # any PRODUCT_GAP, and it did: the gap used to come from a decoded-market
    # mismatch while CANONICAL_COMPILE reported PASS, leaving the compile
    # rejection this test is named for with no coverage at all.
    assert xmr_row["reason_code"] == "compile_rejected"
    assert xmr_row["stages"]["CANONICAL_COMPILE"]["state"] == "FAIL"
    catalogue = latest["protocol.gmx_v2.resource_catalogue_reconciliation.arbitrum.mainnet"]
    assert all(row["reason_code"] is None for row in catalogue["resources"])
    decimals = latest["protocol.gmx_v2.synthetic_decimal_authority.arbitrum.mainnet"]
    assert decimals["verdict"] == "FAIL"
    assert decimals["checks"]["denominator"] == 2
    hype_decimals = next(row for row in decimals["resources"] if row["label"] == "HYPE/USD")
    assert hype_decimals["outcome"] == "DECIMALS_CLOSED"
    assert hype_decimals["authority_index_decimals"] == 18
    assert hype_decimals["gateway_index_decimals"] == 18
    assert hype_decimals["compiled_index_decimals"] == 18
    assert consumer["last_pass_at"] is None
    report = (target / "report.html").read_text(encoding="utf-8")
    assert "Claim boundary" in report
    assert "2</b> PRODUCT_GAP" not in report
    assert "1</b> PRODUCT_GAP" in report
    assert "XMR/USD" in report
    assert "price $100" in report
    assert "source test-authority" in report
    assert 'id="resource-search"' in report
    assert "Attention only" in report


def test_gmx_decoded_market_mismatch_is_its_own_product_gap_mechanism(
    protocol_module: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A compile that SUCCEEDS but returns a foreign market is still a gap.

    This is the second, independent way a resource reaches PRODUCT_GAP, and it
    used to be the only one this file actually exercised -- the modelled
    PRICE_UNAVAILABLE sentinel never matched, so both resources compiled and the
    hardcoded market mismatched. Fixing the sentinel moved the sibling test onto
    the compile-rejection path, which would have left this mechanism uncovered.
    Keep both, and keep them distinguishable by ``reason_code``.
    """
    store = tmp_path / "qa"
    protocol_module.bootstrap_protocol(store)
    catalog = protocol_module.build_protocol_catalog()
    hype = _resource("HYPE", "1")
    resources = [hype]
    snapshot = {
        "schema_version": 1,
        "source_id": "gmxinfra.markets+tokens",
        "base_url": "https://example.invalid",
        "chain": "arbitrum",
        "fetched_at": "2026-08-07T00:00:00+00:00",
        "catalogue_sha256": "a" * 64,
        "entry_count": 1,
        "selection": "fixture",
        "independence": "venue fixture; no product registry",
        "resources": resources,
    }
    monkeypatch.setattr(
        protocol_module,
        "_decode_gmx_open_protection",
        lambda bundle, chain: {
            "acceptable_price_30dec": bundle["metadata"]["acceptable_price_30dec"],
            "market_address": bundle["metadata"]["market_address"],
            "index_token_decimals": bundle["metadata"]["index_token_decimals"],
        },
    )
    monkeypatch.setattr(protocol_module, "_clean_git_provenance", lambda _requested: "d" * 40)
    bundle = tmp_path / "bundle"
    protocol_module.run_gmx_resource_closure(
        output=bundle,
        catalog=catalog,
        chain="arbitrum",
        sdk_provenance=TEST_SDK,
        gateway_client=_fake_client(resources, mismatch_market=True),
        authority_snapshot=snapshot,
    )
    protocol_module.seal_protocol_bundle(bundle=bundle, store=store, catalog=catalog)
    latest = json.loads((store / "index" / "protocol_latest.json").read_text(encoding="utf-8"))
    consumer = latest["protocol.gmx_v2.consumer_closure.arbitrum.mainnet"]

    row = next(r for r in consumer["resources"] if r["label"] == "HYPE/USD")
    assert row["outcome"] == "PRODUCT_GAP"
    assert row["reason_code"] == "protection_decode_failed"
    # The distinguishing fact: the compile itself was fine.
    assert row["stages"]["CANONICAL_COMPILE"]["state"] == "PASS"
    assert row["stages"]["PROTECTION_DECODE"]["state"] == "FAIL"
    assert consumer["verdict"] == "FAIL"


def test_transport_failure_seals_invalid_without_painting_product_fail(
    protocol_module: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(protocol_module, "_clean_git_provenance", lambda _requested: "d" * 40)
    catalog = protocol_module.build_protocol_catalog()
    resource = _resource("XMR", "2")
    snapshot = {
        "chain": "arbitrum",
        "entry_count": 1,
        "resources": [resource],
        "source_id": "fixture",
    }
    bundle = tmp_path / "bundle"
    protocol_module.run_gmx_resource_closure(
        output=bundle,
        catalog=catalog,
        chain="arbitrum",
        gateway_client=_fake_client([resource], transport_error=True),
        authority_snapshot=snapshot,
        sdk_provenance=TEST_SDK,
    )
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))

    assert manifest["verdict"] == "INVALID"
    assert {result["verdict"] for result in manifest["results"]} == {"INVALID"}


def test_transient_transport_failure_is_retried_and_preserved(
    protocol_module: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(protocol_module, "_clean_git_provenance", lambda _requested: "d" * 40)
    catalog = protocol_module.build_protocol_catalog()
    resource = _resource("HYPE", "1")
    snapshot = {
        "chain": "arbitrum",
        "entry_count": 1,
        "resources": [resource],
        "source_id": "fixture",
    }
    client = _fake_client([resource])
    original = client.market.GetPerpMarket
    attempts = 0

    def transient(request: object, *, timeout: float) -> object:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("transient gateway outage")
        return original(request, timeout=timeout)

    client.market.GetPerpMarket = transient
    monkeypatch.setattr(protocol_module.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        protocol_module,
        "_decode_gmx_open_protection",
        lambda bundle, chain: {
            "acceptable_price_30dec": bundle["metadata"]["acceptable_price_30dec"],
            "market_address": bundle["metadata"]["market_address"],
            "index_token_decimals": bundle["metadata"]["index_token_decimals"],
        },
    )
    bundle = tmp_path / "bundle"
    protocol_module.run_gmx_resource_closure(
        output=bundle,
        catalog=catalog,
        chain="arbitrum",
        gateway_client=client,
        authority_snapshot=snapshot,
        sdk_provenance=TEST_SDK,
    )
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    consumer = manifest["results"][1]

    assert manifest["verdict"] == "PASS"
    assert consumer["resources"][0]["attempts"] == 2
    assert consumer["resources"][0]["recovered_transport_failures"][0]["reason_code"] == "gateway_transport"


def test_provider_exhaustion_is_product_gap_not_transport(protocol_module: ModuleType) -> None:
    import grpc

    class ProviderUnavailable(grpc.RpcError):
        def code(self) -> grpc.StatusCode:
            return grpc.StatusCode.UNAVAILABLE

        def details(self) -> str:
            return "All data sources failed: Unknown token: XMR"

    assert protocol_module._is_transport_error(ProviderUnavailable()) is False


def test_provider_rate_limit_is_transport_not_product_gap(protocol_module: ModuleType) -> None:
    import grpc

    class ProviderRateLimited(grpc.RpcError):
        def code(self) -> grpc.StatusCode:
            return grpc.StatusCode.UNAVAILABLE

        def details(self) -> str:
            return "All data sources failed: coingecko rate limited. Retry after 10.0s"

    assert protocol_module._is_transport_error(ProviderRateLimited()) is True


def test_protocol_git_provenance_is_derived_and_fail_closed(
    protocol_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outputs = {
        ("rev-parse", "HEAD"): "a" * 40,
        ("status", "--porcelain"): "",
    }

    def clean_run(argv: list[str], **_kwargs: object) -> object:
        return SimpleNamespace(stdout=outputs[tuple(argv[1:])])

    monkeypatch.setattr(protocol_module.subprocess, "run", clean_run)
    assert protocol_module._clean_git_provenance("auto") == "a" * 40
    with pytest.raises(ValueError, match="does not match"):
        protocol_module._clean_git_provenance("b" * 40)

    outputs[("status", "--porcelain")] = " M qa_protocol.py"
    with pytest.raises(ValueError, match="clean committed"):
        protocol_module._clean_git_provenance("a" * 40)
