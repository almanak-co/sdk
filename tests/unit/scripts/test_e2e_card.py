"""Preparation attack controls; none of these tests claim chain execution."""

from __future__ import annotations

import json
import subprocess
from decimal import Decimal
from types import SimpleNamespace

import pytest
from eth_abi import encode
from web3 import Web3

from qa_lab.e2e_actor_binding import bind_actor_config, materialize_actor
from qa_lab.e2e_card import (
    REPO,
    STRATEGY,
    TEMPLATE,
    Scenario,
    bind_pool_input,
    canonical,
    digest,
    load_json,
    prepare,
    target_interval,
    verify_preparation,
)


@pytest.fixture
def checkout(tmp_path):
    repo = tmp_path / "repo"
    recipe = repo / STRATEGY
    recipe.mkdir(parents=True)
    (recipe / "config.json").write_bytes((REPO / STRATEGY / "config.json").read_bytes())
    (recipe / "strategy.py").write_text("# Owned runtime fixture\n")
    (repo / "almanak").mkdir()
    (repo / "almanak" / "runner.py").write_text("pass\n")
    for args in (
        ["init", "-q"],
        ["add", "."],
        ["-c", "user.name=QA", "-c", "user.email=qa@example.invalid", "commit", "-qm", "fixture"],
    ):
        subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)
    return repo


def _prepare(checkout, tmp_path):
    destination = tmp_path / "run"
    prepare(checkout, REPO / TEMPLATE, destination, "lp-dual-test-001")
    return destination


@pytest.fixture
def actor_binding(checkout, tmp_path, monkeypatch, request):
    from qa_lab import e2e_actor_binding

    monkeypatch.setattr(
        e2e_actor_binding, "validate_owned_quote", lambda *args, **kwargs: {"claim": "geometry tested separately"}
    )
    from almanak.connectors.uniswap_v3.addresses import UNISWAP_V3
    from almanak.framework.anvil.accounts import anvil_default_address
    from qa_lab.chains import TOKENS

    actor_source = checkout / "qa_lab/strategies/lp_stimulus"
    actor_source.mkdir(parents=True)
    for name in ("strategy.py", "__init__.py"):
        (actor_source / name).write_bytes((REPO / "qa_lab/strategies/lp_stimulus" / name).read_bytes())
    if hasattr(request, "param"):
        scenario = load_json(REPO / TEMPLATE)
        scenario["stimulus"]["max_slippage"] = request.param
        template = tmp_path / "actor-scenario.json"
        template.write_bytes(canonical(scenario))
        preparation = tmp_path / "run"
        prepare(checkout, template, preparation, "lp-dual-test-001")
    else:
        preparation = _prepare(checkout, tmp_path)
    anchor = tmp_path / "anchor.json"
    anchor.write_text(json.dumps({"chain_id": 42161, "fork_block": 100, "fork_hash": "0x" + "aa" * 32}))
    manifest_path = tmp_path / "pool-input.json"
    bind_pool_input(preparation, checkout, tmp_path / "subject", anchor, manifest_path)
    identity = {"instance_id": "bound-fork", "chain_id": 42161, "fork_block": 100, "fork_hash": "0x" + "aa" * 32}
    response = encode(["uint256", "uint160", "uint32", "uint256"], [10, 2**96, 1, 50000])
    calls = []

    def call(tx, block_identifier):
        calls.append((tx, block_identifier))
        if tx["data"].startswith("0x70a08231"):
            amount = 10**13 if tx["to"].lower() == TOKENS["arbitrum"]["USDC"][0].lower() else 0
            return encode(["uint256"], [amount])
        return response

    client = SimpleNamespace(
        eth=SimpleNamespace(
            call=call,
            get_block=lambda number: {"hash": bytes.fromhex("aa" * 32)},
            get_balance=lambda wallet, block: 10**18,
            get_transaction_count=lambda wallet, block: 7017,
        )
    )
    context = SimpleNamespace(
        **identity,
        chain="arbitrum",
        network="anvil",
        public_identity=lambda: identity,
        assert_rpc_identity=lambda client_arg=None: client,
    )
    payload = Web3.keccak(text="quoteExactInputSingle((address,address,uint256,uint24,uint160))")[:4] + encode(
        ["address", "address", "uint256", "uint24", "uint160"],
        [TOKENS["arbitrum"]["USDC"][0], TOKENS["arbitrum"]["WETH"][0], 100, 500, 0],
    )
    selected = {
        "amount_in_raw": "100",
        "amount_out_raw": "10",
        "calldata": Web3.to_hex(payload),
        "response": Web3.to_hex(response),
        "weth_usdc_after": "1000000000000",
    }
    quote = {
        "status": "QUOTED",
        "selected": selected,
        "quotes": [selected],
        "max_usdc_raw": "10000000000000",
        "target_min": "990000000000",
        "target_max": "1010000000000",
        "quoter": UNISWAP_V3["arbitrum"]["quoter_v2"],
        "before": {"fork_identity": identity, "block_number": 101, "block_hash": identity["fork_hash"]},
    }
    provisioning = {
        "fork_identity": {**identity, "manifest_sha256": digest(manifest_path.read_bytes())},
        "wallet": anvil_default_address(1),
        "nonce": 7017,
        "usdc_raw": "10000000000000",
        "native_wei": str(10**18),
        "block_number": 101,
        "block_hash": identity["fork_hash"],
    }
    quote_path, provisioning_path = tmp_path / "quote.json", tmp_path / "provisioning.json"
    quote_path.write_text(json.dumps(quote))
    provisioning_path.write_text(json.dumps(provisioning))
    arguments = {
        "preparation": preparation,
        "repo": checkout,
        "quote_path": quote_path,
        "provisioning_path": provisioning_path,
        "context": context,
        "subject_wallet": anvil_default_address(0),
    }
    return SimpleNamespace(arguments=arguments, quote=quote, calls=calls, client=client)


def test_actor_binding_replays_quote_and_preserves_measured_nonce(actor_binding):
    bound = bind_actor_config(**actor_binding.arguments)
    assert bound["config"]["amount_usdc_raw"] == 100
    assert bound["config"]["provisioned_nonce"] == 7017
    assert bound["execution_status"] == "UNMEASURED"
    assert len(actor_binding.calls) == 3
    assert all(block == 101 for _, block in actor_binding.calls)


@pytest.mark.parametrize("actor_binding", ["0.001"], indirect=True)
def test_actor_binding_preserves_a_stricter_frozen_slippage_limit(actor_binding):
    bound = bind_actor_config(**actor_binding.arguments)
    assert bound["config"]["max_slippage"] == "0.001"
    assert bound["config"]["max_swaps"] == 3
    assert (bound["config"]["usdc_decimals"], bound["config"]["weth_decimals"]) == (6, 18)


@pytest.mark.parametrize("fault", ["unreachable", "calldata", "response", "fork", "target"])
def test_actor_binding_refuses_invalid_quote_handoff(actor_binding, fault):
    quote = actor_binding.quote
    if fault == "unreachable":
        quote["status"] = "UNREACHABLE"
    elif fault == "calldata":
        quote["selected"]["calldata"] = "0x"
    elif fault == "response":
        quote["selected"]["response"] = "0x"
    elif fault == "fork":
        quote["before"]["fork_identity"] = {**quote["before"]["fork_identity"], "instance_id": "other"}
    else:
        quote["target_min"], quote["target_max"] = "2000000000000", "3000000000000"
    actor_binding.arguments["quote_path"].write_text(json.dumps(quote))
    with pytest.raises(ValueError):
        bind_actor_config(**actor_binding.arguments)


def test_actor_folder_is_bound_and_never_overwritten(actor_binding):
    output = actor_binding.arguments["preparation"].parent / "actor"
    bound = materialize_actor(output=output, **actor_binding.arguments)
    assert digest((output / "config.json").read_bytes()) == bound["bindings"]["config_sha256"]
    assert load_json(output / "config.json")["provisioned_nonce"] == 7017
    assert (output / "strategy.py").read_bytes() == (REPO / "qa_lab/strategies/lp_stimulus/strategy.py").read_bytes()
    for name in ("quote", "provisioning"):
        raw = (output / f"{name}.json").read_bytes()
        assert raw == actor_binding.arguments[f"{name}_path"].read_bytes()
        assert digest(raw) == bound["bindings"][f"{name}_sha256"]
    with pytest.raises(ValueError, match="fresh directory"):
        materialize_actor(output=output, **actor_binding.arguments)


@pytest.mark.parametrize("name", ["quote", "provisioning"])
def test_actor_materialization_rejects_input_changed_during_verification(actor_binding, monkeypatch, name):
    from qa_lab import e2e_actor_binding

    def changed(**arguments):
        result = bind_actor_config(**arguments)
        source = arguments[f"{name}_path"]
        source.write_bytes(source.read_bytes() + b"\n")
        return result

    monkeypatch.setattr(e2e_actor_binding, "bind_actor_config", changed)
    output = actor_binding.arguments["preparation"].parent / "actor"
    with pytest.raises(ValueError, match="changed during independent verification"):
        materialize_actor(output=output, **actor_binding.arguments)
    assert not output.exists()


def test_actor_materialization_rejects_aliased_input(actor_binding):
    arguments = dict(actor_binding.arguments)
    source = arguments["quote_path"]
    alias = source.with_name("quote-alias.json")
    alias.symlink_to(source)
    arguments["quote_path"] = alias
    output = arguments["preparation"].parent / "actor"
    with pytest.raises(ValueError, match="owned files"):
        materialize_actor(output=output, **arguments)
    assert not output.exists()


def test_actor_binding_rejects_edited_provisioning_nonce(actor_binding):
    path = actor_binding.arguments["provisioning_path"]
    provisioning = load_json(path)
    provisioning["nonce"] = 7018
    path.write_text(json.dumps(provisioning))
    with pytest.raises(ValueError, match="nonce.*disagrees with chain"):
        bind_actor_config(**actor_binding.arguments)


def test_preparation_freezes_recipe_without_claiming_launch_readiness(checkout, tmp_path):
    destination = _prepare(checkout, tmp_path)
    card = load_json(destination / "card.json")
    config = load_json(destination / "config.json")
    assert card["launch_status"] == "BLOCKED"
    assert card["not_certified"] == ["execution", "duration", "books", "cleanup", "runner_recovery"]
    assert config["rebalance_enabled"] is True
    assert config["min_rebalance_interval_seconds"] == 600
    assert config["max_rebalances_per_day"] == 6
    assert config["rebalance_confirm_iterations"] == 3
    funding = load_json(destination / "funding.json")
    native = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    assert funding["subject_requested_token_amounts"][native] == "1"
    assert funding["subject_effective_token_amounts"][native] == "100"
    assert funding["balance_status"] == "UNMEASURED"
    assert card["schema_version"] == 3
    assert verify_preparation(destination, checkout)["launch_status"] == "BLOCKED"
    assert "rebalance_enabled" not in load_json(checkout / STRATEGY / "config.json")


@pytest.mark.parametrize(
    "artifact", ["scenario.json", "config.json", "resources.json", "funding.json", "lifecycle-contract.json"]
)
def test_edited_prepared_artifact_is_rejected(checkout, tmp_path, artifact):
    destination = _prepare(checkout, tmp_path)
    with (destination / artifact).open("a") as stream:
        stream.write("\n")
    with pytest.raises(ValueError, match="Prepared artifact changed"):
        verify_preparation(destination, checkout)


def test_funding_cannot_be_rehashed_to_hide_native_minimum(checkout, tmp_path):
    destination = _prepare(checkout, tmp_path)
    funding = load_json(destination / "funding.json")
    funding["subject_effective_token_amounts"]["0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"] = "1"
    (destination / "funding.json").write_text(json.dumps(funding))
    card = load_json(destination / "card.json")
    card["artifacts"]["funding.json"] = digest((destination / "funding.json").read_bytes())
    (destination / "card.json").write_text(json.dumps(card))
    with pytest.raises(ValueError, match="managed gateway policy"):
        verify_preparation(destination, checkout)


@pytest.mark.parametrize("mutation", ["edit", "extra", "delete", "symlink"])
def test_runtime_change_cannot_reuse_preparation(checkout, tmp_path, mutation):
    destination = _prepare(checkout, tmp_path)
    target = checkout / "almanak" / "runner.py"
    if mutation == "edit":
        target.write_text("raise RuntimeError\n")
    elif mutation == "extra":
        (target.parent / "new_provider.py").write_text("pass\n")
    else:
        target.unlink()
        if mutation == "symlink":
            target.symlink_to(REPO / STRATEGY / "strategy.py")
    with pytest.raises(ValueError, match="resources changed|missing or not owned"):
        verify_preparation(destination, checkout)


def test_preparation_never_overwrites_existing_run(checkout, tmp_path):
    destination = _prepare(checkout, tmp_path)
    before = (destination / "card.json").read_bytes()
    with pytest.raises(ValueError, match="never overwrites"):
        prepare(checkout, REPO / TEMPLATE, destination, "lp-dual-test-001")
    assert (destination / "card.json").read_bytes() == before


def test_preparation_cannot_self_promote(checkout, tmp_path):
    destination = _prepare(checkout, tmp_path)
    card = load_json(destination / "card.json")
    card["launch_status"] = "PASS"
    (destination / "card.json").write_text(json.dumps(card))
    with pytest.raises(ValueError, match="cannot authorize or certify"):
        verify_preparation(destination, checkout)


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("blockers", [], "prerequisites"),
        ("not_certified", [], "prerequisites"),
        ("range_geometry", {}, "geometry"),
        ("source_commit", "f" * 40, "commit changed"),
    ],
)
def test_preparation_metadata_cannot_hide_unknowns_or_change_identity(checkout, tmp_path, field, value, reason):
    destination = _prepare(checkout, tmp_path)
    card = load_json(destination / "card.json")
    card[field] = value
    (destination / "card.json").write_text(json.dumps(card))
    with pytest.raises(ValueError, match=reason):
        verify_preparation(destination, checkout)


def test_forced_decision_config_is_not_silently_inherited(checkout, tmp_path):
    path = checkout / STRATEGY / "config.json"
    config = load_json(path)
    config["force_action"] = "close"
    path.write_text(json.dumps(config))
    with pytest.raises(ValueError, match="not covered"):
        _prepare(checkout, tmp_path)


@pytest.mark.parametrize(
    "key,value",
    [
        ("network", "mainnet"),
        ("chain", "base"),
        ("execution_path", "safe"),
        ("deployment_surface", "hosted"),
        ("required_rebalances", 0),
        ("typo", 1),
    ],
)
def test_card_rejects_other_surfaces_and_unknown_fields(key, value):
    scenario = load_json(REPO / TEMPLATE)
    scenario[key] = value
    with pytest.raises(ValueError):
        Scenario.model_validate(scenario)


def _interval(**overrides):
    kwargs = {
        "initial_price": Decimal("2500"),
        "narrow_lower": Decimal("2375"),
        "narrow_upper": Decimal("2625"),
        "wide_lower": Decimal("2000"),
        "wide_upper": Decimal("3000"),
        "exit_buffer": Decimal("0.01"),
    }
    kwargs.update(overrides)
    return target_interval(Scenario.model_validate(load_json(REPO / TEMPLATE)), **kwargs)


def test_target_clears_narrow_hysteresis_without_disturbing_wide_leg():
    measured = _interval()
    assert measured["target_price_min"] == "2675.00"
    assert measured["target_price_max"] == "2700.00"
    for price in (Decimal(measured["target_price_min"]), Decimal(measured["target_price_max"])):
        assert price > Decimal("2625") + price * Decimal("0.01")
        assert Decimal("2000") < price < Decimal("3000")
    assert measured["claim"] == "range_geometry_only"


@pytest.mark.parametrize(
    "kwargs,reason",
    [
        ({"narrow_upper": Decimal("2670")}, "hysteresis"),
        ({"wide_upper": Decimal("2690")}, "unaffected"),
        ({"narrow_lower": Decimal("2600")}, "nested"),
        ({"initial_price": Decimal("NaN")}, "finite"),
        ({"exit_buffer": Decimal("1")}, "finite"),
    ],
)
def test_actual_opened_ranges_can_refuse_the_planned_stimulus(kwargs, reason):
    with pytest.raises(ValueError, match=reason):
        _interval(**kwargs)


def test_duplicate_json_keys_are_rejected(tmp_path):
    path = tmp_path / "card.json"
    path.write_text('{"network":"mainnet","network":"anvil"}')
    with pytest.raises(ValueError, match="Duplicate JSON key"):
        load_json(path)


def test_wrong_catalog_config_chain_is_rejected(checkout, tmp_path):
    config_path = checkout / STRATEGY / "config.json"
    config = load_json(config_path)
    config["chain"] = "base"
    config_path.write_text(json.dumps(config))
    with pytest.raises(ValueError, match="authored Arbitrum"):
        _prepare(checkout, tmp_path)


def test_bound_subject_uses_exact_prepared_config_and_source(checkout, tmp_path):
    dashboard = checkout / STRATEGY / "dashboard" / "ui.py"
    dashboard.parent.mkdir()
    dashboard.write_text("def render(): return 'owned dashboard'\n")
    preparation = _prepare(checkout, tmp_path)
    anchor = tmp_path / "anchor.json"
    anchor.write_text(json.dumps({"chain_id": 42161, "fork_block": 123, "fork_hash": "0x" + "a" * 64}))
    subject = tmp_path / "subject"
    output = tmp_path / "input.json"
    manifest = bind_pool_input(preparation, checkout, subject, anchor, output)
    assert (subject / "config.json").read_bytes() == (preparation / "config.json").read_bytes()
    assert (subject / "strategy.py").read_bytes() == (checkout / STRATEGY / "strategy.py").read_bytes()
    assert (subject / "dashboard" / "ui.py").read_bytes() == dashboard.read_bytes()
    assert manifest["config_sha256"] == digest((subject / "config.json").read_bytes())
    assert manifest["preparation_sha256"] == digest((preparation / "card.json").read_bytes())
    assert manifest["database_path"] == str(subject / "almanak_state.db")
    with pytest.raises(ValueError, match="fresh subject"):
        bind_pool_input(preparation, checkout, subject, anchor, output)


def test_binding_refuses_wrong_chain_before_creating_subject(checkout, tmp_path):
    preparation = _prepare(checkout, tmp_path)
    anchor = tmp_path / "anchor.json"
    anchor.write_text(json.dumps({"chain_id": 1, "fork_block": 123, "fork_hash": "0x" + "a" * 64}))
    subject = tmp_path / "subject"
    with pytest.raises(ValueError, match="Arbitrum"):
        bind_pool_input(preparation, checkout, subject, anchor, tmp_path / "input.json")
    assert not subject.exists()


def test_subject_worker_uses_frozen_continuous_managed_command(actor_binding):
    from qa_lab.e2e_worker import subject_command

    args = actor_binding.arguments
    command, environment = subject_command(args["preparation"], args["repo"], gateway_port=55391, supervised=True)
    assert command[:6] == ("uv", "run", "--no-sync", "almanak", "strat", "run")
    assert command[command.index("--network") + 1] == "anvil"
    assert command[command.index("--wallet") + 1] == "isolated"
    assert command[command.index("--log-file") + 1] == str(args["preparation"].parent / "runner-events.jsonl")
    assert not {"--once", "--max-iterations", "--teardown-after", "--anvil-port", "--id"}.intersection(command)
    assert environment["ANVIL_FORK_BLOCK_ARBITRUM"] == "100"
    assert environment["PYTHONPATH"] == str(args["repo"])


@pytest.mark.parametrize("fault", ["source", "prior_database", "config", "database_path", "funding", "wallet"])
def test_subject_worker_rejects_materialization_attacks(actor_binding, fault):
    from qa_lab.e2e_worker import subject_command

    args = actor_binding.arguments
    root = args["preparation"].parent
    if fault == "source":
        (root / "subject/strategy.py").write_text("changed")
    elif fault == "prior_database":
        (root / "subject/almanak_state.db").write_bytes(b"old state")
    elif fault == "config":
        (root / "subject/config.json").write_text("{}")
    else:
        path = root / "pool-input.json"
        manifest = load_json(path)
        key, value = {
            "database_path": ("database_path", str(root / "other/almanak_state.db")),
            "funding": ("stimulus_usdc_raw", 1),
            "wallet": ("stimulus_wallet", "0x" + "11" * 20),
        }[fault]
        manifest[key] = value
        path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="materialization|manifest"):
        subject_command(args["preparation"], args["repo"], gateway_port=55391, supervised=True)


def test_subject_worker_cannot_turn_preparation_into_unattended_permission(actor_binding):
    from qa_lab.e2e_worker import subject_command

    args = actor_binding.arguments
    with pytest.raises(ValueError, match="Unattended"):
        subject_command(args["preparation"], args["repo"], gateway_port=55391, supervised=False)


@pytest.mark.parametrize(
    "obligation",
    [
        "wallet_quantities",
        "pool_price_inputs",
        "actor_terminal",
        "actor_quantities",
        "rebalance_price_cycle",
        "stimulus_price_link",
        "residual_policy",
    ],
)
def test_prepared_lifecycle_cannot_drop_a_required_obligation_even_with_rehashed_card(checkout, tmp_path, obligation):
    destination = tmp_path / "prepared"
    prepare(checkout, REPO / TEMPLATE, destination, "frozen-contract-test")
    path = destination / "lifecycle-contract.json"
    contract = load_json(path)
    assert contract["claim_scope"] == {
        "required": ["strategy"],
        "not_applicable": [],
        "unmeasured": ["books", "dashboard", "harness"],
    }
    assert contract["sampled_hold"]["minimum_seconds"] == 5400
    del contract[obligation]
    path.write_bytes(canonical(contract))
    card_path = destination / "card.json"
    card = load_json(card_path)
    card["artifacts"][path.name] = digest(path.read_bytes())
    card_path.write_bytes(canonical(card))
    with pytest.raises(ValueError, match="lifecycle contract differs"):
        verify_preparation(destination, checkout)


def test_preparation_without_prelaunch_contract_cannot_be_upgraded_after_execution(checkout, tmp_path):
    destination = tmp_path / "prepared"
    prepare(checkout, REPO / TEMPLATE, destination, "historical-card-test")
    card_path = destination / "card.json"
    card = load_json(card_path)
    card["schema_version"] = 2
    card["artifacts"].pop("lifecycle-contract.json")
    (destination / "lifecycle-contract.json").unlink()
    card_path.write_bytes(canonical(card))
    with pytest.raises(ValueError, match="cannot authorize or certify"):
        verify_preparation(destination, checkout)


def test_subject_worker_cannot_append_an_earlier_runner_epoch(actor_binding):
    from qa_lab.e2e_worker import subject_command

    args = actor_binding.arguments
    (args["preparation"].parent / "runner-events.jsonl").write_text("earlier run")
    with pytest.raises(ValueError, match="prior process epochs"):
        subject_command(args["preparation"], args["repo"], gateway_port=55391, supervised=True)
