import json
import signal
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.caip import AccountReference
from almanak.configuration.composed import Author as ComposedAuthor
from almanak.connectors.composed.fork import ComposedFork
from almanak.connectors.composition import checkpoint_from_json
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.target_routing import target_connector
from almanak.gateway.service import GatewayService
from almanak.models import Account, AccountBinding, Chain, Eoa, SafeRoles
from scripts.legacy_configuration.author import read_author
from tests.fork import aave_v3_gateway_driver as driver
from tests.fork.test_aave_v3_base_eoa import eoa_configuration
from tests.fork.test_aave_v3_recovery import fault_gateway_driver
from tests.legacy_references import frozen_effective
from tests.lending_captures import read_capture
from tests.uniswap_support import scope_only_json


@pytest.fixture
def config():
    p = Path(__file__).parent / "fixtures/aave-v4-composed-ethereum-initialized.json.gz"
    return checkpoint_from_json(scope_only_json(read_capture(p))).config


@pytest.mark.parametrize("fault", [None, "production", "unbound", "other_protocol"])
def test_crash_fixture_requires_explicit_aave_fork(config, fault):
    if fault == "production":
        config = config.model_copy(update={"production": object()})
    if fault == "unbound":
        config = config.model_copy(update={"fork_bindings": ()})
    if fault == "other_protocol":
        config = SimpleNamespace(connector="curve", production=None, fork_bindings=(object(),))
    if fault is None:
        driver.validate_fault_config(config)
    else:
        with pytest.raises(ValueError, match="identified fork"):
            driver.validate_fault_config(config)


@pytest.mark.parametrize("connector", ["aave_v3", "aave_v4"])
def test_standalone_string_connector_configs_are_aave_components(connector):
    config = SimpleNamespace(connector=connector, production=None, fork_bindings=(object(),))
    driver.validate_fault_config(config)


@pytest.mark.parametrize("kind", ["borrow", "approve", None])
def test_crash_happens_only_after_an_aave_borrow_broadcast(monkeypatch, config, kind):
    pending = (
        None
        if kind is None
        else SimpleNamespace(operation=SimpleNamespace(kind=kind, target=config.target("loan")))
    )
    service = object.__new__(driver.InterruptedService)
    service.journal = SimpleNamespace(load=lambda: SimpleNamespace(pending=pending))
    calls = []
    monkeypatch.setattr(GatewayService, "dispatch", lambda *args: "broadcast response")
    monkeypatch.setattr(driver.os, "kill", lambda pid, sig: calls.append((pid, sig)))
    monkeypatch.setattr(driver.os, "getpid", lambda: 123)
    assert driver.InterruptedService.dispatch(service, "broadcast", {}) == "broadcast response"
    assert calls == ([(123, signal.SIGKILL)] if kind == "borrow" else [])


@pytest.mark.parametrize("profile", ("eoa", "safe_roles"))
@pytest.mark.parametrize(
    "reference", ("bnb_aave_borrow_and_trade_v4", "bnb_aave_pancake_borrow_and_trade")
)
def test_bnb_composition_profiles_route_to_aave_fault_driver(tmp_path, profile, reference):
    if reference == "bnb_aave_pancake_borrow_and_trade":
        author = ComposedAuthor.model_validate_json(
            json.dumps(frozen_effective(reference, "config.yaml"))
        )
    else:
        fixture = Path(__file__).parent / "fixtures/author/candidates" / reference
        author = read_author(fixture / "config.yaml")
    fork = author.fork_config()
    if profile == "eoa":
        fork = eoa_configuration(fork)
    instance = "0x" + "00" * 32
    execution = Chain(chain_id=fork.origin.chain.chain_id, environment="anvil:" + instance)
    fund = Account(chain=execution, address=AccountReference("0x" + "01" * 20))
    if profile == "eoa":
        account = Eoa(kind="eoa", fund=fund)
    else:
        account = SafeRoles(
            kind="safe_roles",
            fund=fund,
            operator=Account(chain=execution, address=AccountReference("0x" + "02" * 20)),
            roles_module=AccountReference("0x" + "03" * 20),
            role_key="0x" + "04" * 32,
        )
    config = ComposedFork.prepare(
        fork,
        (AccountBinding(name="fund", profile=account),),
        execution,
        instance,
        fork.origin,
    )
    (tmp_path / "deployment.json").write_text(config.model_dump_json(indent=2) + "\n")

    driver.validate_fault_config(config)
    assert target_connector(config.target("loan")) == ConnectorId("aave_v3")
    assert fault_gateway_driver(tmp_path).name == "aave_v3_gateway_driver.py"


@pytest.mark.parametrize(
    ("connector", "expected"),
    [("morpho_blue", True), (ConnectorId("morpho_blue"), True), ("aave_v3", False), (None, False)],
)
def test_direct_morpho_config_selects_the_morpho_fault_driver(connector, expected):
    from tests.fork.morpho_blue_gateway_driver import has_morpho_component

    assert has_morpho_component(SimpleNamespace(connector=connector)) is expected
