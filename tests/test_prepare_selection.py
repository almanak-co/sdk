"""Every connector preparation resolves its selection through the shared selection API.

A public root resolves market facts through the read gateway inside `resolve`;
a harness root returns its loaded author and settings. Preparation never
unpacks the selection itself, so both roots reach source validation the same way.
"""

import importlib
import json
from types import SimpleNamespace

import pytest

from almanak.connectors.dex_economic_prepare import PreparationBinding

MODULES = (
    "uniswap_v3",
    "pancakeswap_v3",
    "uniswap_v4",
    "sparklend",
    "morpho_blue",
    "compound_v3",
    "composed",
    "aave_v3",
)


class Validated(Exception):
    pass


def install_validation(monkeypatch, module, calls):
    def validate(*arguments):
        calls.append(("validate", arguments[-2:]))
        raise Validated

    binding = getattr(module, "_binding", None)
    if isinstance(binding, PreparationBinding):
        monkeypatch.setattr(
            PreparationBinding, "validate_sources", lambda self, *arguments: validate(*arguments)
        )
    else:
        monkeypatch.setattr(module, "validate_sources", validate)


@pytest.mark.parametrize("name", MODULES)
def test_preparation_resolves_the_selection_before_validating_sources(name, tmp_path, monkeypatch):
    module = importlib.import_module(f"almanak.connectors.{name}.economic_prepare")
    calls = []
    resolved = (object(), object())

    class Selection:
        def resolve(self, directory, env_file):
            calls.append(("resolve", directory, env_file))
            return resolved

    monkeypatch.setattr("almanak.backtest.inputs.selected", lambda args: Selection())
    install_validation(monkeypatch, module, calls)
    args = SimpleNamespace(output=tmp_path / "prepared", env_file=tmp_path / "operator.env")
    with pytest.raises(Validated):
        module.prepare_locked(args)
    assert calls == [("resolve", args.output.resolve(), args.env_file), ("validate", resolved)]


PROVISION_MODULES = ("uniswap_v4", "sparklend", "morpho_blue", "compound_v3", "composed", "aave_v3")


@pytest.mark.parametrize("name", PROVISION_MODULES)
def test_provisioning_accepts_a_retained_resolution_beside_a_fresh_dataset(
    name, tmp_path, monkeypatch
):
    """The public selection retains resolution.json before the dataset is provisioned."""
    from almanak.backtest.prepared import PROVISIONAL

    module = importlib.import_module(f"almanak.connectors.{name}.economic_prepare")
    monkeypatch.setattr(module, "identity", lambda config, settings: "policy")
    monkeypatch.setattr(module, "require_private_directory", lambda directory: None, raising=False)
    # Uniswap V4 also publishes its provider bindings; the stub settings carry none.
    monkeypatch.setattr(module, "contract_bindings", lambda settings, config: (), raising=False)
    monkeypatch.setattr(module, "GatewayConfig", lambda **fields: dict(fields), raising=False)
    settings = SimpleNamespace(history=())
    directory = tmp_path / "dataset"
    directory.mkdir()
    for provisional in sorted(PROVISIONAL - {"private"}):
        (directory / provisional).write_text("{}")
    module.provision(directory, object(), settings, "facts")
    request = json.loads((directory / "acquisition.json").read_text())
    assert request == {"identity": "policy", "resolution_facts": "facts"}
    with pytest.raises(ValueError, match="cannot change"):
        module.provision(directory, object(), settings, "other facts")
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    (foreign / "notes.txt").write_text("x")
    with pytest.raises(ValueError, match="new or matching"):
        module.provision(foreign, object(), settings)


def test_exchange_pool_provisioning_accepts_a_retained_resolution(tmp_path, monkeypatch):
    from almanak.backtest.prepared import PROVISIONAL
    from almanak.connectors.uniswap_v3.economic_prepare import _binding

    monkeypatch.setattr(_binding.book_type, "identity", lambda config, settings: "policy")
    directory = tmp_path / "dataset"
    directory.mkdir()
    for provisional in sorted(PROVISIONAL - {"private"}):
        (directory / provisional).write_text("{}")
    _binding.provision(directory, object(), object())
    assert json.loads((directory / "acquisition.json").read_text()) == {"identity": "policy"}
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    (foreign / "notes.txt").write_text("x")
    with pytest.raises(ValueError, match="new or matching"):
        _binding.provision(foreign, object(), object())


class Provisioned(Exception):
    pass


@pytest.mark.parametrize("name", MODULES)
def test_preparation_binds_the_selection_facts_into_the_dataset_request(
    name, tmp_path, monkeypatch
):
    module = importlib.import_module(f"almanak.connectors.{name}.economic_prepare")
    calls = []

    class Selection:
        resolution_facts = "facts"

        def resolve(self, directory, env_file):
            return ("author", "settings")

    def provision(*arguments):
        calls.append(arguments[-1])
        raise Provisioned

    monkeypatch.setattr("almanak.backtest.inputs.selected", lambda args: Selection())
    binding = getattr(module, "_binding", None)
    if isinstance(binding, PreparationBinding):
        monkeypatch.setattr(PreparationBinding, "validate_sources", lambda self, *a: "config")
        monkeypatch.setattr(PreparationBinding, "provision", lambda self, *a: provision(*a))
    else:
        monkeypatch.setattr(module, "validate_sources", lambda *a: "config")
        monkeypatch.setattr(module, "provision", provision)
    args = SimpleNamespace(output=tmp_path / "prepared", env_file=None)
    with pytest.raises(Provisioned):
        module.prepare_locked(args)
    assert calls == ["facts"]
