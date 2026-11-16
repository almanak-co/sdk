"""Production signer selection and resource cleanup need no provider or retained evidence."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.gateway import production

CONFIG = SimpleNamespace(production=object())
ENDPOINT = "https://rpc.invalid"


@pytest.fixture
def gateway_base(monkeypatch):
    events = []

    def initialize(self, config, endpoint, signers, *, chain):
        events.append(("initialize", config, endpoint, signers, chain))
        self.signers = signers

    def close(self):
        events.append(("close",))

    monkeypatch.setattr(production.EvmGateway, "__init__", initialize)
    monkeypatch.setattr(production.EvmGateway, "close", close)
    return events


def test_constructor_refuses_invalid_authority_before_opening_resources(gateway_base):
    with pytest.raises(ValueError, match="production configuration"):
        production.ProductionGateway(SimpleNamespace(production=None), ENDPOINT)
    with pytest.raises(ValueError, match="either keystores or a remote signer"):
        production.ProductionGateway(
            CONFIG, ENDPOINT, Path("keys.json"), signer_file=Path("signer.json")
        )
    with pytest.raises(ValueError, match="HTTPS"):
        production.ProductionGateway(CONFIG, "http://rpc.invalid")
    assert gateway_base == []


def test_constructor_selects_keystores_or_remote_signer(gateway_base, monkeypatch):
    keystores = {"fund": object()}
    remote = {"fund": object()}
    seen = []
    monkeypatch.setattr(
        production,
        "load_production_signers",
        lambda config, key, password_file, *, password: (
            seen.append((config, key, password_file, password)) or keystores
        ),
    )
    monkeypatch.setattr(
        "almanak.gateway.signers.load_remote_signers",
        lambda config, path: seen.append((config, path)) or remote,
    )
    key_file = Path("keys.json")
    password_file = Path("password")
    local = production.ProductionGateway(
        CONFIG, ENDPOINT, key_file, password_file, password="secret"
    )
    signer_file = Path("signer.json")
    hosted = production.ProductionGateway(CONFIG, ENDPOINT, signer_file=signer_file)
    empty = production.ProductionGateway(CONFIG, ENDPOINT)
    assert local.signers is keystores and hosted.signers is remote and empty.signers == {}
    assert seen == [
        (CONFIG, key_file, password_file, "secret"),
        (CONFIG, signer_file),
    ]


@pytest.mark.parametrize("remote", [False, True])
def test_constructor_closes_open_resources_when_signer_loading_fails(
    gateway_base, monkeypatch, remote
):
    def fail(*args, **kwargs):
        raise RuntimeError("signer unavailable")

    key_file = None if remote else Path("keys.json")
    signer_file = Path("signer.json") if remote else None
    target = (
        "almanak.gateway.signers.load_remote_signers"
        if remote
        else "almanak.gateway.production.load_production_signers"
    )
    monkeypatch.setattr(target, fail)
    with pytest.raises(RuntimeError, match="signer unavailable"):
        production.ProductionGateway(CONFIG, ENDPOINT, key_file, signer_file=signer_file)
    assert [event[0] for event in gateway_base] == ["initialize", "close"]
