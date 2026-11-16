"""Operator seams for remote signing and frozen operator inputs."""

import json
from types import SimpleNamespace

import pytest
from eth_account import Account

from almanak.gateway import signers as signing
from almanak.gateway.credentials import write_private
from almanak.gateway.signers import Envelope, VerifiedSigner, verify_envelope
from tests.test_production import TEST_KEY, production_state


@pytest.fixture
def production_recorded(recorded):
    return production_state(recorded)


REQUEST = {
    "chainId": 1,
    "nonce": 7,
    "gas": 21000,
    "gasPrice": 10**9,
    "to": "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
    "value": 5,
    "data": "0xabcd",
}


class RecordingRemote:
    """Stands in for a remote signer: signs locally, exposes what it was asked."""

    def __init__(self, key=TEST_KEY):
        self.account = Account.from_key(key)
        self.address = self.account.address
        self.requests = []

    def sign_transaction(self, transaction):
        self.requests.append(transaction)
        return Envelope(bytes(self.account.sign_transaction(transaction).raw_transaction))


def test_verified_signer_accepts_only_envelopes_matching_the_request():
    remote = RecordingRemote()
    signer = VerifiedSigner(remote, remote.address.lower())
    envelope = signer.sign_transaction(REQUEST)
    assert Account.recover_transaction(envelope.raw_transaction) == remote.address
    assert remote.requests == [REQUEST]
    with pytest.raises(ValueError, match="differs"):
        verify_envelope({**REQUEST, "nonce": 8}, envelope.raw_transaction, remote.address.lower())
    with pytest.raises(ValueError, match="differs"):
        verify_envelope(REQUEST, envelope.raw_transaction, "0x" + "11" * 20)
    with pytest.raises(ValueError, match="legacy"):
        verify_envelope(REQUEST, b"\x02" + envelope.raw_transaction, remote.address.lower())


def test_verified_signer_checks_the_request_it_was_given_not_what_the_signer_mutated():
    class Tampering(RecordingRemote):
        def sign_transaction(self, transaction):
            transaction["to"] = "0x" + "22" * 20
            return super().sign_transaction(transaction)

    remote = Tampering()
    request = dict(REQUEST)
    with pytest.raises(ValueError, match="differs"):
        VerifiedSigner(remote, remote.address.lower()).sign_transaction(request)
    assert request == REQUEST


def test_verified_signer_refuses_a_foreign_or_substituted_signer():
    other = RecordingRemote(
        key="0x" + "59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    )
    with pytest.raises(ValueError, match="submitter"):
        VerifiedSigner(other, Account.from_key(TEST_KEY).address.lower())
    expected = Account.from_key(TEST_KEY).address.lower()
    forged = SimpleNamespace(
        address=Account.from_key(TEST_KEY).address, sign_transaction=other.sign_transaction
    )
    with pytest.raises(ValueError, match="differs"):
        VerifiedSigner(forged, expected).sign_transaction(REQUEST)


def test_remote_signers_bind_every_account_through_the_entry_point(
    tmp_path, production_recorded, monkeypatch
):
    seen = []

    def factory(spec, name, address):
        seen.append((spec["url"], name, address))
        return RecordingRemote()

    point = SimpleNamespace(name="remote", load=lambda: factory)
    monkeypatch.setattr(
        signing, "entry_points", lambda group: [point] if group == signing.ENTRY_POINT_GROUP else []
    )
    signer_file = tmp_path / "signer.json"
    write_private(signer_file, json.dumps({"kind": "remote", "url": "https://signer.invalid"}))
    loaded = signing.load_remote_signers(production_recorded.config, signer_file)
    binding = production_recorded.config.accounts.bindings[0]
    assert set(loaded) == {binding.name}
    assert seen == [("https://signer.invalid", binding.name, binding.profile.submitter.address)]
    assert loaded[binding.name].sign_transaction(REQUEST).raw_transaction
    write_private(tmp_path / "unknown.json", json.dumps({"kind": "missing"}))
    with pytest.raises(ValueError, match="missing"):
        signing.load_remote_signers(production_recorded.config, tmp_path / "unknown.json")
    write_private(tmp_path / "shape.json", json.dumps(["remote"]))
    with pytest.raises(ValueError, match="JSON object"):
        signing.load_remote_signers(production_recorded.config, tmp_path / "shape.json")
