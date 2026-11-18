"""Transaction signers bound to configured submitters, local keystores or remote services.

The gateway only ever needs one duck type from a signer: ``address`` at binding time and
``sign_transaction(dict)`` returning an envelope with ``raw_transaction`` and ``hash`` bytes.
Remote signers are discovered through the ``almanak.signers`` entry-point group so an
operator can keep private keys out of the deployment while the SDK still verifies every
returned envelope against the request and the bound submitter before it is retained.

A signer of a Hyperliquid deployment also holds its HyperCore agent keys (blueprint 30,
D9): it generates each one on request and returns only its address, lists the ones it
holds, and signs a Hyperliquid L1 action with one of them. The gateway rebuilds the
action's digest itself and refuses a signature that does not recover to the agent.
"""

import copy
import json
import re
from importlib.metadata import entry_points
from pathlib import Path
from typing import Protocol

from eth_account import Account
from eth_account._utils.legacy_transactions import Transaction
from eth_utils import keccak
from pydantic import ValidationError

from almanak.gateway.credentials import private_text
from almanak.gateway.hypercore_signing import ActionRequest, Signature, SignedAction, verified
from almanak.models import Eoa, SafeRoles

ENTRY_POINT_GROUP = "almanak.signers"


class SignedEnvelope(Protocol):
    raw_transaction: bytes
    hash: bytes


class SignerRefused(Exception):
    """The signer answered and refused to sign. Unlike an unreachable signer, a refusal
    counts against the HyperCore re-plan budget, so a signer that keeps refusing stops
    the attempts instead of meeting a fresh one each time."""


class Signer(Protocol):
    """Signs only; it must never broadcast the bytes or keep them for anyone who might.

    The gateway releases a signed attempt it has not sent as provably never sent, so a
    signer (remote and plugin signers included) that submitted or leaked the envelope
    could let a released attempt execute with an effect the journal never records.
    """

    address: str

    def sign_transaction(self, transaction: dict) -> SignedEnvelope: ...

    def sign_hyperliquid_action(
        self, account: str, agent: str, action: dict, nonce: int, expires_after: int
    ) -> dict:
        """Sign a Hyperliquid L1 action of HyperCore `account` with its `agent` key.

        Only signers of Hyperliquid deployments provide it; it returns the venue's
        signature object (`r`, `s`, `v`) and sends nothing.
        """
        ...


class HyperCoreAgentKeys(Protocol):
    """The agent keys a signer holds for one HyperCore account (D9)."""

    def generate_hyperliquid_agent(self, account: str) -> str:
        """A fresh key's address; the key is held before the address is returned."""
        ...

    def hyperliquid_agents(self, account: str, addresses: frozenset[str]) -> frozenset[str]:
        """Which of `addresses` it holds a key for, verified from durable storage."""
        ...


Address = r"^0x[0-9a-f]{40}$"


class Envelope:
    def __init__(self, raw_transaction: bytes):
        self.raw_transaction = raw_transaction
        self.hash = keccak(raw_transaction)


class VerifiedSigner:
    """Wrap any signer so a wrong or tampered envelope can never reach the journal."""

    def __init__(self, signer, address: str):
        if signer.address.lower() != address:
            raise ValueError("signer address does not match the configured transaction submitter")
        self.inner = signer
        self.address = address

    def sign_transaction(self, transaction: dict) -> SignedEnvelope:
        request = copy.deepcopy(transaction)
        signed = self.inner.sign_transaction(copy.deepcopy(transaction))
        raw = bytes(signed.raw_transaction)
        verify_envelope(request, raw, self.address)
        return Envelope(raw)

    def sign_hyperliquid_action(
        self, account: str, agent: str, request: ActionRequest
    ) -> SignedAction:
        """The signed action, refused unless its signature recovers to `agent` over the
        digest the gateway computes itself, so a signer can substitute neither the key
        nor the action."""
        sign = getattr(self.inner, "sign_hyperliquid_action", None)
        if sign is None:
            raise ValueError("the signer does not sign Hyperliquid actions")
        returned = sign(account, agent, request.wire(), request.nonce, request.expires_after)
        try:
            signature = Signature.model_validate(returned)
        except ValidationError:
            raise ValueError("the signer returned a malformed Hyperliquid signature") from None
        return verified(request, agent, signature)

    def generate_hyperliquid_agent(self, account: str) -> str:
        address = agent_keys(self.inner).generate_hyperliquid_agent(account)
        if not isinstance(address, str) or not re.fullmatch(Address, address.lower()):
            raise ValueError("the signer returned a malformed agent address")
        return address.lower()

    def hyperliquid_agents(self, account: str, addresses: frozenset[str]) -> frozenset[str]:
        asked = frozenset(a.lower() for a in addresses)
        held = agent_keys(self.inner).hyperliquid_agents(account, asked)
        if not all(isinstance(a, str) and re.fullmatch(Address, a.lower()) for a in held):
            raise ValueError("the signer returned a malformed agent address")
        answered = frozenset(a.lower() for a in held)
        if not answered <= asked:
            raise ValueError("the signer vouched for an agent it was not asked about")
        return answered


def agent_keys(signer) -> HyperCoreAgentKeys:
    if not all(
        callable(getattr(signer, name, None))
        for name in ("generate_hyperliquid_agent", "hyperliquid_agents")
    ):
        raise ValueError("the signer holds no HyperCore agent keys")
    return signer


def _data_bytes(data: str | bytes) -> bytes:
    return bytes.fromhex(data[2:]) if isinstance(data, str) else bytes(data)


def verify_envelope(transaction: dict, raw: bytes, address: str):
    try:
        decoded = Transaction.from_bytes(raw)
        sender = Account.recover_transaction(raw).lower()
    except Exception:
        raise ValueError("signer envelope is not a signed legacy transaction") from None
    expected = (
        transaction["nonce"],
        transaction["gasPrice"],
        transaction["gas"],
        transaction["to"].lower(),
        transaction["value"],
        _data_bytes(transaction["data"]),
        transaction["chainId"],
        address,
    )
    actual = (
        decoded.nonce,
        decoded.gasPrice,
        decoded.gas,
        "0x" + decoded.to.hex(),
        decoded.value,
        bytes(decoded.data),
        (decoded.v - 35) // 2,
        sender,
    )
    if decoded.v < 35 or actual != expected:
        raise ValueError("signer returned an envelope that differs from the requested transaction")


def signer_factory(kind: str):
    matches = [point for point in entry_points(group=ENTRY_POINT_GROUP) if point.name == kind]
    if len(matches) != 1:
        raise ValueError(f"no installed signer provides kind {kind!r}")
    return matches[0].load()


def load_remote_signers(config, signer_file: Path) -> dict[str, VerifiedSigner]:
    """Bind one verified signer per configured account from an operator signer file."""
    spec = json.loads(private_text(signer_file))
    if not isinstance(spec, dict) or not isinstance(spec.get("kind"), str):
        raise ValueError("signer file must be a JSON object with a string kind")
    factory = signer_factory(spec["kind"])
    signers = {}
    for binding in config.accounts.bindings:
        if not isinstance(binding.profile, (Eoa, SafeRoles)):
            raise ValueError("remote signer requires a supported execution account")
        address = binding.profile.submitter.address
        signers[binding.name] = VerifiedSigner(factory(spec, binding.name, address), address)
    return signers
