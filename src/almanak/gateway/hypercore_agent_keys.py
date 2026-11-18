"""The agent keys the gateway generates and holds (blueprint 30, 5.4).

Under D9 every agent approval uses a fresh key nobody has seen: HyperCore applies action
9 only to an address it holds no account for, so a key published before its approval
can be blocked by anyone with one send. The gateway generates and holds these keys: on
forks in one file that only its user can read. A production run keeps each key
encrypted under the operator keystore password, as the operator's own key is
(`EncryptedAgentKeys`, section 14.2). Only addresses and signatures leave this module.

It has the shape of a signer's agent keys (`signers.HyperCoreAgentKeys` and
`Signer.sign_hyperliquid_action`), so `VerifiedSigner` checks every address and
signature it returns. One deployment trades for one HyperCore account, its Safe, so the account a
call names is the deployment's own.
"""

import fcntl
import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path

from eth_account import Account

from almanak.gateway import hypercore_signing


class LocalAgentKeys:
    def __init__(self, path: Path, deployment: str):
        if not deployment:
            raise ValueError("agent keys belong to one named deployment")
        self.path = path
        self.deployment = deployment

    def load(self) -> dict:
        if not self.path.exists():
            return {}
        if self.path.stat().st_mode & 0o077:
            raise PermissionError("the agent key file must be readable by its owner only")
        return json.loads(self.path.read_text())

    def generate(self) -> str:
        """A fresh key's address. The key is written before its address is returned, so an
        approval never names an address whose key was lost; callers sharing the file
        update it one at a time."""
        account = Account.create()
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with self.locked():
            stored = self.load()
            stored.setdefault(self.deployment, {})[account.address.lower()] = self.seal(account)
            self.replace(stored)
        return account.address.lower()

    def seal(self, account):
        """The stored form of a fresh key."""
        return account.key.hex()

    def unseal(self, address: str, stored) -> str:
        """The key of `address` from its stored form."""
        return stored

    @contextmanager
    def locked(self):
        """An exclusive lock on the key file, across threads and processes."""
        lock = self.path.with_name(self.path.name + ".lock")
        descriptor = os.open(lock, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            os.close(descriptor)

    def replace(self, stored: dict) -> None:
        # mkstemp creates a new owner-only file exclusively, never one another writer holds.
        descriptor, temporary = tempfile.mkstemp(
            dir=self.path.parent, prefix=self.path.name + ".", suffix=".tmp"
        )
        try:
            with os.fdopen(descriptor, "w") as handle:
                json.dump(stored, handle)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
        except BaseException:
            Path(temporary).unlink(missing_ok=True)
            raise

    def held(self, addresses: frozenset[str] | None = None) -> frozenset[str]:
        """The deployment's agent addresses, or which of `addresses` it holds, each read
        from the key file and checked to derive its address: a stored key that does not
        fails rather than being counted."""
        stored = self.load().get(self.deployment, {})
        if addresses is None:
            return frozenset(stored)
        held = set()
        for address in {a.lower() for a in addresses} & set(stored):
            if Account.from_key(self.unseal(address, stored[address])).address.lower() != address:
                raise ValueError("a held agent key does not derive its address")
            held.add(address)
        return frozenset(held)

    # The signer seam (`VerifiedSigner` checks every answer).

    def generate_hyperliquid_agent(self, account: str) -> str:
        return self.generate()

    def hyperliquid_agents(self, account: str, addresses: frozenset[str]) -> frozenset[str]:
        return self.held(addresses)

    def sign_hyperliquid_action(
        self, account: str, agent: str, action: dict, nonce: int, expires_after: int
    ) -> dict:
        hypercore_signing.parsed(action)
        stored = self.load().get(self.deployment, {}).get(agent.lower())
        if stored is None:
            raise PermissionError("no agent key of this deployment has that address")
        key = self.unseal(agent.lower(), stored)
        return hypercore_signing.sign(key, action, nonce, expires_after).model_dump()


class EncryptedAgentKeys(LocalAgentKeys):
    """A production run's agent keys: each stored as an encrypted keystore under the
    operator keystore password and decrypted once, into this process's memory only."""

    def __init__(self, path: Path, deployment: str, password: str):
        super().__init__(path, deployment)
        if not password:
            raise ValueError("encrypted agent keys need a nonempty keystore password")
        self.password = password
        self.decrypted: dict[str, str] = {}

    def seal(self, account):
        return Account.encrypt(account.key, self.password)

    def unseal(self, address: str, stored) -> str:
        if address not in self.decrypted:
            try:
                self.decrypted[address] = Account.decrypt(stored, self.password).hex()
            except Exception:
                raise ValueError("an encrypted agent key could not be unlocked") from None
        return self.decrypted[address]


class SignerAgentKeys:
    """Custody's agent key holder over a deployment's verified signer: only addresses,
    each checked by `VerifiedSigner`, reach custody."""

    def __init__(self, signer, account: str):
        self.signer = signer
        self.account = account

    def generate(self) -> str:
        return self.signer.generate_hyperliquid_agent(self.account)

    def held(self, addresses: frozenset[str]) -> frozenset[str]:
        return self.signer.hyperliquid_agents(self.account, addresses)
