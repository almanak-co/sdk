"""Operator-controlled encrypted keystores for production transaction submitters."""

import getpass
import json
import os
import secrets
import shutil
import stat
from pathlib import Path

from eth_account import Account as Signer

from almanak.models import Eoa, SafeRoles
from almanak.sandbox import readable_paths, validate_key_path


def private_text(path: Path) -> str:
    validate_key_path(path, readable_paths())
    # Nonblocking, so a FIFO with no writer is refused below instead of waited on.
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor) as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.getuid():
            raise ValueError("credential must be an operator-owned private regular file (0600)")
        return handle.read()


def load_production_signers(config, key_file: Path, password_file: Path | None, *, password=None):
    stores = json.loads(private_text(key_file))
    names = {binding.name for binding in config.accounts.bindings}
    if not isinstance(stores, dict) or set(stores) != names:
        raise ValueError("encrypted keystores must match exactly the configured account names")
    password = keystore_password(password_file, password)
    return {
        binding.name: decrypt_signer(stores[binding.name], password, binding.profile)
        for binding in config.accounts.bindings
    }


def keystore_password(password_file: Path | None, password=None) -> str:
    """The operator keystore password: the ephemeral one given, else its file, else asked."""
    if password is None:
        password = (
            private_text(password_file).rstrip("\n")
            if password_file
            else getpass.getpass("Keystore password: ")
        )
    if not password:
        raise ValueError("keystore password must not be empty")
    return password


def decrypt_signer(keystore, password, profile):
    if not isinstance(profile, (Eoa, SafeRoles)):
        raise ValueError("production signer requires a supported execution account")
    try:
        signer = Signer.from_key(Signer.decrypt(keystore, password))
    except Exception:
        raise ValueError("encrypted keystore could not be unlocked") from None
    if signer.address.lower() != profile.submitter.address:
        raise ValueError("encrypted keystore does not match the configured transaction submitter")
    return signer


def write_private(path: Path, contents: str):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(contents + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def create_wallet(
    directory: Path,
    account: str,
    *,
    password_file: Path | None = None,
    generate_password: bool = False,
):
    """Create one encrypted keystore; never returns private material.

    The password is asked for twice unless an operator password file is named or a
    generated one is requested. A generated password is written beside the keystore
    as a private file, for a caller that must unlock it without a terminal.
    """
    validate_key_path(directory, readable_paths())
    generated = secrets.token_urlsafe(32) if generate_password else None
    if generated is not None:
        password = generated
    elif password_file is not None:
        password = keystore_password(password_file)
    else:
        password = getpass.getpass("New keystore password: ")
        if not password or password != getpass.getpass("Repeat password: "):
            raise ValueError("nonempty matching passwords are required")
    signer = Signer.create()
    encrypted = Signer.encrypt(signer.key, password)
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    try:
        return write_wallet(directory, account, signer.address, encrypted, generated)
    except BaseException:
        # A partial directory would hold a keystore without its record or password, and
        # would refuse a retry at the same path.
        shutil.rmtree(directory, ignore_errors=True)
        raise


def write_wallet(directory, account, address, encrypted, generated):
    write_private(directory / "keystores.json", json.dumps({account: encrypted}))
    write_private(directory / "gateway-auth.token", secrets.token_hex(32))
    result = {
        "account": account,
        "address": address,
        "keystores": str((directory / "keystores.json").resolve()),
        "auth_token_file": str((directory / "gateway-auth.token").resolve()),
    }
    if generated is not None:
        write_private(directory / "keystore-password", generated)
        result["password_file"] = str((directory / "keystore-password").resolve())
    write_private(directory / "wallet.json", json.dumps(result, indent=2))
    return result
