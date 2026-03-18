"""Validate all static contract addresses are valid EIP-55 checksums.

Catches mis-cased addresses that would cause web3.py encode_abi to crash
with InvalidAddress at runtime. See VIB-1433.
"""

import os
import re
from pathlib import Path

import pytest
from web3 import Web3

# Regex matching a quoted 0x address in Python source
_ADDR_RE = re.compile(r'"(0x[0-9a-fA-F]{40})"')


def _extract_addresses_from_module(module) -> list[tuple[str, str, str]]:
    """Extract all 0x addresses from module-level dict constants.

    Returns list of (dict_name, key_path, address) tuples.
    """
    results = []
    for attr_name in dir(module):
        obj = getattr(module, attr_name)
        if attr_name.startswith("_"):
            continue
        # Standalone address string
        if isinstance(obj, str) and re.match(r"^0x[0-9a-fA-F]{40}$", obj):
            results.append((attr_name, "", obj))
        # Nested dict (protocol -> chain -> addresses)
        elif isinstance(obj, dict):
            for chain, chain_data in obj.items():
                if isinstance(chain_data, dict):
                    for key, val in chain_data.items():
                        if isinstance(val, str) and re.match(r"^0x[0-9a-fA-F]{40}$", val):
                            results.append((attr_name, f"{chain}.{key}", val))
                elif isinstance(chain_data, str) and re.match(r"^0x[0-9a-fA-F]{40}$", chain_data):
                    results.append((attr_name, chain, chain_data))
    return results


def test_contracts_py_all_addresses_are_eip55():
    """Every address in almanak/core/contracts.py must be a valid EIP-55 checksum."""
    from almanak.core import contracts

    entries = _extract_addresses_from_module(contracts)
    assert len(entries) > 400, f"Expected 400+ addresses, found {len(entries)} — extraction may be broken"

    failures = []
    for dict_name, key_path, addr in entries:
        expected = Web3.to_checksum_address(addr)
        if addr != expected:
            loc = f"{dict_name}[{key_path}]" if key_path else dict_name
            failures.append(f"  {loc}: {addr} should be {expected}")

    if failures:
        msg = f"Found {len(failures)} addresses with invalid EIP-55 checksums:\n" + "\n".join(failures)
        pytest.fail(msg)


def test_gmx_v2_adapter_addresses_are_eip55():
    """GMX V2 adapter hardcoded addresses must be valid EIP-55 checksums."""
    from almanak.framework.connectors.gmx_v2.adapter import GMX_V2_ADDRESSES, GMX_V2_MARKETS

    failures = []
    for dict_name, addr_dict in [("GMX_V2_ADDRESSES", GMX_V2_ADDRESSES), ("GMX_V2_MARKETS", GMX_V2_MARKETS)]:
        for chain, addrs in addr_dict.items():
            for key, addr in addrs.items():
                if re.match(r"^0x[0-9a-fA-F]{40}$", addr):
                    expected = Web3.to_checksum_address(addr)
                    if addr != expected:
                        failures.append(f"  {dict_name}[{chain}][{key}]: {addr} should be {expected}")

    if failures:
        msg = f"Found {len(failures)} addresses with invalid EIP-55 checksums:\n" + "\n".join(failures)
        pytest.fail(msg)


def test_all_production_addresses_are_eip55():
    """Scan all production Python files under almanak/ for non-EIP-55 addresses.

    This is the broad regression gate — catches addresses in any production file,
    not just contracts.py. Test files are excluded (they may use intentionally
    fake addresses).
    """
    repo_root = Path(__file__).resolve().parents[3]
    almanak_dir = repo_root / "almanak"

    failures = []
    files_scanned = 0

    # Files that intentionally use lowercase addresses as dict/set lookup keys
    # or comparison targets alongside .lower() calls. These are NOT passed to
    # web3 encode_abi and must stay lowercase for correct comparison semantics.
    _LOWERCASE_OK_FILES = {
        "connectors/enso/adapter.py",
        "connectors/pendle/sdk.py",
        "connectors/lifi/adapter.py",
        "connectors/spark/receipt_parser.py",
        "execution/enso_state_provider.py",
        "execution/signer/safe/constants.py",
        "permissions/generator.py",
        "data/pools/reader.py",
        "demo_strategies/pendle_basics/strategy.py",
    }

    for root, _dirs, files in os.walk(almanak_dir):
        # Skip test directories and backtesting (uses lowercase addresses as dict keys)
        if "/tests/" in root or "/test_" in root or "/backtesting/" in root:
            continue
        for fname in files:
            if not fname.endswith(".py") or fname.startswith("test_"):
                continue
            filepath = os.path.join(root, fname)
            # Skip files that intentionally use lowercase for lookup keys
            relpath = os.path.relpath(filepath, almanak_dir)
            if any(relpath.endswith(f) for f in _LOWERCASE_OK_FILES):
                files_scanned += 1
                continue
            files_scanned += 1
            with open(filepath) as fh:
                for lineno, line in enumerate(fh, 1):
                    for m in _ADDR_RE.finditer(line):
                        addr = m.group(1)
                        expected = Web3.to_checksum_address(addr)
                        if addr != expected:
                            rel = os.path.relpath(filepath, repo_root)
                            failures.append(f"  {rel}:{lineno}: {addr} should be {expected}")

    assert files_scanned > 50, f"Only scanned {files_scanned} files — path resolution may be broken"

    if failures:
        msg = f"Found {len(failures)} non-EIP-55 addresses in production code:\n" + "\n".join(failures)
        pytest.fail(msg)
