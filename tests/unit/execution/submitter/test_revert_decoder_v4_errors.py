"""Tests for Uniswap V4 PositionManager / PoolManager custom error selectors (VIB-2703).

Verifies the 7 V4 selectors added to ``KNOWN_CUSTOM_ERRORS`` map to the correct
signature strings, that the source-level dict literal has no duplicate selector
keys (the guard that would catch an accidental double-add of an already-present
selector such as ``0xe450d38c``), that the live decoder resolves a V4 selector,
and that unknown selectors still fall back to a raw-hex string.

Decode entry point under test: ``PublicMempoolSubmitter._decode_revert_data`` —
a synchronous method ``(self, revert_data: str | bytes) -> str`` in
``almanak/framework/execution/submitter/public.py`` (line ~862) that maps a
4-byte selector to ``f"Custom error: {KNOWN_CUSTOM_ERRORS[selector]}"``.
"""

from __future__ import annotations

import ast
import inspect

import pytest
from eth_abi.abi import encode as abi_encode
from eth_utils import keccak

from almanak.framework.execution.submitter import public as public_module
from almanak.framework.execution.submitter.public import (
    KNOWN_CUSTOM_ERRORS,
    PARAMETERIZED_CUSTOM_ERRORS,
    PublicMempoolSubmitter,
)

# The 7 Uniswap V4 selectors added under VIB-2703, with their exact signatures.
V4_SELECTORS: dict[str, str] = {
    "0x0ca968d8": "NotApproved(address)",
    "0x1ad777f8": "TickUpperOutOfBounds(int24)",
    "0x24df576f": "TooMuchRequested()",
    "0x486aa307": "PoolNotInitialized()",
    "0xa74f97ab": "NoLiquidityToReceiveFees()",
    "0xbfb22adf": "DeadlinePassed(uint256)",
    "0xd5e2f7ab": "TickLowerOutOfBounds(int24)",
}

# Pre-existing parameter-less selectors in KNOWN_CUSTOM_ERRORS that are NOT
# keccak(signature)[:4] consistent — legacy labels sourced before keccak
# verification, plus the two non-selector sentinels. Out of scope for
# VIB-5017 (which only relabels 0x675cae38); enumerated here so the general
# keccak-consistency assertion below stays green while still catching any NEW
# parameter-less selector added with a wrong signature label.
KECCAK_INCONSISTENT_PARAMETERLESS: frozenset[str] = frozenset(
    {
        "0x",  # EmptyRevertData() sentinel — not a real selector
        "0x00000000",  # Unknown() sentinel — not a real selector
        "0x0a061d77",  # InsufficientOutputAmount() — legacy label
        "0x39d35496",  # InvalidPool() — legacy label
        "0xcf479181",  # InsufficientBalance() — legacy label
        "0xce30421c",  # TooLittleReceived() — legacy label
    }
)


def _selector_of(signature: str) -> str:
    """Return the 0x-prefixed 4-byte keccak selector for an error signature."""
    return "0x" + keccak(text=signature)[:4].hex()


def _craft_revert(selector: str, arg_types: list[str], values: list) -> str:
    """Build a 0x revert payload: 4-byte selector + ABI-encoded args."""
    return selector + abi_encode(arg_types, values).hex()


@pytest.fixture
def submitter() -> PublicMempoolSubmitter:
    """Construct a submitter for decode-only unit tests."""
    return PublicMempoolSubmitter(rpc_url="http://localhost:8545")


def test_v4_selectors_present_and_mapped() -> None:
    """Each of the 7 V4 selectors must map to its exact signature string."""
    for selector, signature in V4_SELECTORS.items():
        assert selector in KNOWN_CUSTOM_ERRORS, f"missing V4 selector {selector}"
        assert KNOWN_CUSTOM_ERRORS[selector] == signature, (
            f"{selector} maps to {KNOWN_CUSTOM_ERRORS[selector]!r}, expected {signature!r}"
        )


def test_source_level_selector_uniqueness() -> None:
    """No duplicate selector keys in the KNOWN_CUSTOM_ERRORS source literal.

    A dict *literal* silently collapses duplicate string keys, so a runtime
    ``len(dict) == len(set(dict))`` check is tautological and cannot catch an
    accidental double-add (e.g. re-adding the already-present ``0xe450d38c``).
    Instead, parse the module source, locate the ``KNOWN_CUSTOM_ERRORS``
    assignment's ``ast.Dict`` node, collect the string-literal keys in source
    order, and assert there are no duplicates across the whole literal.
    """
    source = inspect.getsource(public_module)
    tree = ast.parse(source)

    dict_node: ast.Dict | None = None
    for node in ast.walk(tree):
        # Match both `KNOWN_CUSTOM_ERRORS = {...}` and the annotated form
        # `KNOWN_CUSTOM_ERRORS: dict[str, str] = {...}`.
        targets: list[ast.expr] = []
        if isinstance(node, ast.Assign):
            targets = list(node.targets)
            value = node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets = [node.target]
            value = node.value
        else:
            continue

        for target in targets:
            if isinstance(target, ast.Name) and target.id == "KNOWN_CUSTOM_ERRORS":
                assert isinstance(value, ast.Dict), (
                    "KNOWN_CUSTOM_ERRORS is not assigned a dict literal"
                )
                dict_node = value
                break
        if dict_node is not None:
            break

    assert dict_node is not None, "could not find KNOWN_CUSTOM_ERRORS dict literal in source"

    keys: list[str] = []
    for key in dict_node.keys:
        assert isinstance(key, ast.Constant) and isinstance(key.value, str), (
            f"non-string-literal key in KNOWN_CUSTOM_ERRORS: {ast.dump(key) if key else key}"
        )
        keys.append(key.value)

    duplicates = sorted({k for k in keys if keys.count(k) > 1})
    assert not duplicates, f"duplicate selector keys in KNOWN_CUSTOM_ERRORS source: {duplicates}"


def test_v4_pool_not_initialized_decodes(submitter: PublicMempoolSubmitter) -> None:
    """The live decoder resolves the V4 PoolNotInitialized() selector."""
    result = submitter._decode_revert_data("0x486aa307")
    assert "PoolNotInitialized" in result
    assert result == "Custom error: PoolNotInitialized()"


def test_unknown_selector_falls_back_to_hex(submitter: PublicMempoolSubmitter) -> None:
    """A selector not in the dict still returns a raw-hex string without raising."""
    result = submitter._decode_revert_data("0xdeadbeef")
    assert "deadbeef" in result.lower()
    assert "Unknown revert" in result


# ---------------------------------------------------------------------------
# VIB-5017: 0x675cae38 is InsufficientToken(), not TooMuchRequested()
# ---------------------------------------------------------------------------


def test_0x675cae38_decodes_as_insufficient_token(
    submitter: PublicMempoolSubmitter,
) -> None:
    """0x675cae38 must decode as InsufficientToken() (VIB-5017 relabel)."""
    assert KNOWN_CUSTOM_ERRORS["0x675cae38"] == "InsufficientToken()"
    result = submitter._decode_revert_data("0x675cae38")
    assert result == "Custom error: InsufficientToken()"


def test_0x675cae38_keccak_matches_insufficient_token() -> None:
    """keccak("InsufficientToken()")[:4] == 0x675cae38, proving the label is
    the keccak-correct signature for the selector (VIB-5017)."""
    assert _selector_of("InsufficientToken()") == "0x675cae38"
    # And the (separate) TooMuchRequested() row keeps its keccak-correct selector.
    assert _selector_of("TooMuchRequested()") == "0x24df576f"
    assert KNOWN_CUSTOM_ERRORS["0x24df576f"] == "TooMuchRequested()"


def test_parameterless_selectors_are_keccak_consistent() -> None:
    """General guard: every parameter-less selector in the table equals
    keccak(signature)[:4], except documented legacy labels / sentinels.

    Catches a NEW parameter-less selector added with a mismatched signature
    (the VIB-5017 class of bug) without forcing the pre-existing legacy rows
    to be rewritten in this PR.
    """
    offenders: list[tuple[str, str, str]] = []
    for selector, signature in KNOWN_CUSTOM_ERRORS.items():
        if not signature.endswith("()"):
            continue  # parameterized — handled separately
        if selector in KECCAK_INCONSISTENT_PARAMETERLESS:
            continue
        calc = _selector_of(signature)
        if calc.lower() != selector.lower():
            offenders.append((selector, signature, calc))
    assert not offenders, f"keccak-inconsistent parameter-less selectors: {offenders}"


# ---------------------------------------------------------------------------
# VIB-5016: ABI-decode parameterized custom-error arguments
# ---------------------------------------------------------------------------


def test_parameterized_map_consistent_with_known_errors() -> None:
    """Every PARAMETERIZED_CUSTOM_ERRORS entry must mirror its label in
    KNOWN_CUSTOM_ERRORS and be keccak-consistent with that signature."""
    for selector, (name, arg_types) in PARAMETERIZED_CUSTOM_ERRORS.items():
        assert selector in KNOWN_CUSTOM_ERRORS, f"{selector} missing from KNOWN_CUSTOM_ERRORS"
        signature = f"{name}({','.join(arg_types)})"
        assert KNOWN_CUSTOM_ERRORS[selector] == signature, (
            f"{selector}: label {KNOWN_CUSTOM_ERRORS[selector]!r} != reconstructed {signature!r}"
        )
        assert _selector_of(signature) == selector, (
            f"{selector}: keccak of {signature!r} does not match"
        )


def test_not_approved_decodes_address(submitter: PublicMempoolSubmitter) -> None:
    """NotApproved(address) surfaces the decoded (checksummed) address arg."""
    addr = "0x000000000000000000000000000000000000dEaD"
    payload = _craft_revert("0x0ca968d8", ["address"], [addr])
    result = submitter._decode_revert_data(payload)
    assert result == "Custom error: NotApproved(0x000000000000000000000000000000000000dEaD)"


def test_deadline_passed_decodes_uint(submitter: PublicMempoolSubmitter) -> None:
    """DeadlinePassed(uint256) surfaces the decoded deadline value."""
    payload = _craft_revert("0xbfb22adf", ["uint256"], [1735689600])
    result = submitter._decode_revert_data(payload)
    assert result == "Custom error: DeadlinePassed(1735689600)"


def test_tick_lower_out_of_bounds_decodes_negative_int24(
    submitter: PublicMempoolSubmitter,
) -> None:
    """TickLowerOutOfBounds(int24) decodes a negative tick (signed int24)."""
    payload = _craft_revert("0xd5e2f7ab", ["int24"], [-887272])
    result = submitter._decode_revert_data(payload)
    assert result == "Custom error: TickLowerOutOfBounds(-887272)"


def test_tick_upper_out_of_bounds_decodes_positive_int24(
    submitter: PublicMempoolSubmitter,
) -> None:
    """TickUpperOutOfBounds(int24) decodes a positive tick (signed int24)."""
    payload = _craft_revert("0x1ad777f8", ["int24"], [887272])
    result = submitter._decode_revert_data(payload)
    assert result == "Custom error: TickUpperOutOfBounds(887272)"


def test_parameterized_truncated_payload_falls_back_to_label(
    submitter: PublicMempoolSubmitter,
) -> None:
    """A parameterized selector with a missing/short payload falls back to the
    label-only signature without raising (VIB-5016 graceful degradation)."""
    # Selector only, no args at all.
    assert (
        submitter._decode_revert_data("0x0ca968d8") == "Custom error: NotApproved(address)"
    )
    # Selector + half a head slot (16 bytes) — too short to decode.
    short = "0x0ca968d8" + "ab" * 16
    assert submitter._decode_revert_data(short) == "Custom error: NotApproved(address)"


def test_parameterized_garbage_payload_falls_back_to_label(
    submitter: PublicMempoolSubmitter,
) -> None:
    """A full-length but undecodable payload falls back to the label safely.

    A 32-byte address slot whose top 12 padding bytes are non-zero is not a
    valid canonical address encoding; eth_abi raises NonEmptyPaddingBytes and
    we degrade to the label-only signature rather than propagating.
    """
    garbage = "0x0ca968d8" + "ff" * 32
    result = submitter._decode_revert_data(garbage)
    assert result == "Custom error: NotApproved(address)"


def test_parameterized_valid_head_with_trailing_garbage_still_decodes(
    submitter: PublicMempoolSubmitter,
) -> None:
    """A valid head slot followed by trailing garbage (even odd-length nibbles)
    still decodes — the decoder slices to the expected head length before
    ``bytes.fromhex`` rather than failing on the trailing bytes (Gemini robustness
    suggestion). The trailing ``f`` is an odd nibble that would raise ValueError
    in ``bytes.fromhex`` if the whole payload were converted.
    """
    addr = "0x000000000000000000000000000000000000dEaD"
    payload = _craft_revert("0x0ca968d8", ["address"], [addr]) + "f"
    result = submitter._decode_revert_data(payload)
    assert result == "Custom error: NotApproved(0x000000000000000000000000000000000000dEaD)"


def test_non_parameterized_known_error_stays_label_only(
    submitter: PublicMempoolSubmitter,
) -> None:
    """A non-parameterized selector with trailing bytes stays label-only and
    never attempts an arg decode (VIB-5016 scope guard)."""
    # InsufficientToken() is not in PARAMETERIZED_CUSTOM_ERRORS.
    payload = "0x675cae38" + "00" * 32
    assert submitter._decode_revert_data(payload) == "Custom error: InsufficientToken()"
