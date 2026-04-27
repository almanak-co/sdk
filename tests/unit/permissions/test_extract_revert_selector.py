"""Unit coverage for ``_selector_from_web3_error`` — the helper that converts
web3.py exceptions into a canonical 4-byte selector for Zodiac authz
classification.

Why this matters: ``ZodiacOrchestrator`` uses the extracted selector to tell
Zodiac ``ConditionViolation`` reverts apart from generic protocol reverts. If
the helper fails to find the selector, a real manifest permission bug looks
like an execution error — the ``uses_zodiac`` marker's entire purpose is to
surface those bugs. web3.py surfaces revert data in at least three shapes
(RPC dict, ``ContractLogicError.data``, embedded in ``str(err)``), and we've
seen all three in the wild.

These are pure-function tests — no Anvil, no web3 provider, no signing.
"""

from __future__ import annotations

import pytest

from tests.intents._permission_onchain_harness import (
    _ZODIAC_AUTHZ_ERROR_SELECTORS,
    _normalise_selector,
    _selector_from_web3_error,
    is_zodiac_authz_revert,
)

# A known Zodiac Roles v2 unified denial selector.
_CONDITION_VIOLATION = "0xd0a9bf58"
# Full 68-byte ConditionViolation(uint8,bytes32) payload (selector + two args).
_CONDITION_VIOLATION_FULL = (
    "0xd0a9bf58"
    "0000000000000000000000000000000000000000000000000000000000000003"
    "abababababababababababababababababababababababababababababababab"
)


class _FakeDictErr(Exception):
    """Mimic ``ValueError({"code": 3, "message": ..., "data": ...})`` —
    the geth-style JSON-RPC payload surfaced as ``err.args[0]`` on
    ``web3._utils.method_formatters`` paths."""


class _FakeContractLogicError(Exception):
    """Mimic web3.py's ``ContractLogicError`` where the raw revert bytes are
    stashed in ``err.data``. Subclass so the type name also appears inside
    ``str(err)``, mirroring the real library shape."""

    def __init__(self, message: str, data: str | bytes | None) -> None:
        super().__init__(message)
        self.data = data


# -----------------------------------------------------------------------------
# _selector_from_web3_error
# -----------------------------------------------------------------------------


def test_extracts_selector_from_rpc_dict_args() -> None:
    """The historical path: ``err.args[0]`` is a dict with ``data`` set."""
    err = _FakeDictErr(
        {
            "code": 3,
            "message": "execution reverted",
            "data": _CONDITION_VIOLATION_FULL,
        }
    )
    assert _selector_from_web3_error(err) == _CONDITION_VIOLATION


def test_extracts_selector_from_nested_rpc_dict() -> None:
    """Some providers wrap the payload in ``{"error": {"data": ...}}``."""
    err = _FakeDictErr(
        {
            "error": {
                "code": 3,
                "data": _CONDITION_VIOLATION_FULL,
            }
        }
    )
    assert _selector_from_web3_error(err) == _CONDITION_VIOLATION


def test_extracts_selector_from_contract_logic_error_data_str() -> None:
    """Modern web3.py: ``ContractLogicError.data`` is a hex string."""
    err = _FakeContractLogicError(
        "execution reverted", _CONDITION_VIOLATION_FULL
    )
    assert _selector_from_web3_error(err) == _CONDITION_VIOLATION


def test_extracts_selector_from_contract_logic_error_data_bytes() -> None:
    """Older web3.py branches put raw bytes on ``.data``."""
    raw = bytes.fromhex(_CONDITION_VIOLATION_FULL.removeprefix("0x"))
    err = _FakeContractLogicError("execution reverted", raw)
    assert _selector_from_web3_error(err) == _CONDITION_VIOLATION


def test_extracts_selector_from_exception_string_fallback() -> None:
    """Worst case: the selector only appears embedded in ``str(err)``."""
    err = Exception(
        f"execution reverted: {_CONDITION_VIOLATION_FULL} — see receipt"
    )
    assert _selector_from_web3_error(err) == _CONDITION_VIOLATION


def test_extraction_priority_args_dict_beats_data_beats_str() -> None:
    """When all three surfaces exist, args-dict wins. (The real-world
    implication: ``ContractLogicError`` providers sometimes inherit an older
    ``args`` payload — we trust the structured source first.)"""
    # Three different selectors — we assert we picked the args-dict one.
    args_sel = "0xaaaaaaaa"
    data_sel = "0xbbbbbbbb"
    str_sel = "0xcccccccc"

    class _Multi(Exception):
        def __init__(self) -> None:
            super().__init__(
                {"data": args_sel + "11" * 4},  # args[0]
            )
            self.data = data_sel + "22" * 4
            self._msg = str_sel + "33" * 4

        def __str__(self) -> str:
            return f"execution reverted: {self._msg}"

    err = _Multi()
    assert _selector_from_web3_error(err) == args_sel


def test_extraction_priority_data_beats_str() -> None:
    """When ``args[0]`` is not a payload dict, ``err.data`` beats the string
    fallback."""
    data_sel = "0xbbbbbbbb"
    str_sel = "0xcccccccc"
    err = _FakeContractLogicError(
        f"execution reverted: {str_sel}11111111", data_sel + "22" * 4
    )
    assert _selector_from_web3_error(err) == data_sel


def test_returns_none_when_no_selector_anywhere() -> None:
    """Unrecognised shape returns ``None`` — caller treats as generic
    revert, which is the conservative default."""
    err = Exception("something went wrong with no hex digits")
    assert _selector_from_web3_error(err) is None


def test_returns_none_on_short_hex() -> None:
    """A ``0x`` + <8 hex chars is not a selector."""
    err = Exception("execution reverted: 0xabcd (too short)")
    assert _selector_from_web3_error(err) is None


def test_handles_memoryview_data() -> None:
    """``err.data`` as a memoryview should still be decoded."""
    raw = memoryview(bytes.fromhex(_CONDITION_VIOLATION_FULL.removeprefix("0x")))
    err = _FakeContractLogicError("execution reverted", raw)
    assert _selector_from_web3_error(err) == _CONDITION_VIOLATION


def test_handles_bytearray_data() -> None:
    """``err.data`` as a bytearray."""
    raw = bytearray.fromhex(_CONDITION_VIOLATION_FULL.removeprefix("0x"))
    err = _FakeContractLogicError("execution reverted", raw)
    assert _selector_from_web3_error(err) == _CONDITION_VIOLATION


def test_extracted_selector_matches_zodiac_set() -> None:
    """End-to-end: the selector we extract should be routable through
    ``is_zodiac_authz_revert``. Guards against case / prefix drift between
    the extractor and the authz set."""
    err = _FakeContractLogicError(
        "execution reverted", _CONDITION_VIOLATION_FULL
    )
    selector = _selector_from_web3_error(err)
    assert selector is not None
    assert selector in _ZODIAC_AUTHZ_ERROR_SELECTORS
    assert is_zodiac_authz_revert(selector) is True


# -----------------------------------------------------------------------------
# _normalise_selector — structural edge cases
# -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        # Plain hex string.
        ("0xdeadbeef00", "0xdeadbeef"),
        # Uppercase normalises to lowercase.
        ("0xDEADBEEF", "0xdeadbeef"),
        # Bytes.
        (bytes.fromhex("deadbeef00"), "0xdeadbeef"),
        # Bytearray.
        (bytearray.fromhex("deadbeef00"), "0xdeadbeef"),
        # Nested dict (``{"data": {"data": ...}}``).
        ({"data": {"data": "0xdeadbeef00"}}, "0xdeadbeef"),
        # Selector-only string (no args).
        ("0xdeadbeef", "0xdeadbeef"),
    ],
)
def test_normalise_selector_accepts_known_shapes(raw: object, expected: str) -> None:
    assert _normalise_selector(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        None,
        "",
        "not-hex",
        "0x12",  # too short
        123,  # int is not a supported shape
        [],  # list is not a supported shape
    ],
)
def test_normalise_selector_rejects_unsupported_shapes(raw: object) -> None:
    assert _normalise_selector(raw) is None
