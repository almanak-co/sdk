"""Unit tests for the shared trading-wallet resolver (VIB-6043).

``almanak/connectors/_strategy_base/base/receipt_wallet.py`` is the single
place every receipt parser now asks "which address is the strategy?". Its
resolution order is load-bearing for accounting correctness, so each step is
pinned here, including the negative cases (never fabricate an address).
"""

from almanak.connectors._strategy_base.base.receipt_wallet import (
    TRADING_WALLET_KEY,
    resolve_trading_wallet,
    safe_wallet_from_receipt,
    stamp_trading_wallet,
)

AGENT_EOA = "0x42657d7c6Fe1bC3FB0af01a702b88cC31A93661b"
SAFE = "0x4c373c8D5c486F601874EF02A2Cc19b5F4E9e837"
OTHER_SAFE = "0x1234567890AbcdEF1234567890aBcdef12345678"
MODULE = "0x1111111111111111111111111111111111111111"

EXECUTION_FROM_MODULE_SUCCESS = "0x6895c13664aa4f67288b25d7a21d7aaa34916e355fb9b6fae0a139a9085becb8"
EXECUTION_SUCCESS = "0x442e715f626346e8c54381002da614f62bee8d27386535b2521ec8540898556e"
TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def _topic_addr(address: str) -> str:
    return "0x" + "0" * 24 + address[2:].lower()


TOKEN = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
POOL = "0x7f90122bf0700f9e7e1f688fe926940e8839f353"


def safe_log(topic: str, safe: str = SAFE) -> dict:
    return {"address": safe, "topics": [topic, _topic_addr(MODULE)], "data": "0x"}


def transfer_log(sender: str, recipient: str, token: str = TOKEN) -> dict:
    """ERC-20 Transfer — the corroboration a Safe candidate must satisfy."""
    return {
        "address": token,
        "topics": [TRANSFER, _topic_addr(sender), _topic_addr(recipient)],
        "data": "0x" + f"{50_000_000:064x}",
    }


def safe_execution_receipt(topic: str, safe: str = SAFE) -> dict:
    """A realistic Safe-executed receipt: the Safe emits AND moves tokens."""
    return {
        "from": AGENT_EOA,
        "logs": [transfer_log(safe, POOL), safe_log(topic, safe)],
    }


# --- resolution order -------------------------------------------------------


def test_stamp_wins_over_everything():
    receipt = stamp_trading_wallet(safe_execution_receipt(EXECUTION_FROM_MODULE_SUCCESS, OTHER_SAFE), SAFE)
    assert resolve_trading_wallet(receipt) == SAFE.lower()


def test_stamp_is_the_only_way_a_safe_becomes_the_wallet():
    """An unstamped Safe receipt resolves to the SENDER, not the Safe.

    Deriving the Safe from ``ExecutionFromModuleSuccess`` / ``ExecutionSuccess``
    would be authentication by event signature, and any contract can emit a log
    with a colliding topic0 — an attacker-chosen address would then take over
    the traded legs. The resolver refuses to guess (PR #3439, flagged
    independently by CodeRabbit and Codex); threading the execution address is
    the fix for a caller with no context.
    """
    assert resolve_trading_wallet(safe_execution_receipt(EXECUTION_FROM_MODULE_SUCCESS)) == AGENT_EOA.lower()
    assert resolve_trading_wallet(safe_execution_receipt(EXECUTION_SUCCESS)) == AGENT_EOA.lower()


def test_look_alike_topic_can_never_displace_the_sender():
    """A spoofed Safe-execution topic must not become the trading wallet."""
    spoofer = "0xBADbadBADbadBADbadBADbadBADbadBADbadBAD0".lower()
    receipt = {
        "from": AGENT_EOA,
        "logs": [
            transfer_log(spoofer, POOL),  # the spoofer even moves tokens...
            safe_log(EXECUTION_FROM_MODULE_SUCCESS, spoofer),
        ],
    }
    assert resolve_trading_wallet(receipt) == AGENT_EOA.lower()  # ...and is still ignored


def test_heuristic_helper_stays_available_but_is_not_wired_in():
    """``safe_wallet_from_receipt`` is opt-in only; it must not affect resolution."""
    receipt = safe_execution_receipt(EXECUTION_FROM_MODULE_SUCCESS)
    assert safe_wallet_from_receipt(receipt) == SAFE.lower()
    assert resolve_trading_wallet(receipt) == AGENT_EOA.lower()


def test_heuristic_helper_refuses_an_emitter_that_moved_nothing():
    spoofer = "0xBADbadBADbadBADbadBADbadBADbadBADbadBAD0".lower()
    receipt = {
        "from": AGENT_EOA,
        "logs": [transfer_log(AGENT_EOA, POOL), safe_log(EXECUTION_FROM_MODULE_SUCCESS, spoofer)],
    }
    assert safe_wallet_from_receipt(receipt) == ""


def test_plain_eoa_receipt_falls_back_to_from():
    assert resolve_trading_wallet({"from": AGENT_EOA, "logs": []}) == AGENT_EOA.lower()


def test_from_address_alias_is_accepted():
    assert resolve_trading_wallet({"from_address": AGENT_EOA, "logs": []}) == AGENT_EOA.lower()


def test_two_distinct_safes_is_ambiguous_for_the_heuristic_helper():
    """Even the opt-in helper refuses to pick between two candidates."""
    receipt = {
        "from": AGENT_EOA,
        "logs": [
            transfer_log(SAFE, POOL),
            transfer_log(OTHER_SAFE, POOL),
            safe_log(EXECUTION_FROM_MODULE_SUCCESS, SAFE),
            safe_log(EXECUTION_SUCCESS, OTHER_SAFE),
        ],
    }
    assert safe_wallet_from_receipt(receipt) == ""
    assert resolve_trading_wallet(receipt) == AGENT_EOA.lower()


def test_same_safe_twice_is_not_ambiguous_for_the_heuristic_helper():
    receipt = {
        "from": AGENT_EOA,
        "logs": [
            transfer_log(SAFE, POOL),
            safe_log(EXECUTION_FROM_MODULE_SUCCESS),
            safe_log(EXECUTION_FROM_MODULE_SUCCESS),
        ],
    }
    assert safe_wallet_from_receipt(receipt) == SAFE.lower()


def test_transfer_logs_alone_never_produce_a_wallet():
    receipt = {
        "logs": [
            {"address": "0xaf88d065e77c8cc2239327c5edb3a432268e5831", "topics": [TRANSFER], "data": "0x"},
        ]
    }
    assert resolve_trading_wallet(receipt) == ""


# --- normalisation / robustness --------------------------------------------


def test_bytes_sender_is_normalised():
    receipt = {"from": bytes.fromhex(AGENT_EOA[2:]), "logs": []}
    assert resolve_trading_wallet(receipt) == AGENT_EOA.lower()


def test_bytes_topics_and_address_are_normalised_by_the_heuristic_helper():
    receipt = {
        "from": AGENT_EOA,
        "logs": [
            {
                "address": bytes.fromhex(TOKEN[2:]),
                "topics": [
                    bytes.fromhex(TRANSFER[2:]),
                    bytes.fromhex(_topic_addr(SAFE)[2:]),
                    bytes.fromhex(_topic_addr(POOL)[2:]),
                ],
                "data": "0x" + f"{1:064x}",
            },
            {
                "address": bytes.fromhex(SAFE[2:]),
                "topics": [bytes.fromhex(EXECUTION_FROM_MODULE_SUCCESS[2:])],
                "data": "0x",
            },
        ],
    }
    assert safe_wallet_from_receipt(receipt) == SAFE.lower()


def test_malformed_values_return_empty_not_garbage():
    assert resolve_trading_wallet({"from": "not-an-address", "logs": []}) == ""
    assert resolve_trading_wallet({"from": "0xdeadbeef", "logs": []}) == ""
    assert resolve_trading_wallet({"from": None, "logs": None}) == ""
    assert resolve_trading_wallet({}) == ""


def test_malformed_logs_do_not_crash():
    receipt = {"from": AGENT_EOA, "logs": ["nonsense", {"topics": None}, {}, {"topics": []}]}
    assert resolve_trading_wallet(receipt) == AGENT_EOA.lower()


# --- stamping ---------------------------------------------------------------


def test_stamp_does_not_mutate_the_input():
    original = {"from": AGENT_EOA, "logs": []}
    stamped = stamp_trading_wallet(original, SAFE)
    assert TRADING_WALLET_KEY not in original
    assert stamped[TRADING_WALLET_KEY] == SAFE.lower()


def test_stamp_with_empty_wallet_leaves_legacy_behaviour():
    for wallet in ("", None, "   ", "0xnope"):
        stamped = stamp_trading_wallet({"from": AGENT_EOA, "logs": []}, wallet)
        assert TRADING_WALLET_KEY not in stamped
        assert resolve_trading_wallet(stamped) == AGENT_EOA.lower()


def test_hex_method_returning_non_string_is_not_a_crash():
    """A ``hex()`` that returns bytes must degrade to "", not raise (CodeRabbit).

    These helpers promise ``""`` for malformed fields; a value whose ``hex()``
    returns a non-string previously reached ``str.startswith`` and raised,
    which would have surfaced as a parser crash rather than an unmeasured
    amount.
    """

    class _BadHex:
        def hex(self):
            return b"not-a-str"

    assert resolve_trading_wallet({"from": _BadHex(), "logs": []}) == ""
    receipt = {
        "from": AGENT_EOA,
        "logs": [{"address": SAFE, "topics": [_BadHex()], "data": "0x"}],
    }
    assert safe_wallet_from_receipt(receipt) == ""
    assert resolve_trading_wallet(receipt) == AGENT_EOA.lower()
