"""Unit tests for the shared LP leg-identity primitives (VIB-6053 / VIB-6045).

The two rules under test:

* :func:`transfers_by_token` is TOKEN-keyed and never magnitude-ordered — the
  decimals-blind raw-integer sort it replaces is what made Aerodrome's leg
  assignment arbitrary (VIB-6045).
* :func:`currencies_for_amounts` binds identity to the venue's own slot order by
  VALUE and FAILS CLOSED (``None``) rather than guessing.

And the predicate pair that reads that output correctly (VIB-6471 / VIB-6476):
:func:`slot_moved_money` / :func:`identity_is_complete` separate "this slot moved
nothing, so its identity is moot" from "this slot moved money and we could not
identify it" — two states the ``None`` return collapses together, and which every
consumer used to conflate by testing ``currency0 and currency1``. The banner for
that class: **an observation's presence is being treated as its success.**
"""

from __future__ import annotations

import pytest

from almanak.connectors._strategy_base.lp_leg_identity import (
    TRANSFER_TOPIC,
    currencies_for_amounts,
    identity_is_complete,
    slot_moved_money,
    transfers_by_token,
)

WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
POOL = "0xc6962004f452be9203591991d15f6b388e09e8d0"
WALLET = "0x1111111111111111111111111111111111111111"
ZERO = "0x0000000000000000000000000000000000000000"


def _topic(address: str) -> str:
    return "0x" + address[2:].rjust(64, "0")


def _transfer_log(token: str, src: str, dst: str, value: int) -> dict:
    return {
        "address": token,
        "topics": [TRANSFER_TOPIC, _topic(src), _topic(dst)],
        "data": "0x" + format(value, "064x"),
    }


class TestTransfersByToken:
    def test_accumulates_per_token_not_by_magnitude(self):
        """A 6-dp and an 18-dp leg keep their own identity regardless of raw size.

        This is the VIB-6045 shape: WETH's raw value (2.9e16) dwarfs USDC's
        (5.5e7), so any magnitude ordering silently swaps the legs.
        """
        logs = [
            _transfer_log(WETH, POOL, WALLET, 29331153505356442),
            _transfer_log(USDC, POOL, WALLET, 55056478),
        ]
        totals = transfers_by_token(logs, chain="arbitrum", from_address=POOL)
        assert totals == {WETH: 29331153505356442, USDC: 55056478}

    def test_sums_split_legs_rather_than_last_write_wins(self):
        logs = [
            _transfer_log(USDC, POOL, WALLET, 40_000_000),
            _transfer_log(USDC, POOL, WALLET, 15_056_478),
        ]
        assert transfers_by_token(logs, chain="arbitrum", from_address=POOL) == {USDC: 55_056_478}

    def test_filters_by_direction(self):
        logs = [
            _transfer_log(USDC, WALLET, POOL, 100),
            _transfer_log(WETH, POOL, WALLET, 200),
        ]
        assert transfers_by_token(logs, chain="arbitrum", to_address=POOL) == {USDC: 100}
        assert transfers_by_token(logs, chain="arbitrum", from_address=POOL) == {WETH: 200}

    def test_skips_lp_token_mint_and_burn(self):
        logs = [
            _transfer_log(POOL, ZERO, WALLET, 999),  # LP-token mint
            _transfer_log(POOL, WALLET, ZERO, 999),  # LP-token burn
            _transfer_log(USDC, POOL, WALLET, 100),
        ]
        assert transfers_by_token(logs, chain="arbitrum", from_address=POOL) == {USDC: 100}

    def test_ignores_non_transfer_and_malformed_logs(self):
        logs = [
            {"address": USDC, "topics": ["0xdeadbeef"], "data": "0x00"},
            {"address": USDC, "topics": [TRANSFER_TOPIC], "data": "0x00"},  # too few topics
            {"address": WETH, "topics": None, "data": None},
            _transfer_log(USDC, POOL, WALLET, 7),
        ]
        assert transfers_by_token(logs, chain="arbitrum", from_address=POOL) == {USDC: 7}

    def test_empty_logs_is_empty_mapping(self):
        assert transfers_by_token([], chain="arbitrum") == {}
        assert transfers_by_token(None, chain="arbitrum") == {}

    def test_omitted_filters_scan_all_transfers(self):
        assert transfers_by_token([_transfer_log(USDC, POOL, WALLET, 7)], chain="arbitrum") == {USDC: 7}

    @pytest.mark.parametrize("filter_name", ["from_address", "to_address"])
    @pytest.mark.parametrize("malformed_filter", ["", "malformed-pool", "0x1234"])
    def test_supplied_malformed_filter_fails_closed(self, filter_name, malformed_filter):
        kwargs = {filter_name: malformed_filter}

        assert transfers_by_token([_transfer_log(USDC, POOL, WALLET, 7)], chain="arbitrum", **kwargs) == {}

    def test_accepts_bytes_topics_and_address(self):
        entry = {
            "address": bytes.fromhex(USDC[2:]),
            "topics": [
                bytes.fromhex(TRANSFER_TOPIC[2:]),
                bytes.fromhex(_topic(POOL)[2:]),
                bytes.fromhex(_topic(WALLET)[2:]),
            ],
            "data": "0x" + format(42, "064x"),
        }
        assert transfers_by_token([entry], chain="arbitrum", from_address=POOL) == {USDC: 42}

    @pytest.mark.parametrize(
        "malformed_emitter",
        [
            "0x",
            "0x" + "g" * 40,
            "0x" + "a" * 39,
            "0x" + "a" * 41,
            bytes.fromhex("aa" * 19),
            bytes.fromhex("aa" * 21),
        ],
    )
    def test_skips_transfer_logs_with_malformed_emitters(self, malformed_emitter):
        entry = _transfer_log(USDC, POOL, WALLET, 42)
        entry["address"] = malformed_emitter

        assert transfers_by_token([entry], chain="arbitrum", from_address=POOL) == {}


class TestCurrenciesForAmounts:
    def test_binds_identity_to_venue_slot_order(self):
        """The uniswap_v3__ethereum evidence shape: amount0 is the 6-dp leg."""
        totals = {USDC: 49644145, WETH: 26524078519924184}
        assert currencies_for_amounts(totals, 49644145, 26524078519924184) == (USDC, WETH)

    def test_binding_is_independent_of_label_order(self):
        """Reversing the venue's slot order reverses the identities, not the amounts."""
        totals = {USDC: 49644145, WETH: 26524078519924184}
        assert currencies_for_amounts(totals, 26524078519924184, 49644145) == (WETH, USDC)

    def test_measured_zero_leg_is_unidentified_but_harmless(self):
        """A measured 0 scales to 0 under any decimals, so leaving it None is lossless."""
        assert currencies_for_amounts({USDC: 100}, 100, 0) == (USDC, None)
        assert currencies_for_amounts({USDC: 100}, 0, 100) == (None, USDC)

    def test_unmeasured_leg_is_none(self):
        assert currencies_for_amounts({USDC: 100}, 100, None) == (USDC, None)
        assert currencies_for_amounts({}, None, None) == (None, None)

    def test_fails_closed_when_no_transfer_matches(self):
        """No guess: an unmatched amount yields None, never a label-order fallback."""
        assert currencies_for_amounts({USDC: 100}, 999, 888) == (None, None)

    def test_degenerate_equal_amounts_are_deterministic(self):
        """Both legs moved the same raw amount — assignment must not depend on dict order."""
        totals = {WETH: 1000, USDC: 1000}
        first = currencies_for_amounts(totals, 1000, 1000)
        second = currencies_for_amounts({USDC: 1000, WETH: 1000}, 1000, 1000)
        assert first == second
        assert set(first) == {WETH, USDC}
        # Ascending address order — the V3-family pool convention.
        assert int(first[0], 16) < int(first[1], 16)

    def test_a_token_is_never_bound_to_both_slots(self):
        assert currencies_for_amounts({USDC: 100}, 100, 100) == (USDC, None)


class TestSlotMovedMoney:
    """VIB-6471 — "did this slot move money?" is the question that makes a ``None``
    currency readable. A slot that moved nothing needs no identity (0 scales to 0
    under any decimals); a slot that DID move and has none is the defect."""

    @pytest.mark.parametrize("amount", [None, 0, "0", "  0  ", 0.0])
    def test_unmeasured_or_zero_did_not_move(self, amount):
        assert slot_moved_money(amount) is False

    @pytest.mark.parametrize("amount", [1, -1, 49644145, 26524078519924184, "55056478"])
    def test_nonzero_moved(self, amount):
        assert slot_moved_money(amount) is True

    @pytest.mark.parametrize("amount", ["", "abc", "1.5", [], {}, object()])
    def test_unparseable_counts_as_did_not_move_and_never_raises(self, amount):
        """This runs on the accounting write path, so a garbage amount must not
        raise. "Did not move" is the permissive reading, which is safe only
        because ``lp_handler._to_human_from_raw`` fails the same conversion and
        books the slot unmeasured rather than mis-scaled."""
        assert slot_moved_money(amount) is False

    @pytest.mark.parametrize("amount", [float("inf"), float("-inf")])
    def test_infinity_is_caught_not_raised(self, amount):
        """``int(float("inf"))`` raises OverflowError, NOT ValueError, so it
        escaped the original except clause and would have propagated out of a
        function whose contract is that it never raises — on the accounting
        write path, where an exception aborts the write. Found by CodeRabbit on
        the PR #3586 panel."""
        assert slot_moved_money(amount) is False


class TestIdentityIsComplete:
    """VIB-6471 / VIB-6476 — every slot that MOVED money carries its own identity.

    The predicate the consumers should have been testing all along. ``currency0 and
    currency1`` answers "are both present?", which is a different question and gives
    the wrong answer in BOTH directions: it reads a routine single-sided close as a
    failed observation, and a present-but-unresolvable pair as a successful one.
    """

    def test_both_slots_moved_and_both_bound(self):
        assert identity_is_complete(USDC, WETH, 49644145, 26524078519924184) is True

    def test_moot_slot_needs_no_identity(self):
        """THE VIB-6471 CASE. A single-sided close: slot 0 moved and is identified,
        slot 1 moved nothing so ``currencies_for_amounts`` left it ``None``. That is
        a COMPLETE observation — a presence test calls it a failure and suppresses
        the realignment that keeps the row's decimals paired with its amounts."""
        assert identity_is_complete(USDC, None, 100, 0) is True
        assert identity_is_complete(None, USDC, 0, 100) is True

    def test_slot_that_moved_without_identity_fails_closed(self):
        """Identity undeterminable — the value-join found no unambiguous match.
        Same ``None`` as the moot slot above, opposite meaning."""
        assert identity_is_complete(None, WETH, 49644145, 26524078519924184) is False
        assert identity_is_complete(USDC, None, 49644145, 26524078519924184) is False
        assert identity_is_complete(None, None, 49644145, 26524078519924184) is False

    def test_neither_slot_moved_is_vacuously_complete(self):
        """Nothing moved, so nothing can be mis-scaled. Callers that need a USABLE
        pair must additionally require at least one bound currency — which is why
        the handler's gate is ``(currency0 or currency1) and identity_is_complete``
        rather than ``identity_is_complete`` alone."""
        assert identity_is_complete(None, None, 0, 0) is True
        assert identity_is_complete(None, None, None, None) is True

    def test_empty_string_is_not_an_identity(self):
        """``""`` is the parser-did-not-emit sentinel (Empty != Zero), not an
        address. A slot that moved money and carries it is unidentified."""
        assert identity_is_complete("", WETH, 100, 200) is False
        assert identity_is_complete(USDC, "", 100, 200) is False
        # ...but an empty identity on a slot that moved nothing is still moot.
        assert identity_is_complete(USDC, "", 100, 0) is True


class TestV3SplitTxCloseBindsIdentity:
    """VIB-6053 — the split-tx close shape that a first pass silently missed.

    A V3 close is a SEQUENCE: `decreaseLiquidity` -> `collect` -> `burn`. All three
    V3-family parsers captured the pool address ONLY from the Burn event, so on the
    collect-only receipt `pool_address` is `""` — and the leg-identity scan, gated
    on it, found no counterparty and emitted no currencies. The row then fell back
    to the intent's label order and stayed transposed.

    This was caught only by reading the persisted ledger amounts of a real fork run:
    the harness reported the cell PASS. A verdict is not proof.

    The pool emits `Collect` as well as `Burn`, so the collect log's own emitter is
    the pool. These tests pin that binding for all three parsers, on a receipt with
    NO Burn event at all.
    """

    POOL = "0x1ac1a8feaaea1900c4166deeed0c11cc10669d36"
    NPM = "0x46a15b0b27311cedf172ab29e4f4766fbe7f4364"
    WALLET = "0x1111111111111111111111111111111111111111"
    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    A0 = 49963987  # USDC, 6dp
    A1 = 26367969791857162  # WETH, 18dp

    def _collect_only_receipt(self, collect_topic: str) -> dict:
        zero = "0x" + format(0, "064x")
        return {
            "logs": [
                _transfer_log(self.USDC, self.POOL, self.WALLET, self.A0),
                _transfer_log(self.WETH, self.POOL, self.WALLET, self.A1),
                {
                    "address": self.POOL,
                    "topics": [collect_topic, _topic(self.NPM), zero, zero],
                    "data": "0x"
                    + format(int(self.WALLET, 16), "064x")
                    + format(self.A0, "064x")
                    + format(self.A1, "064x"),
                },
            ],
            "status": 1,
            "transactionHash": "0x" + "aa" * 32,
            "blockNumber": 1,
            "from": self.WALLET,
        }

    @pytest.mark.parametrize(
        "module_path,cls_name",
        [
            ("almanak.connectors.uniswap_v3.receipt_parser", "UniswapV3ReceiptParser"),
            ("almanak.connectors.sushiswap_v3.receipt_parser", "SushiSwapV3ReceiptParser"),
            ("almanak.connectors.pancakeswap_v3.receipt_parser", "PancakeSwapV3ReceiptParser"),
        ],
    )
    def test_collect_only_receipt_still_binds_identity(self, module_path, cls_name):
        import importlib

        mod = importlib.import_module(module_path)
        parser = getattr(mod, cls_name)(chain="ethereum")
        data = parser.extract_lp_close_data(self._collect_only_receipt(mod.EVENT_TOPICS["Collect"].lower()))

        assert data is not None
        # No Burn event => the registry anchor is legitimately empty ...
        assert data.pool_address == ""
        # ... but identity must STILL be bound, from the Collect emitter.
        assert data.currency0 == self.USDC
        assert data.currency1 == self.WETH
        assert data.amount0_collected == self.A0
        assert data.amount1_collected == self.A1


class TestV3OpenBindsIdentity:
    """VIB-6053 — the LP_OPEN half of the identity binding, on all three V3 parsers.

    Companion to `TestV3SplitTxCloseBindsIdentity`. An LP_OPEN is a single tx (the NPM
    mints and the pool pulls both legs via the mint callback), so unlike the close there
    is no split-tx subtlety — but the binding still has to be exercised, and pancake's
    `extract_lp_open_data` had no test at all before this.

    The V3 mint callback transfers exactly `amount0Owed` / `amount1Owed`, so the
    value-match against the wallet -> pool transfers is exact rather than heuristic.
    """

    POOL = "0x1ac1a8feaaea1900c4166deeed0c11cc10669d36"
    NPM = "0x46a15b0b27311cedf172ab29e4f4766fbe7f4364"
    WALLET = "0x1111111111111111111111111111111111111111"
    USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
    TOKEN_ID = 1_340_010
    LIQ = 500_000
    A0 = 49_644_145  # USDC, 6dp  -> pool slot 0 on ethereum (0xa0b8… < 0xc02a…)
    A1 = 26_524_078_519_924_184  # WETH, 18dp -> pool slot 1

    @staticmethod
    def _npm_for(mod) -> str:
        """Each V3 fork has its OWN NonfungiblePositionManager address.

        Hard-coding one NPM makes the Mint-owner check fail for the other two parsers,
        which turns the test into a skip — i.e. vacuous exactly where it looks covered.
        Read each parser's own registry instead.
        """
        return str(mod.POSITION_MANAGER_ADDRESSES.get("ethereum", "")).lower()

    def _mint_receipt(self, mod, npm: str) -> dict:
        """NPM IncreaseLiquidity + pool Mint + the two callback transfers."""
        zero = "0x" + format(0, "064x")
        mint_topic = mod.EVENT_TOPICS["Mint"].lower()
        inc_topic = mod.EVENT_TOPICS["IncreaseLiquidity"].lower()
        return {
            "logs": [
                # wallet -> pool, exactly the amounts the Mint reports
                _transfer_log(self.USDC, self.WALLET, self.POOL, self.A0),
                _transfer_log(self.WETH, self.WALLET, self.POOL, self.A1),
                # pool Mint: topics = [sig, owner(NPM), tickLower, tickUpper]
                # data = sender(32B) ‖ liquidity ‖ amount0 ‖ amount1
                {
                    "address": self.POOL,
                    "topics": [mint_topic, _topic(npm), zero, zero],
                    "data": "0x"
                    + format(int(npm, 16), "064x")
                    + format(self.LIQ, "064x")
                    + format(self.A0, "064x")
                    + format(self.A1, "064x"),
                },
                # NPM IncreaseLiquidity: topics = [sig, tokenId]
                # data = liquidity ‖ amount0 ‖ amount1
                {
                    "address": npm,
                    "topics": [inc_topic, "0x" + format(self.TOKEN_ID, "064x")],
                    "data": "0x" + format(self.LIQ, "064x") + format(self.A0, "064x") + format(self.A1, "064x"),
                },
            ],
            "status": 1,
            "transactionHash": "0x" + "bb" * 32,
            "blockNumber": 1,
            "from": self.WALLET,
        }

    @pytest.mark.parametrize(
        "module_path,cls_name",
        [
            ("almanak.connectors.uniswap_v3.receipt_parser", "UniswapV3ReceiptParser"),
            ("almanak.connectors.sushiswap_v3.receipt_parser", "SushiSwapV3ReceiptParser"),
            ("almanak.connectors.pancakeswap_v3.receipt_parser", "PancakeSwapV3ReceiptParser"),
        ],
    )
    def test_open_binds_currencies_to_pool_slot_amounts(self, module_path, cls_name):
        import importlib

        mod = importlib.import_module(module_path)
        parser = getattr(mod, cls_name)(chain="ethereum")
        npm = self._npm_for(mod)
        assert npm, f"{cls_name}: no ethereum NPM in POSITION_MANAGER_ADDRESSES"
        data = parser.extract_lp_open_data(self._mint_receipt(mod, npm))

        # No skip: a skip here would make the test vacuous for the parsers it is
        # named after. If the shape is not recognised that is a real failure.
        assert data is not None, f"{cls_name} did not extract LP open data"

        assert data.amount0 == self.A0
        assert data.amount1 == self.A1
        # Identity bound to the SLOT, so the 6-dp leg is slot 0 here regardless of
        # what the user's pool label says.
        assert data.currency0 == self.USDC
        assert data.currency1 == self.WETH


class TestV3OpenGuardBranches:
    """VIB-6053 — the refusal branches of `extract_lp_open_data`.

    These assert what the parser does when a receipt is NOT a well-formed LP open.
    Every one must return `None` — "I did not observe an open" — rather than an
    `LPOpenData` carrying fabricated or partially-decoded values. That distinction
    is the Empty≠Zero contract at the parser boundary: a payload the parser could
    not read must not become a measured row downstream.

    `extract_lp_open_data` had no direct test before this PR, so these branches were
    live-but-unexercised on every PancakeSwap LP open in production.
    """

    NPM_ETH = "0x46a15b0b27311cedf172ab29e4f4766fbe7f4364"
    POOL = "0x1ac1a8feaaea1900c4166deeed0c11cc10669d36"
    WALLET = "0x1111111111111111111111111111111111111111"

    @staticmethod
    def _parser():
        import importlib

        mod = importlib.import_module("almanak.connectors.pancakeswap_v3.receipt_parser")
        return mod.PancakeSwapV3ReceiptParser(chain="ethereum"), mod

    def _receipt(self, logs):
        return {
            "logs": logs,
            "status": 1,
            "transactionHash": "0x" + "cc" * 32,
            "blockNumber": 1,
            "from": self.WALLET,
        }

    def test_empty_logs_yields_no_open_data(self):
        parser, _ = self._parser()
        assert parser.extract_lp_open_data(self._receipt([])) is None

    def test_receipt_without_increase_liquidity_yields_no_open_data(self):
        """A transfer-only receipt is not an LP open — must not be guessed into one."""
        parser, _ = self._parser()
        logs = [_transfer_log(USDC, self.WALLET, self.POOL, 1_000_000)]
        assert parser.extract_lp_open_data(self._receipt(logs)) is None

    def test_increase_liquidity_from_a_foreign_contract_is_ignored(self):
        """Only the canonical NPM may mint a position we account for.

        An IncreaseLiquidity-shaped log emitted by some other contract must not be
        adopted — otherwise any contract could inject a position into the ledger.
        """
        parser, mod = self._parser()
        inc = mod.EVENT_TOPICS["IncreaseLiquidity"].lower()
        logs = [
            {
                "address": "0x" + "99" * 20,  # NOT the NPM
                "topics": [inc, "0x" + format(4242, "064x")],
                "data": "0x" + format(1, "064x") * 3,
            }
        ]
        assert parser.extract_lp_open_data(self._receipt(logs)) is None

    def test_truncated_increase_liquidity_payload_raises_rather_than_decoding_garbage(self):
        """A short payload must NOT decode to partial amounts — it raises.

        Decoding 96 bytes of amounts out of a truncated blob would silently produce a
        real-looking `LPOpenData` with garbage legs — a fabricated measurement, which
        is what Empty≠Zero forbids. The parser refuses LOUDLY here, and the
        `_result` wrapper is the fail-closed boundary that turns that into a typed
        extraction failure rather than a crash on the write path.

        Pinned deliberately: a future "robustness" change that swallowed this into a
        `None` (or worse, a zero-filled LPOpenData) would be a regression, not a
        hardening.
        """
        parser, mod = self._parser()
        inc = mod.EVENT_TOPICS["IncreaseLiquidity"].lower()
        logs = [
            {
                "address": self.NPM_ETH,
                "topics": [inc, "0x" + format(4242, "064x")],
                "data": "0x" + format(1, "064x"),  # 1 word, needs 3
            }
        ]
        with pytest.raises(ValueError, match="Truncated IncreaseLiquidity payload"):
            parser.extract_lp_open_data(self._receipt(logs))

        # The fail-closed wrapper must convert it to a TYPED error, not propagate it and
        # not swallow it into a benign ExtractMissing/None. `getattr(result, "ok", False)`
        # is false for every result dataclass, so it never proved a typed failure.
        from almanak.framework.execution.extract_result import ExtractError

        result = parser.extract_lp_open_data_result(self._receipt(logs))
        assert isinstance(result, ExtractError), (
            f"truncated payload must be a typed ExtractError, got {type(result).__name__}"
        )
        assert "PancakeSwap V3 IncreaseLiquidity decode failed: malformed data" in result.error

    def test_lp_open_result_tags_extractor_exception_after_successful_probe(self):
        parser, _ = self._parser()

        def fail_extract(_receipt):
            raise RuntimeError("open extractor failed")

        parser.extract_lp_open_data = fail_extract
        result = parser.extract_lp_open_data_result(self._receipt([]))

        from almanak.framework.execution.extract_result import ExtractError

        assert isinstance(result, ExtractError)
        assert result.error == "RuntimeError: open extractor failed"

    def test_malformed_token_id_topic_is_skipped_not_crashed(self):
        parser, mod = self._parser()
        inc = mod.EVENT_TOPICS["IncreaseLiquidity"].lower()
        logs = [
            {
                "address": self.NPM_ETH,
                "topics": [inc, "0xnot-a-number"],
                "data": "0x" + format(1, "064x") * 3,
            }
        ]
        assert parser.extract_lp_open_data(self._receipt(logs)) is None

    def test_empty_data_field_is_skipped(self):
        parser, mod = self._parser()
        inc = mod.EVENT_TOPICS["IncreaseLiquidity"].lower()
        logs = [
            {
                "address": self.NPM_ETH,
                "topics": [inc, "0x" + format(4242, "064x")],
                "data": "0x",
            }
        ]
        assert parser.extract_lp_open_data(self._receipt(logs)) is None
