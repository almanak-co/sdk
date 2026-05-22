"""Comprehensive unit tests for the T12 registry-dispatch helpers on
``StrategyRunner`` (VIB-4198).

The L2 contract test exercises the orchestrator end-to-end on the T08
fixtures, but the per-branch behaviour of each helper that
``_maybe_save_ledger_with_registry`` depends on goes uncovered by CI's
narrower test slice (cov 4-18%). With cc² × (1 − cov) as the multiplier,
every cc=6-15 helper in this batch trips the CRAP gate even though each
is structurally simple.

This file pins each branch of the following T12 helpers:

- ``_coerce_receipt_to_dict``
- ``_collect_candidate_receipts``
- ``_receipt_has_lp_topic``
- ``_extract_block_number_from_result``
- ``_intent_fee_tier``
- ``_lookup_open_registry_payload``
- ``_update_lp_registry_id_cache``
- ``_maybe_save_ledger_with_registry`` (orchestrator)

Per blueprint 28 §5 (Migration plan from `_write_ledger_entry`):
``_maybe_save_ledger_with_registry`` is the LP cutover dispatch hook —
when the boot guard has cleared the (Primitive.LP, "lp") cutover, it
routes the atomic write through ``save_ledger_and_registry(mode='registry')``;
otherwise it returns False so the caller falls back to
``save_ledger_entry``. The helpers in this file are the path-applicability
gate + RegistryRow assembly steps it composes from.

Tests bind unbound methods to a ``SimpleNamespace`` ``self`` whose leaf
collaborators are stubbed. Function-under-test branches run unmocked —
identity, payload shape, dispatch decisions are all real production
behaviour exercised within the function body.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.runner.strategy_runner import StrategyRunner

# Take unbound methods off the class so we can drive them without
# invoking ``__init__``.
_coerce_receipt_to_dict = StrategyRunner._coerce_receipt_to_dict
_collect_candidate_receipts = StrategyRunner._collect_candidate_receipts
_receipt_has_lp_topic = StrategyRunner._receipt_has_lp_topic
_extract_block_number_from_result = StrategyRunner._extract_block_number_from_result
_intent_fee_tier = StrategyRunner._intent_fee_tier
_extract_receipt_from_result = StrategyRunner._extract_receipt_from_result


# ---------------------------------------------------------------------------
# _coerce_receipt_to_dict
# ---------------------------------------------------------------------------


class TestCoerceReceiptToDict:
    """Receipt-shape coercion: dict / to_dict / .logs / None."""

    def test_none_returns_none(self) -> None:
        assert _coerce_receipt_to_dict(None) is None

    def test_dict_with_logs_passes_through(self) -> None:
        d = {"logs": [{"topics": ["0xabc"]}]}
        assert _coerce_receipt_to_dict(d) is d

    def test_dict_without_logs_returns_none(self) -> None:
        # No "logs" key → the receipt is not parser-usable.
        assert _coerce_receipt_to_dict({"blockNumber": 100}) is None

    def test_dict_with_explicit_none_logs_returns_none(self) -> None:
        assert _coerce_receipt_to_dict({"logs": None}) is None

    def test_object_with_to_dict_returns_dict(self) -> None:
        class _Receipt:
            def to_dict(self) -> dict:
                return {"logs": ["log-a"]}

        out = _coerce_receipt_to_dict(_Receipt())
        assert out == {"logs": ["log-a"]}

    def test_object_with_to_dict_returning_no_logs_falls_through(self) -> None:
        # to_dict returns a dict without logs — fall through to .logs check.
        class _ReceiptNoLogs:
            def to_dict(self) -> dict:
                return {"blockNumber": 5}

            logs = ["fallback"]

        out = _coerce_receipt_to_dict(_ReceiptNoLogs())
        # to_dict returned a dict-without-logs; .logs attr check returns
        # the constructed dict.
        assert out == {"logs": ["fallback"]}

    def test_object_with_logs_attr_only(self) -> None:
        class _ReceiptObj:
            logs = [{"topic": "0xa"}]

        out = _coerce_receipt_to_dict(_ReceiptObj())
        assert out == {"logs": [{"topic": "0xa"}]}

    def test_object_with_none_logs_attr_returns_none(self) -> None:
        class _ReceiptNoneLogs:
            logs = None

        assert _coerce_receipt_to_dict(_ReceiptNoneLogs()) is None

    def test_arbitrary_object_returns_none(self) -> None:
        # No to_dict, no logs.
        assert _coerce_receipt_to_dict(SimpleNamespace(foo="bar")) is None


# ---------------------------------------------------------------------------
# _collect_candidate_receipts
# ---------------------------------------------------------------------------


class TestCollectCandidateReceipts:
    """All four candidate-source paths."""

    def test_singular_receipt_attr_collected(self) -> None:
        result = SimpleNamespace(
            transaction_receipt={"logs": [{"topics": ["0xa"]}]},
        )
        out = _collect_candidate_receipts(result)
        assert len(out) == 1
        assert out[0]["logs"]

    def test_singular_alternate_attr_names(self) -> None:
        # Each of the 4 alternate attr names is checked.
        for attr in ("receipt", "tx_receipt", "raw_receipt"):
            result = SimpleNamespace(**{attr: {"logs": ["x"]}})
            out = _collect_candidate_receipts(result)
            assert len(out) == 1, f"attr={attr}"

    def test_transaction_results_dict_shape(self) -> None:
        # Local ExecutionResult shape: list of dicts with .receipt + .success.
        result = SimpleNamespace(
            transaction_results=[
                {"success": True, "receipt": {"logs": ["ok"]}},
                {"success": False, "receipt": {"logs": ["skip"]}},
            ]
        )
        out = _collect_candidate_receipts(result)
        # Failed tx is filtered (success=False).
        assert len(out) == 1
        assert out[0]["logs"] == ["ok"]

    def test_transaction_results_object_shape(self) -> None:
        # Local ExecutionResult shape: list of objects with attrs.
        result = SimpleNamespace(
            transaction_results=[
                SimpleNamespace(success=True, receipt={"logs": ["a"]}),
                SimpleNamespace(success=True, receipt={"logs": ["b"]}),
            ]
        )
        out = _collect_candidate_receipts(result)
        assert len(out) == 2

    def test_transaction_results_dict_input_shape(self) -> None:
        # Result is a dict carrying transaction_results key.
        result = {
            "transaction_results": [
                {"success": True, "receipt": {"logs": ["x"]}},
            ]
        }
        out = _collect_candidate_receipts(result)
        assert len(out) == 1

    def test_receipts_list_collected(self) -> None:
        # GatewayExecutionResult shape.
        result = SimpleNamespace(
            receipts=[{"logs": ["a"]}, {"logs": ["b"]}]
        )
        out = _collect_candidate_receipts(result)
        assert len(out) == 2

    def test_transaction_receipts_list_collected(self) -> None:
        # Alternate attribute name.
        result = SimpleNamespace(transaction_receipts=[{"logs": ["x"]}])
        out = _collect_candidate_receipts(result)
        assert len(out) == 1

    def test_no_candidate_returns_empty(self) -> None:
        out = _collect_candidate_receipts(SimpleNamespace())
        assert out == []

    def test_combined_sources_all_collected(self) -> None:
        result = SimpleNamespace(
            receipt={"logs": ["singular"]},
            transaction_results=[
                {"success": True, "receipt": {"logs": ["tx-results"]}},
            ],
            receipts=[{"logs": ["bundle-1"]}, {"logs": ["bundle-2"]}],
        )
        out = _collect_candidate_receipts(result)
        # 1 + 1 + 2 = 4 candidates across all source types.
        assert len(out) == 4

    def test_failed_tx_with_object_shape_filtered(self) -> None:
        result = SimpleNamespace(
            transaction_results=[
                SimpleNamespace(success=False, receipt={"logs": ["dropped"]}),
                SimpleNamespace(success=True, receipt={"logs": ["kept"]}),
            ]
        )
        out = _collect_candidate_receipts(result)
        assert len(out) == 1


# ---------------------------------------------------------------------------
# _receipt_has_lp_topic
# ---------------------------------------------------------------------------


class TestReceiptHasLpTopic:
    """Whether a receipt carries IncreaseLiquidity / DecreaseLiquidity topics."""

    INCREASE_TOPIC = "0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f"
    DECREASE_TOPIC = "0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4"

    def test_increase_liquidity_topic_matches(self) -> None:
        rec = {"logs": [{"topics": [self.INCREASE_TOPIC]}]}
        assert _receipt_has_lp_topic(rec) is True

    def test_decrease_liquidity_topic_matches(self) -> None:
        rec = {"logs": [{"topics": [self.DECREASE_TOPIC]}]}
        assert _receipt_has_lp_topic(rec) is True

    def test_unrelated_topic_returns_false(self) -> None:
        # Approval signature.
        rec = {
            "logs": [
                {"topics": ["0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"]}
            ]
        }
        assert _receipt_has_lp_topic(rec) is False

    def test_empty_logs(self) -> None:
        assert _receipt_has_lp_topic({"logs": []}) is False

    def test_no_logs_key(self) -> None:
        assert _receipt_has_lp_topic({}) is False

    def test_log_with_empty_topics_skipped(self) -> None:
        rec = {"logs": [{"topics": []}]}
        assert _receipt_has_lp_topic(rec) is False

    def test_log_object_shape(self) -> None:
        # log is an object with .topics attr (not a dict).
        rec = {"logs": [SimpleNamespace(topics=[self.INCREASE_TOPIC])]}
        assert _receipt_has_lp_topic(rec) is True

    def test_topic_as_bytes(self) -> None:
        # Topic returned as bytes (web3.py raw form).
        topic_bytes = bytes.fromhex(self.INCREASE_TOPIC[2:])
        rec = {"logs": [{"topics": [topic_bytes]}]}
        assert _receipt_has_lp_topic(rec) is True

    def test_topic_uppercased(self) -> None:
        # Some sources emit upper-case hex.
        rec = {"logs": [{"topics": [self.INCREASE_TOPIC.upper()]}]}
        assert _receipt_has_lp_topic(rec) is True

    def test_topic_without_0x_prefix(self) -> None:
        # Defensive: topic might come in without 0x.
        rec = {"logs": [{"topics": [self.INCREASE_TOPIC[2:]]}]}
        assert _receipt_has_lp_topic(rec) is True

    def test_first_topic_unrelated_others_match_returns_false(self) -> None:
        # Only topics[0] is the event signature — only that one is checked.
        rec = {
            "logs": [
                {
                    "topics": [
                        "0xdeadbeef" + "00" * 28,  # not an LP topic
                        self.INCREASE_TOPIC,  # other indexed topic, irrelevant
                    ]
                }
            ]
        }
        assert _receipt_has_lp_topic(rec) is False

    def test_mixed_logs_one_lp_topic(self) -> None:
        rec = {
            "logs": [
                {"topics": ["0xaaaa" + "00" * 30]},
                {"topics": [self.DECREASE_TOPIC]},
                {"topics": ["0xbbbb" + "00" * 30]},
            ]
        }
        assert _receipt_has_lp_topic(rec) is True


# ---------------------------------------------------------------------------
# _extract_block_number_from_result
# ---------------------------------------------------------------------------


class TestExtractBlockNumberFromResult:
    """Block-number extraction from receipt's blockNumber field."""

    def _result(self, *, block_number: Any = 100) -> SimpleNamespace:
        return SimpleNamespace(receipt={"logs": ["x"], "blockNumber": block_number})

    def test_none_result_returns_none(self) -> None:
        assert _extract_block_number_from_result(None) is None

    def test_integer_block_number_passes_through(self) -> None:
        assert _extract_block_number_from_result(self._result(block_number=12345)) == 12345

    def test_string_block_number_coerced(self) -> None:
        assert _extract_block_number_from_result(self._result(block_number="789")) == 789

    def test_hex_string_block_number_uses_int_no_base(self) -> None:
        # int("0x1A") raises (no base specified) — must return None, not raise.
        assert _extract_block_number_from_result(self._result(block_number="0x1A")) is None

    def test_none_block_number(self) -> None:
        assert _extract_block_number_from_result(self._result(block_number=None)) is None

    def test_garbage_block_number_returns_none(self) -> None:
        assert _extract_block_number_from_result(self._result(block_number="not-a-number")) is None

    def test_no_receipt_in_result_returns_none(self) -> None:
        result = SimpleNamespace()  # no receipt
        assert _extract_block_number_from_result(result) is None

    def test_receipt_without_block_number_returns_none(self) -> None:
        result = SimpleNamespace(receipt={"logs": ["x"]})  # no blockNumber
        assert _extract_block_number_from_result(result) is None


# ---------------------------------------------------------------------------
# _intent_fee_tier
# ---------------------------------------------------------------------------


class TestIntentFeeTier:
    """Recover ``fee_tier`` from intent.protocol_params."""

    def test_none_intent_returns_none(self) -> None:
        assert _intent_fee_tier(None) is None

    def test_intent_without_protocol_params(self) -> None:
        intent = SimpleNamespace()
        assert _intent_fee_tier(intent) is None

    def test_protocol_params_none(self) -> None:
        intent = SimpleNamespace(protocol_params=None)
        assert _intent_fee_tier(intent) is None

    def test_fee_tier_camelcase(self) -> None:
        # feeTier is the camelCase variant some compilers emit.
        intent = SimpleNamespace(protocol_params={"feeTier": 500})
        assert _intent_fee_tier(intent) == 500

    def test_fee_tier_snake_case(self) -> None:
        intent = SimpleNamespace(protocol_params={"fee_tier": 3000})
        assert _intent_fee_tier(intent) == 3000

    def test_fee_tier_takes_precedence_over_camelcase(self) -> None:
        intent = SimpleNamespace(protocol_params={"fee_tier": 500, "feeTier": 3000})
        # snake_case wins (the `or` short-circuit on truthy).
        assert _intent_fee_tier(intent) == 500

    def test_missing_fee_tier_returns_none(self) -> None:
        intent = SimpleNamespace(protocol_params={"other_key": "x"})
        assert _intent_fee_tier(intent) is None

    def test_non_dict_protocol_params(self) -> None:
        intent = SimpleNamespace(protocol_params=["unexpected"])
        assert _intent_fee_tier(intent) is None

    def test_string_fee_tier_coerced(self) -> None:
        intent = SimpleNamespace(protocol_params={"fee_tier": "500"})
        assert _intent_fee_tier(intent) == 500

    def test_garbage_fee_tier_returns_none(self) -> None:
        intent = SimpleNamespace(protocol_params={"fee_tier": "not-a-number"})
        assert _intent_fee_tier(intent) is None

    def test_zero_fee_tier_returns_none_via_or_short_circuit(self) -> None:
        # ``params.get("fee_tier") or params.get("feeTier")`` — 0 is falsy,
        # so it falls to feeTier (also missing) → returns None. This is
        # arguably a bug (0 is a valid fee tier on test pools) but the
        # current contract is what's documented; pin it.
        intent = SimpleNamespace(protocol_params={"fee_tier": 0})
        assert _intent_fee_tier(intent) is None


# ---------------------------------------------------------------------------
# _lookup_open_registry_payload
# ---------------------------------------------------------------------------


class TestLookupOpenRegistryPayload:
    """Look up the OPEN-side registry payload for a close-side write."""

    @staticmethod
    def _runner(rows: Any = None, raises: Exception | None = None) -> SimpleNamespace:
        sm = MagicMock()
        if raises is not None:
            sm.get_position_registry_open_rows = AsyncMock(side_effect=raises)
        else:
            sm.get_position_registry_open_rows = AsyncMock(return_value=rows or [])
        return SimpleNamespace(state_manager=sm)

    @pytest.mark.asyncio
    async def test_token_id_from_parser_when_none_supplied(self) -> None:
        # token_id=None → parser._decreaseliquidity_token_id is called.
        runner = self._runner(rows=[])
        parser = SimpleNamespace(_decreaseliquidity_token_id=MagicMock(return_value=42))
        await StrategyRunner._lookup_open_registry_payload(
            runner,
            deployment_id="dep-1",
            chain="arbitrum",
            token_id=None,
            receipt={"logs": []},
            parser=parser,
        )
        parser._decreaseliquidity_token_id.assert_called_once_with({"logs": []})

    @pytest.mark.asyncio
    async def test_returns_none_when_parser_yields_no_token_id(self) -> None:
        runner = self._runner(rows=[])
        parser = SimpleNamespace(_decreaseliquidity_token_id=MagicMock(return_value=None))
        out = await StrategyRunner._lookup_open_registry_payload(
            runner,
            deployment_id="dep-1",
            chain="arbitrum",
            token_id=None,
            receipt={"logs": []},
            parser=parser,
        )
        assert out is None
        # No state-manager read attempted.
        runner.state_manager.get_position_registry_open_rows.assert_not_called()

    @pytest.mark.asyncio
    async def test_supplied_token_id_skips_parser(self) -> None:
        runner = self._runner(rows=[])
        parser = SimpleNamespace(
            _decreaseliquidity_token_id=MagicMock(return_value=999)
        )
        await StrategyRunner._lookup_open_registry_payload(
            runner,
            deployment_id="dep-1",
            chain="arbitrum",
            token_id=42,
            receipt={"logs": []},
            parser=parser,
        )
        parser._decreaseliquidity_token_id.assert_not_called()

    @pytest.mark.asyncio
    async def test_state_manager_exception_returns_none(self) -> None:
        runner = self._runner(raises=RuntimeError("DB down"))
        out = await StrategyRunner._lookup_open_registry_payload(
            runner,
            deployment_id="dep-1",
            chain="arbitrum",
            token_id=42,
            receipt={"logs": []},
            parser=SimpleNamespace(),
        )
        assert out is None

    @pytest.mark.asyncio
    async def test_matching_row_returns_enriched_payload(self) -> None:
        rows = [
            {
                "payload": {"token_id": "42", "pool_address": "0xabc"},
                "opened_at_block": 100,
                "opened_tx": "0xdeadbeef",
            }
        ]
        runner = self._runner(rows=rows)
        out = await StrategyRunner._lookup_open_registry_payload(
            runner,
            deployment_id="dep-1",
            chain="arbitrum",
            token_id=42,
            receipt={"logs": []},
            parser=SimpleNamespace(),
        )
        assert out is not None
        assert out["token_id"] == "42"
        # Enriched with row-level fields.
        assert out["opened_at_block"] == 100
        assert out["opened_tx"] == "0xdeadbeef"

    @pytest.mark.asyncio
    async def test_non_matching_token_id_returns_none(self) -> None:
        rows = [{"payload": {"token_id": "999", "pool_address": "0xabc"}}]
        runner = self._runner(rows=rows)
        out = await StrategyRunner._lookup_open_registry_payload(
            runner,
            deployment_id="dep-1",
            chain="arbitrum",
            token_id=42,
            receipt={"logs": []},
            parser=SimpleNamespace(),
        )
        assert out is None

    @pytest.mark.asyncio
    async def test_non_dict_payload_skipped(self) -> None:
        rows = [{"payload": "not-a-dict"}, {"payload": {"token_id": "42"}}]
        runner = self._runner(rows=rows)
        out = await StrategyRunner._lookup_open_registry_payload(
            runner,
            deployment_id="dep-1",
            chain="arbitrum",
            token_id=42,
            receipt={"logs": []},
            parser=SimpleNamespace(),
        )
        assert out is not None
        assert out["token_id"] == "42"

    @pytest.mark.asyncio
    async def test_unparseable_token_id_continues(self) -> None:
        # First row's token_id is non-numeric; iteration continues to next.
        rows = [
            {"payload": {"token_id": "garbage"}},
            {"payload": {"token_id": "42"}},
        ]
        runner = self._runner(rows=rows)
        out = await StrategyRunner._lookup_open_registry_payload(
            runner,
            deployment_id="dep-1",
            chain="arbitrum",
            token_id=42,
            receipt={"logs": []},
            parser=SimpleNamespace(),
        )
        assert out is not None
        assert out["token_id"] == "42"

    @pytest.mark.asyncio
    async def test_payload_missing_token_id_uses_zero_default_and_misses(self) -> None:
        # payload.get("token_id", 0) → 0 → int(0) != int(42) → continue.
        rows = [{"payload": {"pool_address": "0xabc"}}]
        runner = self._runner(rows=rows)
        out = await StrategyRunner._lookup_open_registry_payload(
            runner,
            deployment_id="dep-1",
            chain="arbitrum",
            token_id=42,
            receipt={"logs": []},
            parser=SimpleNamespace(),
        )
        assert out is None

    @pytest.mark.asyncio
    async def test_opened_at_block_none_not_added(self) -> None:
        # Row has no opened_at_block → enriched payload doesn't carry it.
        rows = [
            {
                "payload": {"token_id": "42"},
                "opened_at_block": None,
                "opened_tx": "0xabc",
            }
        ]
        runner = self._runner(rows=rows)
        out = await StrategyRunner._lookup_open_registry_payload(
            runner,
            deployment_id="dep-1",
            chain="arbitrum",
            token_id=42,
            receipt={"logs": []},
            parser=SimpleNamespace(),
        )
        assert out is not None
        assert "opened_at_block" not in out
        assert out["opened_tx"] == "0xabc"

    @pytest.mark.asyncio
    async def test_falsy_opened_tx_not_added(self) -> None:
        rows = [
            {
                "payload": {"token_id": "42"},
                "opened_at_block": 100,
                "opened_tx": "",  # falsy
            }
        ]
        runner = self._runner(rows=rows)
        out = await StrategyRunner._lookup_open_registry_payload(
            runner,
            deployment_id="dep-1",
            chain="arbitrum",
            token_id=42,
            receipt={"logs": []},
            parser=SimpleNamespace(),
        )
        assert out is not None
        assert out["opened_at_block"] == 100
        assert "opened_tx" not in out


# ---------------------------------------------------------------------------
# _update_lp_registry_id_cache
# ---------------------------------------------------------------------------


class TestUpdateLpRegistryIdCache:
    """Sync the per-runner ``_lp_registry_id_cache`` after a registry write."""

    POOL = "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443"
    CHAIN = "arbitrum"

    @staticmethod
    def _runner(initial_cache: dict | None = None) -> SimpleNamespace:
        runner = SimpleNamespace()
        if initial_cache is not None:
            runner._lp_registry_id_cache = initial_cache
        return runner

    def test_open_seeds_cache_for_all_univ3_slugs(self) -> None:
        from almanak.framework.migration.backfill import _UNIV3_LP_PROTOCOLS

        runner = self._runner({})
        StrategyRunner._update_lp_registry_id_cache(
            runner,
            chain=self.CHAIN,
            pool_addr=self.POOL,
            token_id=42,
            is_open=True,
        )
        for slug in _UNIV3_LP_PROTOCOLS:
            assert runner._lp_registry_id_cache[(slug, self.CHAIN, self.POOL)] == "42"

    def test_close_evicts_matching_entry(self) -> None:
        from almanak.framework.migration.backfill import _UNIV3_LP_PROTOCOLS

        cache = {(slug, self.CHAIN, self.POOL): "42" for slug in _UNIV3_LP_PROTOCOLS}
        runner = self._runner(cache)
        StrategyRunner._update_lp_registry_id_cache(
            runner,
            chain=self.CHAIN,
            pool_addr=self.POOL,
            token_id=42,
            is_open=False,
        )
        for slug in _UNIV3_LP_PROTOCOLS:
            assert (slug, self.CHAIN, self.POOL) not in runner._lp_registry_id_cache

    def test_close_does_not_evict_non_matching_token_id(self) -> None:
        # Audit P2: another live position holds this key — leave it.
        from almanak.framework.migration.backfill import _UNIV3_LP_PROTOCOLS

        cache = {(slug, self.CHAIN, self.POOL): "999" for slug in _UNIV3_LP_PROTOCOLS}
        runner = self._runner(cache)
        StrategyRunner._update_lp_registry_id_cache(
            runner,
            chain=self.CHAIN,
            pool_addr=self.POOL,
            token_id=42,  # closing a DIFFERENT token_id
            is_open=False,
        )
        # Cache still holds 999 for every slug.
        for slug in _UNIV3_LP_PROTOCOLS:
            assert runner._lp_registry_id_cache[(slug, self.CHAIN, self.POOL)] == "999"

    def test_open_collision_drops_entry_and_warns(self, caplog) -> None:
        # Audit P2: OPEN with different token_id at same key → drop entry.
        from almanak.framework.migration.backfill import _UNIV3_LP_PROTOCOLS

        cache = {(slug, self.CHAIN, self.POOL): "999" for slug in _UNIV3_LP_PROTOCOLS}
        runner = self._runner(cache)
        with caplog.at_level("WARNING"):
            StrategyRunner._update_lp_registry_id_cache(
                runner,
                chain=self.CHAIN,
                pool_addr=self.POOL,
                token_id=42,
                is_open=True,
            )
        for slug in _UNIV3_LP_PROTOCOLS:
            assert (slug, self.CHAIN, self.POOL) not in runner._lp_registry_id_cache
        assert any("multi-NFT collision" in rec.message for rec in caplog.records)

    def test_open_idempotent_when_token_id_same(self) -> None:
        # OPEN with same token_id at same key → leaves cache as-is.
        from almanak.framework.migration.backfill import _UNIV3_LP_PROTOCOLS

        cache = {(slug, self.CHAIN, self.POOL): "42" for slug in _UNIV3_LP_PROTOCOLS}
        runner = self._runner(dict(cache))
        StrategyRunner._update_lp_registry_id_cache(
            runner,
            chain=self.CHAIN,
            pool_addr=self.POOL,
            token_id=42,
            is_open=True,
        )
        for slug in _UNIV3_LP_PROTOCOLS:
            assert runner._lp_registry_id_cache[(slug, self.CHAIN, self.POOL)] == "42"

    def test_empty_pool_address_no_op(self) -> None:
        # No pool_addr → still ensure cache attr is set (never crash).
        runner = self._runner()
        StrategyRunner._update_lp_registry_id_cache(
            runner,
            chain=self.CHAIN,
            pool_addr="",
            token_id=42,
            is_open=True,
        )
        assert runner._lp_registry_id_cache == {}

    def test_runner_without_cache_attr_creates_one(self) -> None:
        # First write into the cache — runner has no _lp_registry_id_cache yet.
        from almanak.framework.migration.backfill import _UNIV3_LP_PROTOCOLS

        runner = SimpleNamespace()  # no _lp_registry_id_cache
        StrategyRunner._update_lp_registry_id_cache(
            runner,
            chain=self.CHAIN,
            pool_addr=self.POOL,
            token_id=42,
            is_open=True,
        )
        assert hasattr(runner, "_lp_registry_id_cache")
        for slug in _UNIV3_LP_PROTOCOLS:
            assert runner._lp_registry_id_cache[(slug, self.CHAIN, self.POOL)] == "42"


# ---------------------------------------------------------------------------
# _extract_receipt_from_result (orchestrator over collect + has-topic)
# ---------------------------------------------------------------------------


class TestExtractReceiptFromResult:
    """Top-level orchestrator: collect candidates → prefer LP topic → fall
    back to last."""

    DECREASE_TOPIC = "0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4"

    def test_none_result_returns_none(self) -> None:
        assert _extract_receipt_from_result(None) is None

    def test_no_candidates_returns_none(self) -> None:
        assert _extract_receipt_from_result(SimpleNamespace()) is None

    def test_lp_topic_candidate_preferred(self) -> None:
        # Two candidates: first has approval, second has DecreaseLiquidity.
        # Per audit P1, the LP-topic one MUST be returned (not the first).
        result = SimpleNamespace(
            receipts=[
                {
                    "logs": [
                        {"topics": ["0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"]}
                    ]
                },
                {"logs": [{"topics": [self.DECREASE_TOPIC]}]},
            ]
        )
        out = _extract_receipt_from_result(result)
        assert out is not None
        # Got the LP-topic-bearing receipt, not the approval.
        assert out["logs"][0]["topics"][0] == self.DECREASE_TOPIC

    def test_fallback_to_last_when_no_lp_topic(self) -> None:
        # No LP topic anywhere — fall back to LAST candidate (terminal TX
        # in a multi-tx bundle).
        result = SimpleNamespace(
            receipts=[
                {"logs": [{"topics": ["0xaaaa" + "00" * 30]}]},
                {"logs": [{"topics": ["0xbbbb" + "00" * 30]}]},
            ]
        )
        out = _extract_receipt_from_result(result)
        assert out is not None
        # Got the LAST receipt (audit P1 fallback).
        assert out["logs"][0]["topics"][0] == "0xbbbb" + "00" * 30

    def test_local_execution_result_shape_picks_up_receipt(self) -> None:
        # Local ExecutionResult shape — the previously-missed dominant
        # case (audit P1).
        result = SimpleNamespace(
            transaction_results=[
                SimpleNamespace(
                    success=True,
                    receipt={"logs": [{"topics": [self.DECREASE_TOPIC]}]},
                )
            ]
        )
        out = _extract_receipt_from_result(result)
        assert out is not None
        assert out["logs"][0]["topics"][0] == self.DECREASE_TOPIC


# ---------------------------------------------------------------------------
# _maybe_save_ledger_with_registry — orchestrator path-applicability gate
# ---------------------------------------------------------------------------


class TestMaybeSaveLedgerWithRegistry:
    """Top-level orchestrator: gate → dispatch → atomic write → cache.

    Each test exercises ONE gate-or-dispatch branch in
    ``_maybe_save_ledger_with_registry`` to drive coverage uniformly.
    The leaf collaborators (parser, state-manager, ``save_ledger_and_registry``,
    sibling helpers) are stubbed so the orchestrator's own conditionals
    run unmocked.
    """

    @staticmethod
    def _runner(*, cutover_active: bool = False) -> SimpleNamespace:
        from almanak.framework.primitives.types import Primitive

        runner = SimpleNamespace()
        runner.config = SimpleNamespace(chain="arbitrum")
        runner.state_manager = MagicMock()
        runner._cutover_complete_cache = (
            {(Primitive.LP, "lp")} if cutover_active else set()
        )
        runner._lookup_open_registry_payload = AsyncMock(return_value=None)
        runner._extract_block_number_from_result = MagicMock(return_value=100)
        runner._extract_receipt_from_result = MagicMock(return_value=None)
        runner._intent_fee_tier = MagicMock(return_value=500)

        # _build_registry_row → return a SimpleNamespace with the keys
        # the orchestrator inspects (status, physical_identity_hash).
        def _stub_build_registry_row(**kwargs: Any) -> SimpleNamespace:
            return SimpleNamespace(
                status=kwargs.get("status"),
                physical_identity_hash=kwargs.get("physical_identity_hash") or "0xpih",
            )

        runner._build_registry_row = _stub_build_registry_row
        runner._registry_intent_type_str = StrategyRunner._registry_intent_type_str
        runner._registry_resolve_chain_and_nft_manager = MagicMock(
            return_value=("arbitrum", "0xc36442b4a4522e871399cd717abdd847ab11fe88")
        )
        runner._registry_resolve_receipt_and_parser = MagicMock(return_value=None)
        runner._build_lp_open_registry_row = MagicMock(return_value=None)
        runner._build_lp_close_registry_row = AsyncMock(return_value=None)
        runner._update_lp_registry_id_cache = MagicMock()
        return runner

    @pytest.mark.asyncio
    async def test_returns_false_when_cutover_not_active(self) -> None:
        runner = self._runner(cutover_active=False)
        intent = SimpleNamespace(
            intent_type=SimpleNamespace(value="LP_OPEN"),
            protocol="uniswap_v3",
        )
        out = await StrategyRunner._maybe_save_ledger_with_registry(
            runner,
            strategy=SimpleNamespace(deployment_id="s", chain="arbitrum"),
            intent=intent,
            result=SimpleNamespace(success=True),
            success=True,
            entry=SimpleNamespace(tx_hash="0xa"),
        )
        assert out is False

    @pytest.mark.asyncio
    async def test_returns_false_for_non_lp_intent_type(self) -> None:
        runner = self._runner(cutover_active=True)
        intent = SimpleNamespace(
            intent_type=SimpleNamespace(value="SWAP"),
            protocol="uniswap_v3",
        )
        out = await StrategyRunner._maybe_save_ledger_with_registry(
            runner,
            strategy=SimpleNamespace(deployment_id="s", chain="arbitrum"),
            intent=intent,
            result=SimpleNamespace(success=True),
            success=True,
            entry=SimpleNamespace(tx_hash="0xa"),
        )
        assert out is False

    @pytest.mark.asyncio
    async def test_returns_false_for_non_univ3_protocol(self) -> None:
        runner = self._runner(cutover_active=True)
        intent = SimpleNamespace(
            intent_type=SimpleNamespace(value="LP_OPEN"),
            protocol="curve",  # not in _UNIV3_LP_PROTOCOLS
        )
        out = await StrategyRunner._maybe_save_ledger_with_registry(
            runner,
            strategy=SimpleNamespace(deployment_id="s", chain="arbitrum"),
            intent=intent,
            result=SimpleNamespace(success=True),
            success=True,
            entry=SimpleNamespace(tx_hash="0xa"),
        )
        assert out is False

    @pytest.mark.asyncio
    async def test_returns_false_when_result_none(self) -> None:
        # Audit P1 — gate on chain truth.
        runner = self._runner(cutover_active=True)
        intent = SimpleNamespace(
            intent_type=SimpleNamespace(value="LP_OPEN"),
            protocol="uniswap_v3",
        )
        out = await StrategyRunner._maybe_save_ledger_with_registry(
            runner,
            strategy=SimpleNamespace(deployment_id="s", chain="arbitrum"),
            intent=intent,
            result=None,
            success=True,
            entry=SimpleNamespace(tx_hash="0xa"),
        )
        assert out is False

    @pytest.mark.asyncio
    async def test_returns_false_when_result_success_false(self) -> None:
        # Chain TX did NOT land — registry write must NOT fire.
        runner = self._runner(cutover_active=True)
        intent = SimpleNamespace(
            intent_type=SimpleNamespace(value="LP_OPEN"),
            protocol="uniswap_v3",
        )
        out = await StrategyRunner._maybe_save_ledger_with_registry(
            runner,
            strategy=SimpleNamespace(deployment_id="s", chain="arbitrum"),
            intent=intent,
            result=SimpleNamespace(success=False),
            success=True,  # framework verdict — but chain truth wins
            entry=SimpleNamespace(tx_hash="0xa"),
        )
        assert out is False

    @pytest.mark.asyncio
    async def test_returns_false_when_no_nft_manager_for_chain(self) -> None:
        runner = self._runner(cutover_active=True)
        runner._registry_resolve_chain_and_nft_manager = MagicMock(return_value=None)
        intent = SimpleNamespace(
            intent_type=SimpleNamespace(value="LP_OPEN"),
            protocol="uniswap_v3",
        )
        out = await StrategyRunner._maybe_save_ledger_with_registry(
            runner,
            strategy=SimpleNamespace(deployment_id="s", chain="unknown"),
            intent=intent,
            result=SimpleNamespace(success=True),
            success=True,
            entry=SimpleNamespace(tx_hash="0xa"),
        )
        assert out is False

    @pytest.mark.asyncio
    async def test_returns_false_when_receipt_unavailable(self) -> None:
        runner = self._runner(cutover_active=True)
        runner._registry_resolve_receipt_and_parser = MagicMock(return_value=None)
        intent = SimpleNamespace(
            intent_type=SimpleNamespace(value="LP_OPEN"),
            protocol="uniswap_v3",
        )
        out = await StrategyRunner._maybe_save_ledger_with_registry(
            runner,
            strategy=SimpleNamespace(deployment_id="s", chain="arbitrum"),
            intent=intent,
            result=SimpleNamespace(success=True),
            success=True,
            entry=SimpleNamespace(tx_hash="0xa"),
        )
        assert out is False

    @pytest.mark.asyncio
    async def test_returns_false_when_open_row_build_fails(self) -> None:
        runner = self._runner(cutover_active=True)
        runner._registry_resolve_receipt_and_parser = MagicMock(
            return_value=({"logs": []}, SimpleNamespace())
        )
        runner._build_lp_open_registry_row = MagicMock(return_value=None)
        intent = SimpleNamespace(
            intent_type=SimpleNamespace(value="LP_OPEN"),
            protocol="uniswap_v3",
        )
        out = await StrategyRunner._maybe_save_ledger_with_registry(
            runner,
            strategy=SimpleNamespace(deployment_id="s", chain="arbitrum"),
            intent=intent,
            result=SimpleNamespace(success=True),
            success=True,
            entry=SimpleNamespace(tx_hash="0xa"),
        )
        assert out is False

    @pytest.mark.asyncio
    async def test_returns_false_when_close_row_build_fails(self) -> None:
        runner = self._runner(cutover_active=True)
        runner._registry_resolve_receipt_and_parser = MagicMock(
            return_value=({"logs": []}, SimpleNamespace())
        )
        runner._build_lp_close_registry_row = AsyncMock(return_value=None)
        intent = SimpleNamespace(
            intent_type=SimpleNamespace(value="LP_CLOSE"),
            protocol="uniswap_v3",
        )
        out = await StrategyRunner._maybe_save_ledger_with_registry(
            runner,
            strategy=SimpleNamespace(deployment_id="s", chain="arbitrum"),
            intent=intent,
            result=SimpleNamespace(success=True),
            success=True,
            entry=SimpleNamespace(tx_hash="0xa"),
        )
        assert out is False

    @pytest.mark.asyncio
    async def test_returns_true_after_atomic_write_for_open(self, monkeypatch) -> None:
        runner = self._runner(cutover_active=True)
        runner._registry_resolve_receipt_and_parser = MagicMock(
            return_value=({"logs": []}, SimpleNamespace())
        )
        registry_row = SimpleNamespace(status="open", physical_identity_hash="0xpih-open")
        payload = {"pool_address": "0xPOOL", "token_id": "42"}
        runner._build_lp_open_registry_row = MagicMock(
            return_value=(registry_row, payload, 42)
        )

        # Stub save_ledger_and_registry at the module level.
        save_mock = AsyncMock(return_value=None)
        monkeypatch.setattr(
            "almanak.framework.accounting.commit.save_ledger_and_registry",
            save_mock,
        )

        intent = SimpleNamespace(
            intent_type=SimpleNamespace(value="LP_OPEN"),
            protocol="uniswap_v3",
        )
        out = await StrategyRunner._maybe_save_ledger_with_registry(
            runner,
            strategy=SimpleNamespace(deployment_id="s", chain="arbitrum"),
            intent=intent,
            result=SimpleNamespace(success=True),
            success=True,
            entry=SimpleNamespace(tx_hash="0xledger"),
        )
        assert out is True
        save_mock.assert_awaited_once()
        # Cache update was invoked with is_open=True.
        runner._update_lp_registry_id_cache.assert_called_once()
        call_kwargs = runner._update_lp_registry_id_cache.call_args.kwargs
        assert call_kwargs["is_open"] is True
        assert call_kwargs["token_id"] == 42

    @pytest.mark.asyncio
    async def test_returns_true_after_atomic_write_for_close(self, monkeypatch) -> None:
        runner = self._runner(cutover_active=True)
        runner._registry_resolve_receipt_and_parser = MagicMock(
            return_value=({"logs": []}, SimpleNamespace())
        )
        registry_row = SimpleNamespace(status="closed", physical_identity_hash="0xpih-close")
        payload = {"pool_address": "0xPOOL", "token_id": "42"}
        runner._build_lp_close_registry_row = AsyncMock(
            return_value=(registry_row, payload, 42)
        )
        save_mock = AsyncMock(return_value=None)
        monkeypatch.setattr(
            "almanak.framework.accounting.commit.save_ledger_and_registry",
            save_mock,
        )

        intent = SimpleNamespace(
            intent_type=SimpleNamespace(value="LP_CLOSE"),
            protocol="uniswap_v3",
        )
        out = await StrategyRunner._maybe_save_ledger_with_registry(
            runner,
            strategy=SimpleNamespace(deployment_id="s", chain="arbitrum"),
            intent=intent,
            result=SimpleNamespace(success=True),
            success=True,
            entry=SimpleNamespace(tx_hash="0xledger"),
        )
        assert out is True
        # Close path → cache update with is_open=False.
        call_kwargs = runner._update_lp_registry_id_cache.call_args.kwargs
        assert call_kwargs["is_open"] is False
        assert call_kwargs["token_id"] == 42
