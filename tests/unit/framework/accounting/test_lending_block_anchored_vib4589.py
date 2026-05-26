"""VIB-4589 / F7 — block-anchored lending state reads.

Pre-fix, all lending post-state captures ran with the JSON-RPC
``"latest"`` block tag. On mainnet that raced the upstream RPC's
receipt indexer: a confirmed WITHDRAW receipt was not yet visible to
the next ``"latest"`` view, so ``getUserAccountData`` returned a
near-full collateral balance instead of the expected ~0. The reader
shipped that stale value into ``accounting_events.payload_json
.collateral_value_after_usd`` and the lifecycle classifier emitted
DECREASE instead of CLOSE — the symptom from
``docs/internal/AccountingLiveMay18.md`` §F7.

The fix pins every post-execution eth_call to ``receipt.block_number``
so the snapshot reflects exactly the state produced by the confirmed
receipt. These tests pin the new contract layer by layer.
"""

from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from almanak.framework.accounting.lending_accounting import (
    _gateway_eth_call,
    capture_lending_post_state,
    read_aave_account_state,
)


def _patch_chain_pool_address(chain: str, address: str):
    """Inject a fake pool address into AAVE_V3_POOL_ADDRESSES.

    The reader looks up the pool from the adapter; tests don't care
    about the real address, only that the lookup succeeds and the
    calldata is sent to ``address``.
    """
    from almanak.connectors.aave_v3 import adapter as aave_adapter

    aave_adapter.AAVE_V3_POOL_ADDRESSES[chain] = address


class TestGatewayEthCallBlockParameter:
    """Layer 1 — the framework-side gateway client passes ``block``
    through to the JSON-RPC params. Integer encoded as hex, string
    passed through, None defaults to ``"latest"``.
    """

    def _capture_params(self) -> tuple[MagicMock, list]:
        """Build a gateway client that captures the params it would
        send. Returns (client, captured) where ``captured`` is the
        list mutated on each ``eth_call`` invocation.
        """
        from almanak.framework.gateway_client import GatewayClient

        client = GatewayClient.__new__(GatewayClient)
        client.config = SimpleNamespace(timeout=30.0)
        captured: list[str] = []

        rpc_stub = MagicMock()

        def _fake_call(request, timeout=None):  # noqa: ARG001
            captured.append(request.params)
            return SimpleNamespace(success=True, result=json.dumps("0x"))

        rpc_stub.Call = _fake_call
        client._rpc_stub = rpc_stub
        return client, captured

    def test_block_none_uses_latest_tag(self) -> None:
        """Default ``block=None`` preserves the legacy ``"latest"`` behaviour."""
        client, captured = self._capture_params()
        client.eth_call("ethereum", "0xpool", "0xdeadbeef")
        assert captured, "eth_call never reached the RPC stub"
        params = json.loads(captured[0])
        assert params == [{"to": "0xpool", "data": "0xdeadbeef"}, "latest"]

    def test_block_int_is_encoded_as_hex(self) -> None:
        """An ``int`` block number is encoded as the JSON-RPC hex string."""
        client, captured = self._capture_params()
        client.eth_call("ethereum", "0xpool", "0xdeadbeef", block=305419896)
        params = json.loads(captured[0])
        assert params == [{"to": "0xpool", "data": "0xdeadbeef"}, hex(305419896)]
        assert params[1] == "0x12345678"

    def test_block_string_passes_through(self) -> None:
        """A pre-encoded string (tag or hex) passes through unchanged.

        Covers ``"pending"`` / ``"safe"`` / ``"finalized"`` / pre-encoded
        ``"0x..."``. The gateway is opaque to the value — the upstream
        RPC validates it.
        """
        client, captured = self._capture_params()
        client.eth_call("ethereum", "0xpool", "0xdeadbeef", block="pending")
        params = json.loads(captured[0])
        assert params[1] == "pending"


class TestReadAaveAccountStateForwardsBlock:
    """Layer 2 — ``read_aave_account_state`` threads ``block`` through to
    every underlying eth_call, including the secondary ``getUserEMode``
    read (so the (collateral, debt, HF, e-mode) tuple is internally
    consistent — no inter-call drift).
    """

    def test_account_data_and_emode_pin_to_same_block(self) -> None:
        chain = "arbitrum"
        pool = "0xPool"
        _patch_chain_pool_address(chain, pool)

        # Build a stub gateway whose eth_call records the block arg.
        eth_call_calls: list[dict] = []
        gateway = MagicMock()

        def _fake_eth_call(chain_arg, to_arg, data_arg, *, block=None):  # noqa: ARG001
            eth_call_calls.append({"to": to_arg, "data": data_arg, "block": block})
            # getUserAccountData returns 6 uint256 words; getUserEMode 1 uint256.
            if len(data_arg) > 10 and data_arg.startswith("0xbf92857c"):
                # totalCollateralBase=10^8 (1 USD), totalDebt=0, HF=10^18, threshold/ltv=0
                return "0x" + ("0" * 63 + "1") + "0" * 64 + "0" * 64 + "0" * 64 + "0" * 64 + ("0" * 63 + "1") + "0" * 192
            return "0x" + ("0" * 64)  # e-mode = 0

        gateway.eth_call.side_effect = _fake_eth_call

        result = read_aave_account_state(gateway, chain, "0xwallet", block=12345)

        # Both eth_calls fired with the same explicit block — getUserAccountData + getUserEMode.
        assert result is not None
        assert len(eth_call_calls) == 2
        assert eth_call_calls[0]["block"] == 12345
        assert eth_call_calls[1]["block"] == 12345


class TestCaptureLendingPostStateForwardsBlock:
    """Layer 4 — the public ``capture_lending_post_state`` entry point
    forwards ``block`` all the way to the per-protocol reader.

    Mirror of the bug ``capture_lending_post_state`` was added to fix.
    Without this guard, callers would pass ``block=receipt.block_number``
    that the inner readers silently drop, defeating the F7 fix.
    """

    def test_post_state_pins_to_block_for_aave_v3(self) -> None:
        chain = "arbitrum"
        _patch_chain_pool_address(chain, "0xPool")
        recorded_block: list[int | str | None] = []

        gateway = MagicMock()

        def _fake_eth_call(chain_arg, to_arg, data_arg, *, block=None):  # noqa: ARG001
            recorded_block.append(block)
            # Empty position state — valid response shape, near-zero values.
            return "0x" + "0" * (6 * 64)

        gateway.eth_call.side_effect = _fake_eth_call

        intent = SimpleNamespace(
            intent_type=SimpleNamespace(value="WITHDRAW"),
            protocol="aave_v3",
        )

        capture_lending_post_state(
            intent=intent,
            chain=chain,
            wallet_address="0xwallet",
            gateway_client=gateway,
            price_oracle=None,
            block=42_000_000,
        )

        # Both inner calls (getUserAccountData + getUserEMode) must
        # pin to the supplied receipt block.
        assert recorded_block, "no eth_call reached the gateway"
        assert all(b == 42_000_000 for b in recorded_block), (
            f"expected every inner eth_call to pin to block=42_000_000 but got {recorded_block}"
        )


class TestGatewayEthCallLegacyClientFallback:
    """Layer 2 mid-layer — the ``_gateway_eth_call`` helper's compatibility
    behaviour with a legacy gateway client that doesn't yet accept the
    ``block`` kwarg.

    Policy (VIB-4589):

    - Unpinned reads (``block is None`` or ``block == "latest"``) MAY fall
      back to the 3-arg form so existing integration tests / older mocks
      keep working — they were already returning ``"latest"`` data.
    - Pinned reads (an int block_number, or any non-``"latest"`` tag) MUST
      fail closed (return ``None`` with a WARNING log). Silently dropping
      to ``"latest"`` here would reintroduce the exact upstream-indexer
      race this PR closes — turning a code-path bug into a stale-state
      mainnet incident.
    """

    @staticmethod
    def _legacy_client():
        client = MagicMock(spec=["eth_call"])
        call_args: list = []

        def _legacy_eth_call(chain, to, data):
            call_args.append((chain, to, data))
            return "0xresult"

        # MagicMock with spec=['eth_call'] auto-creates eth_call accepting
        # any signature. Replace with a real function that rejects the
        # ``block`` kwarg (TypeError) to simulate the legacy client.
        client.eth_call = _legacy_eth_call
        return client, call_args

    def test_legacy_client_unpinned_read_falls_back(self) -> None:
        """``block=None`` (or ``"latest"``) → legacy 3-arg form is acceptable."""
        client, call_args = self._legacy_client()
        result = _gateway_eth_call(client, "ethereum", "0xpool", "0xdata", block=None)
        assert result == "0xresult"
        assert call_args == [("ethereum", "0xpool", "0xdata")]

        client, call_args = self._legacy_client()
        result = _gateway_eth_call(
            client, "ethereum", "0xpool", "0xdata", block="latest"
        )
        assert result == "0xresult"
        assert call_args == [("ethereum", "0xpool", "0xdata")]

    def test_legacy_client_pinned_read_fails_closed(self, caplog) -> None:
        """``block=<int>`` against a legacy client → return ``None``, do NOT
        downgrade to ``"latest"`` (would re-open the VIB-4589 race).
        """
        client, call_args = self._legacy_client()
        with caplog.at_level("WARNING", logger="almanak.framework.accounting.lending_accounting"):
            result = _gateway_eth_call(
                client, "ethereum", "0xpool", "0xdata", block=12345
            )
        assert result is None
        # Legacy 3-arg form must NOT have been invoked — we refused the read.
        assert call_args == []
        assert any("refusing to fall back to 'latest'" in r.message for r in caplog.records)


class TestLastReceiptBlockHelper:
    """Layer 3 — the runner helper that extracts ``block_number`` from
    an ``ExecutionResult``. The post-state read site uses this to pin
    every confirmed-tx reader call to the receipt's block.
    """

    def test_none_when_execution_result_is_none(self) -> None:
        from almanak.framework.runner.strategy_runner import _last_receipt_block

        assert _last_receipt_block(None) is None

    def test_none_when_no_successful_receipts(self) -> None:
        from almanak.framework.runner.strategy_runner import _last_receipt_block

        failed = SimpleNamespace(
            transaction_results=[
                SimpleNamespace(success=False, receipt=SimpleNamespace(block_number=100)),
                SimpleNamespace(success=False, receipt=None),
            ],
        )
        assert _last_receipt_block(failed) is None

    def test_returns_last_successful_receipt_block(self) -> None:
        """Multi-tx bundle — take the LAST successful receipt's block
        so the post-state read reflects the whole bundle's effect, not
        an intermediate snapshot.
        """
        from almanak.framework.runner.strategy_runner import _last_receipt_block

        result = SimpleNamespace(
            transaction_results=[
                SimpleNamespace(success=True, receipt=SimpleNamespace(block_number=100)),
                SimpleNamespace(success=True, receipt=SimpleNamespace(block_number=101)),
                SimpleNamespace(success=False, receipt=None),  # later fail; skip
            ],
        )
        # Iteration runs in reverse, skips failed → reaches the success=True
        # receipt at block 101.
        assert _last_receipt_block(result) == 101

    def test_skips_zero_block_sentinel(self) -> None:
        """``block_number == 0`` is a sentinel for missing data, not a
        real block (Ethereum genesis is irrelevant for production
        receipts). Skip and continue searching.
        """
        from almanak.framework.runner.strategy_runner import _last_receipt_block

        result = SimpleNamespace(
            transaction_results=[
                SimpleNamespace(success=True, receipt=SimpleNamespace(block_number=99)),
                SimpleNamespace(success=True, receipt=SimpleNamespace(block_number=0)),
            ],
        )
        assert _last_receipt_block(result) == 99

    def test_handles_dict_shaped_results(self) -> None:
        """Dict-shaped ``transaction_results`` (the framework already
        supports this shape in ``_collect_candidate_receipts``) — same
        contract: pick the last successful entry's block.
        """
        from almanak.framework.runner.strategy_runner import _last_receipt_block

        result = {
            "transaction_results": [
                {"success": True, "receipt": {"block_number": 200}},
                {"success": True, "receipt": {"blockNumber": 201}},  # camel form
            ]
        }
        assert _last_receipt_block(result) == 201

    def test_handles_hex_string_block_numbers(self) -> None:
        """Some receipt shapes (raw JSON-RPC) carry block as a 0x-prefixed
        hex string. Coerce before comparing — otherwise the post-state read
        falls back to ``"latest"`` and silently re-opens the indexer race.
        """
        from almanak.framework.runner.strategy_runner import _last_receipt_block

        result = SimpleNamespace(
            transaction_results=[
                SimpleNamespace(
                    success=True, receipt={"block_number": "0x12d4abc"}
                ),
            ],
        )
        assert _last_receipt_block(result) == 0x12D4ABC

    def test_rejects_bool_block_number(self) -> None:
        """``bool`` is an ``int`` subclass — guard against a caller bug
        leaking ``True`` through as if it were block 1."""
        from almanak.framework.runner.strategy_runner import _last_receipt_block

        result = SimpleNamespace(
            transaction_results=[
                SimpleNamespace(success=True, receipt=SimpleNamespace(block_number=True)),
            ],
        )
        assert _last_receipt_block(result) is None


# Decimal import to keep the suite usable as a standalone module
# (some assertions may compare with Decimal in future additions).
_ = Decimal
