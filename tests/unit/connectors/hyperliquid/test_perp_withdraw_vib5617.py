"""VIB-5617: PERP_WITHDRAW vocabulary + Hyperliquid CoreWriter compiler.

A first-class ``PERP_WITHDRAW`` intent verb over the CoreWriter encoders
(VIB-5615/5617). A withdraw is a cash movement (off-chain HyperCore account → L1),
not a trade: on Hyperliquid it compiles to a TWO-action CoreWriter bundle — a
``usdClassTransfer`` (action 7) rotating USDC perp→spot, then a ``spotSend``
(action 6) to the USDC system address that HyperCore bridges back to the SENDER's
HyperEVM wallet.

These tests cover the vocabulary contract (fields, factory, round-trip,
fail-closed validation, taxonomy classification) and the compiler (byte-exact
calldata for BOTH bundle legs via the connector's own encoders, USDC/system-address
targeting, and every fail-closed guard). Both legs are verified against the
connector's own ``build_usd_class_transfer_calldata`` / ``build_usdc_withdraw_calldata``
— NOT a ``cast``/re-encoded literal — so a wrong action-id / scale constant can't be
masked (the GMX-selector trap): the action ids (7 / 6), headers (``01000007`` /
``01000006``) and scales (1e6 ntl / weiDecimals=8) are pinned byte-exact in
``test_sdk.py``.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.connectors.hyperliquid.addresses import (
    CORE_WRITER_ADDRESS,
    USDC_SPOT_SYSTEM_ADDRESS,
)
from almanak.connectors.hyperliquid.compiler import HyperliquidCompiler
from almanak.connectors.hyperliquid.sdk import (
    build_usd_class_transfer_calldata,
    build_usdc_withdraw_calldata,
)
from almanak.framework.intents.compiler_models import CompilationStatus
from almanak.framework.intents.vocabulary import Intent, IntentType, PerpWithdrawIntent
from almanak.framework.primitives.taxonomy import record_for
from almanak.framework.primitives.types import AccountingCategory, EventKind

_WALLET = "0x" + "11" * 20


def _ctx(wallet: str = _WALLET, chain: str = "hyperevm"):
    # PERP_WITHDRAW needs no eth_call (the amount is resolved before compile and
    # the destination is the sender's own wallet), so services is inert here.
    services = SimpleNamespace(eth_call=lambda *a, **k: None)
    return SimpleNamespace(chain=chain, wallet_address=wallet, services=services, protocol="hyperliquid")


# --------------------------------------------------------------------------- #
# Vocabulary
# --------------------------------------------------------------------------- #


class TestVocabulary:
    def test_intent_type_declared(self) -> None:
        assert IntentType.PERP_WITHDRAW.value == "PERP_WITHDRAW"

    def test_factory_builds_withdraw_intent(self) -> None:
        intent = Intent.perp_withdraw(amount=Decimal("6.99"), protocol="hyperliquid", chain="hyperevm")
        assert isinstance(intent, PerpWithdrawIntent)
        assert intent.intent_type == IntentType.PERP_WITHDRAW
        assert intent.amount == Decimal("6.99")
        assert intent.asset == "USDC"
        assert intent.protocol == "hyperliquid"
        assert intent.chain == "hyperevm"
        assert intent.destination is None

    def test_factory_defaults(self) -> None:
        intent = Intent.perp_withdraw(amount=Decimal("1"))
        assert intent.asset == "USDC"
        assert intent.protocol == "hyperliquid"
        assert intent.chain is None

    def test_amount_all_marker_accepted(self) -> None:
        intent = Intent.perp_withdraw(amount="all")
        assert intent.amount == "all"
        assert intent.is_chained_amount is True

    def test_rejects_non_positive_amount(self) -> None:
        with pytest.raises(ValueError, match="amount must be positive"):
            PerpWithdrawIntent(amount=Decimal("0"))
        with pytest.raises(ValueError, match="amount must be positive"):
            PerpWithdrawIntent(amount=Decimal("-1"))

    def test_rejects_bad_amount_string(self) -> None:
        # Pydantic strict typing rejects a non-'all' string at the type stage,
        # or the model-validator rejects it — either way construction fails.
        with pytest.raises(Exception, match="amount|Decimal|all"):
            PerpWithdrawIntent(amount="everything")

    def test_rejects_empty_asset(self) -> None:
        with pytest.raises(ValueError, match="asset must be a non-empty token symbol"):
            PerpWithdrawIntent(amount=Decimal("1"), asset="   ")

    def test_rejects_malformed_destination(self) -> None:
        with pytest.raises(ValueError, match="destination must be a 0x-prefixed 20-byte EVM address"):
            PerpWithdrawIntent(amount=Decimal("1"), destination="0xdeadbeef")

    def test_accepts_wellformed_destination(self) -> None:
        intent = PerpWithdrawIntent(amount=Decimal("1"), destination=_WALLET)
        assert intent.destination == _WALLET

    def test_serialize_round_trip(self) -> None:
        intent = Intent.perp_withdraw(amount=Decimal("6.99"), destination=_WALLET)
        data = intent.serialize()
        assert data["type"] == "PERP_WITHDRAW"
        restored = PerpWithdrawIntent.deserialize(data)
        assert restored.amount == intent.amount
        assert restored.asset == intent.asset
        assert restored.destination == intent.destination
        assert restored.intent_type == IntentType.PERP_WITHDRAW

    def test_serialize_round_trip_all_marker(self) -> None:
        intent = Intent.perp_withdraw(amount="all")
        data = intent.serialize()
        assert data["amount"] == "all"
        restored = PerpWithdrawIntent.deserialize(data)
        assert restored.amount == "all"

    def test_generic_deserialize_routes_to_perp_withdraw(self) -> None:
        intent = Intent.perp_withdraw(amount=Decimal("2.5"))
        restored = Intent.deserialize(Intent.serialize(intent))
        assert isinstance(restored, PerpWithdrawIntent)
        assert restored.amount == Decimal("2.5")


# --------------------------------------------------------------------------- #
# Taxonomy
# --------------------------------------------------------------------------- #


class TestTaxonomy:
    def test_classified_no_accounting_perp_primitive(self) -> None:
        rec = record_for("PERP_WITHDRAW")
        # Domain primitive is PERP but it is deliberately NO_ACCOUNTING (cash
        # movement, not a position open/close) — no phantom PnL leg.
        assert rec.accounting_category == AccountingCategory.NO_ACCOUNTING
        assert rec.event_kind == EventKind.NONE
        assert rec.position_type is None
        assert rec.required_lifecycle == ()


# --------------------------------------------------------------------------- #
# Compiler
# --------------------------------------------------------------------------- #


class TestCompiler:
    # Drive ``compile_perp_withdraw`` directly against a fake context (mirrors
    # test_compiler.py driving compile_perp_open directly) — the top-level
    # ``.compile()`` context-type gate is exercised by TestTopLevelDispatch.
    def test_withdraw_builds_two_action_bundle(self) -> None:
        # PERP_WITHDRAW emits a TWO-action CoreWriter bundle: usdClassTransfer
        # (perp->spot) THEN spotSend (spot->L1). Both legs are pinned byte-exact
        # against the connector's OWN encoders — NOT cast/re-encoded calldata,
        # which would silently mask a wrong action-id (VIB-5568 trap).
        intent = Intent.perp_withdraw(amount=Decimal("6.99"), chain="hyperevm")
        result = HyperliquidCompiler().compile_perp_withdraw(_ctx(), intent)

        assert result.status == CompilationStatus.SUCCESS
        assert len(result.transactions) == 2

        transfer_tx, withdraw_tx = result.transactions
        for tx in (transfer_tx, withdraw_tx):
            assert tx.to == CORE_WRITER_ADDRESS
            assert tx.value == 0
            assert tx.tx_type == "perp_withdraw"

        # Leg 1: usdClassTransfer perp->spot (to_perp=False), 1e6 ntl scale.
        expected_transfer = "0x" + build_usd_class_transfer_calldata(Decimal("6.99"), to_perp=False).hex()
        assert transfer_tx.data == expected_transfer
        # Leg 2: spotSend USDC->system-address bridge, weiDecimals=8 scale.
        expected_withdraw = "0x" + build_usdc_withdraw_calldata(Decimal("6.99")).hex()
        assert withdraw_tx.data == expected_withdraw
        # Order is load-bearing: rotate perp->spot BEFORE the spot-account bridge.
        assert transfer_tx.data != withdraw_tx.data

        # The bundle's gas estimate is the sum of both legs.
        assert result.total_gas_estimate == transfer_tx.gas_estimate + withdraw_tx.gas_estimate
        # action_bundle mirrors the two txs in order.
        assert len(result.action_bundle.transactions) == 2

    def test_withdraw_quantizes_both_legs_to_same_6dp_amount(self) -> None:
        # VIB-5617 audit / Codex P1: the two legs use DIFFERENT venue scales
        # (usdClassTransfer 1e6, spotSend 1e8). A >6-dp amount must be quantized
        # DOWN to 6-dp and BOTH legs built from that same value — otherwise leg 1
        # moves a 6-dp-floored value perp->spot while leg 2 tries to bridge the
        # full 8-dp amount, leaving spot short so HyperCore async-rejects the
        # bridge while the EVM txs still report success.
        intent = Intent.perp_withdraw(amount=Decimal("1.00000001"), chain="hyperevm")
        result = HyperliquidCompiler().compile_perp_withdraw(_ctx(), intent)

        assert result.status == CompilationStatus.SUCCESS
        transfer_tx, withdraw_tx = result.transactions
        # BOTH legs built from the 6-dp-quantized 1.000000, NOT the raw 1.00000001.
        assert transfer_tx.data == "0x" + build_usd_class_transfer_calldata(Decimal("1.000000"), to_perp=False).hex()
        assert withdraw_tx.data == "0x" + build_usdc_withdraw_calldata(Decimal("1.000000")).hex()
        assert result.action_bundle.metadata["amount"] == "1.000000"

    def test_withdraw_below_6dp_minimum_fails_closed(self) -> None:
        # An amount that quantizes DOWN to zero at the 6-dp scale must fail closed,
        # never emit a zero-amount no-op bundle.
        intent = Intent.perp_withdraw(amount=Decimal("0.0000009"), chain="hyperevm")
        result = HyperliquidCompiler().compile_perp_withdraw(_ctx(), intent)
        assert result.status == CompilationStatus.FAILED

    def test_metadata_records_bridge_and_scale(self) -> None:
        intent = Intent.perp_withdraw(amount=Decimal("6.99"))
        result = HyperliquidCompiler().compile_perp_withdraw(_ctx(), intent)
        md = result.action_bundle.metadata
        assert md["asset"] == "USDC"
        # Quantized DOWN to the 6-dp usdClassTransfer scale (both legs share it).
        assert md["amount"] == "6.990000"
        assert md["spot_token_index"] == 0
        assert md["wei_decimals"] == 8  # LOAD-BEARING: 8, not the 1e6 perp ntl scale
        assert md["bridge"] == "hypercore->hyperevm"
        assert md["destination"] == _WALLET
        # Both legs are recorded so the bundle is self-describing.
        assert md["legs"] == ["usd_class_transfer_perp_to_spot", "spot_send_bridge_to_l1"]

    def test_system_address_is_the_bridge_target_in_calldata(self) -> None:
        # The spotSend destination inside the action blob is the USDC system
        # address (HyperCore reads it as a bridge). It appears in the SECOND
        # (spotSend) leg's calldata — the first leg is the perp->spot transfer.
        intent = Intent.perp_withdraw(amount=Decimal("6.99"))
        result = HyperliquidCompiler().compile_perp_withdraw(_ctx(), intent)
        withdraw_tx = result.transactions[1]
        assert USDC_SPOT_SYSTEM_ADDRESS.lower()[2:] in withdraw_tx.data.lower()

    def test_explicit_sender_destination_ok(self) -> None:
        intent = Intent.perp_withdraw(amount=Decimal("1"), destination=_WALLET)
        result = HyperliquidCompiler().compile_perp_withdraw(_ctx(wallet=_WALLET), intent)
        assert result.status == CompilationStatus.SUCCESS

    def test_non_sender_destination_fails_closed(self) -> None:
        # A non-sender destination is a plain spot transfer, NOT a bridge — it
        # cannot land funds on L1, so refuse rather than silently mis-send.
        other = "0x" + "22" * 20
        intent = Intent.perp_withdraw(amount=Decimal("1"), destination=other)
        result = HyperliquidCompiler().compile_perp_withdraw(_ctx(wallet=_WALLET), intent)
        assert result.status == CompilationStatus.FAILED
        assert "sender's own wallet" in (result.error or "")

    def test_non_finite_amount_rejected_by_validator(self) -> None:
        # Decimal('NaN') <= 0 is False, so the positivity guard alone would let a
        # NaN/Infinity amount slip through (Gemini). The intent validator rejects
        # every non-finite amount before it can reach the compiler/encoder.
        import pytest as _pytest

        for bad in (Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")):
            with _pytest.raises(ValueError):
                PerpWithdrawIntent(amount=bad, chain="hyperevm")

    def test_non_usdc_asset_fails_closed(self) -> None:
        intent = PerpWithdrawIntent(amount=Decimal("1"), asset="ETH", chain="hyperevm")
        result = HyperliquidCompiler().compile_perp_withdraw(_ctx(), intent)
        assert result.status == CompilationStatus.FAILED
        assert "only USDC" in (result.error or "")

    def test_unresolved_all_marker_fails_closed(self) -> None:
        # The runner resolves 'all' to a concrete Decimal before compile; a bare
        # marker reaching the compiler has no offline free-margin read → refuse.
        intent = Intent.perp_withdraw(amount="all", chain="hyperevm")
        result = HyperliquidCompiler().compile_perp_withdraw(_ctx(), intent)
        assert result.status == CompilationStatus.FAILED
        assert "resolved positive Decimal" in (result.error or "")

    def test_wrong_chain_fails_closed(self) -> None:
        intent = Intent.perp_withdraw(amount=Decimal("1"), chain="arbitrum")
        result = HyperliquidCompiler().compile_perp_withdraw(_ctx(chain="arbitrum"), intent)
        assert result.status == CompilationStatus.FAILED
        assert "hyperevm" in (result.error or "")


# --------------------------------------------------------------------------- #
# Top-level compiler dispatch (teardown calls the TOP level, not the connector)
# --------------------------------------------------------------------------- #


class TestTopLevelDispatch:
    def test_compiler_dispatches_perp_withdraw(self) -> None:
        """IntentCompiler.compile() must route PERP_WITHDRAW to the connector.

        Teardown / the runner call IntentCompiler.compile (the top level); a
        connector-only compile would pass while the top-level path 404s. This
        pins the dispatch branch exists and reaches the hyperliquid compiler.
        """
        from almanak.framework.intents.compiler import IntentCompiler
        from almanak.framework.intents.compiler_models import IntentCompilerConfig

        compiler = IntentCompiler(
            chain="hyperevm",
            wallet_address=_WALLET,
            config=IntentCompilerConfig(allow_placeholder_prices=True),
        )
        intent = Intent.perp_withdraw(amount=Decimal("6.99"), chain="hyperevm")
        result = compiler.compile(intent)
        assert result.status == CompilationStatus.SUCCESS, result.error
        assert len(result.transactions) == 2
        assert result.transactions[0].to == CORE_WRITER_ADDRESS
        assert result.transactions[1].to == CORE_WRITER_ADDRESS
        # Leg 1 = usdClassTransfer (perp->spot); leg 2 = spotSend bridge (spot->L1).
        expected_transfer = "0x" + build_usd_class_transfer_calldata(Decimal("6.99"), to_perp=False).hex()
        expected_withdraw = "0x" + build_usdc_withdraw_calldata(Decimal("6.99")).hex()
        assert result.transactions[0].data == expected_transfer
        assert result.transactions[1].data == expected_withdraw
