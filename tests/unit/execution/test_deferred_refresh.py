"""Unit tests for deferred transaction refresh logic.

Tests the refresh_deferred_bundle() function which re-fetches fresh calldata
from aggregator protocols (LiFi, Enso) immediately before execution.

VIB-6228 rewrote three tests in this file — ``test_refresh_failure_falls_back_
to_stale_data``, ``test_unknown_protocol_passes_through_with_warning`` and
``test_missing_route_params_passes_through``. They asserted the fail-open they
were meant to catch: each one pinned "a deferred bundle that could not be
refreshed is submitted with its expired calldata" as correct behaviour, so the
defect could not be fixed without a test going red. They are now the
fail-closed assertions for the same three paths.
"""

import contextlib
import copy
import logging
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.deferred_refresh_registry import (
    DeferredRefreshCapability,
    DeferredRefreshConnector,
    DeferredRefreshRegistry,
)
from almanak.connectors.enso.deferred_refresh_provider import (
    ANVIL_MIN_SLIPPAGE_BPS,
    EnsoDeferredRefreshConnector,
)
from almanak.framework.execution import deferred_refresh
from almanak.framework.execution.deferred_refresh import refresh_deferred_bundle
from almanak.framework.execution.interfaces import DeferredRefreshError
from almanak.framework.execution.simulator.config import is_local_rpc
from almanak.framework.models.reproduction_bundle import ActionBundle


class _MockDeferredRefreshConnector(DeferredRefreshConnector, DeferredRefreshCapability):
    """Test connector that forwards refresh calls to a mock."""

    kind = ProtocolKind.SWAP

    def __init__(self, protocol: str, mock: MagicMock) -> None:
        self.protocol = ProtocolName(protocol)
        self._mock = mock

    def refresh_transaction(
        self,
        metadata: dict[str, Any],
        wallet_address: str,
        *,
        rpc_url: str | None = None,
        managed_fork: bool | None = None,
    ) -> dict[str, Any]:
        self.last_managed_fork = managed_fork
        return self._mock(metadata, wallet_address, rpc_url)


def _patch_registry(connector: DeferredRefreshConnector):
    """Patch deferred_refresh to use a temporary registry with one connector."""
    registry = DeferredRefreshRegistry()
    registry.register(connector)
    return patch.object(deferred_refresh, "DEFERRED_REFRESH_REGISTRY", registry)


def _patch_refresher(protocol: str, mock: MagicMock):
    """Patch the dispatch entry for a protocol with a mock refresh connector."""
    return _patch_registry(_MockDeferredRefreshConnector(protocol, mock))


WALLET = "0x1234567890abcdef1234567890abcdef12345678"

APPROVE_SELECTOR = "0x095ea7b3"
# The spender the compiler approved, and the spender a refreshed route names
# instead. Real 20-byte addresses: the guard rejects anything that is not one, so
# a symbolic placeholder like "0xEnsoRouter" cannot stand in — the same fixture
# unrealism that let the MAX_UINT256 substitution ship untested.
ORIGINAL_SPENDER = "0x1111111254eeb25477b68fb85ed929f73a960582"
FRESH_SPENDER = "0x2222222222222222222222222222222222222222"
# A valid refreshed target, for cases whose subject is some OTHER field.
VALID_TO = "0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"
# Matches metadata["amount_in"] — the whole point of the approval is that it is
# sized to the swap, not unbounded.
APPROVED_AMOUNT = 100_000_000
MAX_UINT256 = 2**256 - 1


def _encode_approve(spender: str, amount: int) -> str:
    """Encode a real ``approve(address,uint256)`` payload via ``eth_abi``."""
    return APPROVE_SELECTOR + abi_encode(["address", "uint256"], [spender, amount]).hex()


def _decode_approve(calldata: str) -> tuple[str, int]:
    """Decode an ``approve`` payload back to ``(spender, amount)``.

    Deliberately uses ``eth_abi`` rather than the offsets the production code
    slices with, so an assertion here cannot agree with the implementation by
    sharing its arithmetic. This is the "prove it on decoded calldata" contract:
    asserting on a hand-written expected string would pass against a payload no
    EVM could parse.
    """
    assert calldata.startswith(APPROVE_SELECTOR), calldata
    spender, amount = abi_decode(["address", "uint256"], bytes.fromhex(calldata[10:]))
    return spender.lower(), amount


def _make_approve_tx(spender: str = ORIGINAL_SPENDER, amount: int = APPROVED_AMOUNT) -> dict:
    """Create a standard ERC-20 approve transaction dict."""
    return {
        "to": "0xTokenAddress",
        "value": "0",
        "data": _encode_approve(spender, amount),
        "gas_estimate": 50000,
        "description": "Approve USDC",
        "tx_type": "approve",
    }


def _make_lifi_bundle(deferred: bool = True) -> ActionBundle:
    """Create a LiFi ActionBundle with optional deferred swap."""
    approve_tx = _make_approve_tx()
    swap_tx = {
        "to": "0xLiFiDiamond",
        "value": "0",
        "data": "0xstale_lifi_calldata",
        "gas_estimate": 200000,
        "description": "Swap via LiFi",
        "tx_type": "swap_deferred" if deferred else "swap",
    }
    metadata = {
        "from_token": {"symbol": "USDC", "address": "0xUSDC", "chain": "arbitrum"},
        "to_token": {"symbol": "WETH", "address": "0xWETH", "chain": "arbitrum"},
        "amount_in": "100000000",
        "protocol": "lifi",
        "deferred_swap": deferred,
        "route_params": {
            "from_chain_id": 42161,
            "to_chain_id": 42161,
            "from_token": "0xUSDC",
            "to_token": "0xWETH",
            "from_amount": "100000000",
            "from_address": WALLET,
            "slippage": 0.05,
        },
    }
    return ActionBundle(
        intent_type="SWAP",
        transactions=[approve_tx, swap_tx],
        metadata=metadata,
    )


def _make_enso_bundle() -> ActionBundle:
    """Create an Enso ActionBundle with deferred swap."""
    approve_tx = _make_approve_tx()
    swap_tx = {
        "to": "0xe5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5",
        "value": "0",
        "data": "0xstale_enso_calldata",
        "gas_estimate": 200000,
        "description": "Swap via Enso",
        "tx_type": "swap_deferred",
    }
    metadata = {
        "from_token": {"symbol": "USDC", "address": "0xUSDC", "chain": "arbitrum"},
        "to_token": {"symbol": "WETH", "address": "0xWETH", "chain": "arbitrum"},
        "amount_in": "100000000",
        "protocol": "enso",
        "chain": "arbitrum",
        "deferred_swap": True,
        "route_params": {
            "token_in": "0xUSDC",
            "token_out": "0xWETH",
            "amount_in": "100000000",
            "slippage_bps": 500,
        },
    }
    return ActionBundle(
        intent_type="SWAP",
        transactions=[approve_tx, swap_tx],
        metadata=metadata,
    )


class TestDeferredRefresh:
    """Tests for refresh_deferred_bundle()."""

    def test_non_deferred_bundle_passes_through(self):
        """Non-deferred bundles are returned unchanged (zero overhead path)."""
        bundle = _make_lifi_bundle(deferred=False)
        # Remove deferred_swap from metadata
        bundle.metadata["deferred_swap"] = False

        result = refresh_deferred_bundle(bundle, WALLET)

        # Should be the exact same object (not a copy)
        assert result is bundle
        assert result.transactions[1]["data"] == "0xstale_lifi_calldata"

    def test_lifi_deferred_bundle_gets_refreshed(self):
        """LiFi deferred bundle gets fresh transaction data."""
        mock_refresh_lifi = MagicMock(
            return_value={
                "to": "0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
                "value": 0,
                "data": "0x66726573685f6c696669",
                "gas_estimate": 250000,
                "description": "Swap USDC -> WETH via LiFi",
                "tx_type": "swap",
            }
        )

        bundle = _make_lifi_bundle(deferred=True)
        before = copy.deepcopy((bundle.intent_type, bundle.transactions, bundle.metadata))
        with _patch_refresher("lifi", mock_refresh_lifi):
            result = refresh_deferred_bundle(bundle, WALLET)

        # Should be a different object
        assert result is not bundle

        # Approve TX should be untouched
        assert result.transactions[0]["data"] == _encode_approve(ORIGINAL_SPENDER, APPROVED_AMOUNT)
        assert result.transactions[0]["tx_type"] == "approve"

        # Swap TX should be updated with fresh data
        swap_tx = result.transactions[1]
        assert swap_tx["to"] == "0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1"
        assert swap_tx["data"] == "0x66726573685f6c696669"
        assert swap_tx["gas_estimate"] == 250000
        assert swap_tx["tx_type"] == "swap"  # _deferred suffix stripped
        assert swap_tx["description"] == "Swap USDC -> WETH via LiFi"

        # The caller's bundle is untouched even on the SUCCESS path -- the refresh
        # happens on a deep copy. Asserted with the same whole-bundle comparison
        # the refusal paths use, rather than spot-checking two fields.
        assert before == (bundle.intent_type, bundle.transactions, bundle.metadata)

    def test_enso_deferred_bundle_gets_refreshed(self):
        """Enso deferred bundle gets fresh transaction data."""
        mock_refresh_enso = MagicMock(
            return_value={
                "to": "0xb2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2",
                "value": 0,
                "data": "0x66726573685f656e736f",
                "gas_estimate": 180000,
                "tx_type": "swap",
            }
        )

        bundle = _make_enso_bundle()
        with _patch_refresher("enso", mock_refresh_enso):
            result = refresh_deferred_bundle(bundle, WALLET)

        assert result is not bundle

        swap_tx = result.transactions[1]
        assert swap_tx["to"] == "0xb2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2"
        assert swap_tx["data"] == "0x66726573685f656e736f"
        assert swap_tx["tx_type"] == "swap"  # _deferred suffix stripped

    def test_unknown_protocol_refuses_since_VIB_6228(self):
        """No registered provider => refuse; the stale route must not be submitted."""
        bundle = _make_lifi_bundle(deferred=True)
        bundle.metadata["protocol"] = "unknown_dex"

        with _bundle_must_not_change(bundle):
            with pytest.raises(DeferredRefreshError) as exc_info:
                refresh_deferred_bundle(bundle, WALLET)

        assert "no deferred-refresh provider is registered" in str(exc_info.value)

    def test_refresh_failure_refuses_since_VIB_6228(self):
        """A failed route fetch refuses instead of falling back to stale data.

        This is the motivating defect: pre-VIB-6228 this returned the original
        bundle, so an expired LiFi/Enso route was submitted on-chain whenever
        the refresh API blipped.
        """
        mock_refresh_lifi = MagicMock(side_effect=Exception("API timeout"))

        bundle = _make_lifi_bundle(deferred=True)
        with _patch_refresher("lifi", mock_refresh_lifi):
            with _bundle_must_not_change(bundle):
                with pytest.raises(DeferredRefreshError) as exc_info:
                    refresh_deferred_bundle(bundle, WALLET)

        assert "API timeout" in str(exc_info.value), "the upstream cause must stay diagnosable"
        assert exc_info.value.__cause__ is not None, "the original exception must be chained"

    def test_provider_returning_none_refuses_since_VIB_6228(self):
        """A provider that returns ``None`` is a refusal, not a pass-through.

        The second fail-open on the same path: ``fresh_tx is None`` returned the
        un-refreshed bundle with no log line at all.
        """
        mock_refresh_lifi = MagicMock(return_value=None)

        bundle = _make_lifi_bundle(deferred=True)
        with _patch_refresher("lifi", mock_refresh_lifi):
            with _bundle_must_not_change(bundle):
                with pytest.raises(DeferredRefreshError) as exc_info:
                    refresh_deferred_bundle(bundle, WALLET)

        assert "returned no transaction data" in str(exc_info.value)

    def test_missing_route_params_refuses_since_VIB_6228(self):
        """deferred_swap=True with no route_params is a bundle defect, not a no-op."""
        bundle = _make_lifi_bundle(deferred=True)
        del bundle.metadata["route_params"]

        with _bundle_must_not_change(bundle):
            with pytest.raises(DeferredRefreshError) as exc_info:
                refresh_deferred_bundle(bundle, WALLET)

        assert "no route_params" in str(exc_info.value)

    def test_a_provider_that_mutates_nested_metadata_then_fails_cannot_reach_the_caller(self):
        """The metadata copy must be DEEP, forced by a provider that mutates nested input.

        Connector refresh hooks are handed the metadata and may legitimately
        adjust it before calling out (Enso widens ``route_params["slippage_bps"]``
        on local forks). Nothing else in this file forces a provider to mutate
        *nested* state, so a regression from ``copy.deepcopy`` to a shallow
        ``dict(...)`` would leave every other test green while letting a provider
        scribble on the caller's own ``route_params`` — including on the path
        where it then fails and we refuse.

        Found by the Phase 0b spec critique, round 5: the whole-bundle snapshot in
        ``_bundle_must_not_change`` can only catch this if some provider actually
        attempts the mutation. Pinned as mutation M10 in the card's D3.F6.
        """

        def hostile(metadata, wallet_address, rpc_url):
            metadata["route_params"]["slippage"] = 0.99
            metadata["route_params"]["injected"] = "should never reach the caller"
            metadata["protocol"] = "tampered"
            raise RuntimeError("route API exploded after mangling the request")

        bundle = _make_lifi_bundle(deferred=True)
        with _patch_refresher("lifi", MagicMock(side_effect=hostile)):
            with _bundle_must_not_change(bundle):
                with pytest.raises(DeferredRefreshError):
                    refresh_deferred_bundle(bundle, WALLET)

    def test_a_provider_that_mutates_nested_metadata_and_succeeds_cannot_reach_the_caller(self):
        """Negative control for the above: same mutation, successful refresh.

        Without this, the deep-copy assertion would only be pinned on the refusal
        path — and the success path is the one that actually runs in production.
        """

        def hostile(metadata, wallet_address, rpc_url):
            metadata["route_params"]["injected"] = "should never reach the caller"
            return {
                "to": "0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
                "value": 0,
                "data": "0x66726573685f6c696669",
                "gas_estimate": 250000,
                "tx_type": "swap",
            }

        bundle = _make_lifi_bundle(deferred=True)
        with _patch_refresher("lifi", MagicMock(side_effect=hostile)):
            with _bundle_must_not_change(bundle):
                result = refresh_deferred_bundle(bundle, WALLET)

        # The refresh DID happen on the copy...
        assert result.transactions[1]["data"] == "0x66726573685f6c696669"
        # ...and the hook's edit is visible there, but not on the caller's bundle.
        assert result.metadata["route_params"]["injected"] == "should never reach the caller"
        assert "injected" not in bundle.metadata["route_params"]

    @pytest.mark.parametrize(
        ("label", "fresh_tx", "expected"),
        [
            # Absent fields. Each is installed with a bare subscript, so absence
            # must refuse rather than raise KeyError.
            ("data missing", {"to": VALID_TO, "value": 0, "gas_estimate": 1}, "missing required transaction field"),
            ("to missing", {"value": 0, "data": "0xdead", "gas_estimate": 1}, "missing required transaction field"),
            ("value missing", {"to": VALID_TO, "data": "0xdead", "gas_estimate": 1}, "missing required transaction"),
            ("gas missing", {"to": VALID_TO, "value": 0, "data": "0xdead"}, "missing required transaction field"),
            # `data` IS the calldata that gets signed, so it is held strictest.
            ("data None", {"to": VALID_TO, "value": 0, "data": None, "gas_estimate": 1}, "not a 0x-prefixed hex"),
            ("data empty", {"to": VALID_TO, "value": 0, "data": "0x", "gas_estimate": 1}, "not a 0x-prefixed hex"),
            ("data non-hex", {"to": VALID_TO, "value": 0, "data": "0xzz", "gas_estimate": 1}, "not a 0x-prefixed hex"),
            ("data odd len", {"to": VALID_TO, "value": 0, "data": "0xabc", "gas_estimate": 1}, "not a 0x-prefixed hex"),
            ("data unprefixed", {"to": VALID_TO, "value": 0, "data": "dead", "gas_estimate": 1}, "not a 0x-prefixed"),
            ("data non-string", {"to": VALID_TO, "value": 0, "data": 42, "gas_estimate": 1}, "not a 0x-prefixed hex"),
            # `to` is the transaction DESTINATION — syntactic address validation
            # only. Constraining WHICH addresses are acceptable is the deferred
            # router-allowlist question; that a non-address cannot be signed is not.
            ("to None", {"to": None, "value": 0, "data": "0xdead", "gas_estimate": 1}, "not a 20-byte address"),
            ("to empty", {"to": "", "value": 0, "data": "0xdead", "gas_estimate": 1}, "not a 20-byte address"),
            ("to short", {"to": "0x" + "a" * 39, "value": 0, "data": "0xdead", "gas_estimate": 1}, "not a 20-byte"),
            ("to long", {"to": "0x" + "a" * 41, "value": 0, "data": "0xdead", "gas_estimate": 1}, "not a 20-byte"),
            ("to non-hex", {"to": "0x" + "z" * 40, "value": 0, "data": "0xdead", "gas_estimate": 1}, "not a 20-byte"),
            ("to symbolic", {"to": "not-an-address", "value": 0, "data": "0xdead", "gas_estimate": 1}, "not a 20-byte"),
            ("to non-string", {"to": 42, "value": 0, "data": "0xdead", "gas_estimate": 1}, "not a 20-byte address"),
            # Numerics.
            ("value non-numeric", {"to": VALID_TO, "value": "abc", "data": "0xdead", "gas_estimate": 1}, "not an int"),
            ("gas non-numeric", {"to": VALID_TO, "value": 0, "data": "0xdead", "gas_estimate": None}, "not an int"),
        ],
    )
    def test_malformed_fresh_transaction_refuses_since_VIB_6228(self, label, fresh_tx, expected):
        """A truthy-but-malformed provider response is NOT a successful refresh.

        Every field here is installed onto the deferred transaction with a bare
        subscript, so before this validation:

        * a response missing ``data`` raised ``KeyError`` and escaped the pipeline
          as an opaque "Unexpected error";
        * a response carrying ``data: None`` was worse — it installed ``None`` as
          the transaction's calldata, stripped the ``_deferred`` suffix, and
          returned a bundle that *looked* successfully refreshed.

        The response is third-party API output, so presence and type are both
        checked, and ``data`` most strictly of all because it **is** the calldata
        that gets signed. Found by the Phase 0b spec critique, round 8; pinned as
        mutation M14.
        """
        bundle = _make_lifi_bundle(deferred=True)
        with _patch_refresher("lifi", MagicMock(return_value=fresh_tx)):
            with _bundle_must_not_change(bundle):
                with pytest.raises(DeferredRefreshError) as exc_info:
                    refresh_deferred_bundle(bundle, WALLET)

        assert expected in str(exc_info.value), f"{label}: got {exc_info.value}"

    @pytest.mark.parametrize(
        "valid_to",
        [
            VALID_TO,
            # EIP-55 mixed case. A validator demanding lowercase would refuse
            # every real LiFi/Enso response — a guard that becomes an outage.
            "0x1111111254EEB25477B68fb85Ed929f73A960582",
            # Uppercase 0X prefix; still the same 20 bytes.
            "0Xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
        ],
    )
    def test_a_well_formed_fresh_transaction_is_accepted(self, valid_to):
        """Negative control: the validator must not reject legitimate responses.

        Without this, "validate the response" and "reject every response" are
        indistinguishable, and a validator that bricked every deferred swap on
        twelve chains would pass all nineteen refusal cases above.
        """
        bundle = _make_lifi_bundle(deferred=True)
        fresh = {
            "to": valid_to,
            "value": 0,
            "data": "0x" + "ab" * 32,
            "gas_estimate": 250000,
            "tx_type": "swap",
        }
        with _patch_refresher("lifi", MagicMock(return_value=fresh)):
            result = refresh_deferred_bundle(bundle, WALLET)

        assert result.transactions[1]["data"] == "0x" + "ab" * 32
        assert result.transactions[1]["tx_type"] == "swap"
        # The target is installed verbatim -- validation must not silently
        # rewrite it (a normalised `to` would change what the operator sees).
        assert result.transactions[1]["to"] == valid_to

    @pytest.mark.parametrize("numeric_as_string", ["0", "1000000000000000000"])
    def test_string_encoded_numerics_are_accepted(self, numeric_as_string):
        """JSON APIs routinely quote large integers; that must not be a refusal.

        A validator demanding ``isinstance(int)`` would refuse every LiFi response
        that renders ``value`` as a decimal string — a guard failing on its own
        correct input, which is the failure mode the checksummed-address test
        guards against on the other axis.
        """
        bundle = _make_lifi_bundle(deferred=True)
        fresh = {
            "to": "0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
            "value": numeric_as_string,
            "data": "0x" + "cd" * 4,
            "gas_estimate": "250000",
            "tx_type": "swap",
        }
        with _patch_refresher("lifi", MagicMock(return_value=fresh)):
            result = refresh_deferred_bundle(bundle, WALLET)

        # Accepted AND normalised. Leniency without normalisation just moves the
        # failure one stage downstream: `_build_unsigned_transactions` computes
        # `int(gas_estimate * gas_buffer_multiplier)`, and `"250000" * 1.2` is a
        # TypeError -- so a response this validator explicitly accepted would have
        # blown up as an opaque "Unexpected error" instead. Found by the Codex
        # auditor on the diff that introduced the leniency.
        assert result.transactions[1]["gas_estimate"] == 250000
        assert isinstance(result.transactions[1]["gas_estimate"], int)
        assert result.transactions[1]["value"] == numeric_as_string
        # Prove the arithmetic the next stage performs actually works, rather than
        # asserting a type and hoping that is the property that mattered.
        assert int(result.transactions[1]["gas_estimate"] * 1.2) == 300000
        assert int(result.transactions[1]["value"]) == int(numeric_as_string)

    def test_a_null_tx_type_is_not_a_deferred_leg_and_does_not_crash(self):
        """``tx_type: None`` must not raise ``AttributeError`` on ``.endswith``.

        Third instance of the ``dict.get(k, default)`` trap in this one function —
        the default does not fire when the key exists holding ``None``, and a
        bundle-supplied value's type is not guaranteed. Reproduced before fixing:
        ``AttributeError: 'NoneType' object has no attribute 'endswith'``, escaping
        as the opaque "Unexpected error" this module exists to eliminate.

        A transaction with no usable ``tx_type`` is simply not a deferred leg, so
        the real deferred leg beside it must still refresh normally. Found by the
        CodeRabbit GitHub review, which also correctly noted that
        ``_repoint_approval`` already got this right on the line above while this
        one did not.
        """
        bundle = _make_lifi_bundle(deferred=True)
        bundle.transactions[0]["tx_type"] = None

        mock = MagicMock(
            return_value={
                "to": "0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
                "value": 0,
                "data": "0x66726573685f6c696669",
                "gas_estimate": 250000,
                "tx_type": "swap",
            }
        )
        with _patch_refresher("lifi", mock):
            result = refresh_deferred_bundle(bundle, WALLET)

        # The None-typed leg is untouched, and the genuine deferred leg refreshed.
        assert result.transactions[0]["tx_type"] is None
        assert result.transactions[1]["data"] == "0x66726573685f6c696669"
        assert result.transactions[1]["tx_type"] == "swap"

    def test_two_deferred_transactions_refuse_rather_than_leaving_one_stale(self):
        """A single refresh response cannot make two legs current.

        The pre-round-10 loop replaced the first `*_deferred` match and ``break``ed,
        so a second deferred leg kept **both** its stale calldata and its
        ``_deferred`` suffix — and was then built and submitted, defeating the
        entire point of Step 0. Reproduced before fixing: leg 2 came back with
        ``data="0xstale2"`` and ``tx_type="bridge_deferred"``.

        No producer emits two deferred legs today (LiFi and Enso each mark a single
        swap/bridge leg), so this is defence-in-depth. It is enforced anyway
        because the cost of being wrong is submitting expired calldata and the
        check is one line. Found by the Phase 0b spec critique, round 10; pinned as
        mutation M16.
        """
        bundle = _make_lifi_bundle(deferred=True)
        bundle.transactions.append(
            {
                "to": "0xb2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2",
                "value": "0",
                "data": "0xstale_second_leg",
                "gas_estimate": 100000,
                "tx_type": "bridge_deferred",
            }
        )
        mock = MagicMock(return_value=_fresh_tx_with_approval(FRESH_SPENDER))

        with _patch_refresher("lifi", mock):
            with _bundle_must_not_change(bundle):
                with pytest.raises(DeferredRefreshError) as exc_info:
                    refresh_deferred_bundle(bundle, WALLET)

        message = str(exc_info.value)
        assert "2 transactions with a '_deferred' tx_type" in message
        # Both offending tx_types are named, so the bundle defect is diagnosable
        # without re-running.
        assert "swap_deferred" in message and "bridge_deferred" in message

    def test_no_deferred_transaction_to_replace_refuses_since_VIB_6228(self):
        """A bundle whose refresh cannot be applied must not execute as-is.

        The fourth fail-open: the fresh route was fetched successfully but no
        transaction carried a ``*_deferred`` tx_type, so the original bundle was
        returned — its calldata still the stale calldata the metadata declared
        stale.
        """
        mock_refresh_lifi = MagicMock(
            return_value={
                "to": "0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
                "value": 0,
                "data": "0x66726573685f6c696669",
                "gas_estimate": 250000,
                "tx_type": "swap",
            }
        )

        bundle = _make_lifi_bundle(deferred=True)
        bundle.transactions[1]["tx_type"] = "swap"  # suffix lost / never set

        with _patch_refresher("lifi", mock_refresh_lifi):
            with _bundle_must_not_change(bundle):
                with pytest.raises(DeferredRefreshError) as exc_info:
                    refresh_deferred_bundle(bundle, WALLET)

        assert "'_deferred' tx_type" in str(exc_info.value)

    def test_declared_managed_fork_widens_enso_slippage(self):
        """On a DECLARED managed fork, slippage_bps is widened to ANVIL_MIN_SLIPPAGE_BPS."""
        mock_refresh_enso = MagicMock(
            return_value={
                "to": "0xe5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5",
                "value": 0,
                "data": "0x6672657368",
                "gas_estimate": 180000,
                "tx_type": "swap",
            }
        )

        bundle = _make_enso_bundle()
        # Set tight slippage (50 bps = 0.5%)
        bundle.metadata["route_params"]["slippage_bps"] = 50

        connector = EnsoDeferredRefreshConnector()

        with patch.object(connector, "_refresh_from_adapter", mock_refresh_enso):
            with _patch_registry(connector):
                result = refresh_deferred_bundle(bundle, WALLET, rpc_url="http://localhost:8545", managed_fork=True)

        # Slippage should have been widened in the result
        assert result.metadata["route_params"]["slippage_bps"] == ANVIL_MIN_SLIPPAGE_BPS
        # Original bundle must NOT be mutated
        assert bundle.metadata["route_params"]["slippage_bps"] == 50
        mock_refresh_enso.assert_called_once()
        # Verify widened slippage was passed to the API call (not applied after)
        called_metadata = mock_refresh_enso.call_args[0][0]
        assert called_metadata["route_params"]["slippage_bps"] == ANVIL_MIN_SLIPPAGE_BPS

    def test_managed_fork_keeps_wide_slippage_unchanged(self):
        """If slippage is already >= ANVIL_MIN_SLIPPAGE_BPS, don't change it."""
        mock_refresh_enso = MagicMock(
            return_value={
                "to": "0xe5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5",
                "value": 0,
                "data": "0x6672657368",
                "gas_estimate": 180000,
                "tx_type": "swap",
            }
        )

        bundle = _make_enso_bundle()
        # Already wide slippage (1000 bps = 10%)
        bundle.metadata["route_params"]["slippage_bps"] = 1000

        connector = EnsoDeferredRefreshConnector()

        with patch.object(connector, "_refresh_from_adapter", mock_refresh_enso):
            with _patch_registry(connector):
                result = refresh_deferred_bundle(bundle, WALLET, rpc_url="http://127.0.0.1:8545", managed_fork=True)

        # Should not widen further
        assert result.metadata["route_params"]["slippage_bps"] == 1000
        # Verify original wide slippage was passed to API call unchanged
        called_metadata = mock_refresh_enso.call_args[0][0]
        assert called_metadata["route_params"]["slippage_bps"] == 1000

    def test_managed_fork_enso_missing_slippage_bps_is_left_unchanged(self):
        """Missing slippage_bps should not crash the Enso deferred refresh path."""
        mock_refresh_enso = MagicMock(
            return_value={
                "to": "0xe5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5",
                "value": 0,
                "data": "0x6672657368",
                "gas_estimate": 180000,
                "tx_type": "swap",
            }
        )

        bundle = _make_enso_bundle()
        del bundle.metadata["route_params"]["slippage_bps"]
        connector = EnsoDeferredRefreshConnector()

        with patch.object(connector, "_refresh_from_adapter", mock_refresh_enso):
            with _patch_registry(connector):
                result = refresh_deferred_bundle(bundle, WALLET, rpc_url="http://localhost:8545", managed_fork=True)

        assert "slippage_bps" not in result.metadata["route_params"]
        called_metadata = mock_refresh_enso.call_args[0][0]
        assert "slippage_bps" not in called_metadata["route_params"]

    def test_mainnet_rpc_does_not_widen_slippage(self):
        """Mainnet RPC URLs should NOT trigger slippage widening."""
        mock_refresh_enso = MagicMock(
            return_value={
                "to": "0xe5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5",
                "value": 0,
                "data": "0x6672657368",
                "gas_estimate": 180000,
                "tx_type": "swap",
            }
        )

        bundle = _make_enso_bundle()
        bundle.metadata["route_params"]["slippage_bps"] = 50

        connector = EnsoDeferredRefreshConnector()

        with patch.object(connector, "_refresh_from_adapter", mock_refresh_enso):
            with _patch_registry(connector):
                result = refresh_deferred_bundle(
                    bundle, WALLET, rpc_url="https://arb-mainnet.g.alchemy.com/v2/key", managed_fork=False
                )

        # Slippage should NOT have been widened
        assert result.metadata["route_params"]["slippage_bps"] == 50
        # Verify original tight slippage was passed to API call unchanged
        called_metadata = mock_refresh_enso.call_args[0][0]
        assert called_metadata["route_params"]["slippage_bps"] == 50

    def test_production_rpc_on_local_port_does_not_widen_slippage(self):
        """ALM-3184 negative control: a local-SHAPED URL must not widen slippage.

        ``is_local_rpc`` returns True for ANY host on port 8545-8550, so before
        this fix a production RPC proxy on ``:8545`` had its Enso
        ``minAmountOut`` bound relaxed from 0.5% to 5% on real mainnet swaps.
        Reverting the fix turns this assertion red.
        """
        mock_refresh_enso = MagicMock(
            return_value={
                "to": "0xe5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5",
                "value": 0,
                "data": "0x6672657368",
                "gas_estimate": 180000,
                "tx_type": "swap",
            }
        )

        bundle = _make_enso_bundle()
        bundle.metadata["route_params"]["slippage_bps"] = 50

        connector = EnsoDeferredRefreshConnector()

        for url in (
            "http://rpc-proxy.internal.example:8545",
            "https://anvil-cluster.rpc.example.com/v2/key",
        ):
            with patch.object(connector, "_refresh_from_adapter", mock_refresh_enso):
                with _patch_registry(connector):
                    result = refresh_deferred_bundle(bundle, WALLET, rpc_url=url, managed_fork=False)

            assert result.metadata["route_params"]["slippage_bps"] == 50, url
            assert mock_refresh_enso.call_args[0][0]["route_params"]["slippage_bps"] == 50, url

    def test_undeclared_endpoint_does_not_widen_slippage(self):
        """Undeclared ⇒ production (no widening), on a fork-shaped URL.

        ALM-3184 P1: undeclared no longer means "go and find out". The runtime
        probe is gone, so this is pure declaration resolution — no socket, no
        shared cache, nothing to isolate.
        """
        mock_refresh_enso = MagicMock(
            return_value={
                "to": "0xe5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5",
                "value": 0,
                "data": "0x6672657368",
                "gas_estimate": 180000,
                "tx_type": "swap",
            }
        )

        bundle = _make_enso_bundle()
        bundle.metadata["route_params"]["slippage_bps"] = 50

        connector = EnsoDeferredRefreshConnector()

        with patch.object(connector, "_refresh_from_adapter", mock_refresh_enso):
            with _patch_registry(connector):
                result = refresh_deferred_bundle(bundle, WALLET, rpc_url="http://127.0.0.1:8545")

        assert result.metadata["route_params"]["slippage_bps"] == 50

    def test_lifi_on_managed_fork_does_not_widen_slippage(self):
        """LiFi bundles on a managed fork should NOT trigger Enso slippage widening."""
        mock_refresh_lifi = MagicMock(
            return_value={
                "to": "0xd4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4",
                "value": 0,
                "data": "0x66726573685f6c696669",
                "gas_estimate": 200000,
                "tx_type": "swap",
            }
        )

        bundle = _make_lifi_bundle(deferred=True)
        # LiFi uses "slippage" not "slippage_bps"
        bundle.metadata["route_params"]["slippage"] = 0.005

        with _patch_refresher("lifi", mock_refresh_lifi):
            result = refresh_deferred_bundle(bundle, WALLET, rpc_url="http://localhost:8545", managed_fork=True)

        # Should succeed without crashing; slippage unchanged
        assert result.metadata["route_params"]["slippage"] == 0.005
        assert "slippage_bps" not in result.metadata["route_params"]

    def test_bridge_deferred_tx_type_is_handled(self):
        """Bridge transactions with _deferred suffix are also refreshed."""
        mock_refresh_lifi = MagicMock(
            return_value={
                "to": "0xc3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3",
                "value": 0,
                "data": "0x66726573685f627269646765",
                "gas_estimate": 300000,
                "tx_type": "bridge",
            }
        )

        bundle = _make_lifi_bundle(deferred=True)
        bundle.transactions[1]["tx_type"] = "bridge_deferred"

        with _patch_refresher("lifi", mock_refresh_lifi):
            result = refresh_deferred_bundle(bundle, WALLET)

        swap_tx = result.transactions[1]
        assert swap_tx["tx_type"] == "bridge"  # _deferred suffix stripped
        assert swap_tx["data"] == "0x66726573685f627269646765"


def _assert_only_the_spender_word_changed(before: dict, after: dict, expected_spender: str) -> None:
    """The repoint must touch bytes 10..73 of ``data`` and nothing else at all.

    "Only the spender is repointed" is a claim about the whole transaction, not
    just about the two words a decoder returns. Checking the decoded
    ``(spender, amount)`` pair leaves the approval's **non-calldata** fields
    unasserted — and ``to`` is the *token contract* the allowance is granted on,
    so an implementation that repointed the spender correctly while clobbering
    ``to`` would approve the fresh spender on the wrong token and satisfy every
    decode-based assertion. ``tx_type`` matters too: flipping it away from
    ``"approve"`` would silently drop the leg out of every downstream filter.

    Raised by the UAT-GATE Phase 0b spec critique, round 4.
    """
    assert set(before) == set(after), "the repoint must not add or remove transaction fields"
    for key in before:
        if key != "data":
            assert after[key] == before[key], f"the repoint mutated {key!r}: {before[key]!r} -> {after[key]!r}"

    assert after["data"][:10] == before["data"][:10] == APPROVE_SELECTOR, "the selector must be untouched"
    assert after["data"][74:] == before["data"][74:], "the amount word must be untouched"
    assert len(after["data"]) == len(before["data"]) == 138
    # ...and the one word that IS expected to change actually did.
    assert after["data"][10:74] == expected_spender.lower().removeprefix("0x").rjust(64, "0")


@contextlib.contextmanager
def _bundle_must_not_change(bundle: ActionBundle):
    """The caller's bundle must be *wholly* unchanged after a refusal.

    Compares a pre-call deep snapshot of everything — every transaction field and
    the full nested metadata, including ``route_params`` — rather than spot-checking
    named fields.

    Every refusal path is expected to leave the caller's object alone, but the
    approval-repoint refusals are the ones where that is *not* obvious: they fire
    **after** the deferred transaction has already been swapped, so an
    implementation that patched in place rather than on a deep copy would refuse
    correctly while leaving the caller holding a half-refreshed bundle — a
    partially-mutated object still reachable by anything keeping the original
    reference.

    Two UAT-GATE Phase 0b spec critiques shaped this:

    * round 1 — the property was asserted only for the four pre-approval refusal
      paths, so it was being *inferred* for the approval paths;
    * round 3 — the first version of this helper checked three named fields on one
      transaction, so an implementation that mutated ``to`` / ``value`` /
      ``gas_estimate`` / metadata / nested ``route_params`` while leaving those
      three intact would still pass. Enumerating the fields that must not change
      loses to comparing the whole object; only the second scales as the bundle
      shape grows.
    """
    before = copy.deepcopy((bundle.intent_type, bundle.transactions, bundle.metadata))
    yield
    after = (bundle.intent_type, bundle.transactions, bundle.metadata)
    assert after == before, (
        "the caller's bundle was mutated on a refusal path — a refusal must leave "
        f"the caller's object wholly untouched.\nbefore: {before}\nafter:  {after}"
    )


def _fresh_tx_with_approval(approval_address: str) -> dict:
    """A successful refresh whose route wants a different approval spender."""
    return {
        "to": "0xa1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
        "value": 0,
        "data": "0x66726573685f6c696669",
        "gas_estimate": 250000,
        "tx_type": "swap",
        "approval_address": approval_address,
    }


class TestApprovalRepoint:
    """The approval rewrite must repoint the spender WITHOUT widening the amount.

    Pre-VIB-6228 this branch had zero test coverage: every mock in this file
    returned a fresh_tx without ``approval_address``, so nothing ever executed
    the rewrite that substituted ``MAX_UINT256``.
    """

    def test_repoint_never_writes_max_uint256(self):
        """The motivating defect, asserted first and on decoded calldata."""
        bundle = _make_lifi_bundle(deferred=True)
        mock = MagicMock(return_value=_fresh_tx_with_approval(FRESH_SPENDER))

        with _patch_refresher("lifi", mock):
            result = refresh_deferred_bundle(bundle, WALLET)

        _, amount = _decode_approve(result.transactions[0]["data"])
        assert amount != MAX_UINT256, "a refreshed route must not turn a bounded approval unlimited"

    @pytest.mark.parametrize(
        "original_amount",
        [
            # Zero is a legal, meaningful approval: it revokes an allowance. An
            # implementation that widened only zero-valued approvals to MAX would
            # pass every other value in this matrix and every D3.F6 mutation.
            0,
            1,  # the smallest non-zero allowance
            APPROVED_AMOUNT,  # the ordinary case (100 USDC at 6dp)
            2**128,  # large but not MAX
            # An input of MAX must come back OUT as MAX. The rule is "preserve the
            # compiled amount", NOT "never emit MAX": if a compiler legitimately
            # sized an unlimited approval, silently shrinking it would be its own
            # defect. What VIB-6228 forbids is this function *introducing* MAX
            # where the compiler asked for a bound.
            MAX_UINT256,
        ],
    )
    def test_repoint_moves_the_spender_and_preserves_the_amount(self, original_amount):
        """Both halves of the word pair, decoded independently of the slicing.

        Parametrised over four distinct amounts because a single fixture cannot
        establish a *preservation* rule: an implementation that hardcoded the one
        fixture value would satisfy every other assertion in this class and would
        still kill the D3.F6 M4 mutant, while corrupting every approval compiled
        for a different amount. (Raised by the Phase 0b spec critique, round 2.)
        """
        bundle = _make_lifi_bundle(deferred=True)
        bundle.transactions[0] = _make_approve_tx(amount=original_amount)
        before_approve = copy.deepcopy(bundle.transactions[0])
        mock = MagicMock(return_value=_fresh_tx_with_approval(FRESH_SPENDER))

        with _patch_refresher("lifi", mock):
            result = refresh_deferred_bundle(bundle, WALLET)

        spender, amount = _decode_approve(result.transactions[0]["data"])
        assert spender == FRESH_SPENDER.lower()
        assert amount == original_amount, "the amount must be carried over, not re-derived"
        # "Only the spender is repointed" covers the whole transaction, not just
        # the two words a decoder returns -- `to` is the token the allowance is
        # granted on.
        _assert_only_the_spender_word_changed(before_approve, result.transactions[0], FRESH_SPENDER)
        # The original bundle is untouched (the rewrite happens on the copy).
        assert _decode_approve(bundle.transactions[0]["data"]) == (ORIGINAL_SPENDER.lower(), original_amount)

    def test_matching_spender_leaves_the_approval_byte_identical(self, caplog):
        """Negative control: an unchanged spender means no rewrite at all.

        Without this, an implementation that refuses or rewrites *every*
        approval would still pass the assertions above.
        """
        bundle = _make_lifi_bundle(deferred=True)
        original_calldata = bundle.transactions[0]["data"]
        mock = MagicMock(return_value=_fresh_tx_with_approval(ORIGINAL_SPENDER))

        with caplog.at_level(logging.INFO, logger=deferred_refresh.__name__):
            with _patch_refresher("lifi", mock):
                result = refresh_deferred_bundle(bundle, WALLET)

        assert result.transactions[0]["data"] == original_calldata
        assert "Updated approval spender" not in caplog.text

    def test_spender_with_leading_zero_byte_is_not_spuriously_rewritten(self, caplog):
        """Comparison is word-vs-word, not ``lstrip('0')``-vs-address.

        The pre-VIB-6228 comparison stripped every leading zero from the padded
        spender word, so ``0x00b3..`` never compared equal to itself and the
        approval was needlessly rebuilt — which, with the MAX substitution
        attached, escalated an unchanged spender into an unlimited allowance.

        The log assertion is the load-bearing one, and deliberately so. Once the
        amount is preserved, rebuilding an approval for the *same* spender is
        byte-idempotent, so calldata equality alone cannot tell the two
        comparisons apart (verified: reverting to the ``lstrip`` form leaves
        every calldata assertion in this file green). What survives is a log line
        claiming the route moved its spender when it did not — which is what an
        operator reads when auditing an unexpected approval.
        """
        leading_zero_spender = "0x00b3f1e2c4d5a6978899aabbccddeeff00112233"
        approve_tx = _make_approve_tx(spender=leading_zero_spender)
        bundle = _make_lifi_bundle(deferred=True)
        bundle.transactions[0] = approve_tx
        original_calldata = approve_tx["data"]
        mock = MagicMock(return_value=_fresh_tx_with_approval(leading_zero_spender))

        with caplog.at_level(logging.INFO, logger=deferred_refresh.__name__):
            with _patch_refresher("lifi", mock):
                result = refresh_deferred_bundle(bundle, WALLET)

        assert result.transactions[0]["data"] == original_calldata
        assert "Updated approval spender" not in caplog.text

    def test_a_real_spender_change_does_log_the_repoint(self, caplog):
        """Negative control for the two assertions above.

        They check the *absence* of a log line, so they would both pass against
        an implementation that never logs at all.
        """
        bundle = _make_lifi_bundle(deferred=True)
        mock = MagicMock(return_value=_fresh_tx_with_approval(FRESH_SPENDER))

        with caplog.at_level(logging.INFO, logger=deferred_refresh.__name__):
            with _patch_refresher("lifi", mock):
                refresh_deferred_bundle(bundle, WALLET)

        assert "Updated approval spender" in caplog.text
        assert ORIGINAL_SPENDER in caplog.text and FRESH_SPENDER in caplog.text

    def test_malformed_approval_calldata_refuses(self):
        """An approve payload whose amount cannot be read is a refusal.

        Rewriting it would mean inventing an amount — which is exactly what the
        MAX substitution did.
        """
        bundle = _make_lifi_bundle(deferred=True)
        bundle.transactions[0]["data"] = APPROVE_SELECTOR + "dead"  # truncated
        mock = MagicMock(return_value=_fresh_tx_with_approval(FRESH_SPENDER))

        with _patch_refresher("lifi", mock):
            with _bundle_must_not_change(bundle):
                with pytest.raises(DeferredRefreshError) as exc_info:
                    refresh_deferred_bundle(bundle, WALLET)

        assert "well-formed approve(address,uint256)" in str(exc_info.value)

    @pytest.mark.parametrize(
        ("label", "malformed_data", "expected"),
        [
            # Complete spender word, TRUNCATED amount.
            ("truncated amount", APPROVE_SELECTOR + FRESH_SPENDER[2:].rjust(64, "0") + "dead", "well-formed approve"),
            # Complete spender word, non-hex amount of the right length.
            ("non-hex amount", APPROVE_SELECTOR + FRESH_SPENDER[2:].rjust(64, "0") + "z" * 64, "not valid hex"),
        ],
    )
    def test_malformed_calldata_refuses_even_when_the_spender_is_unchanged(self, label, malformed_data, expected):
        """Payload validation must run BEFORE the same-spender fast path.

        Every other malformed-calldata case pairs a bad payload with a genuinely
        *different* fresh spender, and the unchanged-spender cases all use valid
        payloads — so nothing pinned the **ordering**. An implementation that
        compared the encoded spender first and returned early on a match would pass
        every one of them while accepting an ``approve`` payload whose amount cannot
        be read, in direct contradiction of the contract.

        Here the malformed payload's spender word *is* the fresh spender, so the
        fast path would fire. The refusal must win. Found by the Phase 0b spec
        critique, round 11; pinned as mutation M17.
        """
        bundle = _make_lifi_bundle(deferred=True)
        bundle.transactions[0]["data"] = malformed_data
        mock = MagicMock(return_value=_fresh_tx_with_approval(FRESH_SPENDER))

        with _patch_refresher("lifi", mock):
            with _bundle_must_not_change(bundle):
                with pytest.raises(DeferredRefreshError) as exc_info:
                    refresh_deferred_bundle(bundle, WALLET)

        assert expected in str(exc_info.value), f"{label}: got {exc_info.value}"

    def test_non_hex_approval_calldata_refuses(self):
        """Correct length, unparseable content — still cannot carry the amount over."""
        bundle = _make_lifi_bundle(deferred=True)
        bundle.transactions[0]["data"] = APPROVE_SELECTOR + "z" * 128
        mock = MagicMock(return_value=_fresh_tx_with_approval(FRESH_SPENDER))

        with _patch_refresher("lifi", mock):
            with _bundle_must_not_change(bundle):
                with pytest.raises(DeferredRefreshError) as exc_info:
                    refresh_deferred_bundle(bundle, WALLET)

        assert "not valid hex" in str(exc_info.value)

    def test_null_approval_calldata_is_left_untouched_not_rewritten(self):
        """``tx["data"] = None`` takes the leave-alone branch, and cannot crash.

        Two distinct properties, both asserted:

        1. **No crash.** ``dict.get(k, "")`` yields ``None`` — not ``""`` — when
           the key exists holding ``None``, so the default is no protection and the
           pre-fix code would raise ``AttributeError`` on ``.lower()``.
        2. **Left untouched, not rewritten.** ``None`` is not a *recognisable
           approve payload*, so it takes the same branch as
           ``test_non_approve_calldata_is_left_untouched``: there is no amount to
           preserve and nothing is being widened, so there is nothing to refuse.
           This is deliberately NOT the "amount cannot be read" refusal, which
           applies to payloads that DO carry the ``approve`` selector.

        Asserting only "does not raise" would leave (2) unpinned — an
        implementation that quietly rewrote a ``None`` payload into a fresh
        approval would satisfy a no-crash assertion. (Ambiguity raised by the
        Phase 0b spec critique, round 2.)
        """
        bundle = _make_lifi_bundle(deferred=True)
        bundle.transactions[0]["data"] = None
        mock = MagicMock(return_value=_fresh_tx_with_approval(FRESH_SPENDER))

        with _patch_refresher("lifi", mock):
            result = refresh_deferred_bundle(bundle, WALLET)

        assert result.transactions[0]["data"] is None, "a null payload must not be rewritten into an approval"
        assert result.transactions[0]["tx_type"] == "approve"
        # ...and the swap leg still got refreshed, so this is the leave-alone
        # branch rather than an early bail-out that skipped the whole refresh.
        assert result.transactions[1]["data"] == "0x66726573685f6c696669"

    def test_non_approve_calldata_is_left_untouched(self):
        """A tx labelled ``approve`` that is not an approve call is left alone.

        Deliberate asymmetry with the malformed-payload case above: there is no
        amount to preserve and nothing is being widened, so there is nothing to
        refuse.
        """
        bundle = _make_lifi_bundle(deferred=True)
        bundle.transactions[0]["data"] = "0xa9059cbb" + "0" * 128  # transfer(), not approve()
        original_calldata = bundle.transactions[0]["data"]
        mock = MagicMock(return_value=_fresh_tx_with_approval(FRESH_SPENDER))

        with _patch_refresher("lifi", mock):
            result = refresh_deferred_bundle(bundle, WALLET)

        assert result.transactions[0]["data"] == original_calldata
        # ...and the swap leg still got refreshed.
        assert result.transactions[1]["data"] == "0x66726573685f6c696669"

    @pytest.mark.parametrize(
        "bad_address",
        [
            # Malformed strings.
            "0xabc",
            "0x" + "1" * 39,
            "0x" + "1" * 41,
            "not-an-address",
            "0x" + "z" * 40,
            # Non-STRING JSON values. ``approval_address`` is lifted straight out
            # of a third-party route-API response, so its *type* is as untrusted
            # as its content. Before this was handled, each of these raised
            # ``AttributeError: 'int'/'list'/'dict' object has no attribute
            # 'lower'``, which escaped the pipeline as an opaque "Unexpected
            # error" — losing the diagnosis and the refusal's classification.
            # (Found by the Phase 0b spec critique, round 5; reproduced on all
            # three shapes before fixing.)
            12345,
            ["0x" + "22" * 20],
            {"address": "0x" + "22" * 20},
            True,
        ],
    )
    def test_non_address_approval_spender_refuses(self, bad_address):
        """A spender that is not a 20-byte address must not reach calldata.

        ``zfill`` on a truncated address yields a syntactically valid word
        naming a *different* contract — an approval to the wrong spender, which
        is indistinguishable from a correct one once mined. A non-string value
        must take the same refusal, not a different exception type.
        """
        bundle = _make_lifi_bundle(deferred=True)
        mock = MagicMock(return_value=_fresh_tx_with_approval(bad_address))

        with _patch_refresher("lifi", mock):
            with _bundle_must_not_change(bundle):
                with pytest.raises(DeferredRefreshError) as exc_info:
                    refresh_deferred_bundle(bundle, WALLET)

        assert "not a 20-byte address" in str(exc_info.value)

    def test_uppercase_approval_calldata_is_still_recognised_and_repointed(self):
        """Hex case is irrelevant to the EVM, so it must be irrelevant here.

        ``0x095EA7B3…`` (uppercase selector, uppercase words, uppercase ``0X``
        prefix) encodes exactly the same ``approve(address,uint256)`` bytes as the
        lowercase rendering. A case-sensitive ``startswith`` would classify it as
        "not a recognisable approve payload", take the leave-alone branch, and
        return a **successful** bundle in which the swap leg was repointed to a new
        router while the approval still names the old spender — the wrong-spender
        approval of VIB-229, produced silently on the success path.

        The pre-VIB-6228 code did not normalise case, so this is a real behaviour
        change that had no test. Found by the Phase 0b spec critique, round 7;
        pinned as mutation M13.
        """
        lower = _encode_approve(ORIGINAL_SPENDER, APPROVED_AMOUNT)
        upper = "0X" + lower[2:].upper()
        assert upper != lower and bytes.fromhex(upper[2:]) == bytes.fromhex(lower[2:]), "same bytes, different text"

        bundle = _make_lifi_bundle(deferred=True)
        bundle.transactions[0]["data"] = upper
        before_approve = copy.deepcopy(bundle.transactions[0])
        mock = MagicMock(return_value=_fresh_tx_with_approval(FRESH_SPENDER))

        with _patch_refresher("lifi", mock):
            result = refresh_deferred_bundle(bundle, WALLET)

        spender, amount = _decode_approve(result.transactions[0]["data"])
        assert spender == FRESH_SPENDER.lower(), "an uppercase payload must still be repointed"
        assert amount == APPROVED_AMOUNT, "the amount must survive the case normalisation"
        # Non-calldata fields untouched; the payload is byte-equivalent apart from
        # the spender word (the rebuild normalises the text to lowercase, which the
        # EVM cannot distinguish).
        for key in before_approve:
            if key != "data":
                assert result.transactions[0][key] == before_approve[key], key
        assert result.transactions[0]["data"][74:] == lower[74:], "amount word preserved"

    def test_same_address_in_checksummed_form_is_recognised_as_unchanged(self, caplog):
        """A mixed-case form of the SAME spender must not read as a change.

        The comparison is word-vs-word on lowercased hex, so the EIP-55
        checksummed rendering of an address already encoded in the approval is
        recognised as identical. A case-*sensitive* comparison would instead
        rebuild byte-identical calldata and log a repoint that never happened —
        which, by the same reasoning as
        ``test_spender_with_leading_zero_byte_is_not_spuriously_rewritten``, is
        the observable defect: an operator auditing an unexpected approval would
        see the route claiming it moved spender when it did not.

        Route APIs return checksummed addresses in practice, so this is the
        realistic shape of the bug, not a contrived one. Found by the Phase 0b
        spec critique, round 6; pinned as mutation M12.
        """
        # Same 20 bytes as ORIGINAL_SPENDER, EIP-55 mixed case.
        same_address_checksummed = "0x1111111254EEB25477B68fb85Ed929f73A960582"
        assert same_address_checksummed.lower() == ORIGINAL_SPENDER.lower(), "fixture must be the same address"

        bundle = _make_lifi_bundle(deferred=True)
        original_calldata = bundle.transactions[0]["data"]
        mock = MagicMock(return_value=_fresh_tx_with_approval(same_address_checksummed))

        with caplog.at_level(logging.INFO, logger=deferred_refresh.__name__):
            with _patch_refresher("lifi", mock):
                result = refresh_deferred_bundle(bundle, WALLET)

        assert result.transactions[0]["data"] == original_calldata
        assert "Updated approval spender" not in caplog.text

    def test_checksummed_fresh_spender_is_accepted(self):
        """Negative control for the validator: mixed-case EIP-55 input must work.

        A validator that only accepted lowercase would refuse every real LiFi /
        Enso response, turning a safety guard into an outage.
        """
        checksummed = "0x1111111254EEB25477B68fb85Ed929f73A960582"
        bundle = _make_lifi_bundle(deferred=True)
        bundle.transactions[0] = _make_approve_tx(spender=FRESH_SPENDER)
        mock = MagicMock(return_value=_fresh_tx_with_approval(checksummed))

        with _patch_refresher("lifi", mock):
            result = refresh_deferred_bundle(bundle, WALLET)

        spender, amount = _decode_approve(result.transactions[0]["data"])
        assert spender == checksummed.lower()
        assert amount == APPROVED_AMOUNT


class TestRefusalRetryability:
    """A refusal must not convert a transient route outage into a hard failure.

    ``DeferredRefreshError`` reaches the runner as ``ExecutionResult.error``, and
    the retry decision there is ``inner_runner._is_retryable`` — a substring
    blocklist over that message. These tests pin the cross-layer property rather
    than trusting the message wording to stay compatible.
    """

    def test_transient_route_failure_stays_retryable(self):
        from almanak.framework.runner.inner_runner import _is_retryable

        mock = MagicMock(side_effect=TimeoutError("read timeout"))
        bundle = _make_lifi_bundle(deferred=True)

        with _patch_refresher("lifi", mock):
            with _bundle_must_not_change(bundle):
                with pytest.raises(DeferredRefreshError) as exc_info:
                    refresh_deferred_bundle(bundle, WALLET)

        assert _is_retryable(str(exc_info.value)) is True
        assert exc_info.value.recoverable is True

    def test_is_retryable_actually_discriminates(self):
        """Negative control for the assertion above.

        ``_is_retryable`` defaults to True, so the previous test would also pass
        against a function that returned True unconditionally.
        """
        from almanak.framework.runner.inner_runner import _is_retryable

        assert _is_retryable("execution reverted") is False


class TestIsLocalRpc:
    """Tests for is_local_rpc() — the SIMULATION-VENDOR local-RPC heuristic.

    Since ALM-3184 no money-path guard consults this: it grants fork status to
    any host on port 8545-8550. The cases below pin the heuristic's shape for
    the one caller it is still correct for (skipping Tenderly/Alchemy against
    fork state), and document why it is unsafe elsewhere.
    """

    def test_localhost(self):
        assert is_local_rpc("http://localhost:8545") is True

    def test_localhost_with_path(self):
        assert is_local_rpc("http://localhost:8545/some/path") is True

    def test_127_0_0_1(self):
        assert is_local_rpc("http://127.0.0.1:8545") is True

    def test_0_0_0_0(self):
        assert is_local_rpc("http://0.0.0.0:8545") is True

    def test_alchemy_url(self):
        assert is_local_rpc("https://arb-mainnet.g.alchemy.com/v2/key") is False

    def test_infura_url(self):
        assert is_local_rpc("https://mainnet.infura.io/v3/key") is False

    def test_none(self):
        assert is_local_rpc(None) is False

    def test_empty(self):
        assert is_local_rpc("") is False
