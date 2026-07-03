"""Regression tests for the Hyperliquid (HyperEVM CoreWriter) Safe manifest.

Hyperliquid cannot use compilation-based permission discovery: both PERP_OPEN
and PERP_CLOSE require live HyperCore reads (oracle precompile ``0x0807``,
position precompile ``0x0800``) to anchor a fail-closed order, so the compiler
hard-fails offline and emits no transaction to discover a selector from. The
connector therefore declares the single fixed CoreWriter permission via
``static_permissions`` (``permission_hints.py``).

These tests pin that the Safe manifest authorises ``CoreWriter.sendRawAction``
for perp intents, scopes it to perp intent types (least privilege), and — the
important one — that the authorised selector is the REAL keccak of the signature,
not a hand-typed constant that could silently diverge from the calldata the
compiler emits (the class of bug where ``cast`` re-encodes and masks a wrong
selector). Without this manifest a Safe-wallet hyperliquid strategy reverts at
``execTransactionWithRole`` with a Zodiac Roles authorization failure.
"""

from __future__ import annotations

from eth_utils import function_signature_to_4byte_selector

from almanak.connectors.hyperliquid.addresses import CORE_WRITER_ADDRESS
from almanak.framework.permissions.generator import generate_manifest

_CORE_WRITER = CORE_WRITER_ADDRESS.lower()
# Derived from the signature here — NOT copied from the connector — so the test
# fails if the connector ever authorises a selector that isn't the real
# sendRawAction(bytes) 4-byte selector.
_SEND_RAW_ACTION_SEL = "0x" + function_signature_to_4byte_selector("sendRawAction(bytes)").hex()


def _core_writer_selectors(intent_types: list[str], chain: str = "hyperevm") -> set[str]:
    manifest = generate_manifest(
        strategy_name="hyperliquid-manifest-regression",
        chain=chain,
        supported_protocols=["hyperliquid"],
        intent_types=intent_types,
    )
    return {
        sel.selector.lower()
        for perm in manifest.permissions
        if perm.target.lower() == _CORE_WRITER
        for sel in perm.function_selectors
    }


class TestHyperliquidManifest:
    def test_perp_manifest_authorises_corewriter_send_raw_action(self) -> None:
        """A perp strategy must be authorised to call CoreWriter.sendRawAction —
        the sole execution path for open AND close on HyperEVM.
        """
        selectors = _core_writer_selectors(["PERP_OPEN", "PERP_CLOSE"])
        assert _SEND_RAW_ACTION_SEL in selectors, (
            "Safe manifest missing CoreWriter.sendRawAction — a Safe-wallet "
            "hyperliquid strategy would revert at execTransactionWithRole."
        )

    def test_authorised_selector_matches_real_signature(self) -> None:
        """The manifest selector must equal keccak('sendRawAction(bytes)')[:4].

        Pins against a hand-typed selector drifting from the encoder in sdk.py
        (SELECTOR_SEND_RAW_ACTION). If they diverge, the Safe authorises one
        selector while the compiler emits calldata with another → every order
        is rejected on-chain despite a green manifest.
        """
        selectors = _core_writer_selectors(["PERP_OPEN", "PERP_CLOSE"])
        assert selectors == {_SEND_RAW_ACTION_SEL}, (
            f"CoreWriter must authorise exactly the real sendRawAction selector; "
            f"got {selectors}, expected {{{_SEND_RAW_ACTION_SEL}}}"
        )

    def test_open_only_and_close_only_each_authorise_corewriter(self) -> None:
        """CoreWriter is the execution path for BOTH directions — each alone
        must carry the permission (open-only or close-only strategies).
        """
        assert _SEND_RAW_ACTION_SEL in _core_writer_selectors(["PERP_OPEN"])
        assert _SEND_RAW_ACTION_SEL in _core_writer_selectors(["PERP_CLOSE"])

    def test_non_perp_manifest_excludes_corewriter(self) -> None:
        """Least privilege: a manifest requested for a non-perp intent type must
        NOT authorise CoreWriter (the static entry is scoped via intent_types).
        """
        assert _core_writer_selectors(["SWAP"]) == set()

    def test_perp_withdraw_authorises_corewriter(self) -> None:
        """VIB-5617: PERP_WITHDRAW (a CoreWriter spotSend HyperCore→L1 bridge)
        reuses the SAME (CoreWriter, sendRawAction) target/selector — no new
        Zodiac target — so it must be authorised by the same static entry.
        Without this a Safe sweeping parked HyperCore funds reverts at
        execTransactionWithRole.
        """
        assert _SEND_RAW_ACTION_SEL in _core_writer_selectors(["PERP_WITHDRAW"])
        # Withdraw-only strategy still carries the permission.
        assert _core_writer_selectors(["PERP_WITHDRAW"]) == {_SEND_RAW_ACTION_SEL}

    def test_corewriter_send_allowed_is_false(self) -> None:
        """CoreWriter calls carry value == 0 (margin is on HyperCore, gas is
        native HYPE) — the Safe must not be granted native-value send on it.
        """
        manifest = generate_manifest(
            strategy_name="hyperliquid-send-allowed-regression",
            chain="hyperevm",
            supported_protocols=["hyperliquid"],
            intent_types=["PERP_OPEN", "PERP_CLOSE"],
        )
        cw = [p for p in manifest.permissions if p.target.lower() == _CORE_WRITER]
        assert cw, "CoreWriter permission missing"
        assert all(not p.send_allowed for p in cw), "CoreWriter must not allow native-value send (value==0)"
