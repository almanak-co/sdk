"""Compiler tests for VaultDepositIntent / VaultRedeemIntent chain support.

VIB-3827 — without an explicit chain pre-check, the compiler used to fall
through to the adapter constructor which raised a generic ``ValueError``
("Invalid chain: sonic ..."). The state-machine categorizer never matched
that string against its ``permanent_keywords`` list, so the runner retried
the intent indefinitely on a deterministic mis-configuration.

These tests verify:

* Unsupported vault x chain combinations (e.g. metamorpho on Sonic, blocked on
  VIB-2281) FAIL FAST with a clear typed error whose message is classifiable
  as ``COMPILATION_PERMANENT`` by the state machine.
* Supported combinations are NOT short-circuited by the new pre-check (the
  guard must be a strict superset rejection, not a regression on the happy
  path) — the request reaches the adapter as before.
* The same fail-fast contract applies to the redeem lane (where stale state
  on a removed chain would otherwise crash-loop).

The compiler does on-chain reads via the gateway when it builds the deposit/
redeem TX, so the "happy path" assertion goes only as far as proving the
chain check itself did not reject — full deposit-TX construction is covered
elsewhere by the connector-level integration tests.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.intents import VaultDepositIntent, VaultRedeemIntent
from almanak.framework.intents.compiler import (
    CompilationStatus,
    IntentCompiler,
    IntentCompilerConfig,
)
from almanak.framework.intents.state_machine import IntentStateMachine

VAULT_ADDR = "0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB"
TEST_WALLET = "0x1234567890123456789012345678901234567890"


def _make_compiler(chain: str) -> IntentCompiler:
    """Build an IntentCompiler with a connected mock gateway client.

    The mock satisfies the ``self._gateway_client is None or not
    .is_connected`` precondition gate so the unsupported-chain check runs.
    No adapter call is exercised on the unsupported path (the chain check
    short-circuits first); on the supported path we only verify the check
    did not reject — adapter construction is covered separately.
    """
    gateway = MagicMock()
    gateway.is_connected = True
    return IntentCompiler(
        chain=chain,
        wallet_address=TEST_WALLET,
        config=IntentCompilerConfig(allow_placeholder_prices=True),
        gateway_client=gateway,
    )


# =============================================================================
# Unsupported chain — VAULT_DEPOSIT
# =============================================================================


class TestVaultDepositUnsupportedChain:
    def test_metamorpho_sonic_fails_fast(self) -> None:
        """metamorpho on Sonic must FAIL with a permanent-classifiable error."""
        compiler = _make_compiler("sonic")
        intent = VaultDepositIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            amount=Decimal("100"),
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED, (
            f"Expected FAILED on unsupported vault x chain, got {result.status}"
        )
        # Error message must include the unsupported chain and the protocol.
        assert "metamorpho" in (result.error or "").lower()
        assert "sonic" in (result.error or "").lower()
        # Generic operator hint — no hardcoded ticket id (which goes stale
        # the moment the dependency closes).
        assert "vault registry" in (result.error or "").lower()

    def test_metamorpho_sonic_classifies_as_permanent(self) -> None:
        """The state-machine categorizer must mark this error non-retryable.

        Without the magic "not supported" substring, the runner treats the
        failure as transient and retries forever (29-Apr Sonic incident).
        """
        compiler = _make_compiler("sonic")
        intent = VaultDepositIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            amount=Decimal("100"),
        )
        result = compiler.compile(intent)

        sm = IntentStateMachine(intent, compiler)
        # _categorize_error is the same surface the runner consults to
        # decide retry vs. fail-fast in handle_failure().
        category = sm._categorize_error(result.error or "")
        assert category == "COMPILATION_PERMANENT", (
            f"Sonic vault deposit must classify as COMPILATION_PERMANENT, got {category!r}. "
            f"Error string was: {result.error!r}"
        )

    def test_unknown_protocol_fails_fast(self) -> None:
        """Unknown protocol on any chain still fails (existing registry behaviour).

        VaultDepositIntent's pydantic validator rejects unknown protocols at
        construction time, so this is a regression guard — the construction
        path must continue to fail before the compile path.
        """
        with pytest.raises(ValueError, match="Invalid vault protocol"):
            VaultDepositIntent(
                protocol="silo_v2_native",
                vault_address=VAULT_ADDR,
                amount=Decimal("100"),
            )

    def test_unknown_protocol_via_model_construct_classifies_as_permanent(self) -> None:
        """Regression guard for the Codex P2 finding on PR #1998.

        ``model_construct`` bypasses the pydantic validator, which is the path
        taken when an intent is rebuilt from serialized state during a
        state-machine restore. If the registered adapter set has shrunk since
        the intent was serialized (adapter removed, plugin reload, etc.), the
        compiler hits ``is_vault_chain_supported`` → False, then would have
        called ``supported_vault_chains`` which raises ``KeyError`` on an
        unknown protocol. The broad ``except`` would strip the message to the
        bare protocol name, missing every ``permanent_keywords`` entry and
        re-classifying a deterministic mis-config as transient — the very
        failure VIB-3827 set out to eliminate.

        The compiler now splits unknown-protocol from unknown-chain so the
        unknown-protocol message keeps the classifiable phrasing.
        """
        compiler = _make_compiler("ethereum")
        intent = VaultDepositIntent.model_construct(
            protocol="ghost_protocol",
            vault_address=VAULT_ADDR,
            amount=Decimal("100"),
        )
        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "ghost_protocol" in (result.error or "")
        assert "not supported" in (result.error or "").lower()

        sm = IntentStateMachine(intent, compiler)
        category = sm._categorize_error(result.error or "")
        assert category == "COMPILATION_PERMANENT", (
            f"Unknown vault protocol must classify as COMPILATION_PERMANENT, got {category!r}. "
            f"Error string was: {result.error!r}"
        )


# =============================================================================
# Supported chain — VAULT_DEPOSIT (regression guard)
# =============================================================================


class TestVaultDepositSupportedChainNotShortCircuited:
    def test_metamorpho_ethereum_passes_chain_check(self) -> None:
        """Ethereum must NOT be rejected by the chain pre-check.

        Compilation itself will fail later (the mock gateway can't service the
        adapter's eth_call), but the failure mode must NOT be the new
        "not supported on chain 'ethereum'" message — that would mean the new
        guard regressed the happy path. We assert the negative.
        """
        compiler = _make_compiler("ethereum")
        intent = VaultDepositIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            amount=Decimal("100"),
        )

        result = compiler.compile(intent)

        # The chain check itself must NOT have rejected ethereum.
        chain_check_msg = "is not supported on chain 'ethereum'"
        assert chain_check_msg not in (result.error or ""), (
            f"Chain pre-check incorrectly rejected ethereum: {result.error!r}"
        )

    def test_metamorpho_base_passes_chain_check(self) -> None:
        compiler = _make_compiler("base")
        intent = VaultDepositIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            amount=Decimal("100"),
        )

        result = compiler.compile(intent)

        chain_check_msg = "is not supported on chain 'base'"
        assert chain_check_msg not in (result.error or ""), (
            f"Chain pre-check incorrectly rejected base: {result.error!r}"
        )


# =============================================================================
# Unsupported chain — VAULT_REDEEM (lane symmetry)
# =============================================================================


class TestVaultRedeemUnsupportedChain:
    def test_metamorpho_sonic_redeem_fails_fast(self) -> None:
        """Redeem lane must mirror the deposit lane's fail-fast contract."""
        compiler = _make_compiler("sonic")
        intent = VaultRedeemIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            shares=Decimal("10"),
        )

        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "metamorpho" in (result.error or "").lower()
        assert "sonic" in (result.error or "").lower()

    def test_metamorpho_sonic_redeem_classifies_as_permanent(self) -> None:
        compiler = _make_compiler("sonic")
        intent = VaultRedeemIntent(
            protocol="metamorpho",
            vault_address=VAULT_ADDR,
            shares="all",
        )
        result = compiler.compile(intent)

        sm = IntentStateMachine(intent, compiler)
        category = sm._categorize_error(result.error or "")
        assert category == "COMPILATION_PERMANENT", (
            f"Sonic vault redeem must classify as COMPILATION_PERMANENT, got {category!r}. "
            f"Error string was: {result.error!r}"
        )

    def test_unknown_protocol_via_model_construct_classifies_as_permanent_redeem(self) -> None:
        """Redeem-lane mirror of the Codex P2 unknown-protocol regression guard.

        Same scenario as the deposit-lane test: stale state restored via
        ``model_construct`` could carry a no-longer-registered protocol. The
        compiler must reject it with a permanent-classifiable message.
        """
        compiler = _make_compiler("ethereum")
        intent = VaultRedeemIntent.model_construct(
            protocol="ghost_protocol",
            vault_address=VAULT_ADDR,
            shares=Decimal("1"),
        )
        result = compiler.compile(intent)

        assert result.status == CompilationStatus.FAILED
        assert "ghost_protocol" in (result.error or "")
        assert "not supported" in (result.error or "").lower()

        sm = IntentStateMachine(intent, compiler)
        category = sm._categorize_error(result.error or "")
        assert category == "COMPILATION_PERMANENT", (
            f"Unknown vault protocol on redeem must classify as COMPILATION_PERMANENT, got {category!r}. "
            f"Error string was: {result.error!r}"
        )


# =============================================================================
# Registry helpers
# =============================================================================


class TestVaultRegistryChainHelpers:
    def test_is_vault_chain_supported_metamorpho_ethereum(self) -> None:
        from almanak.framework.connectors.vaults import is_vault_chain_supported

        assert is_vault_chain_supported("metamorpho", "ethereum") is True
        assert is_vault_chain_supported("metamorpho", "base") is True

    def test_is_vault_chain_supported_metamorpho_sonic_false(self) -> None:
        from almanak.framework.connectors.vaults import is_vault_chain_supported

        assert is_vault_chain_supported("metamorpho", "sonic") is False

    def test_is_vault_chain_supported_unknown_protocol_false(self) -> None:
        from almanak.framework.connectors.vaults import is_vault_chain_supported

        assert is_vault_chain_supported("nonexistent", "ethereum") is False

    def test_is_vault_chain_supported_case_insensitive(self) -> None:
        from almanak.framework.connectors.vaults import is_vault_chain_supported

        assert is_vault_chain_supported("MetaMorpho", "Ethereum") is True
        assert is_vault_chain_supported("METAMORPHO", "SONIC") is False

    def test_supported_vault_chains_metamorpho(self) -> None:
        from almanak.framework.connectors.vaults import supported_vault_chains

        chains = supported_vault_chains("metamorpho")
        assert chains is not None
        assert "ethereum" in chains
        assert "base" in chains
        assert "sonic" not in chains

    def test_supported_vault_chains_unknown_protocol_raises(self) -> None:
        from almanak.framework.connectors.vaults import supported_vault_chains

        with pytest.raises(KeyError):
            supported_vault_chains("definitely_not_a_protocol")

    def test_register_with_supported_chains_then_query(self) -> None:
        """Adapters opting into the chain set must be queryable end-to-end."""
        from almanak.framework.connectors.vaults import (
            _REGISTRY,
            is_vault_chain_supported,
            register_vault_adapter,
            supported_vault_chains,
        )

        def _stub_factory(**kwargs: object) -> object:
            return object()

        register_vault_adapter(
            "test_chained_proto",
            _stub_factory,
            supported_chains=["arbitrum", "Optimism"],
        )
        try:
            chains = supported_vault_chains("test_chained_proto")
            assert chains == frozenset({"arbitrum", "optimism"})
            assert is_vault_chain_supported("test_chained_proto", "arbitrum") is True
            assert is_vault_chain_supported("test_chained_proto", "optimism") is True
            assert is_vault_chain_supported("test_chained_proto", "ethereum") is False
        finally:
            _REGISTRY.pop("test_chained_proto", None)

    def test_register_without_supported_chains_legacy_behaviour(self) -> None:
        """Adapters that don't declare a chain set keep the previous "opaque" semantics.

        ``is_vault_chain_supported`` returns True (we cannot statically prove
        non-support); ``supported_vault_chains`` returns ``None``. This keeps
        third-party adapters that haven't been migrated yet working.
        """
        from almanak.framework.connectors.vaults import (
            _REGISTRY,
            is_vault_chain_supported,
            register_vault_adapter,
            supported_vault_chains,
        )

        def _stub_factory(**kwargs: object) -> object:
            return object()

        register_vault_adapter("test_legacy_proto", _stub_factory)
        try:
            assert supported_vault_chains("test_legacy_proto") is None
            assert is_vault_chain_supported("test_legacy_proto", "ethereum") is True
            assert is_vault_chain_supported("test_legacy_proto", "sonic") is True
        finally:
            _REGISTRY.pop("test_legacy_proto", None)
