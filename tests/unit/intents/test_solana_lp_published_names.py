"""Every Solana LP protocol name the catalogue publishes must actually compile.

VIB-6231. ``almanak info matrix --json`` publishes each connector's manifest
``name`` -- ``orca`` / ``meteora`` / ``raydium``. Solana LP dispatch used a
hand-maintained set holding only the connectors' *compiler* keys
(``orca_whirlpools`` / ``meteora_dlmm`` / ``raydium_clmm``), so every name in
the published catalogue was rejected at compile time:

    solana LP_OPEN protocol='orca' -> FAILED
        "Protocol 'orca' is not supported for LP_OPEN on Solana.
         Supported: meteora_dlmm, orca_whirlpools, raydium_clmm"

A documented name that fails to compile is the worst class of catalogue error:
the user follows the published catalogue and gets a rejection. The routing table
is now derived from the connector manifests, so the two cannot drift.

The load-bearing assertion is :meth:`TestPublishedNamesReachTheCompiler` --
the published name must get *past the protocol gate*. It is checked against the
already-working compiler-key spelling rather than against a fixed error string,
so the test cannot pass by both spellings being broken the same way.
"""

from __future__ import annotations

import pytest

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.framework.chain_family import _svm_dispatch
from almanak.framework.chain_family._svm_dispatch import (
    _SOLANA_DEFAULT_LP_PROTOCOL,
    _solana_lp_routing,
    resolve_solana_lp_protocol,
    solana_lp_spellings,
)
from almanak.framework.intents.compiler_models import CompilationStatus

# The three connectors that declare Solana LP intents, as (published name,
# compiler key). Derived-vs-declared parity is asserted separately in
# ``TestRoutingIsDerivedFromTheManifests``; this literal is the human-readable
# statement of what the catalogue promises.
PUBLISHED_TO_COMPILER_KEY = {
    "orca": "orca_whirlpools",
    "meteora": "meteora_dlmm",
    "raydium": "raydium_clmm",
}


def _compile_lp_open(chain: str, protocol: str):
    """Compile a Solana LP_OPEN intent through the real family dispatch.

    Imported lazily from the characterization suite so this module owns no
    duplicate compiler-construction scaffolding.
    """
    from tests.unit.intents.test_compiler_swap_lp_characterization import (
        _make_compiler,
        _make_lp_intent,
    )

    return _make_compiler(chain=chain).compile(_make_lp_intent(protocol=protocol))


def _rejected_by_protocol_gate(result, intent_label: str = "LP_OPEN") -> bool:
    """True when the protocol gate refused the name (vs. the compiler running)."""
    return result.status is CompilationStatus.FAILED and f"is not supported for {intent_label} on Solana" in (
        result.error or ""
    )


class TestPublishedNamesReachTheCompiler:
    """The motivating defect: the published name was refused before compiling."""

    @pytest.mark.parametrize(("published", "compiler_key"), sorted(PUBLISHED_TO_COMPILER_KEY.items()))
    def test_published_name_is_not_rejected_by_the_protocol_gate(self, published: str, compiler_key: str) -> None:
        result = _compile_lp_open("solana", published)
        assert not _rejected_by_protocol_gate(result), (
            f"protocol={published!r} is published by `almanak info matrix` but Solana LP "
            f"dispatch refused it: {result.error!r}"
        )

    @pytest.mark.parametrize(("published", "compiler_key"), sorted(PUBLISHED_TO_COMPILER_KEY.items()))
    def test_published_name_and_compiler_key_land_identically(self, published: str, compiler_key: str) -> None:
        """Both spellings must be observationally identical.

        Comparing the two outcomes -- rather than asserting a fixed error --
        means this cannot pass by both spellings being rejected the same way.
        """
        via_published = _compile_lp_open("solana", published)
        via_compiler_key = _compile_lp_open("solana", compiler_key)
        assert via_published.status is via_compiler_key.status
        assert via_published.error == via_compiler_key.error
        # Guard the comparison itself: if the compiler-key spelling ever starts
        # being gate-rejected, "identical" would become vacuously true.
        assert not _rejected_by_protocol_gate(via_compiler_key), (
            f"the {compiler_key!r} spelling is now gate-rejected too, so the parity assertion above proves nothing"
        )

    def test_a_genuinely_unsupported_protocol_is_still_rejected(self) -> None:
        """The gate still refuses a non-Solana LP protocol -- it did not go open."""
        result = _compile_lp_open("solana", "uniswap_v3")
        assert _rejected_by_protocol_gate(result)

    def test_rejection_message_advertises_the_published_names(self) -> None:
        """The "Supported:" hint must name what the catalogue shows the user.

        Previously it listed the compiler keys, i.e. it pointed users at three
        spellings the catalogue never mentions.
        """
        result = _compile_lp_open("solana", "uniswap_v3")
        error = result.error or ""
        for published in PUBLISHED_TO_COMPILER_KEY:
            assert published in error, f"{published!r} missing from the supported-protocol hint: {error!r}"


class TestSolanaOnlyProtocolOnAnEvmChain:
    """Both spellings must produce the canonical cross-chain error."""

    # Parametrized over the explicit literal, NOT over ``solana_lp_spellings()``:
    # parametrizing over the live derived set means a regression that *shrinks*
    # the set silently shrinks the test matrix instead of failing it.
    @pytest.mark.parametrize(
        "spelling",
        sorted(PUBLISHED_TO_COMPILER_KEY) + sorted(PUBLISHED_TO_COMPILER_KEY.values()),
    )
    def test_canonical_only_supported_on_solana_error(self, spelling: str) -> None:
        result = _compile_lp_open("ethereum", spelling)
        assert result.status is CompilationStatus.FAILED
        assert "only supported on Solana" in (result.error or ""), (
            f"protocol={spelling!r} on ethereum should hit the connector's own chain check, got: {result.error!r}"
        )

    def test_an_evm_protocol_on_an_evm_chain_stays_on_the_evm_path(self) -> None:
        """Negative control for the widened cross-chain gate.

        Widening the set of spellings that route into SVM must not capture an
        ordinary EVM LP intent.

        Asserts the *routing* property, not end-to-end compilation success:
        without a live gateway/RPC this intent legitimately fails later at
        Uniswap V3 pool validation, so asserting SUCCESS would make the test
        pass or fail on whether the runner happens to have RPC credentials
        rather than on the behaviour under test.
        """
        result = _compile_lp_open("ethereum", "uniswap_v3")
        assert "only supported on Solana" not in (result.error or ""), (
            f"an EVM LP intent was captured by the Solana cross-chain gate: {result.error!r}"
        )
        assert not _rejected_by_protocol_gate(result), (
            f"an EVM LP intent hit the Solana protocol gate: {result.error!r}"
        )


class TestRoutingIsDerivedFromTheManifests:
    """The table is derived, not a second copy that can drift."""

    def test_derived_set_matches_the_declaring_connectors(self) -> None:
        expected: dict[str, str] = {}
        for connector in CONNECTOR_REGISTRY.all():
            if "solana" not in (connector.strategy_chains or ()):
                continue
            if not {"LP_OPEN", "LP_CLOSE"}.intersection(connector.strategy_intents or ()):
                continue
            (compiler_key,) = tuple(connector.compiler_keys)
            for spelling in (connector.name, compiler_key, *connector.aliases):
                expected[spelling.lower()] = compiler_key
        assert dict(_solana_lp_routing().compiler_key_by_spelling) == expected

    def test_published_names_are_the_manifest_names(self) -> None:
        assert set(_solana_lp_routing().published_names) == set(PUBLISHED_TO_COMPILER_KEY)

    def test_every_published_name_resolves_to_a_registered_compiler(self) -> None:
        from almanak.connectors._strategy_base.compiler_registry import get_compiler

        for published, compiler_key in PUBLISHED_TO_COMPILER_KEY.items():
            resolved = resolve_solana_lp_protocol(published)
            assert resolved == compiler_key
            assert get_compiler(resolved) is not None

    def test_default_lp_protocol_is_routable(self) -> None:
        """A protocol-less LP intent falls back to this key -- it must resolve."""
        assert resolve_solana_lp_protocol(_SOLANA_DEFAULT_LP_PROTOCOL) is not None

    @pytest.mark.parametrize("spelling", ["ORCA", "Orca-Whirlpools", "MeTeOrA"])
    def test_resolution_is_case_and_hyphen_insensitive(self, spelling: str) -> None:
        from almanak.connectors._strategy_base.protocol_aliases import normalize_protocol

        assert resolve_solana_lp_protocol(normalize_protocol("solana", spelling)) is not None

    def test_unknown_protocol_resolves_to_none(self) -> None:
        assert resolve_solana_lp_protocol("definitely_not_a_protocol") is None

    def test_spellings_set_carries_both_families(self) -> None:
        """``solana_lp_spellings()`` backs ``SvmFamily``'s cross-chain gate.

        If it carried only one spelling family, a Solana-only protocol declared
        on an EVM chain under the other spelling would fall through to the EVM
        path and produce a misleading error instead of "only supported on
        Solana".
        """
        spellings = solana_lp_spellings()
        missing = (set(PUBLISHED_TO_COMPILER_KEY) | set(PUBLISHED_TO_COMPILER_KEY.values())) - spellings
        assert not missing, f"cross-chain gate would miss these spellings: {sorted(missing)}"


class TestDerivationFailsLoudOnAnAmbiguousManifest:
    """Negative control: the derivation must not silently pick a compiler key.

    A connector declaring Solana LP intents with two compiler keys has no
    single answer for "which one does my published name route to". Guessing is
    how the original hand-maintained table went wrong, so the derivation raises.
    """

    def test_two_compiler_keys_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class _AmbiguousConnector:
            name = "twoheaded"
            aliases = ()
            strategy_chains = ("solana",)
            strategy_intents = ("LP_OPEN",)
            compiler_keys = frozenset({"twoheaded_a", "twoheaded_b"})

        monkeypatch.setattr(
            _svm_dispatch.CONNECTOR_REGISTRY,
            "all",
            lambda: (_AmbiguousConnector(),),
        )
        _svm_dispatch._solana_lp_routing.cache_clear()
        try:
            with pytest.raises(RuntimeError, match="cannot decide which one"):
                _svm_dispatch._solana_lp_routing()
        finally:
            _svm_dispatch._solana_lp_routing.cache_clear()

    def test_a_spelling_claimed_by_two_connectors_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class _Left:
            name = "left"
            aliases = ("shared",)
            strategy_chains = ("solana",)
            strategy_intents = ("LP_OPEN",)
            compiler_keys = frozenset({"left_clmm"})

        class _Right:
            name = "right"
            aliases = ("shared",)
            strategy_chains = ("solana",)
            strategy_intents = ("LP_OPEN",)
            compiler_keys = frozenset({"right_clmm"})

        monkeypatch.setattr(_svm_dispatch.CONNECTOR_REGISTRY, "all", lambda: (_Left(), _Right()))
        _svm_dispatch._solana_lp_routing.cache_clear()
        try:
            with pytest.raises(RuntimeError, match="claimed by both"):
                _svm_dispatch._solana_lp_routing()
        finally:
            _svm_dispatch._solana_lp_routing.cache_clear()

    def test_cache_is_restored_for_subsequent_tests(self) -> None:
        """Sanity: the monkeypatched derivations above did not leak."""
        assert set(_solana_lp_routing().published_names) == set(PUBLISHED_TO_COMPILER_KEY)
