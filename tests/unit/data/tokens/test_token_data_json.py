"""VIB-2952 regression guards: the JSON-backed token registry stays sane.

These tests catch three classes of data-level bugs that would otherwise only
show up when a strategy runs:

* JSON schema drift (missing ``var_name``, invalid decimals, etc.).
* Python import surface regressions (a legacy ``from ...defaults import USDC``
  that stops working silently).
* Duplicate addresses on the same chain — which would let the resolver
  return different metadata depending on key choice.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

DATA_DIR = Path(__file__).resolve().parents[4] / "almanak" / "framework" / "data" / "tokens" / "data"

PY_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
EVM_ADDR_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
SOLANA_ADDR_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


@pytest.fixture(scope="module")
def tokens_blob() -> dict:
    return json.loads((DATA_DIR / "tokens.json").read_text())


@pytest.fixture(scope="module")
def chains_blob() -> dict:
    return json.loads((DATA_DIR / "chains.json").read_text())


@pytest.fixture(scope="module")
def aliases_blob() -> dict:
    return json.loads((DATA_DIR / "symbol_aliases.json").read_text())


# Some Solana-chain native entries carry the EVM sentinel because the
# framework uses that sentinel across chains to mean "native gas token".
# Accept either format on Solana to match the legacy defaults.py behavior.
EVM_NATIVE_SENTINEL = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"


def _is_valid_address(addr: str, chain: str) -> bool:
    if chain == "solana":
        if bool(SOLANA_ADDR_RE.match(addr)):
            return True
        return addr == EVM_NATIVE_SENTINEL
    return bool(EVM_ADDR_RE.match(addr))


class TestTokensJsonSchema:
    def test_tokens_json_has_meta_and_tokens(self, tokens_blob: dict) -> None:
        assert "tokens" in tokens_blob
        assert isinstance(tokens_blob["tokens"], list)
        assert tokens_blob["tokens"], "tokens.json must not be empty"

    def test_every_record_has_required_fields(self, tokens_blob: dict) -> None:
        required = {"symbol", "name", "decimals", "addresses", "var_name"}
        for rec in tokens_blob["tokens"]:
            missing = required - rec.keys()
            assert not missing, f"{rec.get('var_name')}: missing fields {missing}"

    def test_decimals_in_range(self, tokens_blob: dict) -> None:
        for rec in tokens_blob["tokens"]:
            dec = rec["decimals"]
            assert isinstance(dec, int), f"{rec['var_name']}: decimals must be int"
            assert 0 <= dec <= 77, f"{rec['var_name']}: decimals {dec} out of range"

    def test_var_names_are_python_identifiers_and_unique(self, tokens_blob: dict) -> None:
        seen: set[str] = set()
        for rec in tokens_blob["tokens"]:
            var = rec["var_name"]
            assert PY_IDENT_RE.match(var), f"var_name {var!r} not a valid Python identifier"
            assert var not in seen, f"duplicate var_name: {var}"
            seen.add(var)

    def test_addresses_are_valid_format(self, tokens_blob: dict) -> None:
        for rec in tokens_blob["tokens"]:
            for chain, addr in rec["addresses"].items():
                assert _is_valid_address(addr, chain), (
                    f"{rec['var_name']} on {chain}: invalid address format {addr!r}"
                )

    def test_no_unhandled_symbol_collisions_per_chain(self, tokens_blob: dict) -> None:
        """Multiple records sharing ``(chain, symbol)`` are allowed in the
        JSON file (the fetcher appends chain-suffixed ``var_name``s for
        long-tail tokens), but the resolver uses first-write-wins on the
        symbol index, so the FIRST entry must be the hand-curated one.

        This test enforces that invariant: for every ``(chain, symbol)``
        collision, the lowest-indexed record in the JSON must be an
        ``in_default_set=True`` token that existed in the original
        ``defaults.py`` (i.e. pre-VIB-2951). If a future fetch run ever
        changes ordering so a CoinGecko-sourced record shadows a
        hand-curated one, this test fails and callers don't silently
        see the wrong contract.
        """
        from collections import defaultdict

        # Only ``in_default_set=True`` records make it into
        # ``DEFAULT_TOKENS`` and therefore into the resolver's static
        # symbol index. Records excluded from the default set (e.g.
        # ``USDC_SOL``, which aliases an existing USDC address) are
        # importable for legacy reasons but never participate in
        # symbol-index collisions, so they'd cause both false-fails and
        # miss real regressions if we ordered collisions across them.
        by_key: dict[tuple[str, str], list[tuple[int, dict]]] = defaultdict(list)
        for idx, rec in enumerate(tokens_blob["tokens"]):
            if not rec.get("in_default_set", True):
                continue
            for chain in rec.get("addresses", {}):
                key = (chain.lower(), rec["symbol"].upper())
                by_key[key].append((idx, rec))

        def _is_handcurated(rec: dict) -> bool:
            return not rec.get("source", "").startswith("coingecko")

        collisions = {k: v for k, v in by_key.items() if len(v) > 1}
        for (chain, sym), entries in collisions.items():
            _first_idx, first_rec = entries[0]
            loser_recs = [r for _, r in entries[1:]]
            # Only a problem if a HAND-CURATED record sits behind a
            # fetcher-sourced one. Two fetcher records colliding with
            # each other is non-ideal but not a correctness regression
            # against the pre-VIB-2951 baseline.
            if not _is_handcurated(first_rec) and any(_is_handcurated(r) for r in loser_recs):
                losers = [r["var_name"] for r in loser_recs]
                pytest.fail(
                    f"collision ({chain}, {sym}): resolver would keep "
                    f"{first_rec['var_name']!r} (source={first_rec.get('source')!r}) "
                    f"over hand-curated entries {losers!r}. Hand-curated records "
                    "must appear first in tokens.json so first-write-wins picks them."
                )

    def test_no_duplicate_addresses_per_chain(self, tokens_blob: dict) -> None:
        """A (chain, address) pair maps to exactly one token record.

        Known exception: ``WETH_E_AVALANCHE`` shares an address with
        ``WETH`` on Avalanche because WETH.e *is* the canonical wrapped ETH
        on that chain; both records are kept for readable imports but point
        to the same contract. Anything else must be unique.
        """
        known_overlaps: set[tuple[str, str]] = {
            # WETH / WETH_E_AVALANCHE (bridged WETH is the canonical WETH on Avax)
            ("avalanche", "0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab"),
            # USDT / USDT0_XLAYER (the native X-Layer USDT is the LayerZero USDT0)
            ("xlayer", "0x779ded0c9e1022225f8e0630b35a9b54be713736"),
            # USDC / USDC_SOL and USDT / USDT_SOL are kept as importable
            # aliases but excluded from DEFAULT_TOKENS (see in_default_set flag).
            ("solana", "epjfwdd5aufqssqem2qn1xzybapc8g4weggkzwytdt1v"),
            ("solana", "es9vmfrzacermjfrf4h2fyd4kconky11mcce8benwnyb"),
            # MATIC / POL_POLYGON: POL is the Sep-2024 rename of MATIC on
            # Polygon (1:1). Both symbols are kept so users can query by either
            # name, and both point at the EVM native-sentinel address because
            # they represent the same native gas token (VIB-3137).
            ("polygon", "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
        }

        seen: dict[tuple[str, str], str] = {}
        for rec in tokens_blob["tokens"]:
            for chain, addr in rec["addresses"].items():
                chain_l = chain.lower()
                # Solana mints are case-sensitive base58; every other chain
                # is EVM hex (case-insensitive). Normalize the same way
                # ``resolver._normalize_address_for_chain`` does so this
                # test matches what the resolver actually indexes on.
                norm_addr = addr if chain_l == "solana" else addr.lower()
                key = (chain_l, norm_addr)
                # Known-overlap allow-list stores lowercased Solana entries
                # too, so lowercase ours only for that check.
                if (chain_l, norm_addr.lower()) in known_overlaps:
                    continue
                if key in seen:
                    pytest.fail(
                        f"duplicate ({chain}, {addr}): {seen[key]!r} and {rec['var_name']!r}"
                    )
                seen[key] = rec["var_name"]


class TestChainsJsonSchema:
    def test_every_chain_has_wrapped_native(self, chains_blob: dict) -> None:
        for chain, cfg in chains_blob["chains"].items():
            assert "wrapped_native_address" in cfg, f"{chain}: missing wrapped_native_address"
            addr = cfg["wrapped_native_address"]
            assert _is_valid_address(addr, chain), f"{chain}: invalid wrapped native {addr!r}"


class TestSymbolAliasesJsonSchema:
    def test_aliases_are_valid_addresses(self, aliases_blob: dict) -> None:
        for chain, chain_aliases in aliases_blob["aliases"].items():
            for alias, addr in chain_aliases.items():
                assert alias == alias.upper(), f"alias {alias!r} must be uppercase"
                assert _is_valid_address(addr, chain), (
                    f"alias {chain}/{alias}: invalid address {addr!r}"
                )


class TestLegacyImportsStillWork:
    """Every var_name in tokens.json must be importable from defaults.py."""

    def test_well_known_vars_importable(self) -> None:
        from almanak.framework.data.tokens.defaults import (  # noqa: F401
            AAVE,
            ARB,
            AVAX,
            BNB,
            DAI,
            ETH,
            LINK,
            OP,
            USDC,
            USDT,
            WAVAX,
            WBNB,
            WBTC,
            WETH,
            WMATIC,
        )

    def test_bridged_and_specialty_vars_importable(self) -> None:
        from almanak.framework.data.tokens.defaults import (  # noqa: F401
            AUSD,
            BTC_B,
            USDBC,
            USDC_E_ARBITRUM,
            USDC_E_AVALANCHE,
            USDC_E_OPTIMISM,
            USDC_E_POLYGON,
            USDC_SOL,
            USDT_SOL,
            WEETH,
            stETH,
            swETH,
            wstETH,
        )

    def test_every_var_name_resolves_in_defaults_module(self, tokens_blob: dict) -> None:
        from almanak.framework.data.tokens import defaults

        for rec in tokens_blob["tokens"]:
            var = rec["var_name"]
            assert hasattr(defaults, var), f"defaults.py missing attribute {var!r}"
            tok = getattr(defaults, var)
            assert tok.symbol, f"{var}: empty symbol"


class TestResolverSymbolCollisionBehavior:
    """Direct resolver-behavior regression guards for the symbol-collision
    blocker found by the PR audit (2026-04-17).

    MNT on Mantle must resolve to the native sentinel (is_native=True)
    even though a CoinGecko-sourced ``MNT_MANTLE`` record also exists.
    AAVE on Base must resolve to the hand-curated address even though a
    fetcher-added ``AAVE_BASE`` record also exists. Without first-write-
    wins in ``_build_static_indices``, both break silently and every
    auto-wrap / approval on those chains goes to the wrong contract.
    """

    @pytest.fixture()
    def isolated_resolver(self, tmp_path):
        from almanak.framework.data.tokens.resolver import TokenResolver

        TokenResolver._instance = None
        return TokenResolver(cache_file=str(tmp_path / "cache.json"))

    def test_mnt_mantle_is_native_sentinel(self, isolated_resolver) -> None:
        tok = isolated_resolver.resolve("MNT", "mantle", skip_gateway=True)
        assert tok.is_native, f"MNT on mantle must stay native-sentinel, got {tok.address}"

    def test_aave_base_keeps_hand_curated_address(self, isolated_resolver) -> None:
        tok = isolated_resolver.resolve("AAVE", "base", skip_gateway=True)
        # Hand-curated AAVE on Base (Aave DAO governance token).
        assert tok.address.lower() == "0x18c11fd286c5ec11c3b683caa813b77f5163a122", (
            f"AAVE on base resolved to {tok.address!r} -- hand-curated record was "
            "shadowed by a fetcher-added duplicate. See PR audit blocker #1."
        )

    def test_usdc_solana_keeps_circle_mint(self, isolated_resolver) -> None:
        tok = isolated_resolver.resolve("USDC", "solana", skip_gateway=True)
        # Circle's native USDC mint on Solana mainnet.
        assert tok.address == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


class TestCompilerUsesSharedWrappedNative:
    """Regression guard for VIB-2896: compiler must not carry its own
    wrapped-native dict."""

    def test_compiler_imports_wrapped_native(self) -> None:
        import inspect

        from almanak.framework.data.tokens.defaults import WRAPPED_NATIVE
        from almanak.framework.intents import compiler as compiler_module

        # Verify the function's code object exists (tripwire against anyone
        # accidentally deleting the helper). Use a local name so we don't
        # leave a stray attribute on ``compiler_module`` for the rest of
        # the test session.
        code = compiler_module.IntentCompiler._get_wrapped_native_address.__code__
        assert code is not None

        # Structural check: the post-VIB-2896 implementation must read the
        # shared ``WRAPPED_NATIVE`` rather than inlining a per-chain dict.
        src = inspect.getsource(compiler_module.IntentCompiler._get_wrapped_native_address)
        assert "WRAPPED_NATIVE" in src, "compiler must read WRAPPED_NATIVE, not inline dicts"

        # And the canonical dict must at minimum cover the EVM chains we
        # advertise in demo strategies.
        for chain in ("ethereum", "arbitrum", "optimism", "base", "polygon", "avalanche", "bsc"):
            assert chain in WRAPPED_NATIVE, f"{chain} missing from WRAPPED_NATIVE"
