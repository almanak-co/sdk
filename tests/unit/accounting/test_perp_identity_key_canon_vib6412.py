"""VIB-6412 — one on-chain perp position must mint exactly ONE identity key.

ALM-3094 made GMX V2's compiler accept ``ETH/USD``, ``ETH-USD``, ``ETH_USD`` and
``ETH:USD`` for a single market. The identity keys that feed FIFO lot matching
were built from the RAW ``intent.market``, so each spelling minted a distinct
key and a close under one spelling would not match the open under another —
wrong cost basis, wrong realized PnL.

Two properties are pinned here, and BOTH matter:

1. **Collapse** — every GMX spelling produces one key, at all three identity
   sites. Reverting ``perp_market_identity_key`` in ``almanak/core/perp_markets.py``
   turns these red; that is the negative control.
2. **No migration** — the key for the slash form (and for every non-GMX venue)
   is BYTE-IDENTICAL to what the pre-fix code produced. A 206-DB sweep of every
   historical run found GMX perp rows exclusively in the slash form, so this fix
   re-keys no existing row and needs no ``matching_policy_version`` bump. If this
   half ever fails, a migration boundary has been introduced and the fix is wrong.
"""

from __future__ import annotations

import pytest

from almanak.core.perp_markets import perp_market_identity_key

# The spellings ALM-3094's compiler accepts for ONE GMX market.
GMX_SPELLINGS = ["ETH/USD", "ETH-USD", "ETH_USD", "ETH:USD", "eth/usd", " ETH-USD "]


def _legacy_market_key(market: str) -> str:
    """The pre-VIB-6412 derivation, verbatim, for byte-identity comparison."""
    return str(market or "").strip().lower().replace(" ", "_")


def _key(market: str) -> str:
    return perp_market_identity_key(market).lower().replace(" ", "_")


class TestGmxSpellingsCollapseToOneKey:
    """Property 1 — the collapse. These are the negative control."""

    @pytest.mark.parametrize("market", GMX_SPELLINGS)
    def test_every_gmx_spelling_yields_the_slash_key(self, market: str) -> None:
        assert _key(market) == "eth/usd"

    def test_open_and_close_under_different_spellings_share_one_key(self) -> None:
        """The actual defect: an OPEN and its CLOSE spelled differently."""
        assert _key("ETH/USD") == _key("ETH-USD"), "a close must match the lots its open created"

    def test_distinct_markets_still_produce_distinct_keys(self) -> None:
        """Collapsing separators must not collapse genuinely different markets."""
        assert _key("ETH/USD") != _key("BTC/USD")
        assert _key("AVAX/USD") != _key("ETH/USD")

    def test_a_differing_quote_is_not_collapsed_into_the_usd_pair(self) -> None:
        """Only the SEPARATOR is normalised — Drift's PERP quote stays distinct.

        This is the property that makes venue-agnostic canonicalisation safe: a
        collision would need two markets differing ONLY by separator, which no
        venue lists as separate products.
        """
        assert _key("SOL-PERP") != _key("SOL/USD")
        assert _key("ETH-PERP") != _key("ETH/USD")


class TestNoMigrationBoundary:
    """Property 2 — every key that could already exist on disk is unchanged.

    Measured: a sweep of all 206 SQLite DBs in the repo found 15 distinct perp
    identity strings — GMX and Hyperliquid rows all in the slash form, and zero
    Drift rows. Those are the forms pinned here.
    """

    @pytest.mark.parametrize(
        "market",
        [
            "ETH/USD",  # GMX, arbitrum + avalanche
            "BTC/USD",
            "AVAX/USD",
            "eth/usd",  # already-lowered, as persisted
        ],
    )
    def test_gmx_slash_form_is_byte_identical_to_the_legacy_key(self, market: str) -> None:
        assert _key(market) == _legacy_market_key(market)

    @pytest.mark.parametrize("market", ["ETH/USD", "ETH", "BTC", "HYPE"])
    def test_hyperliquid_persisted_forms_are_byte_identical(self, market: str) -> None:
        """HL's persisted key is slash (`…:eth/usd`); bare coins stay bare."""
        assert _key(market) == _legacy_market_key(market)

    def test_raw_market_address_is_preserved(self) -> None:
        address = "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
        assert _key(address) == _legacy_market_key(address)

    @pytest.mark.parametrize("market", ["SOL-PERP", "ETH-PERP", "BTC-PERPETUAL"])
    def test_venue_suffixed_forms_are_byte_identical_too(self, market: str) -> None:
        """Venue-suffixed markets are NOT re-keyed — no form moves at all.

        An earlier revision let `SOL-PERP` become `sol/perp` and argued the
        shape change was free because the 206-DB sweep found zero Drift rows.
        A panel reviewer (Codex, P1 on #3565) pushed back, and the objection
        holds where it matters: **the repo corpus is not production.** A hosted
        deployment with an open Drift position would have its opening lot keyed
        `sol-perp` and its close keyed `sol/perp` — an orphaned lot, i.e. wrong
        realized PnL, in a population we cannot measure from here.

        Rather than argue reachability, the transform was narrowed to the
        collision it actually exists for. `SOL-PERP` is not a second spelling of
        `SOL/PERP`: nothing writes the latter, so collapsing them buys nothing.
        With this, EVERY persisted form is byte-identical and the
        no-migration claim needs no corpus evidence to hold.
        """
        assert _key(market) == _legacy_market_key(market)

    def test_venue_suffix_separator_variants_stay_distinct(self) -> None:
        """An accepted limitation, pinned rather than left implicit.

        `SOL-PERP` and `SOL/PERP` mint different keys. Hyperliquid's
        `markets.py` accepts BOTH spellings for one market, so an open under one
        and a close under the other orphans the lot. This is NOT a regression —
        `main` did not collapse them either — and closing it by emitting
        `f"{base}-{quote}"` would re-key any history written with `/PERP`, the
        very corpus argument this PR retired for Drift. Pinned so the divergence
        is a recorded decision; the dominant design is ticketed
        (delta review finding 3, PR #3565).
        """
        assert _key("SOL-PERP") != _key("SOL/PERP")
        assert _key("SOL-PERP") == _legacy_market_key("SOL-PERP")

    def test_venue_suffix_does_not_disable_the_pair_collapse(self) -> None:
        """The narrowing must not cost the PR its own goal.

        A quote that is a real currency still collapses across separators;
        only venue-suffix quotes are exempt.
        """
        assert _key("ETH-USD") == _key("ETH/USD")
        assert _key("SOL-PERP") != _key("SOL/USD")


class TestDegenerateInputs:
    """The seam must never raise — it sits on the persistence path."""

    @pytest.mark.parametrize("market", ["", "   ", None, 42])
    def test_unusable_markets_return_empty_not_raise(self, market: object) -> None:
        assert perp_market_identity_key(market) == ""

    def test_seam_takes_no_protocol_argument(self) -> None:
        """Venue-agnostic by construction.

        A per-venue exemption set would hardcode protocol names in
        `almanak/core/`, which the chain/protocol coupling ratchet
        (VIB-4851/4852) rejects — `core/` is not a canonical home for them.
        """
        import inspect

        params = inspect.signature(perp_market_identity_key).parameters
        assert list(params) == ["market"], "the seam must not grow a venue dimension"


class TestLiveIdentitySites:
    """The three sites that actually persist a key must all use the seam."""

    def test_accounting_position_key_collapses_spellings(self) -> None:
        from almanak.framework.accounting.perp_accounting import build_perp_accounting_event

        def _event(market: str):
            intent = type(
                "I",
                (),
                {
                    "intent_type": "PERP_OPEN",
                    "market": market,
                    "protocol": "gmx_v2",
                    "collateral_token": "USDC",
                    "is_long": True,
                },
            )()
            result = type("R", (), {"tx_hash": "0xabc"})()
            return build_perp_accounting_event(
                intent=intent,
                result=result,
                deployment_id="deployment:test",
                cycle_id="c1",
                execution_mode="live",
                chain="arbitrum",
                wallet_address="0xWALLET",
            )

        slash, dash = _event("ETH/USD"), _event("ETH-USD")
        assert slash is not None and dash is not None
        assert slash.position_key == dash.position_key
        # and unchanged from what already exists on disk
        assert slash.position_key == "perp:gmx_v2:arbitrum:0xwallet:eth/usd"

    def test_runner_and_observability_sites_route_through_the_seam(self) -> None:
        """Static guard: neither live site may re-derive the key from raw market."""
        from pathlib import Path

        import almanak.framework.observability.position_events as pe
        import almanak.framework.runner.strategy_runner as sr

        for module in (sr, pe):
            src = Path(module.__file__).read_text()
            assert "perp_market_identity_key" in src, (
                f"{module.__name__} must build its perp identity key through the canonicalisation seam (VIB-6412)"
            )
