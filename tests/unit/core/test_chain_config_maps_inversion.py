"""Equivalence harness for the VIB-4851 CLI chain->config-map inversion.

Three hand-maintained CLI maps were folded onto the chain registry and now
derive from ``ChainDescriptor.rpc`` via
``almanak.core.chains._helpers``:

  * ``permissions._CHAIN_RPC_TEMPLATES`` -> ``alchemy_rpc_url_template_for``
    (backed by ``rpc.alchemy_prefix``, EVM-gated).
  * ``backtest.helpers.BLOCKS_PER_DAY`` -> ``blocks_per_day_map`` /
    ``blocks_per_day_for`` (backed by ``rpc.block_time_seconds``).
  * ``replay.CHAIN_BLOCK_TIMES`` (only its ``.keys()`` were consumed by the
    ``--chain`` ``click.Choice``) -> ``sorted(blocks_per_day_map().keys())``;
    ``block_time_for`` reproduces the values.

This test freezes the OLD literals verbatim and asserts the derived lookups
reproduce them -- proving the data, not the design -- mirroring the A1/A2
``test_native_symbols_inversion`` / ``test_chain_id_maps_inversion`` harnesses
and the W5 ``test_chain_descriptor_w5_fields`` snapshots.

The Alchemy template surface intentionally WIDENS: the descriptor declares
``alchemy_prefix`` for more EVM chains than the legacy 8-entry CLI dict. The
widening is pinned by name below so any further drift is an explicit diff. The
EVM gate is load-bearing -- ``solana`` carries an ``alchemy_prefix`` but the
permissions CLI is Zodiac/EVM-only, so it must NOT yield a template (preserving
``_resolve_rpc_url(None, "solana") is None``).
"""

from __future__ import annotations

import pytest

from almanak.core.chains import ChainRegistry
from almanak.core.chains._helpers import (
    alchemy_rpc_url_template_for,
    block_time_for,
    blocks_per_day_for,
    blocks_per_day_map,
)
from almanak.core.enums import ChainFamily

# =============================================================================
# Frozen historical snapshots — the legacy literals these helpers now derive.
# Copied verbatim from origin/main so a future regression diff is obvious.
# =============================================================================

# permissions._CHAIN_RPC_TEMPLATES (8 entries), with the literal ``{key}``
# placeholder preserved.
FROZEN_CHAIN_RPC_TEMPLATES: dict[str, str] = {
    "base": "https://base-mainnet.g.alchemy.com/v2/{key}",
    "arbitrum": "https://arb-mainnet.g.alchemy.com/v2/{key}",
    "ethereum": "https://eth-mainnet.g.alchemy.com/v2/{key}",
    "avalanche": "https://avax-mainnet.g.alchemy.com/v2/{key}",
    "mantle": "https://mantle-mainnet.g.alchemy.com/v2/{key}",
    "bsc": "https://bnb-mainnet.g.alchemy.com/v2/{key}",
    "optimism": "https://opt-mainnet.g.alchemy.com/v2/{key}",
    "polygon": "https://polygon-mainnet.g.alchemy.com/v2/{key}",
}

# backtest.helpers.BLOCKS_PER_DAY (6 entries).
FROZEN_BLOCKS_PER_DAY: dict[str, int] = {
    "ethereum": 7200,
    "arbitrum": 345600,
    "optimism": 43200,
    "polygon": 43200,
    "base": 43200,
    "avalanche": 43200,
}

# replay.CHAIN_BLOCK_TIMES (6 entries).
FROZEN_CHAIN_BLOCK_TIMES: dict[str, float] = {
    "ethereum": 12.0,
    "arbitrum": 0.25,
    "optimism": 2.0,
    "polygon": 2.0,
    "base": 2.0,
    "avalanche": 2.0,
}


# =============================================================================
# Alchemy RPC URL template — byte-equivalence on the legacy 8.
# =============================================================================


class TestAlchemyRpcUrlTemplate:
    """``alchemy_rpc_url_template_for`` reproduces the legacy 8-entry CLI dict."""

    @pytest.mark.parametrize(
        "chain_name,expected_template",
        sorted(FROZEN_CHAIN_RPC_TEMPLATES.items()),
    )
    def test_template_byte_equivalent(self, chain_name: str, expected_template: str) -> None:
        derived = alchemy_rpc_url_template_for(chain_name)
        assert derived is not None, f"{chain_name} lost its Alchemy RPC template"
        # The caller does ``.replace("{key}", api_key)``; assert that substitution
        # reproduces the legacy URL exactly.
        assert derived.replace("{key}", "K") == expected_template.replace("{key}", "K")

    def test_template_keeps_literal_key_placeholder(self) -> None:
        # The doubled ``{{key}}`` in the helper must yield a literal ``{key}``
        # the permissions CLI then substitutes.
        assert "{key}" in alchemy_rpc_url_template_for("ethereum")

    def test_alias_resolves_to_canonical_template(self) -> None:
        # ``bnb`` resolves to ``bsc`` (which uses the ``bnb`` Alchemy prefix).
        assert alchemy_rpc_url_template_for("bnb") == FROZEN_CHAIN_RPC_TEMPLATES["bsc"]

    def test_alchemy_widening_documented_by_name(self) -> None:
        """The descriptor declares ``alchemy_prefix`` for more EVM chains than
        the legacy 8. Pin the widening delta by name so any further drift is an
        explicit, reviewed diff rather than a silent surface expansion.
        """
        evm_prefixed = {
            d.name for d in ChainRegistry.all() if d.family is ChainFamily.EVM and d.rpc.alchemy_prefix is not None
        }
        assert evm_prefixed - set(FROZEN_CHAIN_RPC_TEMPLATES) == {
            "linea",
            "monad",
            "plasma",
            "robinhood",
            "sonic",
            "xlayer",
        }


# =============================================================================
# EVM-gate / anti-widening pins — the load-bearing guards.
# =============================================================================


class TestEvmGateAndAntiWidening:
    """The EVM gate and the block-time membership must not widen silently."""

    def test_solana_yields_no_template_despite_alchemy_prefix(self) -> None:
        # solana carries an alchemy_prefix but is non-EVM -> Zodiac/EVM-only
        # permissions CLI must not offer it (preserves _resolve_rpc_url None).
        assert alchemy_rpc_url_template_for("solana") is None

    def test_unknown_chain_yields_no_template(self) -> None:
        assert alchemy_rpc_url_template_for("not-a-chain") is None

    def test_chain_with_prefix_but_no_block_time_has_no_blocks_per_day(self) -> None:
        # bsc has an alchemy_prefix but no block_time_seconds -> not in the
        # BLOCKS_PER_DAY membership.
        assert blocks_per_day_for("bsc") is None

    def test_blocks_per_day_membership_did_not_widen(self) -> None:
        # The replay --chain choice keys derive from this map; assert it stays
        # the legacy 6 and never widens to every block-time-bearing chain.
        assert set(blocks_per_day_map()) == set(FROZEN_CHAIN_BLOCK_TIMES)


# =============================================================================
# BLOCKS_PER_DAY — byte-equivalence of the derived map + per-chain accessor.
# =============================================================================


class TestBlocksPerDay:
    """``blocks_per_day_map`` / ``blocks_per_day_for`` reproduce the legacy 6."""

    def test_map_byte_equivalent(self) -> None:
        assert dict(blocks_per_day_map()) == FROZEN_BLOCKS_PER_DAY

    @pytest.mark.parametrize(
        "chain_name,expected",
        sorted(FROZEN_BLOCKS_PER_DAY.items()),
    )
    def test_per_chain_accessor_byte_equivalent(self, chain_name: str, expected: int) -> None:
        assert blocks_per_day_for(chain_name) == expected

    def test_backtest_helpers_back_compat_view_matches_history(self) -> None:
        # The re-exported module-level name must stay byte-equivalent for
        # downstream importers (``from ...backtest.helpers import BLOCKS_PER_DAY``).
        from almanak.framework.cli.backtest.helpers import BLOCKS_PER_DAY

        assert dict(BLOCKS_PER_DAY) == FROZEN_BLOCKS_PER_DAY

    @pytest.mark.parametrize(
        "chain_name",
        sorted(FROZEN_BLOCKS_PER_DAY),
    )
    def test_round_reproduces_legacy_values_exactly(self, chain_name: str) -> None:
        # round(86400 / block_time) must reproduce each legacy blocks/day value.
        assert round(86400 / FROZEN_CHAIN_BLOCK_TIMES[chain_name]) == FROZEN_BLOCKS_PER_DAY[chain_name]


# =============================================================================
# CHAIN_BLOCK_TIMES — replay --chain choice keys + block-time values.
# =============================================================================


class TestChainBlockTimes:
    """The replay ``--chain`` choice keys + ``block_time_for`` reproduce the
    legacy ``CHAIN_BLOCK_TIMES`` (only its keys were ever consumed).
    """

    def test_replay_choice_keys_match_history(self) -> None:
        assert sorted(blocks_per_day_map().keys()) == sorted(FROZEN_CHAIN_BLOCK_TIMES)

    @pytest.mark.parametrize(
        "chain_name,expected",
        sorted(FROZEN_CHAIN_BLOCK_TIMES.items()),
    )
    def test_block_time_for_byte_equivalent(self, chain_name: str, expected: float) -> None:
        assert block_time_for(chain_name) == expected


# =============================================================================
# Defensive: a non-positive block time (invalid data) must not divide by zero.
# =============================================================================


class TestNonPositiveBlockTimeGuard:
    """A non-positive ``block_time_seconds`` (invalid descriptor data) yields
    ``None`` rather than a ``ZeroDivisionError`` (Gemini review). ``BLOCKS_PER_DAY``
    is evaluated at module import, so an unguarded divide would crash the
    backtest CLI on a single bad descriptor.
    """

    @pytest.mark.parametrize("bad", [0.0, -1.0])
    def test_blocks_per_day_for_guards_non_positive(self, monkeypatch: pytest.MonkeyPatch, bad: float) -> None:
        import almanak.core.chains._helpers as helpers

        monkeypatch.setattr(helpers, "block_time_for", lambda chain: bad)
        assert helpers.blocks_per_day_for("ethereum") is None
