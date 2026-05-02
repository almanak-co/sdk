"""VIB-3878 — Solana teardown SolanaForkManager dispatch.

Pins the ``_resolve_anvil_chain_dispatch`` helper that ``cli/teardown.py``
relies on to decide whether to (a) start an EVM Anvil fork via
``ManagedGateway(anvil_chains=...)``, (b) start a ``SolanaForkManager`` for a
Solana strategy, or (c) both for a multi-chain strategy.

Without this dispatch, a Solana-only teardown on ``--network anvil`` would fall
through with ``anvil_chains=[]`` AND no Solana fork — the balance probe then
hits a dead RPC, ``get_open_positions()`` swallows the error, and VIB-3705's
no-op log line silently exits 0 leaving Solana positions stranded (same failure
mode VIB-3819 plugged for EVM chains).
"""

from __future__ import annotations

from almanak.framework.cli.run_helpers import (
    NON_ANVIL_CHAINS,
    _resolve_anvil_chain_dispatch,
)


def test_mainnet_returns_no_forks() -> None:
    """``--network mainnet`` never starts forks, regardless of chain."""
    assert _resolve_anvil_chain_dispatch("mainnet", "solana", {}) == ([], False)
    assert _resolve_anvil_chain_dispatch("mainnet", "base", {}) == ([], False)
    assert _resolve_anvil_chain_dispatch("mainnet", "ethereum", {"chains": ["base", "solana"]}) == (
        [],
        False,
    )


def test_anvil_evm_only_starts_anvil_fork() -> None:
    """Single EVM chain on ``--network anvil`` → only Anvil fork."""
    anvil_chains, solana_needed = _resolve_anvil_chain_dispatch("anvil", "base", {})
    assert anvil_chains == ["base"]
    assert solana_needed is False


def test_anvil_solana_only_starts_solana_fork() -> None:
    """Solana-only strategy on ``--network anvil`` → no Anvil fork, but Solana
    needed. This is the case VIB-3878 fixes: previously fell through with
    anvil_chains=[] AND no Solana fork."""
    anvil_chains, solana_needed = _resolve_anvil_chain_dispatch("anvil", "solana", {})
    assert anvil_chains == []
    assert solana_needed is True


def test_anvil_multichain_evm_plus_solana_starts_both() -> None:
    """Multi-chain EVM + Solana → both Anvil and Solana forks."""
    anvil_chains, solana_needed = _resolve_anvil_chain_dispatch(
        "anvil", "base", {"chains": ["base", "solana"]}
    )
    assert anvil_chains == ["base"]
    assert solana_needed is True


def test_anvil_chains_field_takes_precedence_over_primary_chain() -> None:
    """``config["chains"]`` (multi-chain spec) overrides scalar ``primary_chain``.

    Mirrors run_helpers.py:907-910 ordering — a multi-chain strategy with
    ``chains: [base, optimism]`` and ``primary_chain="ethereum"`` (decorator
    default) must use the explicit list, not silently boot an Ethereum fork.
    """
    anvil_chains, solana_needed = _resolve_anvil_chain_dispatch(
        "anvil", "ethereum", {"chains": ["base", "optimism"]}
    )
    assert anvil_chains == ["base", "optimism"]
    assert solana_needed is False


def test_anvil_case_insensitive_solana_detection() -> None:
    """Mixed-case ``Solana`` / ``SOLANA`` config still routes to Solana fork."""
    _, solana_needed = _resolve_anvil_chain_dispatch("anvil", "Solana", {})
    assert solana_needed is True
    _, solana_needed = _resolve_anvil_chain_dispatch("anvil", None, {"chains": ["SOLANA"]})
    assert solana_needed is True


def test_anvil_missing_chain_returns_empty() -> None:
    """No chain configured → no forks (caller will hard-fail elsewhere)."""
    assert _resolve_anvil_chain_dispatch("anvil", None, {}) == ([], False)


def test_non_anvil_chains_set_includes_solana() -> None:
    """Pin contract: ``solana`` must be in the non-Anvil set so it stays out
    of ``anvil_chains``. If a future chain (e.g. ``aptos``) gets added it must
    be pinned here too — the ``_resolve_anvil_chain_dispatch`` filter reads
    from this set."""
    assert "solana" in NON_ANVIL_CHAINS


def test_anvil_solana_only_via_chains_list() -> None:
    """Solana via ``chains: [solana]`` (multi-chain spec with one entry) also routes."""
    anvil_chains, solana_needed = _resolve_anvil_chain_dispatch(
        "anvil", None, {"chains": ["solana"]}
    )
    assert anvil_chains == []
    assert solana_needed is True


def test_anvil_malformed_chains_int_falls_back_to_primary(caplog) -> None:
    """CodeRabbit P2: ``chains: 5`` (int, not list) must NOT silently start
    nothing — fall back to ``primary_chain`` and warn. Previously a typo
    in config silently degraded to "start nothing", same silent-failure
    class VIB-3819 was fixing for."""
    import logging

    with caplog.at_level(logging.WARNING, logger="almanak.framework.cli.run_helpers"):
        anvil_chains, solana_needed = _resolve_anvil_chain_dispatch(
            "anvil", "base", {"chains": 5}
        )
    assert anvil_chains == ["base"]
    assert solana_needed is False
    assert any("malformed config['chains']" in r.message for r in caplog.records)


def test_anvil_malformed_chains_dict_falls_back_to_primary(caplog) -> None:
    """``chains: {}`` (dict, not list) → fall back to primary + warn."""
    import logging

    with caplog.at_level(logging.WARNING, logger="almanak.framework.cli.run_helpers"):
        anvil_chains, solana_needed = _resolve_anvil_chain_dispatch(
            "anvil", "ethereum", {"chains": {}}
        )
    assert anvil_chains == ["ethereum"]
    assert solana_needed is False


def test_anvil_malformed_chains_with_no_primary_returns_empty(caplog) -> None:
    """Malformed chains AND no primary → empty list, but still warn so the
    operator sees the typo."""
    import logging

    with caplog.at_level(logging.WARNING, logger="almanak.framework.cli.run_helpers"):
        anvil_chains, solana_needed = _resolve_anvil_chain_dispatch("anvil", None, {"chains": 99})
    assert anvil_chains == []
    assert solana_needed is False
    assert any("malformed config['chains']" in r.message for r in caplog.records)


def test_anvil_string_chains_value_accepted() -> None:
    """A scalar string ``chains: "base"`` is normalized to a single-chain list."""
    anvil_chains, solana_needed = _resolve_anvil_chain_dispatch("anvil", None, {"chains": "base"})
    assert anvil_chains == ["base"]
    assert solana_needed is False
