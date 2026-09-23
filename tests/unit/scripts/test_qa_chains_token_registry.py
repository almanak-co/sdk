"""Every money-lane token entry must be reachable through `token()`."""

import pytest

from qa_lab.chains import POA_CHAINS, TOKENS, token


def test_every_token_key_is_uppercase():
    """`token()` uppercases the symbol, then does a case-sensitive lookup.

    A mixed-case key is therefore unreachable by construction: the funder
    refuses with "token WSTETH unknown on base" while the entry sits right
    there in the table. Caught on a live mainnet run after wstETH was added in
    the protocol registry's own casing.
    """
    offenders = [(chain, sym) for chain, toks in TOKENS.items() for sym in toks if sym != sym.upper()]
    assert not offenders, f"unreachable TOKENS keys (token() uppercases): {offenders}"


@pytest.mark.parametrize("chain", sorted(TOKENS))
def test_every_declared_token_resolves(chain):
    """Resolve each symbol the way every caller does, including odd casings."""
    for sym in TOKENS[chain]:
        address, decimals = token(chain, sym)
        assert address.startswith("0x") and len(address) == 42, (chain, sym, address)
        assert 0 < decimals <= 18, (chain, sym, decimals)
        # A caller passing the protocol registry's casing must resolve identically.
        assert token(chain, sym.lower()) == (address, decimals)


def test_poa_chains_are_known_chains():
    """A PoA entry for a chain that does not exist would silently never apply."""
    from qa_lab.chains import CHAINS

    unknown = sorted(POA_CHAINS - set(CHAINS))
    assert not unknown, f"POA_CHAINS names chains absent from CHAINS: {unknown}"


def test_avalanche_is_declared_poa():
    """Avalanche C-Chain emits >32-byte extraData on a few percent of blocks.

    web3's validator rejects those, and because fill_transaction_defaults reads
    get_block("latest") once per transaction build, omitting it strands funds
    intermittently rather than failing outright.
    """
    assert "avalanche" in POA_CHAINS


@pytest.mark.parametrize("chain", sorted(TOKENS))
def test_every_token_is_priced_under_its_own_identity(chain, monkeypatch):
    """The symbol `ax price` is actually invoked with must name the asset held.

    `wallet_value()` reads each balance at the TOKENS address but asks for the
    quote by ticker, and `_assert_quote_identity` runs only for pegged
    identities -- so for an unpegged asset nothing else compares the two. A
    symbol rewrite that lands on a different contract values one asset at
    another's price with no guard in between, and one that lands on a ticker the
    registry does not carry on this chain raises SystemExit mid-lane, after a
    position is open and while the sweep still holds the balance.

    The argv is read back from the call `ax_price` really makes, so a derivation
    that stops matching `price_identity` fails here instead of agreeing with a
    copy of itself.
    """
    import subprocess as _subprocess

    from almanak.framework.data.tokens.defaults import DEFAULT_TOKENS
    from qa_lab import chains as chains_module

    native = chains_module.native_symbol(chain).upper()
    quoted: dict[str, str] = {}

    def _capture(argv, **kwargs):
        quoted["sym"] = argv[argv.index("price") + 1]
        return _subprocess.CompletedProcess(argv, 0, stdout='{"status":"success","data":{"price":"1"}}', stderr="")

    monkeypatch.setattr(chains_module.subprocess, "run", _capture)
    monkeypatch.setattr(chains_module, "active_context", lambda: None)

    for sym, (address, _) in TOKENS[chain].items():
        quoted.clear()
        chains_module.ax_price(sym, chain, None, strict_peg=False)
        asked = quoted["sym"]
        resolved = next(
            (
                token_.get_address(chain)
                for token_ in DEFAULT_TOKENS
                if token_.symbol.upper() == asked.upper() and chain in token_.chains
            ),
            None,
        )
        assert resolved is not None, f"{chain}/{sym} is priced as {asked}, which the registry lacks on {chain}"
        if asked.upper() != native:
            assert resolved.lower() == address.lower(), (
                f"{chain}/{sym} is valued at {address} but priced as {asked}, which resolves to {resolved}"
            )
