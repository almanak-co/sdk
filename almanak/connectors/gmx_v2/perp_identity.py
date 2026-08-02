"""GMX V2 venue position identity (VIB-6287).

Emits up to two token kinds, per the alias-set contract in
``almanak/connectors/_strategy_base/perp_identity.py``:

===================================================================  ===========================================
token                                                                emitted when
===================================================================  ===========================================
``gmx_v2:key:{chain}:{positionKey}``                                 the row already carries the venue position
                                                                     key as its ``position_id`` (**ADOPT** — every
                                                                     WARM producer does), or the key is derivable
                                                                     from the wallet (**DERIVE**)
``gmx_v2:sem:{chain}:{market_addr}:{collateral_addr}:{side}``        market AND collateral both resolve to
                                                                     addresses AND the side is measured
===================================================================  ===========================================

ADOPT is the general mechanism, shared with every venue. DERIVE is a GMX-specific
supplement: GMX's ``PositionStoreUtils`` derives the key as

.. code-block:: solidity

    keccak256(abi.encode(account, market, collateralToken, isLong))

Verified against the mainnet run of record: with the observed wallet, the
Arbitrum ETH/USD market, USDC collateral and ``isLong=true`` this reproduces
``0xbf58e0307a44a17ea51e30850651f5269c9fc0f306990576c015e9a88ac9bafa``
byte-identically; ``isLong=false`` yields a different key. **Do not assume other
venues have such a formula** — ``aster_perps`` / ``pancakeswap_perps`` assign a
``tradeHash`` per open call with no pure function to it at all.

The ``sem`` token is where the symbol-vs-address polysemy is resolved, on BOTH
axes and chain-scoped. Address-shaped values pass through, symbols resolve
through ``GMX_V2_MARKETS`` / ``GMX_V2_TOKENS``, and anything that resolves to
neither yields NO token — never a degraded one, because an under-specified token
is the only way this design can over-collapse, and over-collapse strands funds
silently.

CATALOGUE DEFECT — FIXED in VIB-6155, and worth keeping as the worked example of
how this module fails. ``GMX_V2_MARKETS`` used to list
``0xB7e69749E3d2EDd90ea59A4932EFEa2D41E245d7`` for BOTH ``arbitrum:AVAX/USD``
and ``avalanche:ETH/USD`` — the Arbitrum entry held the Avalanche address. Four
further rows were wrong in the same table; ``Reader.getMarket()`` found all five.

Chain-scoped resolution meant OVER-COLLAPSE was impossible. The damage was on the
other axis: the symbol side resolved through the wrong catalogue entry while the
address side — emitted by the settlement reconciler and the adapter's on-chain
discovery — carried the market the chain actually reports. The two ``sem`` tokens
were disjoint and one position enumerated as two. Direction is over-split ⇒ loud
false FAILED ⇒ fail-safe. Found by the #3534 audit panel, which caught an earlier
version of this paragraph claiming the opposite ("both rows resolve through the
same wrong entry, so they still agree") — true only when BOTH rows are in symbol
space, which is precisely not the premise here.

Two things survive the fix. First, the failure SHAPE is generic: any catalogue
row that disagrees with the chain reproduces it, so nothing here should assume
``GMX_V2_MARKETS`` is right — that is what the ``sem``-token discipline above is
for. Second, no in-tree assertion can detect the next one. Reproducing the defect
needs the address the CHAIN reports, which is not in this tree, and the obvious
symbol-plus-symbol reproduction passes because both sides resolve through the
same wrong entry. The guard therefore lives on-chain, in
``tests/audit/test_gmx_v2_market_identity.py``, and NOT in this module's census.

WALLET SCOPE UNDER SAFE / ZODIAC — **traced, and it is correct**: the venue key
is scoped to the account that submits the order, which under Safe/Zodiac is the
Safe (it is ``msg.sender`` at the GMX router). The teardown lane resolves
``_teardown_wallet_for_chain`` → ``strategy.wallet_address``, and
``cli/_run_components.py`` sets ``wallet_address = safe_address or eoa_address``,
so DERIVE already receives the Safe. The derivation itself is verified against
the real mainnet key: ``keccak(abi.encode(0xafeB2f5c…, 0x70d95587…, 0xaf88d065…,
True))`` reproduces ``0xbf58e0307a44a17ea51e30850651f5269c9fc0f306990576c015e9a88ac9bafa``
byte-identically, and ``isLong=False`` differs.

What remains genuinely unconfirmed is narrower than the earlier note implied: no
end-to-end Safe-wallet perp teardown has been run in this tree. Were the resolved
wallet ever the EOA on a Safe deployment, DERIVE would yield a key the venue
never used ⇒ over-split ⇒ fail-safe.
"""

from __future__ import annotations

import logging
from typing import Any

from almanak.connectors._strategy_base.perp_identity import is_residual_marked
from almanak.connectors.gmx_v2.addresses import GMX_V2_MARKETS, GMX_V2_TOKENS

logger = logging.getLogger(__name__)

_SLUG = "gmx_v2"
# A GMX position key is a keccak256 digest: "0x" + 64 hex characters, exactly.
_VENUE_KEY_LEN = 66
_ADDRESS_LEN = 42

# Detail keys a producer may write the market / collateral under. Both spaces
# (symbol and address) occur under every one of these names depending on the
# producer, which is why resolution — not key precedence — is the fix.
_MARKET_KEYS: tuple[str, ...] = ("market_address", "market")
_COLLATERAL_KEYS: tuple[str, ...] = ("collateral_address", "collateral_token")


def _is_hex_of_length(value: Any, length: int) -> bool:
    if not isinstance(value, str):
        return False
    text = value.strip()
    if len(text) != length or not text.lower().startswith("0x"):
        return False
    try:
        int(text, 16)
    except ValueError:
        return False
    return True


def _first_present(details: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    """First non-empty value among ``keys``. Empty ≠ Zero: absent stays ``None``."""
    for key in keys:
        value = details.get(key)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return None


def _resolve_address(table: dict[str, dict[str, str]], chain: str, value: str | None) -> str | None:
    """Resolve a producer-written market / collateral to a lower-cased address.

    Returns ``None`` — never a guess — when the value is neither an address nor
    a symbol catalogued for ``chain``.
    """
    if not value:
        return None
    if _is_hex_of_length(value, _ADDRESS_LEN):
        return value.lower()
    entries = table.get(chain)
    if not entries:
        return None
    # Catalogue symbols carry canonical case ("ETH/USD", "USDC", "BTC.b",
    # "WETH.e") and producers do not reliably preserve it, so match
    # case-insensitively. This folds a SYMBOL for lookup; it never folds an
    # emitted token.
    wanted = value.lower()
    for symbol, address in entries.items():
        if symbol.lower() == wanted:
            return str(address).lower()

    # MARKET ALIASES — the execution path accepts more spellings than the
    # catalogue keys, and an identity hook that accepts fewer leaves exactly the
    # positions this ticket is about unnamed (#3534 panel, Codex P1).
    #
    # `GMXV2SDK.get_market_address` resolves `ETH`, `WETH`, `BTC`, `WBTC` — and
    # `AVAX` / `WAVAX` where the market is wired — all case-insensitively. A
    # strategy that writes `details["market"] = "ETH"` therefore opens a real
    # position, while this hook emitted NO `sem` token for it: the HOT row went
    # unnamed, the registry row carried the resolved address, the two never
    # intersected, and the duplicate enumeration survived. The fix was inert for
    # every alias-using strategy.
    #
    # Derived, NOT enumerated: a catalogue key is `<SYM>/USD`, so `<SYM>` and the
    # wrapped `W<SYM>` are tried against it. A new market added to the catalogue is
    # covered automatically, with no list to keep in step — and the census test
    # asserts exactly that equivalence for every catalogue entry, so it fails if
    # this ever stops matching the execution path's alias surface.
    #
    # Fail-safe either way: an alias that resolves to nothing still yields no
    # token, which is over-split (loud), never a collapse.
    for candidate in (f"{wanted}/usd", f"{wanted[1:]}/usd" if wanted.startswith("w") else None):
        if not candidate:
            continue
        for symbol, address in entries.items():
            if symbol.lower() == candidate:
                return str(address).lower()
    return None


def _side(position: Any, details: dict[str, Any]) -> str | None:
    """``"long"`` / ``"short"``, or ``None`` when the side is UNMEASURED.

    Empty ≠ Zero: an unmeasured side must yield no token. A long and a short in
    the same market are DIFFERENT positions with different venue keys, so
    defaulting to long would merge two live positions into one — the
    over-collapse that strands funds.
    """
    is_long = details.get("is_long")
    if isinstance(is_long, bool):
        return "long" if is_long else "short"
    # ``details["side"]`` is read because shipped producers write it:
    # `gmx_v2_directional_perp/strategy.py:424` and
    # `hyperliquid_trailing_perp/strategy.py:556` both emit
    # ``{"market", "side", "size_usd"}``.
    #
    # HONEST SCOPE — reading it does NOT currently fix those demos, and an earlier
    # version of this comment claimed it did. Measured: all three shipped perp
    # demos write NO collateral, and both this hook and the framework default
    # require market AND collateral AND side, so those rows still emit no token and
    # still fall through to raw ``position_id``. The read is INERT for them today.
    # It is kept because it is correct and because the demos are one missing
    # ``collateral_token`` away from being served — but "the demos are covered" was
    # a false claim in a money-path comment, which is the thing this ticket exists
    # to stop (#3534 panel).
    #
    # Reading one more key can only ADD identity information, so the polarity is
    # toward collapsing — safe here only because ``side`` occupies the same
    # long/short value space as ``direction`` and is normalised through the same
    # vocabulary below. A key with a DIFFERENT value space must never be added
    # here; that is VIB-6287's own defect.
    text = (
        str(getattr(position, "direction", None) or details.get("direction") or details.get("side") or "")
        .strip()
        .lower()
    )
    if text in ("long", "buy"):
        return "long"
    if text in ("short", "sell"):
        return "short"
    return None


def gmx_v2_perp_identity(position: Any, *, wallet_address: str | None) -> frozenset[str]:
    """Every token GMX V2 is CERTAIN names ``position``. Never raises."""
    try:
        chain = str(getattr(position, "chain", "") or "").strip().lower()
        if not chain:
            return frozenset()
        details = getattr(position, "details", None)
        if not isinstance(details, dict):
            details = {}

        # A residual is NOT a position. GMX surfaces pending unfilled orders and
        # unverified sweeps as ``PositionType.PERP`` rows carrying a ``kind``
        # marker (VIB-5116), and such a row can name the same market, collateral
        # and side as a real open position while being a DIFFERENT thing holding
        # its own collateral in the OrderVault. Naming it would let the union
        # merge the two and suppress one — and a suppressed residual is never
        # recovered. It has no venue position key, so the venue cannot name it:
        # emit nothing and let it keep its own raw identity.
        if is_residual_marked(details):
            return frozenset()

        tokens: set[str] = set()

        # ADOPT — the row already carries the venue's own key.
        adopted_key: str | None = None
        position_id = getattr(position, "position_id", None)
        if _is_hex_of_length(position_id, _VENUE_KEY_LEN):
            adopted_key = str(position_id).strip().lower()
            tokens.add(f"{_SLUG}:key:{chain}:{adopted_key}")

        market = _resolve_address(GMX_V2_MARKETS, chain, _first_present(details, _MARKET_KEYS))
        collateral = _resolve_address(GMX_V2_TOKENS, chain, _first_present(details, _COLLATERAL_KEYS))
        side = _side(position, details)
        if market and collateral and side:
            # DERIVE — recompute the venue key the way GMX does on-chain.
            wallet = str(wallet_address or "").strip()
            derived_key = (
                _derive_position_key(wallet, market, collateral, side == "long")
                if _is_hex_of_length(wallet, _ADDRESS_LEN)
                else None
            )

            # A ROW MAY NEVER NAME TWO POSITIONS.
            #
            # ADOPT reads the venue key off `position_id`; DERIVE recomputes one
            # from `details`. If they disagree, the row is INTERNALLY INCONSISTENT
            # — it is describing position A by id and position B by attributes —
            # and emitting both would make it a BRIDGE between two genuinely
            # distinct positions.
            #
            # Under the old pairwise pass that was harmless. Under the transitive
            # closure this PR introduces it is a REGRESSION vs `main`: union-find
            # merges A and B into one component and suppresses every registry row
            # in it but the first. Nothing builds a closing intent for a suppressed
            # row and nothing raises — the silent strand this module's own contract
            # calls "strictly worse". Demonstrated by the #3534 panel against real
            # catalogue addresses; both row orderings drop the second position.
            #
            # Refusing to name such a row costs nothing real: it falls through to
            # the raw `position_id`, i.e. over-split — loud, recoverable, and no
            # worse than `main`. When the two keys AGREE, DERIVE is redundant with
            # ADOPT anyway, so nothing is lost in the healthy case either.
            #
            # This is not hypothetical: the KNOWN CATALOGUE DEFECT above is exactly
            # an id/attribute disagreement with a benign target today, and
            # `_perp_position_key` pairs a receipt-measured `position_id` with the
            # intent's market — any keeper-tx mis-attribution produces this shape.
            if adopted_key is not None and derived_key is not None and derived_key != adopted_key:
                logger.debug(
                    "gmx_v2 identity refused: position_id names %s but details derive %s — "
                    "one row cannot name two positions",
                    adopted_key,
                    derived_key,
                )
                return frozenset({f"{_SLUG}:key:{chain}:{adopted_key}"})

            # A ROW MAY EMIT AT MOST ONE IDENTITY FAMILY.
            #
            # The disagreement check above can only run when DERIVE is computable,
            # which needs a wallet. Without one, a row carrying an ADOPT key AND a
            # `sem` token asserts that `position_id` and `details` describe the same
            # position with nothing having verified it — and if they disagree it
            # BRIDGES two distinct positions under the transitive closure. Executed
            # by the #3534 panel: three registry rows describing two positions
            # enumerate as ONE with no wallet, where `main` gives three. That is an
            # over-collapse regression vs `main`, in the direction that strands.
            #
            # `wallet is None` is reachable by three deliberate fallbacks, not by
            # misconfiguration: `_teardown_wallet_for_chain` yields "" when the
            # per-chain map misses, `wallet_for` swallows resolver exceptions, and
            # `reconcile_lp_with_registry(wallet_for_chain=...)` defaults to None for
            # every other caller.
            #
            # So an unverifiable row emits ONLY its adopted key. It still names
            # itself with the venue's own authoritative identity — it simply does not
            # also vouch for attributes nobody checked.
            #
            # WHY NOT `frozenset()` HERE: VIB-6329 makes a registered hook's empty
            # emission fall directly to raw `position_id`, avoiding the old lossy
            # DEFAULT-namespace detour. But raw-id space still cannot intersect a
            # venue-key-only registry row, so it manufactures a duplicate whenever
            # the other producer names the same key through the hook. That duplicate
            # flips `type_counts[PERP] == 2` and retroactively tightens the VIB-5494
            # guard — a false FAILED. Emitting the adopted key alone keeps the row in
            # venue space and preserves ADOPT<->ADOPT joins.
            #
            # COST, stated: on the no-wallet path the key<->sem bridge is gone, so a
            # symbol-space row and a key-space row no longer collapse. Over-split,
            # loud, and exactly `main`'s behaviour. ADOPT<->ADOPT and sem<->sem joins
            # are untouched. Production threads a wallet, so the mainnet path keeps
            # the full fix.
            if adopted_key is not None and derived_key is None:
                return frozenset(tokens)

            tokens.add(f"{_SLUG}:sem:{chain}:{market}:{collateral}:{side}")
            if derived_key is not None:
                tokens.add(f"{_SLUG}:key:{chain}:{derived_key}")

        return frozenset(tokens)
    except Exception:  # pragma: no cover - defensive; hooks must never raise
        logger.debug("gmx_v2 position identity resolution failed", exc_info=True)
        return frozenset()


def _derive_position_key(wallet: str, market: str, collateral: str, is_long: bool) -> str:
    """``keccak256(abi.encode(account, market, collateralToken, isLong))``."""
    # Imported lazily: the ABI codec is only needed on the DERIVE path, and this
    # module is loaded during connector manifest hydration.
    from eth_abi import encode
    from eth_utils import keccak, to_checksum_address

    packed = encode(
        ["address", "address", "address", "bool"],
        [
            to_checksum_address(wallet),
            to_checksum_address(market),
            to_checksum_address(collateral),
            is_long,
        ],
    )
    return "0x" + keccak(packed).hex()


__all__ = ["gmx_v2_perp_identity"]
