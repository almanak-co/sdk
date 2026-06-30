"""Pendle teardown on-chain closure verifier (VIB-3808 / VIB-5487).

A Pendle teardown closes two distinct holdings, both surfaced by the teardown
registry-enumeration read path (``registry_enumeration._position_info_from_pendle_registry_row``):

  * a **PT** holding (``kind="pt"``, ``PositionType.TOKEN``) — the principal
    token is an ordinary ERC-20 the strategy swaps/redeems at teardown, and
  * an **LP** holding (``kind="lp"``, ``PositionType.LP``) — a Pendle AMM LP
    position closed via the strategy's own ``LP_CLOSE``.

Before this hook the S6 post-teardown verify had NO per-position on-chain
closure authority for Pendle: ``_reconcile_one`` returned ``UNVERIFIABLE`` and
no post-condition was registered, so a residual PT/LP balance was reported
optimistically as closed (the inverse of the VIB-3741/3742 LP $1.16-leak this
whole seam exists to prevent). This hook reads on-chain truth and fails the
teardown closed when any non-zero residual remains.

Two facts make the residual read a single ERC-20 ``balanceOf``:

  * **The LP token IS the market.** A Pendle market address is itself the
    ERC-20 LP token (there is no separate LP contract), so the LP residual is
    ``balanceOf(market, wallet)``.
  * **PT resolves from the market.** ``market.readTokens()`` (the no-arg view
    at 4-byte selector ``0x2c8ce6bc``; see
    ``almanak.connectors.pendle.on_chain_reader.READ_TOKENS_SELECTOR`` /
    ``MARKET_ABI``) returns ``(SY, PT, YT)``. We resolve PT from the market and
    read ``balanceOf(PT, wallet)``.

Closure rule — **exact-0 wei is closed; any non-zero residual is NOT closed.**
There is no oracle in this hook, so we cannot convert a residual to USD to apply
the teardown swap floor (``config.min_swap_value_usd`` is a *swap* floor, not a
closure floor). The V3 sibling uses the same exact-0 rule. Fail-closed is the
safe error direction here: a residual that later proves to be sub-dust is mopped
up by the separate consolidation / swap-clamp lane (Seam 9), and a FAILED
verification is loud-but-non-blocking under teardown's inverted failure
semantics — it never strands a risk-reducing intent. The opposite error
(reporting a still-funded position as closed) is the leak we must never make.

Gateway boundary: every on-chain read goes through the supplied
``gateway_client`` (``query_erc20_balance`` / ``eth_call``). There is NO direct
network egress from this framework/connector strategy-side path — ``rpc_url`` is
accepted to satisfy the ``TeardownPostCondition`` protocol but is intentionally
NOT consumed (framework code MUST cross the gateway boundary). The only
non-gateway dependency is ``eth_abi.decode`` to unpack the ``readTokens`` return
— pure ABI decoding, not egress. NEVER raises: any failure (missing client,
gateway error, malformed response, unknown kind) returns
``ClosureCheckResult(closed=False, error=...)`` so an unverifiable position can
never read as closed.

Ethena note
-----------
Ethena (``protocol="ethena"``, ``STAKE``/``UNSTAKE`` holding sUSDe directly) is
deliberately NOT given a teardown post-condition. Its sUSDe is a NO_ACCOUNTING
wallet token that emits zero accounting events, so it is never written to the
``position_registry`` and never reaches the teardown post-condition DISPATCH
(``teardown_manager`` only dispatches enumerated registry positions — LP /
lending / perp / Pendle). Ethena sUSDe is swept by the Seam-9 measured-ledger /
swap-clamp consolidation lane (``swap_clamp.read_no_accounting_ledger_rows`` +
``consolidation.derive_strategy_token_universe``, already green for the STAKE
family), and Pendle PT-sUSDe markets are covered by this hook's PT path. An
``"ethena"`` hook would be dead code.
"""

from __future__ import annotations

from typing import Any

from almanak.connectors._strategy_base.teardown_post_condition import ClosureCheckResult

# The no-arg ``readTokens()`` view on a Pendle market contract returns
# ``(SY, PT, YT)``. Selector mirrors
# ``almanak.connectors.pendle.on_chain_reader.READ_TOKENS_SELECTOR`` — kept as a
# local literal so this hook does not import the on-chain reader's direct-egress
# web3 machinery (gateway-boundary rule).
_READ_TOKENS_SELECTOR = "0x2c8ce6bc"


def _resolve_pt_address(
    gateway_client: Any,
    chain: str,
    market_address: str,
    block: int | str | None,
) -> tuple[str | None, str | None]:
    """Resolve the PT address for ``market_address`` via gateway ``readTokens()``.

    Returns ``(pt_address, None)`` on success or ``(None, error)`` on any
    failure — a gateway/RPC error, a ``None`` result, or a malformed (too
    short / non-decodable) return. Callers fail-closed on a non-``None`` error.
    """
    try:
        raw = gateway_client.eth_call(chain=chain, to=market_address, data=_READ_TOKENS_SELECTOR, block=block)
    except Exception as exc:  # noqa: BLE001 — fail-closed
        return None, f"Pendle readTokens eth_call raised: {exc}"

    if raw is None:
        return None, "Pendle readTokens eth_call returned None (gateway/RPC error); cannot resolve PT — fail-closed"

    try:
        from eth_abi import decode  # ABI-decode utility, NOT network egress

        hex_body = raw[2:] if raw.startswith("0x") else raw
        decoded = decode(["address", "address", "address"], bytes.fromhex(hex_body))
    except Exception as exc:  # noqa: BLE001 — fail-closed on malformed return
        return None, f"Pendle readTokens returned undecodable data ({raw!r}): {exc}"

    # readTokens() -> (SY, PT, YT); PT is index 1.
    pt_address = decoded[1]
    if not pt_address or int(pt_address, 16) == 0:
        return None, f"Pendle readTokens returned a zero PT address for market {market_address!r} — fail-closed"
    return pt_address, None


def pendle_teardown_post_condition(
    position: Any,
    wallet_address: str,
    gateway_client: Any | None = None,
    rpc_url: str | None = None,
    block: int | str | None = None,
) -> ClosureCheckResult:
    """Verify a Pendle PT or LP holding has zero residual balance on-chain.

    Reads the residual ERC-20 balance via the gateway and reports closure:

    - ``kind="lp"`` — the LP token IS the market address
      (``details["market_id"]`` or ``position.position_id``). Residual is
      ``query_erc20_balance(market, wallet)``.
    - ``kind="pt"`` — resolve PT from ``market.readTokens()`` (gateway
      ``eth_call``, selector ``0x2c8ce6bc``), then residual is
      ``query_erc20_balance(PT, wallet)``.

    Closure: ``balance == 0`` → ``closed=True``; any non-zero balance →
    ``closed=False`` with a residual map. A ``None`` balance (gateway/RPC
    error) or any other failure → ``closed=False`` with an ``error`` string.
    Fail-closed: an unknown on-chain state must NOT be reported as closed.

    ``block`` is pinned to the close-tx receipt's block (VIB-5140) so the read
    cannot race a replica trailing the writer. ``rpc_url`` is intentionally NOT
    consumed — framework code crosses the gateway boundary only; tests inject a
    fake ``gateway_client``.
    """
    protocol = (getattr(position, "protocol", "") or "").lower() or "pendle"
    position_id = str(getattr(position, "position_id", "") or "")

    chain = getattr(position, "chain", None) or ""
    if not chain:
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error="Pendle post-condition needs position.chain; none found",
        )

    if gateway_client is None:
        # Framework rule: no egress from the strategy container. Without a
        # gateway client there is no authoritative way to read on-chain truth —
        # fail-closed so a missing client is loud, not a silent pass.
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=(
                "Pendle post-condition requires a gateway_client to read on-chain residual "
                "balance (LP token / PT balanceOf). None supplied — verification cannot proceed."
            ),
        )

    details = getattr(position, "details", None) or {}

    # Resolve the holding kind. Prefer the explicit registry discriminator; fall
    # back to position_type (LP -> "lp", TOKEN -> "pt") so a position that lost
    # its details.kind still routes correctly.
    kind = str(details.get("kind") or "").strip().lower()
    if not kind:
        position_type_raw = getattr(position, "position_type", None)
        position_type_value = (getattr(position_type_raw, "value", None) or str(position_type_raw or "")).upper()
        if position_type_value == "LP":
            kind = "lp"
        elif position_type_value == "TOKEN":
            kind = "pt"

    # The market address anchors both kinds (LP token IS the market; PT resolves
    # from market.readTokens()).
    market_address = details.get("market_id") or position_id
    if not market_address:
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error="Pendle post-condition needs a market address (details['market_id'] or position_id); none found",
        )
    market_address = str(market_address)

    token_address: str
    if kind == "lp":
        token_address = market_address
    elif kind == "pt":
        pt_address, err = _resolve_pt_address(gateway_client, chain, market_address, block)
        if err is not None or pt_address is None:
            return ClosureCheckResult(
                closed=False,
                protocol=protocol,
                position_id=position_id,
                error=err or "Pendle post-condition: PT address unresolved (readTokens returned no PT)",
            )
        token_address = pt_address
    else:
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=(
                f"Pendle post-condition: unknown holding kind {kind!r} "
                "(expected 'pt' or 'lp'); cannot verify on-chain closure — fail-closed"
            ),
        )

    try:
        balance = gateway_client.query_erc20_balance(
            chain=chain,
            token_address=token_address,
            wallet_address=wallet_address,
            block=block,
        )
    except Exception as exc:  # noqa: BLE001 — fail-closed
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=f"Pendle query_erc20_balance raised: {exc}",
        )

    if balance is None:
        return ClosureCheckResult(
            closed=False,
            protocol=protocol,
            position_id=position_id,
            error=(
                "Pendle query_erc20_balance returned None (gateway/RPC error); cannot confirm closure — fail-closed"
            ),
        )

    if int(balance) == 0:
        return ClosureCheckResult(closed=True, protocol=protocol, position_id=position_id)

    return ClosureCheckResult(
        closed=False,
        protocol=protocol,
        position_id=position_id,
        residual={"token": token_address, "balance": int(balance), "kind": kind, "market_id": market_address},
    )


__all__ = ["pendle_teardown_post_condition"]
