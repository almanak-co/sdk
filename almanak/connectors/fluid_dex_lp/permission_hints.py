"""Fluid SmartLending DEX LP permission hints (VIB-5032 / VIB-5125).

``fluid_dex_lp`` exposes ``LP_OPEN`` / ``LP_CLOSE`` over Fluid SmartLending
fungible ERC-20-share wrappers. This module wires the connector into the
synthetic Zodiac discovery matrix (VIB-5125) so its intent tests run under the
default-on ``ZodiacOrchestrator`` instead of carrying ``@pytest.mark.no_zodiac``.

Why STATIC permissions (not a compilation-based ``build_discovery_vectors``)
============================================================================
Synthetic permission discovery normally seeds intents and compiles them with a
real ``IntentCompiler`` to extract ``(target, selector)`` pairs offline. That
path does **not** work for ``fluid_dex_lp``, proven empirically (VIB-5125
de-risk probe against the real compiler):

* ``FluidDexLpCompiler._build_sdk`` fails closed without a gateway/RPC, and the
  LP_OPEN body issues LIVE on-chain reads — ``check_deposit_enabled`` (the 51013
  pre-flight) and ``quote_deposit_shares`` (the DEX estimate revert-carrier).
  An LP_OPEN "compile" only succeeds when a live node answers those reads; the
  discovery driver does NOT thread an RPC into this connector (it passes
  ``compiler_rpc=None`` unless ``needs_rpc_discovery`` is set, and even then the
  fork URL is not wired through), so a compilation-based vector is
  non-deterministic — it silently depends on a public-RPC fallback.
* LP_CLOSE can NEVER compile during discovery: ``compile_lp_close`` reads the
  caller's live share balance and refuses with "No position to close" for the
  synthetic discovery wallet (``0x0``), which holds no shares.
* Only the all-ERC-20 wrapper (fSL9) could compile at all — the native-leg
  wrapper (fSL5) is refused at compile (VIB-5121) and the deposit-disabled
  wrapper (fSL12) reverts the 51013 pre-flight.

This is the same situation TraderJoe V2's LP selectors face ("compilation-based
discovery fails in offline mode … Static permissions ensure the manifest always
includes the LBRouter selectors") — so we follow that precedent and pin the
exact ``(target, selector)`` triples the compiled bundles emit:

* ERC-20 ``approve(address,uint256)`` (``0x095ea7b3``) on each non-native token
  leg, spender = the wrapper. (Unlike the Uniswap-style connectors, the
  fluid_dex_lp ``LPOpenIntent.pool`` is the *wrapper address*, not a
  ``"token0/token1"`` pair, so the harness's ``_intent_token_symbols`` extracts
  no symbols and the framework's ERC-20-approve discovery would otherwise miss
  these legs.) Scoped to ``LP_OPEN`` — only the deposit funds-pull needs an
  approve.
* Wrapper ``deposit(uint256,uint256,uint256,address)`` (``0xfad3cc4b``), scoped
  to ``LP_OPEN``.
* Wrapper ``withdraw(uint256,uint256,uint256,address)`` (``0xd331bef7``), scoped
  to ``LP_CLOSE``.

Selectors verified on-chain (``connectors/fluid/smart_lending_sdk.py`` docstring
+ ``docs/internal/qa/fluid-smartlending-validation-2026-06-12.md``) and against
the live compiler in the VIB-5125 de-risk probe.

Native-leg wrappers excluded (consistent with the live refusal)
===============================================================
``FLUID_SMARTLENDING_MARKETS`` includes fSL5 (FLUID / native-ETH). The compiler
refuses any native-leg wrapper at COMPILE (``FluidDexLpCompiler._refuse_native``,
VIB-5032 v1 scope; native accounting is VIB-5121). A discovery entry for fSL5
would authorise a flow the compiler will never produce, so we skip native-leg
wrappers here — the static surface covers exactly the wrappers that compile and
execute today. This does NOT depend on VIB-5121 landing first.

Deposit-disabled wrappers (fSL12) ARE included: ``deposit_enabled`` is a live,
flip-able state the compiler re-checks every time (the 51013 pre-flight), so the
manifest authorising the selector is correct — the compiler, not the manifest,
is the gate on a disabled pool. Pinning the selector keeps the surface stable if
the wrapper is later re-enabled.
"""

from __future__ import annotations

from almanak.connectors._fluid_core.addresses import (
    FLUID_DEX_LP_NATIVE_SENTINEL,
    FLUID_SMARTLENDING_MARKETS,
    is_native_leg,
)
from almanak.framework.permissions.hints import PermissionHints, StaticPermissionEntry

# ERC-20 ``approve(address,uint256)`` — emitted on each funded token leg
# (spender = wrapper) by the LP_OPEN compile path before ``deposit``.
_ERC20_APPROVE_SELECTOR = "0x095ea7b3"
_ERC20_APPROVE_SIG = "approve(address,uint256)"

# SmartLending wrapper selectors (verified on-chain, smart_lending_sdk.py).
_DEPOSIT_SELECTOR = "0xfad3cc4b"  # deposit(uint256 token0Amt, uint256 token1Amt, uint256 minShares, address to)
_DEPOSIT_SIG = "deposit(uint256,uint256,uint256,address)"
_WITHDRAW_SELECTOR = "0xd331bef7"  # withdraw(uint256 token0Amt, uint256 token1Amt, uint256 maxShares, address to)
_WITHDRAW_SIG = "withdraw(uint256,uint256,uint256,address)"


def _build_static_permissions() -> dict[str, list[StaticPermissionEntry]]:
    """Pin the (token approve / wrapper deposit / wrapper withdraw) surface.

    One ``deposit`` entry (LP_OPEN-scoped) and one ``withdraw`` entry
    (LP_CLOSE-scoped) per non-native wrapper, plus an ``approve`` entry per
    non-native token leg (LP_OPEN-scoped, spender = wrapper). Native-leg
    wrappers are skipped — they are refused at compile (VIB-5121).
    """
    result: dict[str, list[StaticPermissionEntry]] = {}
    for chain, wrappers in FLUID_SMARTLENDING_MARKETS.items():
        entries: list[StaticPermissionEntry] = []
        # De-dupe token approve entries: several wrappers share USDC as a leg, so
        # a single ``(USDC, approve)`` rule must not be emitted multiple times.
        approve_targets: dict[str, str] = {}  # token_address_lower -> symbol (for label)
        for wrapper, entry in wrappers.items():
            if is_native_leg(entry):
                # Native-leg wrapper (e.g. fSL5 FLUID/ETH): refused at compile —
                # do not authorise a flow that can never execute today. Shared
                # predicate with ``FluidDexLpCompiler._refuse_native`` (single
                # source of truth in ``fluid.addresses``), so the discovery
                # surface cannot drift from the compiler's native refusal.
                continue
            # ``or wrapper`` (not the get-default) so an explicit ``None`` symbol
            # falls back to the wrapper address rather than the string "None".
            symbol = str(entry.get("symbol") or wrapper)
            wrapper_addr = wrapper.lower()
            entries.append(
                StaticPermissionEntry(
                    target=wrapper_addr,
                    label=f"Fluid SmartLending {symbol} (deposit)",
                    selectors={_DEPOSIT_SELECTOR: _DEPOSIT_SIG},
                    # LP_OPEN compiles to [approve(s)..., deposit]. ``deposit`` is
                    # only emitted on the open path — scope it so LP_CLOSE-only
                    # manifests stay least-privilege.
                    intent_types=frozenset({"LP_OPEN"}),
                )
            )
            entries.append(
                StaticPermissionEntry(
                    target=wrapper_addr,
                    label=f"Fluid SmartLending {symbol} (withdraw)",
                    selectors={_WITHDRAW_SELECTOR: _WITHDRAW_SIG},
                    # ``withdraw`` is the LP_CLOSE drain — scope to LP_CLOSE.
                    intent_types=frozenset({"LP_CLOSE"}),
                )
            )
            # Both ERC-20 legs may be funded on an LP_OPEN (the deposit pulls via
            # ``transferFrom`` after an ``approve``), so authorise an approve on
            # each non-native leg with the wrapper as the spender.
            # ``or ""`` (not the get-default) so an explicit ``None`` leg value
            # yields "" — which the guard below skips — rather than the string
            # "None" leaking in as a bogus approve target.
            for leg_addr, leg_symbol in (
                (str(entry.get("token0") or ""), str(entry.get("token0_symbol") or "")),
                (str(entry.get("token1") or ""), str(entry.get("token1_symbol") or "")),
            ):
                if not leg_addr or leg_addr.lower() == FLUID_DEX_LP_NATIVE_SENTINEL.lower():
                    continue
                approve_targets.setdefault(leg_addr.lower(), leg_symbol or leg_addr)

        for token_addr, token_symbol in approve_targets.items():
            entries.append(
                StaticPermissionEntry(
                    target=token_addr,
                    label=f"ERC-20: {token_symbol} (Fluid DEX LP approve)",
                    selectors={_ERC20_APPROVE_SELECTOR: _ERC20_APPROVE_SIG},
                    # ``approve`` is only needed by the LP_OPEN funds-pull.
                    intent_types=frozenset({"LP_OPEN"}),
                )
            )

        if entries:
            result[chain] = entries
    return result


PERMISSION_HINTS = PermissionHints(
    # Synthetic-discovery participation (VIB-4928 derivation; wired by VIB-5125).
    # Declaring LP_OPEN / LP_CLOSE makes ``fluid_dex_lp`` a member of
    # ``synthetic_intents._lp_protocols()`` so the manifest generator + teardown
    # complement machinery recognise it. The actual ``(target, selector)`` rules
    # come from ``static_permissions`` below (compilation-based discovery is
    # RPC-bound for this connector — see module docstring), NOT from a
    # ``build_discovery_vectors`` override.
    synthetic_discovery_intents=frozenset({"LP_OPEN", "LP_CLOSE"}),
    static_permissions=_build_static_permissions(),
    selector_labels={
        _DEPOSIT_SELECTOR: _DEPOSIT_SIG,
        _WITHDRAW_SELECTOR: _WITHDRAW_SIG,
    },
)
