"""Deferred transaction refresh for protocols with stale calldata.

Some aggregator protocols return transaction calldata that goes stale quickly.
These protocols mark their swap transactions as "deferred" at compile time and
store route parameters in the ActionBundle metadata.

This module provides a single function, ``refresh_deferred_bundle``, that the
ExecutionOrchestrator calls immediately before building unsigned transactions.
It re-fetches fresh calldata from the protocol API and patches the deferred
transaction in the bundle. If the fresh quote routes through a different
spender, the approval transaction is also updated to match.

For bundles without ``metadata["deferred_swap"] == True`` the function is a
no-op (zero overhead for all non-deferred protocols).

Fail-closed contract (VIB-6228)
-------------------------------
``deferred_swap = True`` is the compiler declaring *this calldata is known
stale, do not submit it as-is*. Every path that ends without a successful
refresh therefore raises :class:`DeferredRefreshError` rather than returning
the un-refreshed bundle. Before VIB-6228 four separate paths returned it
instead — a failed route fetch, a missing route-params block, an unregistered
protocol, and a bundle with no ``*_deferred`` transaction to replace — each of
which submitted expired aggregator calldata to the chain and logged a warning
nobody was watching. The Solana lane
(``framework/execution/solana/planner.py``) has always failed closed here; this
brings the twelve EVM chains that route through LiFi / Enso in line.

Refusing costs a trade. Submitting expired route calldata costs the trade
*plus* gas, and — because an aggregator route encodes its own minimum-output
bound — can execute against a route whose economics no longer hold.
"""

import copy
import logging
from typing import Any

from almanak.connectors._strategy_deferred_refresh_registry import DEFERRED_REFRESH_REGISTRY

from ..models.reproduction_bundle import ActionBundle
from .interfaces import DeferredRefreshError

logger = logging.getLogger(__name__)

# An ERC-20 ``approve(address,uint256)`` payload is fixed-width by the ABI:
#   "0x" (2) + selector (8) + spender word (64) + amount word (64) = 138 chars.
_APPROVE_SELECTOR = "0x095ea7b3"
_SPENDER_WORD = slice(10, 74)
_AMOUNT_WORD = slice(74, 138)
_APPROVE_CALLDATA_LEN = 138
_HEX_DIGITS = frozenset("0123456789abcdef")

# Fields of a provider's refresh response that get installed onto the deferred
# transaction. Each is read with a bare subscript below, so absence must be a
# refusal rather than a KeyError escaping as an "Unexpected error".
_REQUIRED_FRESH_FIELDS = ("to", "value", "data", "gas_estimate")


def refresh_deferred_bundle(
    action_bundle: ActionBundle,
    wallet_address: str,
    rpc_url: str | None = None,
    *,
    managed_fork: bool | None = None,
) -> ActionBundle:
    """Refresh stale deferred transaction data in an ActionBundle.

    If the bundle's metadata contains ``deferred_swap: True``, this function
    re-fetches fresh transaction data from the originating protocol (LiFi or
    Enso) and replaces the deferred transaction fields (to, value, data,
    gas_estimate, tx_type).  If the fresh quote returns a different approval
    spender, the approval transaction is also updated to match.

    For non-deferred bundles the original bundle is returned immediately.

    Args:
        action_bundle: The ActionBundle to refresh.
        wallet_address: Wallet address for the fresh quote request.
        rpc_url: RPC URL passed through to connector-owned refresh hooks for
            network-sensitive refresh adjustments.
        managed_fork: Tri-state managed-fork declaration (ALM-3184) passed
            through to refresh hooks that relax an on-chain safety bound on a
            fork (Enso widens ``minAmountOut`` slippage to 5%). ``None`` means
            undeclared, which resolves to production via
            ``almanak.framework.execution.fork_signal``. It must never be
            inferred from ``rpc_url``.

    Returns:
        A new ActionBundle with fresh transaction data, or the original
        bundle unchanged when no refresh was needed.

    Raises:
        DeferredRefreshError: The bundle declared itself deferred but could not
            be refreshed. See the module docstring for why this is a refusal
            rather than a fallback.
    """
    metadata = action_bundle.metadata
    if not metadata.get("deferred_swap"):
        return action_bundle

    # ``get(k, default)`` returns None when the key exists holding None, so the
    # default cannot be relied on here — both of these are read as strings below.
    protocol = metadata.get("protocol") or ""
    if not metadata.get("route_params"):
        raise DeferredRefreshError(
            "the bundle carries no route_params, so no fresh route can be requested",
            protocol=protocol,
            recoverable=False,
        )

    # Deep-copy metadata upfront so we never mutate the caller's bundle and
    # connector-owned refresh hooks can safely adjust request metadata before
    # making the fresh API call.
    refresh_metadata = copy.deepcopy(metadata)

    fresh_tx = _fetch_fresh_transaction(
        protocol, refresh_metadata, wallet_address, rpc_url=rpc_url, managed_fork=managed_fork
    )

    # Build the new bundle with the (potentially widened) metadata
    new_bundle = ActionBundle(
        intent_type=action_bundle.intent_type,
        transactions=copy.deepcopy(action_bundle.transactions),
        metadata=refresh_metadata,
    )

    # Exactly one transaction may be deferred: a single refresh response carries
    # one route, so it can only make one leg current. Counting first (rather than
    # replacing the first match and breaking) is what makes that an enforced
    # invariant instead of an assumption — with a break, a second `*_deferred` leg
    # kept BOTH its stale calldata and its suffix and was then built and
    # submitted, defeating the whole point of Step 0. No producer emits two today
    # (LiFi and Enso each mark a single swap/bridge leg), so this is
    # defence-in-depth; it is enforced because the cost of being wrong is
    # submitting expired calldata, and the check is one line.
    # `isinstance`, not `get("tx_type", "")`: the default does not fire when the key
    # exists holding None, and a bundle-supplied value's type is not guaranteed. This
    # is the third instance of that trap in this function (see the `protocol` and
    # `data` reads); `tx_type: None` raised AttributeError on `.endswith` and escaped
    # as the opaque "Unexpected error" this module exists to eliminate. A transaction
    # with no usable tx_type is simply not a deferred leg.
    deferred = [
        tx
        for tx in new_bundle.transactions
        if isinstance(tx.get("tx_type"), str) and tx["tx_type"].endswith("_deferred")
    ]

    if not deferred:
        raise DeferredRefreshError(
            "the bundle declares a deferred swap but carries no transaction with a "
            "'_deferred' tx_type to replace, so the fresh route could not be applied",
            protocol=protocol,
            recoverable=False,
        )
    if len(deferred) > 1:
        raise DeferredRefreshError(
            f"the bundle carries {len(deferred)} transactions with a '_deferred' tx_type "
            f"({[tx.get('tx_type') for tx in deferred]}), but a single refresh response can only "
            f"make one of them current — refusing rather than submitting the rest stale",
            protocol=protocol,
            recoverable=False,
        )

    tx = deferred[0]
    tx["to"] = fresh_tx["to"]
    # Normalise the numerics rather than installing them as the provider sent
    # them. ``_validate_fresh_transaction`` deliberately ACCEPTS string-encoded
    # integers (JSON APIs routinely quote large numbers, and refusing them would
    # brick deferred swaps on every chain) — but the consumer two steps later does
    # ``int(gas_estimate * self.gas_buffer_multiplier)``, and ``"250000" * 1.2``
    # is a TypeError. Accepting an input and then handing it on in a form the
    # next stage cannot use converts a tolerated response into an opaque
    # execution failure, which is the very class of defect this module now guards
    # against. Coerce at the boundary; validation has already proved these parse.
    tx["value"] = str(int(fresh_tx["value"]))
    tx["data"] = fresh_tx["data"]
    tx["gas_estimate"] = int(fresh_tx["gas_estimate"])
    tx["tx_type"] = tx["tx_type"].removesuffix("_deferred")
    if "description" in fresh_tx:
        tx["description"] = fresh_tx["description"]

    # Patch approval tx if the fresh quote uses a different spender
    fresh_approval_address = fresh_tx.get("approval_address", "")
    if fresh_approval_address:
        _repoint_approval(new_bundle, fresh_approval_address, protocol)

    logger.info(f"Refreshed deferred {protocol} transaction with fresh route data")
    return new_bundle


def _repoint_approval(
    bundle: ActionBundle,
    fresh_approval_address: str,
    protocol: str,
) -> None:
    """Point the bundle's approval at ``fresh_approval_address``, in place.

    Only the **spender** word is rewritten; the amount word is carried over
    verbatim from the compiled approval.

    Preserving the amount is not merely the safer choice, it is the correct
    one: a refresh re-quotes the same ``amount_in`` (the route params' input
    amount is never re-derived here — Enso's hook only widens slippage), so the
    approval the compiler sized for the original route is exactly the approval
    the fresh route needs. Before VIB-6228 this substituted ``MAX_UINT256``,
    silently converting a bounded, compile-time-validated allowance into an
    unlimited one, on an address the route API supplied moments earlier — the
    VIB-6151 failure class reached from the opposite direction.

    Only the first ``approve`` transaction is considered, matching the
    pre-VIB-6228 loop. A payload that is not a well-formed
    ``approve(address,uint256)`` call, or a fresh spender that is not a
    20-byte address, is a refusal: it cannot be rewritten without inventing an
    amount or a spender, and a malformed rewrite either reverts on-chain after
    burning gas or approves the wrong contract.
    """
    for tx in bundle.transactions:
        if tx.get("tx_type") != "approve":
            continue

        # Hex is case-insensitive; normalise once so word comparison and the
        # rebuilt payload agree. ``get("data", "")`` would yield None when the key
        # exists holding None, and the value is bundle-supplied so its type is not
        # guaranteed either — anything non-str is simply not a payload this
        # function can parse.
        raw_data = tx.get("data")
        current_data = raw_data.lower() if isinstance(raw_data, str) else ""
        if not current_data.startswith(_APPROVE_SELECTOR):
            # Not an approve payload this function can parse. Leave it alone
            # rather than guess: nothing here is being made less safe. But SAY so —
            # this module's whole thesis is that a failure nobody can see is the
            # dangerous kind, and an unparsed approval next to a repointed router is
            # exactly the shape that produced VIB-229.
            logger.warning(
                f"Deferred {protocol} route names approval spender {fresh_approval_address}, but the "
                f"bundle's approve leg carries no recognisable approve(address,uint256) payload "
                f"({raw_data!r}); leaving it untouched — the swap may hit the new router without "
                f"allowance"
            )
            return

        if len(current_data) != _APPROVE_CALLDATA_LEN:
            raise DeferredRefreshError(
                f"the bundle's approval calldata is not a well-formed approve(address,uint256) "
                f"payload ({len(current_data)} chars, expected {_APPROVE_CALLDATA_LEN}), so its "
                f"approved amount cannot be carried over to the fresh spender",
                protocol=protocol,
                recoverable=False,
            )

        spender_word = current_data[_SPENDER_WORD]
        amount_word = current_data[_AMOUNT_WORD]
        if not _is_word(spender_word) or not _is_word(amount_word):
            raise DeferredRefreshError(
                "the bundle's approval calldata is not valid hex, so its approved amount "
                "cannot be carried over to the fresh spender",
                protocol=protocol,
                recoverable=False,
            )

        fresh_word = _address_word(fresh_approval_address, protocol)
        if spender_word == fresh_word:
            return

        tx["data"] = _APPROVE_SELECTOR + fresh_word + amount_word
        logger.info(
            f"Updated approval spender: 0x{spender_word[24:]} -> {fresh_approval_address} (approved amount preserved)"
        )
        return
    else:
        # No approve leg at all, yet the fresh route named a spender.
        #
        # Warn rather than refuse, and the decisive reason is NOT the ambiguity of
        # not having an allowance read — it is that this bundle shape is legitimate
        # and common, for two independent reasons: the compiler omits the approve
        # when the ORIGINAL spender's on-chain allowance already covers the amount
        # (`intents/compiler.py` returns `[]`), AND a native-token input never has an
        # approve leg at all. Refusing here would brick every native-input deferred
        # swap outright.
        #
        # Only the first of those is a concern, and only when the fresh spender
        # differs from the original — then the swap meets a router holding no
        # allowance and reverts. Step 0 cannot tell that case from "the fresh
        # spender already has an allowance" without an on-chain read, which it has
        # by design (it speaks to the route API, not the chain). Bounding it
        # properly belongs with the allowance-read / router-allowlist work.
        #
        # The message names both shapes rather than asserting the revert: for a
        # native-token input the revert claim would simply be wrong, and a warning
        # that misdescribes a healthy swap is how operators learn to ignore warnings.
        logger.warning(
            f"Deferred {protocol} route names approval spender {fresh_approval_address}, but the "
            f"bundle carries no approve leg to repoint. Expected when the input is a native token, "
            f"or when the ORIGINAL spender already held sufficient allowance — in the latter case, "
            f"if the fresh spender differs, the swap will revert for want of allowance."
        )


def _is_word(candidate: str) -> bool:
    """True iff ``candidate`` is a lowercase 32-byte hex ABI word."""
    return len(candidate) == 64 and all(char in _HEX_DIGITS for char in candidate)


def _is_address(value: Any) -> bool:
    """True iff ``value`` is a ``0x``-prefixed 20-byte hex address string.

    Syntax only — this deliberately says nothing about *which* addresses are
    acceptable. Constraining the refreshed target to a set of known routers is a
    policy question (an aggregator's value proposition is routing through
    arbitrary downstream contracts) and is tracked separately on VIB-6228. What
    is unambiguous is that a transaction destination which is not an address at
    all cannot be signed, so accepting one is never right.
    """
    if not isinstance(value, str):
        return False
    # Require the 0x prefix, as the docstring says and as ``_is_calldata`` already
    # does. Without it a bare 40-hex string passed, and would then be installed as
    # ``tx["to"]`` un-prefixed. No provider emits that today; the point is that the
    # helper should not contradict its own contract.
    if value[:2].lower() != "0x":
        return False
    raw = value[2:].lower()
    return len(raw) == 40 and all(char in _HEX_DIGITS for char in raw)


def _address_word(address: Any, protocol: str) -> str:
    """Left-pad a 20-byte hex address into a 32-byte ABI word.

    Validates rather than coerces: ``zfill`` over an empty or truncated string
    yields a syntactically valid word naming the *wrong* spender, which is
    indistinguishable from a correct one once it is on-chain.

    ``address`` is annotated ``Any`` on purpose — it is a value lifted straight
    out of a third-party route-API JSON response, so its *type* is as untrusted
    as its content. A non-string (a number, list or object) must produce the same
    clean refusal as a malformed string; calling ``.lower()`` on it first would
    raise ``AttributeError`` and escape the pipeline as an opaque
    "Unexpected error", losing both the diagnosis and the refusal's
    classification.
    """
    if not _is_address(address):
        # recoverable=True, deliberately. `address` is `fresh_tx["approval_address"]`
        # — the ROUTE API's response, not the bundle. Every other refusal derived from
        # provider data (missing fields, bad `data`/`to`/`value`/`gas_estimate` in
        # `_validate_fresh_transaction`) uses the transient default; this was the one
        # row classifying provider output as a bundle defect. It matters because
        # teardown recompiles on every same-level attempt, so a re-quote would very
        # likely return a well-formed address — tagging it `[permanent]` forfeited
        # that leg after a single try. `[permanent]` now means exactly "bundle or
        # registry defect"; `[transient]` means "the provider handed us something
        # unusable". (Found by the pr-auditor reviewing this PR's own fix round.)
        raise DeferredRefreshError(
            f"the refreshed route named {address!r} as its approval spender, which is not a "
            f"20-byte address, so the approval cannot be safely repointed",
            protocol=protocol,
        )
    return address.lower().removeprefix("0x").rjust(64, "0")


def _fetch_fresh_transaction(
    protocol: str,
    metadata: dict[str, Any],
    wallet_address: str,
    *,
    rpc_url: str | None = None,
    managed_fork: bool | None = None,
) -> dict[str, Any]:
    """Dispatch to the connector-owned refresh provider.

    Args:
        protocol: Protocol name ("lifi" or "enso").
        metadata: Bundle metadata containing route_params.
        wallet_address: Wallet address for the quote request.
        rpc_url: RPC URL forwarded to the provider.
        managed_fork: Tri-state managed-fork declaration forwarded to the
            provider (ALM-3184).

    Returns:
        Fresh transaction data.

    Raises:
        DeferredRefreshError: No provider is registered for ``protocol``, the
            fetch itself failed, or the provider returned nothing.
    """
    refresher = DEFERRED_REFRESH_REGISTRY.lookup(protocol)
    if refresher is None:
        raise DeferredRefreshError(
            "no deferred-refresh provider is registered for this protocol, so its stale route data cannot be replaced",
            protocol=protocol,
            recoverable=False,
        )

    try:
        fresh_tx = refresher.refresh_transaction(metadata, wallet_address, rpc_url=rpc_url, managed_fork=managed_fork)
    except DeferredRefreshError:
        raise
    except Exception as exc:
        # Log the traceback here: the exception the pipeline surfaces carries
        # only ``str(exc)``, and a route-API failure is usually diagnosed from
        # the stack.
        logger.exception(f"Failed to refresh deferred {protocol} transaction")
        raise DeferredRefreshError(
            f"the fresh route request failed: {exc}",
            protocol=protocol,
        ) from exc

    if fresh_tx is None:
        raise DeferredRefreshError(
            "the refresh provider returned no transaction data, so the stale route data cannot be replaced",
            protocol=protocol,
        )
    _validate_fresh_transaction(fresh_tx, protocol)
    return fresh_tx


def _validate_fresh_transaction(fresh_tx: dict[str, Any], protocol: str) -> None:
    """Check the provider's response carries the fields we are about to install.

    A truthy-but-malformed response is not a successful refresh. Without this,
    every field is read with a bare subscript, so a response missing ``data``
    raises ``KeyError`` and escapes the pipeline as an opaque "Unexpected error" —
    and a response carrying ``data: None`` is worse: it installs ``None`` as the
    transaction's calldata, strips the ``_deferred`` suffix, and returns a bundle
    that *looks* successfully refreshed.

    The response is third-party API output, so presence and type are both checked.
    ``data`` is held to the strictest standard of the four because it **is** the
    calldata that gets signed.
    """
    missing = [field for field in _REQUIRED_FRESH_FIELDS if field not in fresh_tx]
    if missing:
        raise DeferredRefreshError(
            f"the refreshed route is missing required transaction field(s) {missing}, so it "
            f"cannot replace the stale calldata",
            protocol=protocol,
        )

    data = fresh_tx["data"]
    if not isinstance(data, str) or not _is_calldata(data):
        raise DeferredRefreshError(
            f"the refreshed route's calldata is not a 0x-prefixed hex payload ({data!r}), so it cannot be signed",
            protocol=protocol,
        )

    target = fresh_tx["to"]
    if not _is_address(target):
        raise DeferredRefreshError(
            f"the refreshed route names {target!r} as its target, which is not a 20-byte address",
            protocol=protocol,
        )

    # Parsing is necessary but not sufficient: `int()` happily accepts `-1`, `0`,
    # `True` and `2.9`. A negative `value` is nonsense, `gas_estimate=0` yields
    # `int(0 * 1.2) == 0` and a guaranteed out-of-gas, and a float would silently
    # truncate wei. Same defect class as the missing coercion this validator's own
    # acceptance created — "it parses" is not "it is usable".
    for field, minimum in (("value", 0), ("gas_estimate", 1)):
        raw = fresh_tx[field]
        if isinstance(raw, bool) or isinstance(raw, float):
            raise DeferredRefreshError(
                f"the refreshed route's {field} is a {type(raw).__name__} ({raw!r}); an integer "
                f"(or its decimal-string form) is required so no value is silently truncated",
                protocol=protocol,
            )
        try:
            parsed = int(raw)
        except (TypeError, ValueError):
            raise DeferredRefreshError(
                f"the refreshed route's {field} is not an integer ({raw!r})",
                protocol=protocol,
            ) from None
        if parsed < minimum:
            raise DeferredRefreshError(
                f"the refreshed route's {field} is {parsed}, below the minimum {minimum}",
                protocol=protocol,
            )


def _is_calldata(data: str) -> bool:
    """True iff ``data`` is a ``0x``-prefixed, even-length, non-empty hex payload."""
    body = data[2:].lower()
    return data[:2].lower() == "0x" and len(body) > 0 and len(body) % 2 == 0 and all(c in _HEX_DIGITS for c in body)
