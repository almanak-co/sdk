"""PrimitiveMoneyLeg — the typed money-leg extraction contract.

VIB-5212 (US-008), root of Workstream 2 (the accounting invariant / contract
layer). Unblocks US-009 (wire the ledger dispatcher to prefer declared legs),
US-010 (Lido), and US-011 (Trader Joe V2).

Why this exists
---------------
Today the transaction ledger *guesses* an intent's money legs. The dispatcher
``almanak/framework/observability/ledger.py:_extract_tokens_and_amounts`` walks
a precedence chain of loosely-typed intent attributes
(``from_token`` > ``borrow_token`` > ``supply_token`` > ``token``;
``amount`` > ``borrow_amount`` > ``supply_amount``), with bespoke per-intent
branches for SWAP / LP_OPEN / LP_CLOSE / PERP_OPEN / lending, falling through to
``_extract_from_intent_fallback`` for everything else. Each new primitive that
does not fit the guess re-emits empty ``amount_in`` / ``amount_out`` until a
follow-up patch teaches the dispatcher its shape — the "patch-hub" failure mode
the VIB-5200 epic is closing.

``PrimitiveMoneyLeg`` inverts the control flow: a **connector declares** the
money legs its intent moves, as a typed object, instead of the ledger reverse-
engineering them. A leg is a single ``(role, token, amount)`` fact:

* ``role``   — what the leg *is* (an INPUT spent, an OUTPUT received, or the
  PRINCIPAL the action operates on), as a closed :class:`MoneyLegRole` enum.
* ``token``  — the token identity the ledger records (symbol *or* address; the
  ledger treats it opaquely, see ``transaction_ledger.token_in``).
* ``amount`` — a :class:`~almanak.framework.accounting.measured.MeasuredMoney`
  in **human units** (the VIB-5036 ledger contract), so the Empty≠Zero
  discipline (blueprint 27 §10.10) is carried *by construction*: a leg can be
  measured (incl. measured zero), unmeasured, or absent — never silently
  coerced.

Scope (VIB-5212 is the CONTRACT DEFINITION only)
------------------------------------------------
This module defines the typed contract and its aggregation semantics. It does
**not** wire the ledger dispatcher to consume declared legs — that is US-009.
Pure value types: no I/O, no gateway calls, no token resolution. Frozen +
slotted, mirroring :class:`MeasuredMoney` and the typed event models.

Mapping to the ledger's flat columns (informative — implemented in US-009)
--------------------------------------------------------------------------
The ledger's ``_TokensAndAmounts`` is a flat ``(token_in, amount_in,
token_out, amount_out, effective_price, slippage_bps)`` tuple. The role-tagged
legs project onto it per primitive (the dispatcher's job, not this contract's):

* SWAP        — INPUT → (token_in, amount_in); OUTPUT → (token_out, amount_out).
* LP_OPEN     — two INPUT legs (deposits) → in/out slots for lane symmetry.
* LP_CLOSE    — two OUTPUT legs (proceeds) → in/out slots.
* PERP_OPEN   — PRINCIPAL (collateral) → (token_in, amount_in).
* REPAY /
  WITHDRAW    — PRINCIPAL (receipt-resolved amount) → (token_in, amount_in).

``effective_price`` / ``slippage_bps`` are trade-*quality* metadata, not money
legs, and are deliberately out of scope here.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any

from almanak.framework.accounting.measured import MeasuredMoney

# A measured-zero seed for folding leg amounts. ``MeasuredMoney`` intentionally
# defines no ``__radd__`` / ``__mul__`` (so ``sum(...)`` and scalar arithmetic
# can't silently fabricate a value), therefore every aggregation MUST start from
# an explicit measured zero and fold with ``+``. Module-level constant so the
# seed is shared and never re-typed by hand at call sites.
_MEASURED_ZERO = MeasuredMoney.measured(Decimal("0"))


class MoneyLegRole(Enum):
    """What a money leg *is* relative to the intent that moves it.

    A closed set so the ledger dispatcher (US-009) can map legs onto the flat
    ``transaction_ledger`` columns by role rather than by guessing from
    loosely-typed intent attributes.

    Attributes:
        INPUT: A token the action *spends* (e.g. a SWAP ``from_token``, an
            LP deposit). Projects onto ``token_in`` / ``amount_in``.
        OUTPUT: A token the action *receives* (e.g. a SWAP ``to_token``, LP
            close proceeds). Projects onto ``token_out`` / ``amount_out``.
        PRINCIPAL: The notional the action operates *on* without it being a
            two-sided swap — perp collateral, a lending repay / withdraw /
            supply / borrow amount. Projects onto ``token_in`` / ``amount_in``,
            but is tagged distinctly so a consumer never mistakes a principal
            for a swap input.
    """

    INPUT = "input"
    OUTPUT = "output"
    PRINCIPAL = "principal"


@dataclass(frozen=True, slots=True)
class PrimitiveMoneyLeg:
    """One typed money leg of an intent: ``(role, token, amount)``.

    Immutable value object. Build via the intention-revealing classmethods
    (:meth:`input`, :meth:`output`, :meth:`principal`) rather than the raw
    constructor; the raw constructor still validates its invariants so an
    ill-typed leg is impossible to build.

    Attributes:
        role: The leg's :class:`MoneyLegRole`.
        token: Token identity as the ledger records it — a symbol (``"USDC"``)
            or an address (``"0x..."``). May be ``""`` when the connector knows
            an amount but not yet the token identity (Empty≠Zero: an unknown
            token is the empty string, never a fabricated one); validated to be
            a ``str``.
        amount: The leg amount as a :class:`MeasuredMoney` in **human units**
            (never raw on-chain integers — the VIB-5036 ledger contract). The
            three Empty≠Zero states (measured / unmeasured / absent) are carried
            by the type, so a missing leg amount can never masquerade as a
            measured zero.
    """

    role: MoneyLegRole
    token: str
    amount: MeasuredMoney

    def __post_init__(self) -> None:
        if not isinstance(self.role, MoneyLegRole):
            raise TypeError(f"PrimitiveMoneyLeg.role must be a MoneyLegRole, got {type(self.role).__name__}.")
        if not isinstance(self.token, str):
            raise TypeError(
                "PrimitiveMoneyLeg.token must be a str (symbol or address; '' when unknown), "
                f"got {type(self.token).__name__}."
            )
        if not isinstance(self.amount, MeasuredMoney):
            raise TypeError(
                "PrimitiveMoneyLeg.amount must be a MeasuredMoney (carries Empty≠Zero by "
                f"construction), got {type(self.amount).__name__}. Wrap via "
                "MeasuredMoney.measured() / .unmeasured() / .absent() / .from_raw()."
            )

    # -- explicit constructors -------------------------------------------------

    @classmethod
    def input(cls, token: str, amount: MeasuredMoney) -> PrimitiveMoneyLeg:
        """A token the action spends (→ ``token_in`` / ``amount_in``)."""
        return cls(MoneyLegRole.INPUT, token, amount)

    @classmethod
    def output(cls, token: str, amount: MeasuredMoney) -> PrimitiveMoneyLeg:
        """A token the action receives (→ ``token_out`` / ``amount_out``)."""
        return cls(MoneyLegRole.OUTPUT, token, amount)

    @classmethod
    def principal(cls, token: str, amount: MeasuredMoney) -> PrimitiveMoneyLeg:
        """The notional the action operates on (collateral / lending amount)."""
        return cls(MoneyLegRole.PRINCIPAL, token, amount)

    # -- serialization ---------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """A JSON-safe view for the ``transaction_ledger.extracted_data`` blob.

        Mirrors the ``to_dict()`` convention the other typed extracted-data
        objects follow (``SwapAmounts`` / ``LPOpenData`` / …) so
        ``serialize_extracted_data`` records a clean, queryable dict (tagged with
        ``_type``) instead of a Python ``repr`` string. The amount serializes via
        :meth:`MeasuredMoney.to_payload` so the three Empty≠Zero states survive
        the blob: measured → ``str(amount)``, unmeasured → ``None``, absent → ``""``.
        """
        return {"role": self.role.value, "token": self.token, "amount": self.amount.to_payload()}


@dataclass(frozen=True, slots=True)
class PrimitiveMoneyLegs:
    """The full set of money legs a connector declares for one intent.

    Models an intent's input leg(s), output leg(s), and principal source(s) as
    one typed object — the unit the ledger dispatcher (US-009) will consume in
    place of guessing. Immutable; ``legs`` is normalized to a ``tuple`` so the
    object is hashable and safe to share.

    The aggregation helpers (:meth:`total`, :meth:`total_input`,
    :meth:`total_output`, :meth:`total_principal`) seed with a measured zero and
    fold with ``MeasuredMoney.__add__``, so a single unmeasured / absent leg
    propagates: the total of a role is measured **iff every** leg of that role
    is measured (an empty role totals to a measured zero — there is nothing
    un-measured to taint it). Summing is only *semantically* meaningful across
    same-denomination legs (e.g. two USD-denominated principal legs); its
    primary purpose is faithful Empty≠Zero state propagation, not cross-token
    arithmetic.

    ``legs`` is annotated ``Sequence`` because the constructor accepts any
    sequence (``list`` / ``tuple``) and :meth:`__post_init__` normalizes it to
    an immutable ``tuple`` for hashing; downstream reads see that tuple.
    """

    legs: Sequence[PrimitiveMoneyLeg]

    def __post_init__(self) -> None:
        # Accept any iterable of legs but store an immutable, hashable tuple.
        legs = tuple(self.legs)
        for leg in legs:
            if not isinstance(leg, PrimitiveMoneyLeg):
                raise TypeError(
                    f"PrimitiveMoneyLegs.legs must contain only PrimitiveMoneyLeg, got {type(leg).__name__}."
                )
        # Frozen dataclass: bypass the immutability guard to normalize once.
        object.__setattr__(self, "legs", legs)

    @classmethod
    def of(cls, *legs: PrimitiveMoneyLeg) -> PrimitiveMoneyLegs:
        """Build from positional legs: ``PrimitiveMoneyLegs.of(a, b, c)``."""
        return cls(legs)

    @classmethod
    def stake_mint(
        cls,
        *,
        staked_token: str,
        staked_amount: MeasuredMoney,
        minted_token: str,
        minted_amount: MeasuredMoney,
    ) -> PrimitiveMoneyLegs:
        """The canonical stake/mint money-leg pair: an INPUT staked asset and an
        OUTPUT minted receipt token.

        The shape is the *pattern*, not the protocol — any stake-and-mint connector
        (Lido ETH→stETH/wstETH, Ethena USDe→sUSDe, …) declares its STAKE legs with
        this one constructor rather than re-deriving the role layout. It projects
        onto the flat ledger as a two-sided action (INPUT → ``token_in`` /
        ``amount_in``; OUTPUT → ``token_out`` / ``amount_out``) via
        :func:`~almanak.framework.observability.ledger._extract_from_declared_legs`.

        Amounts are :class:`MeasuredMoney`, so Empty≠Zero is carried by
        construction: an unresolved mint amount stays unmeasured and projects to
        ``""`` — never a fabricated measured zero (blueprint 27 §10.10).
        """
        return cls.of(
            PrimitiveMoneyLeg.input(staked_token, staked_amount),
            PrimitiveMoneyLeg.output(minted_token, minted_amount),
        )

    # -- role views ------------------------------------------------------------

    @staticmethod
    def _require_role(role: MoneyLegRole) -> None:
        """Fail loud on a non-enum role.

        A typo'd string (``"input"`` instead of ``MoneyLegRole.INPUT``) would
        otherwise match no leg via ``is`` and silently return an empty view /
        a measured zero — exactly the silent-wrong-value the Empty≠Zero
        discipline exists to prevent. Mirrors the type validation in
        :meth:`PrimitiveMoneyLeg.__post_init__`.
        """
        if not isinstance(role, MoneyLegRole):
            raise TypeError(f"role must be a MoneyLegRole, got {type(role).__name__}.")

    def by_role(self, role: MoneyLegRole) -> tuple[PrimitiveMoneyLeg, ...]:
        """All legs with ``role``, in declaration order."""
        self._require_role(role)
        return tuple(leg for leg in self.legs if leg.role is role)

    @property
    def input_legs(self) -> tuple[PrimitiveMoneyLeg, ...]:
        """The INPUT legs, in declaration order."""
        return self.by_role(MoneyLegRole.INPUT)

    @property
    def output_legs(self) -> tuple[PrimitiveMoneyLeg, ...]:
        """The OUTPUT legs, in declaration order."""
        return self.by_role(MoneyLegRole.OUTPUT)

    @property
    def principal_legs(self) -> tuple[PrimitiveMoneyLeg, ...]:
        """The PRINCIPAL legs, in declaration order."""
        return self.by_role(MoneyLegRole.PRINCIPAL)

    # -- aggregation (propagates Empty≠Zero state) -----------------------------

    def total(self, role: MoneyLegRole) -> MeasuredMoney:
        """Sum the amounts of all legs with ``role``, seeded with measured zero.

        Folds with ``MeasuredMoney.__add__`` so any unmeasured / absent leg of
        the role makes the total non-measured (the join of the leg states). An
        empty role totals to a measured zero — the seed — because there is no
        leg whose missing measurement could taint it.
        """
        self._require_role(role)
        acc = _MEASURED_ZERO
        for leg in self.legs:
            if leg.role is role:
                acc = acc + leg.amount
        return acc

    def total_input(self) -> MeasuredMoney:
        """Total of the INPUT legs (measured iff every input leg is measured)."""
        return self.total(MoneyLegRole.INPUT)

    def total_output(self) -> MeasuredMoney:
        """Total of the OUTPUT legs (measured iff every output leg is measured)."""
        return self.total(MoneyLegRole.OUTPUT)

    def total_principal(self) -> MeasuredMoney:
        """Total of the PRINCIPAL legs (measured iff every principal leg is measured)."""
        return self.total(MoneyLegRole.PRINCIPAL)

    # -- serialization ---------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """A JSON-safe view (list of leg dicts) for the ledger extracted-data blob.

        Lets ``serialize_extracted_data`` (``observability/ledger.py``) record the
        declared legs as a clean, tagged dict — the same ``to_dict()`` contract the
        other typed extracted-data objects honour — rather than a ``repr`` string.
        """
        return {"legs": [leg.to_dict() for leg in self.legs]}


__all__ = [
    "MoneyLegRole",
    "PrimitiveMoneyLeg",
    "PrimitiveMoneyLegs",
]
