"""Typed accessor for the per-primitive ``matching_policy_version`` map.

VIB-4195 (T09 of the multi-position-tracking shred): wraps the existing
:data:`almanak.framework.accounting.payload_schemas.MATCHING_POLICY_VERSIONS`
dict (landed in PR #2192 / VIB-4162) in a single typed accessor so callers
index by typed :class:`Primitive` enum, not by raw dict key. The dict
itself is **NOT relocated** — only wrapped. Both the existing
:func:`almanak.framework.accounting.writer.augment_accounting_payload`
chokepoint and the upcoming ``save_ledger_and_registry`` atomic primitive
(VIB-4197 / T11) consume the same accessor, so a future bump (e.g. LP v3
→ v4) is one edit applied uniformly across both write lanes.

Attribute-rebind safety
-----------------------

The accessor reads the source module's attribute at **call-time** rather
than capturing a local-name binding at import-time. A
``payload_schemas.MATCHING_POLICY_VERSIONS = {...}`` rebind on the source
module (used by tests + future migrations) is observed by the accessor;
it is NOT shadowed by a stale snapshot. The same property protects against
the silently-stale-version-stamp anti-pattern: if a future migration ever
replaces the dict object on the source module, every accessor caller
follows automatically.

Both ``MATCHING_POLICY_VERSIONS.clear()`` (in-place mutation) and
``payload_schemas.MATCHING_POLICY_VERSIONS = {}`` (attribute rebind)
surface as :class:`KeyError` from the accessor — never as a silent
default. See ``docs/internal/uat-cards/VIB-4195.md`` D3.F6 for the
fault-injection contract.
"""

from __future__ import annotations

from almanak.framework.accounting import payload_schemas
from almanak.framework.primitives.types import Primitive


class MatchingPolicy:
    """Typed accessor namespace for ``matching_policy_version`` lookups.

    The class has no instance state — :meth:`for_primitive` is a static
    method on the namespace. Calling ``MatchingPolicy()`` is meaningless
    and not part of the public surface.
    """

    @staticmethod
    def for_primitive(p: Primitive) -> int:
        """Return the per-primitive ``matching_policy_version`` for ``p``.

        Reads
        :data:`almanak.framework.accounting.payload_schemas.MATCHING_POLICY_VERSIONS`
        at call-time. Raises :class:`KeyError` if ``p`` is not present in
        the dict (whether because a Primitive was added without updating
        the dict, or because the dict was cleared / rebound by a test).
        Never returns a silent default.

        Type guard
        ----------

        :class:`Primitive` is a :class:`enum.StrEnum`, so
        ``MATCHING_POLICY_VERSIONS["lp"]`` would silently succeed —
        ``Primitive.LP`` hashes equal to its string value. That is exactly
        the silent-default vulnerability VIB-4162 was filed to prevent (a
        caller that thinks it's looking up "the LP version" via a stringly-
        typed payload field would silently get the right answer 99% of the
        time and the wrong answer when the same string is keyed somewhere
        else). The accessor enforces ``isinstance(p, Primitive)`` BEFORE
        the lookup so any non-``Primitive`` input — including a string that
        collides with a Primitive's value — raises :class:`TypeError`.

        Parameters
        ----------
        p
            A :class:`Primitive` enum member.

        Returns
        -------
        int
            The matching-policy version stamped on every typed accounting
            event whose ``record_for(event_type).primitive == p``.

        Raises
        ------
        TypeError
            ``p`` is not a :class:`Primitive` instance.
        KeyError
            ``p`` is a :class:`Primitive` but not a key in
            ``payload_schemas.MATCHING_POLICY_VERSIONS`` (the dict has
            been mutated / cleared / rebound, or a new ``Primitive`` was
            added to the enum without updating the dict).
        """
        if not isinstance(p, Primitive):
            raise TypeError(
                f"MatchingPolicy.for_primitive expected a Primitive enum member, "
                f"got {type(p).__name__}({p!r}). Primitive is a StrEnum, so a bare "
                f"string would silently collide with the enum's value — pass "
                f"Primitive.<NAME> explicitly."
            )
        # Resolve through the source module so attribute rebinds on
        # `payload_schemas` are observed (rebind safety contract). Do NOT
        # rebind `MATCHING_POLICY_VERSIONS` to a local name here — that
        # would freeze the dict-object identity at import time and a
        # rebind on the source module would be silently ignored.
        return payload_schemas.MATCHING_POLICY_VERSIONS[p]
