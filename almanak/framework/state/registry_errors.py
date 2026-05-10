"""Typed exceptions for the position registry write surface.

Separate module so it does NOT subclass :class:`AccountingPersistenceError`.
Per PRD §"Loud Failure, Not Confusing Failure" and Linear ticket VIB-4200,
the auto-mode collision case MUST be a distinct exception class — a caller
that does ``except AccountingPersistenceError`` MUST NOT inadvertently
swallow the collision case.

The error is raised by the local SQLite backend when the partial unique
index ``ix_registry_auto_mode`` rejects a second handle-less ``status='open'``
write for the same ``(deployment_id, chain, accounting_category,
semantic_grouping_key)`` group. The Postgres equivalent ships in T19
(VIB-4205) via the metrics-database repo migration; this module's surface
is mode-agnostic so the hosted-mode RPC handler will reuse it without
modification.

Design rationale:

- **Distinct from AccountingPersistenceError** — collisions are programming
  bugs (the strategy author forgot a ``registry_handle``), not
  infrastructure failures (DB unreachable, schema drift). The VIB-3762
  paper-mode-leniency rule that lets ``AccountingPersistenceError`` log+continue
  in paper mode does NOT apply: a collision MUST surface uniformly across
  ``live`` / ``paper`` / ``dry_run`` so the bug cannot ship to live
  unnoticed.

- **Doc-pointer URL in message** — strategy authors read this error during
  incident response. The message MUST contain a URL the author can paste
  into a browser to reach the relevant blueprint section, AND the
  conflicting-row metadata so the author can grep their logs and
  ``position_registry`` for the colliding group.

- **≤3 lines** — operators scan logs in pagers / dashboards; a 10-line
  stack-trace-shaped message is unreadable.

- **NO taxonomy lookup** — this module operates on already-validated
  ``RegistryRow`` instances and ``position_registry`` row reads. It does NOT
  call :func:`record_for` or :func:`classify` on intent strings; those
  validations happen upstream in ``commit.py``. Static anti-bypass tests
  pin this discipline.
"""

from __future__ import annotations

# Stable, public URL the error message points at. The blueprint lives at
# this path in every checkout; rendering at this URL works for the public
# almanak-sdk mirror and (with a redirect) for any internal MkDocs site.
# The choice of an explicit URL rather than a repo-relative path satisfies
# the Linear ticket's "doc-pointer URL" criterion regardless of where
# the operator reads the log from.
DOC_POINTER_URL = "https://github.com/Almanak-Labs/almanak-sdk/blob/main/blueprints/28-position-registry.md"


class RegistryAutoCollisionError(Exception):
    """Raised when an auto-mode (no ``registry_handle``) registry open
    collides with an existing open row in the same partial-unique-index
    group.

    The partial unique index ``ix_registry_auto_mode`` is defined on
    ``(deployment_id, chain, accounting_category, semantic_grouping_key)``
    where ``status = 'open' AND handle IS NULL`` (see
    ``almanak/framework/state/backends/sqlite.py:677``). It enforces the
    contract: at most one handle-less open row per
    ``(deployment_id, chain, accounting_category, semantic_grouping_key)``
    group.

    A second open with a *different* ``physical_identity_hash`` in the same
    group violates this index and produces this exception. (A second open
    with the *same* ``physical_identity_hash`` is the idempotent-retry
    case and is handled by the ``ON CONFLICT (deployment_id, chain,
    primitive, physical_identity_hash) DO UPDATE`` clause without raising.)

    The fix from the strategy author's perspective is to supply a
    ``registry_handle`` on the second open — the partial index excludes
    rows where ``handle IS NOT NULL``, so two opens in the same group with
    distinct handles coexist.

    This class is intentionally NOT a subclass of
    :class:`almanak.framework.state.exceptions.AccountingPersistenceError`.
    A caller that does ``except AccountingPersistenceError`` MUST NOT match
    this exception. Callers that want to handle the collision case
    explicitly must catch :class:`RegistryAutoCollisionError`.

    Attributes:
        semantic_grouping_key: The conflicting group key (e.g.
            ``"arbitrum:0xC31..."`` for UniV3 LP, ``"arbitrum:0xMARKET:1234567890"``
            for Pendle LP).
        existing_physical_identity_hash: The ``physical_identity_hash`` of
            the row that already occupies the group (the winner).
        opened_tx: The ``opened_tx`` of the existing row, so an operator
            can grep the log / chain explorer for the winning transaction.
        accounting_category: The conflicting ``accounting_category`` enum
            string value (``'lp'``, ``'pendle_lp'``, ``'perp'``, …).

    Raised by:
        :meth:`almanak.framework.state.backends.sqlite.SQLiteStore.save_ledger_and_registry_atomic`
        on the auto-mode path. The :class:`StateManager` wrapper lets the
        exception propagate without conversion to
        :class:`AccountingPersistenceError`.
    """

    def __init__(
        self,
        *,
        semantic_grouping_key: str,
        existing_physical_identity_hash: str,
        opened_tx: str,
        accounting_category: str,
    ) -> None:
        # Validate inputs to fail fast on a buggy detector that hands us
        # empty strings (would render an unactionable error message). Each
        # field is load-bearing for the operator's incident-response grep.
        if not semantic_grouping_key:
            raise ValueError(
                "RegistryAutoCollisionError.semantic_grouping_key must be non-empty",
            )
        if not existing_physical_identity_hash:
            raise ValueError(
                "RegistryAutoCollisionError.existing_physical_identity_hash must be non-empty",
            )
        # ``opened_tx`` may be empty in legacy/back-filled rows where the
        # original transaction hash was not captured (see
        # ``docs/internal/migration-cutover-position-registry.md`` §3.5). We
        # warn rather than reject — the error is still actionable from
        # ``semantic_grouping_key`` + ``existing_physical_identity_hash``.
        # Empty string is preserved verbatim so callers can detect the
        # legacy case from the field.
        if not accounting_category:
            raise ValueError(
                "RegistryAutoCollisionError.accounting_category must be non-empty",
            )

        self.semantic_grouping_key: str = semantic_grouping_key
        self.existing_physical_identity_hash: str = existing_physical_identity_hash
        self.opened_tx: str = opened_tx
        self.accounting_category: str = accounting_category

        # Three-line scannable message. Each line carries one piece of
        # incident-response signal:
        #   Line 1: what failed + the conflicting group (operator greps log
        #           for the group key).
        #   Line 2: existing-row identity (operator queries position_registry
        #           by hash + opened_tx).
        #   Line 3: doc-pointer URL (author learns the rule, supplies a
        #           registry_handle on the next iteration).
        # The message is constructed in ``__init__`` so ``str(exc)`` always
        # returns the same text — repeated formatting in __str__ would risk
        # drift if the fields are mutated post-init (defensive, even though
        # the fields are immutable-by-convention).
        message = (
            f"Auto-mode registry collision on accounting_category={accounting_category!r} "
            f"semantic_grouping_key={semantic_grouping_key!r} "
            f"(existing position has the group; supply a unique registry_handle to coexist).\n"
            f"Existing: physical_identity_hash={existing_physical_identity_hash!r} "
            f"opened_tx={opened_tx!r}.\n"
            f"See {DOC_POINTER_URL} for the auto-mode collision rule."
        )
        super().__init__(message)


__all__ = ["RegistryAutoCollisionError", "DOC_POINTER_URL"]
