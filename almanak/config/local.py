"""Local-mode configuration sibling.

Phase 0 skeleton: empty subclass of :class:`BaseConfig`. Mode-specific fields
land in later phases:

* Phase 4 — ``strategy_folder``, ``db_path``, ``private_key``, ``backtest``,
  ``simulation`` (the local runner + paper / backtest surface).

See ``docs/internal/config-service-plan.md`` for the full migration order.
"""

from almanak.config.base import BaseConfig


class LocalConfig(BaseConfig):
    """Local-mode config (folder-scoped, sqlite-backed).

    Phase 0 skeleton — no fields beyond ``BaseConfig.gateway``. Subsequent
    phases attach the local-only submodels (runtime, simulation, backtest)
    and secrets sourced from ``.env``.
    """


__all__ = ["LocalConfig"]
