"""Centralized PostgreSQL schema management for the gateway.

All PostgreSQL DDL (lifecycle tables, strategy state tables) is consolidated
here and applied once at gateway startup via ``ensure_schema()``.  Individual
stores no longer create their own tables -- they rely on this module.
"""

import logging
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

logger = logging.getLogger(__name__)


def _strip_schema_param(database_url: str) -> tuple[str, str | None]:
    """Strip ``schema`` query parameter from a database URL.

    asyncpg does not support a ``?schema=`` query parameter, so we strip it
    and return the schema name separately.  Callers should set
    ``search_path`` on each connection instead.

    Returns:
        ``(clean_url, schema_name)`` -- *schema_name* is ``None`` when the
        parameter is absent.
    """
    parsed = urlparse(database_url)
    params = parse_qsl(parsed.query, keep_blank_values=True)
    schema = next((value for key, value in params if key == "schema"), None)
    clean_params = [(key, value) for key, value in params if key != "schema"]
    if schema == "":
        schema = None
    if schema is None and len(clean_params) == len(params):
        return database_url, None
    clean_query = urlencode(clean_params)
    clean_url = urlunparse(parsed._replace(query=clean_query))
    return clean_url, schema


# ---------------------------------------------------------------------------
# Consolidated DDL
# ---------------------------------------------------------------------------

POSTGRES_SCHEMA = """
-- Lifecycle tables ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS v2_agent_state (
    agent_id          TEXT PRIMARY KEY,
    state             TEXT NOT NULL,
    state_changed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat_at TIMESTAMPTZ,
    error_message     TEXT,
    iteration_count   BIGINT DEFAULT 0,
    source            TEXT NOT NULL DEFAULT 'gateway'
);

-- Migration: add source column to existing installations
ALTER TABLE v2_agent_state ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'gateway';

CREATE TABLE IF NOT EXISTS v2_agent_command (
    id            BIGSERIAL PRIMARY KEY,
    agent_id      TEXT NOT NULL,
    command       TEXT NOT NULL,
    issued_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    issued_by     TEXT NOT NULL,
    processed_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_v2_agent_command_pending
    ON v2_agent_command (agent_id, id DESC)
    WHERE processed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_v2_agent_state_heartbeat
    ON v2_agent_state (state, last_heartbeat_at)
    WHERE state = 'RUNNING';

-- Strategy state tables ----------------------------------------------------
-- Single row per agent with CAS (Compare-And-Swap) via version field.
-- Each gateway serves exactly one strategy; no two strategies share a gateway.
CREATE TABLE IF NOT EXISTS v2_strategy_state (
    agent_id        TEXT PRIMARY KEY,
    version         BIGINT NOT NULL DEFAULT 1,
    state_data      JSONB NOT NULL,
    schema_version  INTEGER NOT NULL DEFAULT 1,
    checksum        VARCHAR(64),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Portfolio snapshots table -------------------------------------------------
CREATE TABLE IF NOT EXISTS v2_portfolio_snapshots (
    id                 BIGSERIAL PRIMARY KEY,
    agent_id           TEXT NOT NULL,
    timestamp          TIMESTAMPTZ NOT NULL,
    iteration_number   INTEGER DEFAULT 0,
    total_value_usd    TEXT NOT NULL,
    available_cash_usd TEXT NOT NULL,
    value_confidence   TEXT DEFAULT 'HIGH',
    positions_json     JSONB NOT NULL,
    chain              TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_portfolio_snapshots_agent_time
    ON v2_portfolio_snapshots (agent_id, timestamp);

CREATE INDEX IF NOT EXISTS idx_v2_portfolio_snapshots_cleanup
    ON v2_portfolio_snapshots (created_at);

-- Portfolio metrics table ---------------------------------------------------
CREATE TABLE IF NOT EXISTS v2_portfolio_metrics (
    agent_id          TEXT PRIMARY KEY,
    initial_value_usd TEXT NOT NULL,
    initial_timestamp TIMESTAMPTZ NOT NULL,
    deposits_usd      TEXT DEFAULT '0',
    withdrawals_usd   TEXT DEFAULT '0',
    gas_spent_usd     TEXT DEFAULT '0',
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- CLOB orders table ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS v2_clob_orders (
    id                 BIGSERIAL PRIMARY KEY,
    agent_id           TEXT NOT NULL,
    order_id           TEXT NOT NULL,
    market_id          TEXT NOT NULL,
    token_id           TEXT NOT NULL,
    side               TEXT NOT NULL,
    status             TEXT NOT NULL,
    price              TEXT NOT NULL,
    size               TEXT NOT NULL,
    filled_size        TEXT DEFAULT '0',
    average_fill_price TEXT,
    fills              JSONB DEFAULT '[]',
    order_type         TEXT DEFAULT 'GTC',
    intent_id          TEXT,
    error              TEXT,
    metadata           JSONB DEFAULT '{}',
    submitted_at       TIMESTAMPTZ NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_v2_clob_orders_agent_order
    ON v2_clob_orders (agent_id, order_id);

CREATE INDEX IF NOT EXISTS idx_v2_clob_orders_status
    ON v2_clob_orders (agent_id, status);

CREATE INDEX IF NOT EXISTS idx_v2_clob_orders_market
    ON v2_clob_orders (agent_id, market_id, status);
"""


async def ensure_schema(database_url: str) -> None:
    """Create all gateway PostgreSQL tables (idempotent).

    Opens a short-lived asyncpg connection, runs the consolidated DDL, and
    disconnects.  Safe to call on every startup -- every statement uses
    ``CREATE TABLE IF NOT EXISTS`` / ``CREATE INDEX IF NOT EXISTS``.

    If the URL contains a ``?schema=`` parameter the corresponding
    ``search_path`` is set before executing DDL so that tables land in the
    correct schema.
    """
    import asyncpg

    clean_url, schema = _strip_schema_param(database_url)

    conn = await asyncpg.connect(clean_url, statement_cache_size=0)
    try:
        async with conn.transaction():
            if schema:
                await conn.fetchval(
                    "SELECT pg_catalog.set_config('search_path', $1, true)",
                    schema,
                )
            await conn.execute(POSTGRES_SCHEMA)
    finally:
        await conn.close()
