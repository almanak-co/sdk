"""Gateway schema helpers.

Historically this module ran Postgres DDL at gateway startup. That path
silently drifted production and has been retired: the ``metrics-database``
repo's migrations are now the sole source of Postgres schema. This module
retains shared URL helpers plus a reference copy of the legacy DDL; the SDK
must not execute Postgres schema changes at gateway startup.
"""

from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


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
CREATE TABLE IF NOT EXISTS agent_state (
    deployment_id           TEXT PRIMARY KEY,
    state                   TEXT NOT NULL,
    state_changed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat_at       TIMESTAMPTZ,
    error_message           TEXT,
    iteration_count         BIGINT DEFAULT 0,
    source                  TEXT NOT NULL DEFAULT 'gateway',
    running_almanak_version TEXT
);

-- Migration: add source column to existing installations
ALTER TABLE agent_state ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'gateway';
-- Migration: add running_almanak_version column to existing installations
ALTER TABLE agent_state ADD COLUMN IF NOT EXISTS running_almanak_version TEXT;

CREATE TABLE IF NOT EXISTS agent_command (
    id            BIGSERIAL PRIMARY KEY,
    deployment_id TEXT NOT NULL,
    command       TEXT NOT NULL,
    issued_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    issued_by     TEXT NOT NULL,
    processed_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_agent_command_pending
    ON agent_command (deployment_id, id DESC)
    WHERE processed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_agent_state_heartbeat
    ON agent_state (state, last_heartbeat_at)
    WHERE state = 'RUNNING';

-- Strategy state tables ----------------------------------------------------
-- Single row per deployment with CAS (Compare-And-Swap) via version field.
-- Each gateway serves exactly one strategy; no two strategies share a gateway.
CREATE TABLE IF NOT EXISTS strategy_state (
    deployment_id   TEXT PRIMARY KEY,
    version         BIGINT NOT NULL DEFAULT 1,
    state_data      JSONB NOT NULL,
    schema_version  INTEGER NOT NULL DEFAULT 1,
    checksum        VARCHAR(64),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Portfolio snapshots table -------------------------------------------------
CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id                 BIGSERIAL PRIMARY KEY,
    deployment_id      TEXT NOT NULL,
    timestamp          TIMESTAMPTZ NOT NULL,
    iteration_number   INTEGER DEFAULT 0,
    total_value_usd    TEXT NOT NULL,
    available_cash_usd TEXT NOT NULL,
    value_confidence   TEXT DEFAULT 'HIGH',
    positions_json     JSONB NOT NULL,
    chain              TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_portfolio_snapshots_agent_time
    ON portfolio_snapshots (deployment_id, timestamp);

CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_cleanup
    ON portfolio_snapshots (created_at);

-- Migration: add Phase 4 columns to portfolio_snapshots
ALTER TABLE portfolio_snapshots ADD COLUMN IF NOT EXISTS cycle_id TEXT DEFAULT '';
ALTER TABLE portfolio_snapshots ADD COLUMN IF NOT EXISTS execution_mode TEXT DEFAULT '';

-- Portfolio metrics table ---------------------------------------------------
CREATE TABLE IF NOT EXISTS portfolio_metrics (
    deployment_id     TEXT PRIMARY KEY,
    initial_value_usd TEXT NOT NULL,
    initial_timestamp TIMESTAMPTZ NOT NULL,
    deposits_usd      TEXT DEFAULT '0',
    withdrawals_usd   TEXT DEFAULT '0',
    gas_spent_usd     TEXT DEFAULT '0',
    cycle_id          TEXT DEFAULT '',
    execution_mode    TEXT DEFAULT '',
    is_complete       BOOLEAN DEFAULT TRUE,
    total_value_usd   TEXT DEFAULT '0',
    positions_json    JSONB DEFAULT '[]',
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Migration: add Phase 4 columns to portfolio_metrics
ALTER TABLE portfolio_metrics ADD COLUMN IF NOT EXISTS cycle_id TEXT DEFAULT '';
ALTER TABLE portfolio_metrics ADD COLUMN IF NOT EXISTS execution_mode TEXT DEFAULT '';
ALTER TABLE portfolio_metrics ADD COLUMN IF NOT EXISTS is_complete BOOLEAN DEFAULT TRUE;
ALTER TABLE portfolio_metrics ADD COLUMN IF NOT EXISTS total_value_usd TEXT DEFAULT '0';
ALTER TABLE portfolio_metrics ADD COLUMN IF NOT EXISTS positions_json JSONB DEFAULT '[]';

-- CLOB orders table ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS clob_orders (
    id                 BIGSERIAL PRIMARY KEY,
    deployment_id      TEXT NOT NULL,
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

CREATE UNIQUE INDEX IF NOT EXISTS idx_clob_orders_agent_order
    ON clob_orders (deployment_id, order_id);

CREATE INDEX IF NOT EXISTS idx_clob_orders_status
    ON clob_orders (deployment_id, status);

CREATE INDEX IF NOT EXISTS idx_clob_orders_market
    ON clob_orders (deployment_id, market_id, status);

-- Timeline events table (dashboard data) ------------------------------------
CREATE TABLE IF NOT EXISTS timeline_events (
    id            BIGSERIAL PRIMARY KEY,
    event_id      TEXT NOT NULL UNIQUE,
    deployment_id TEXT NOT NULL,
    timestamp     TIMESTAMPTZ NOT NULL,
    event_type    TEXT NOT NULL,
    description   TEXT,
    tx_hash       TEXT,
    chain         TEXT,
    details_json  JSONB,
    cycle_id      TEXT DEFAULT '',
    phase         TEXT DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_timeline_events_agent_time
    ON timeline_events (deployment_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_timeline_events_agent_type
    ON timeline_events (deployment_id, event_type);

CREATE INDEX IF NOT EXISTS idx_timeline_events_cycle_id
    ON timeline_events (cycle_id) WHERE cycle_id != '';

-- Transaction ledger -- structured trade records (VIB-2402) ----------------
CREATE TABLE IF NOT EXISTS transaction_ledger (
    id                TEXT PRIMARY KEY,
    cycle_id          TEXT NOT NULL,
    deployment_id     TEXT NOT NULL,
    timestamp         TIMESTAMPTZ NOT NULL,
    intent_type       TEXT NOT NULL,
    token_in          TEXT,
    amount_in         TEXT,
    token_out         TEXT,
    amount_out        TEXT,
    effective_price   TEXT,
    slippage_bps      REAL,
    gas_used          BIGINT,
    gas_usd           TEXT,
    tx_hash           TEXT,
    chain             TEXT,
    protocol          TEXT,
    success           BOOLEAN NOT NULL DEFAULT TRUE,
    error             TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_transaction_ledger_agent_time
    ON transaction_ledger (deployment_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_transaction_ledger_cycle_id
    ON transaction_ledger (cycle_id);

CREATE INDEX IF NOT EXISTS idx_transaction_ledger_intent_type
    ON transaction_ledger (deployment_id, intent_type);

-- Migration: add Phase 4 columns to transaction_ledger
ALTER TABLE transaction_ledger ADD COLUMN IF NOT EXISTS execution_mode TEXT DEFAULT '';
"""
