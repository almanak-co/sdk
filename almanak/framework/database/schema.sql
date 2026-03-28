-- Almanak Strategy Framework PostgreSQL Schema
-- Version: 2.0
-- Description: Complete schema for strategy state persistence, event tracking, and operator tools

-- ============================================================================
-- EXTENSIONS
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================================
-- CUSTOM TYPES (ENUMS)
-- ============================================================================

-- Strategy operational status
CREATE TYPE strategy_status AS ENUM (
    'INITIALIZING',
    'RUNNING',
    'PAUSED',
    'STUCK',
    'ERROR',
    'TERMINATED'
);

-- Supported blockchain networks
CREATE TYPE chain_type AS ENUM (
    'ethereum',
    'arbitrum',
    'optimism',
    'polygon',
    'base',
    'avalanche',
    'bsc'
);

-- Strategy event types for timeline
CREATE TYPE event_type AS ENUM (
    -- Transaction events
    'TX_SUBMITTED',
    'TX_CONFIRMED',
    'TX_FAILED',
    'TX_CANCELLED',
    'TX_REPLACED',
    -- Position events
    'POSITION_OPENED',
    'POSITION_CLOSED',
    'POSITION_ADJUSTED',
    -- State events
    'STATE_CHANGED',
    'STUCK_DETECTED',
    'STUCK_RESOLVED',
    -- Risk events
    'RISK_GUARD_TRIGGERED',
    'CIRCUIT_BREAKER_TRIGGERED',
    -- Config events
    'CONFIG_UPDATED',
    'CONFIG_ROLLBACK',
    -- Operator events
    'OPERATOR_ACTION_REQUESTED',
    'OPERATOR_ACTION_EXECUTED',
    'OPERATOR_ACTION_CANCELLED',
    -- Alert events
    'ALERT_TRIGGERED',
    'ALERT_ACKNOWLEDGED',
    'ALERT_ESCALATED',
    'ALERT_RESOLVED',
    -- Lifecycle events
    'STRATEGY_DEPLOYED',
    'STRATEGY_PAUSED',
    'STRATEGY_RESUMED',
    'STRATEGY_TERMINATED',
    -- Error events
    'ERROR_OCCURRED',
    'ERROR_RECOVERED'
);

-- Stuck reason classifications
CREATE TYPE stuck_reason AS ENUM (
    -- Transaction issues
    'GAS_PRICE_BLOCKED',
    'NONCE_CONFLICT',
    'TRANSACTION_REVERTED',
    'NOT_INCLUDED_TIMEOUT',
    -- Balance issues
    'INSUFFICIENT_BALANCE',
    'INSUFFICIENT_GAS',
    'ALLOWANCE_MISSING',
    -- Protocol issues
    'SLIPPAGE_EXCEEDED',
    'POOL_LIQUIDITY_LOW',
    'ORACLE_STALE',
    'PROTOCOL_PAUSED',
    -- System issues
    'RPC_FAILURE',
    'RECEIPT_PARSE_FAILED',
    'STATE_CONFLICT',
    -- Risk guard issues
    'RISK_GUARD_BLOCKED',
    'CIRCUIT_BREAKER',
    -- Unknown
    'UNKNOWN'
);

-- Alert severity levels
CREATE TYPE severity_level AS ENUM (
    'LOW',
    'MEDIUM',
    'HIGH',
    'CRITICAL'
);

-- Alert status
CREATE TYPE alert_status AS ENUM (
    'PENDING',
    'SENT',
    'ACKNOWLEDGED',
    'ESCALATED',
    'RESOLVED',
    'EXPIRED'
);

-- Available operator actions
CREATE TYPE operator_action AS ENUM (
    'BUMP_GAS',
    'CANCEL_TX',
    'PAUSE',
    'RESUME',
    'EMERGENCY_UNWIND',
    'RETRY',
    'SKIP',
    'MANUAL_REVIEW'
);

-- Operator card event types
CREATE TYPE card_event_type AS ENUM (
    'STUCK',
    'ERROR',
    'ALERT',
    'WARNING'
);

-- ============================================================================
-- TABLES
-- ============================================================================

-- --------------------------------------------------------------------------
-- strategies: Core strategy metadata and status
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS strategies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    chain chain_type NOT NULL,
    protocol VARCHAR(100),
    status strategy_status NOT NULL DEFAULT 'INITIALIZING',

    -- Strategy configuration
    config_json JSONB NOT NULL DEFAULT '{}',

    -- Metadata
    description TEXT,
    tags VARCHAR(50)[] DEFAULT ARRAY[]::VARCHAR[],

    -- Ownership
    owner_id VARCHAR(255),
    wallet_address VARCHAR(42),

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_active_at TIMESTAMP WITH TIME ZONE,
    paused_at TIMESTAMP WITH TIME ZONE,

    -- Constraints
    CONSTRAINT strategies_name_unique UNIQUE (name),
    CONSTRAINT strategies_wallet_format CHECK (
        wallet_address IS NULL OR wallet_address ~ '^0x[a-fA-F0-9]{40}$'
    )
);

-- --------------------------------------------------------------------------
-- v2_strategy_state: Single row per strategy with CAS (Compare-And-Swap) via version
-- --------------------------------------------------------------------------
-- This relational schema is distinct from the deployed gateway PostgreSQL
-- schema in almanak.gateway.database, which uses agent_id for platform mode.
CREATE TABLE IF NOT EXISTS v2_strategy_state (
    strategy_id UUID PRIMARY KEY REFERENCES strategies(id) ON DELETE CASCADE,

    -- Version for CAS semantics - incremented on each update
    version BIGINT NOT NULL DEFAULT 1,

    -- State data
    state_data JSONB NOT NULL,

    -- State metadata
    schema_version INTEGER NOT NULL DEFAULT 1,
    checksum VARCHAR(64),  -- SHA-256 of state_data for integrity verification

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT v2_strategy_state_version_positive CHECK (version > 0),
    CONSTRAINT v2_strategy_state_schema_version_positive CHECK (schema_version > 0)
);

-- --------------------------------------------------------------------------
-- strategy_events: Chronological event timeline for strategies
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS strategy_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    strategy_id UUID NOT NULL REFERENCES strategies(id) ON DELETE CASCADE,

    -- Event information
    event_type event_type NOT NULL,
    event_data JSONB NOT NULL DEFAULT '{}',
    description TEXT,

    -- Transaction context (optional)
    tx_hash VARCHAR(66),
    block_number BIGINT,

    -- Event metadata
    severity severity_level,
    source VARCHAR(100),  -- Component that generated the event

    -- Timestamps
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT strategy_events_tx_hash_format CHECK (
        tx_hash IS NULL OR tx_hash ~ '^0x[a-fA-F0-9]{64}$'
    ),
    CONSTRAINT strategy_events_block_positive CHECK (
        block_number IS NULL OR block_number >= 0
    )
);

-- --------------------------------------------------------------------------
-- strategy_versions: Code and config version tracking
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS strategy_versions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    strategy_id UUID NOT NULL REFERENCES strategies(id) ON DELETE CASCADE,

    -- Version identification
    version_id VARCHAR(100) NOT NULL,  -- e.g., v_strategy_20260115120000
    code_hash VARCHAR(64) NOT NULL,     -- SHA-256 of strategy code
    code_version VARCHAR(50),           -- Semantic version e.g., 1.2.3

    -- Configuration snapshot
    config_json JSONB NOT NULL DEFAULT '{}',

    -- Connector versions
    connector_versions JSONB DEFAULT '{}',  -- e.g., {"aave_v3": "1.0.0"}

    -- Performance metrics (optional, populated after running)
    performance_metrics JSONB,

    -- Version metadata
    is_active BOOLEAN NOT NULL DEFAULT FALSE,
    rollback_from VARCHAR(100),  -- version_id this was rolled back from
    notes TEXT,

    -- Audit fields
    created_by VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    deployed_at TIMESTAMP WITH TIME ZONE,
    deactivated_at TIMESTAMP WITH TIME ZONE,

    -- Constraints
    CONSTRAINT strategy_versions_version_id_unique UNIQUE (version_id),
    CONSTRAINT strategy_versions_code_hash_format CHECK (
        code_hash ~ '^[a-fA-F0-9]{64}$'
    )
);

-- Unique partial index for active version per strategy
CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_versions_active
    ON strategy_versions (strategy_id)
    WHERE is_active = TRUE;

-- --------------------------------------------------------------------------
-- alerts: Alert instances with status tracking
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alerts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    strategy_id UUID NOT NULL REFERENCES strategies(id) ON DELETE CASCADE,

    -- Alert identification
    alert_key VARCHAR(255) NOT NULL,  -- e.g., strategy_id:condition:timestamp

    -- Alert content
    condition VARCHAR(100) NOT NULL,  -- AlertCondition enum value
    severity severity_level NOT NULL,
    title VARCHAR(255) NOT NULL,
    message TEXT NOT NULL,

    -- Alert context
    context_json JSONB NOT NULL DEFAULT '{}',
    threshold_value DECIMAL,
    actual_value DECIMAL,

    -- Status tracking
    status alert_status NOT NULL DEFAULT 'PENDING',

    -- Channel tracking (which channels have been notified)
    channels_notified VARCHAR(50)[] DEFAULT ARRAY[]::VARCHAR[],

    -- Acknowledgment
    acknowledged_at TIMESTAMP WITH TIME ZONE,
    acknowledged_by VARCHAR(255),

    -- Escalation tracking
    escalation_level INTEGER DEFAULT 0,
    escalated_at TIMESTAMP WITH TIME ZONE,

    -- Resolution
    resolved_at TIMESTAMP WITH TIME ZONE,
    resolution_notes TEXT,

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE,

    -- Constraints
    CONSTRAINT alerts_escalation_level_valid CHECK (escalation_level >= 0 AND escalation_level <= 4)
);

-- --------------------------------------------------------------------------
-- operator_cards: Operator action cards for strategy issues
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS operator_cards (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    strategy_id UUID NOT NULL REFERENCES strategies(id) ON DELETE CASCADE,

    -- Card identification
    event_type card_event_type NOT NULL,
    reason stuck_reason,

    -- Card content
    severity severity_level NOT NULL,
    risk_description TEXT NOT NULL,

    -- Context data
    context_json JSONB NOT NULL DEFAULT '{}',  -- TX details, market conditions, etc.
    position_summary JSONB NOT NULL DEFAULT '{}',  -- Current funds, exposure

    -- Actions
    suggested_actions JSONB NOT NULL DEFAULT '[]',  -- List of SuggestedAction
    available_actions operator_action[] NOT NULL DEFAULT ARRAY[]::operator_action[],

    -- Auto-remediation
    auto_remediation_action operator_action,
    auto_remediation_scheduled_at TIMESTAMP WITH TIME ZONE,
    auto_remediation_executed_at TIMESTAMP WITH TIME ZONE,

    -- Status
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    action_taken operator_action,
    action_taken_at TIMESTAMP WITH TIME ZONE,
    action_taken_by VARCHAR(255),
    action_result JSONB,

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMP WITH TIME ZONE
);

-- Unique partial index for active card per strategy
CREATE UNIQUE INDEX IF NOT EXISTS idx_operator_cards_active
    ON operator_cards (strategy_id)
    WHERE is_active = TRUE;

-- ============================================================================
-- INDEXES
-- ============================================================================

-- strategies indexes
CREATE INDEX IF NOT EXISTS idx_strategies_status ON strategies (status);
CREATE INDEX IF NOT EXISTS idx_strategies_chain ON strategies (chain);
CREATE INDEX IF NOT EXISTS idx_strategies_protocol ON strategies (protocol);
CREATE INDEX IF NOT EXISTS idx_strategies_owner ON strategies (owner_id);
CREATE INDEX IF NOT EXISTS idx_strategies_updated_at ON strategies (updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_strategies_tags ON strategies USING GIN (tags);

-- v2_strategy_state indexes (strategy_id is PK, no extra index needed)
CREATE INDEX IF NOT EXISTS idx_v2_strategy_state_created_at ON v2_strategy_state (created_at DESC);

-- strategy_events indexes
CREATE INDEX IF NOT EXISTS idx_strategy_events_strategy_id ON strategy_events (strategy_id);
CREATE INDEX IF NOT EXISTS idx_strategy_events_type ON strategy_events (event_type);
CREATE INDEX IF NOT EXISTS idx_strategy_events_timestamp ON strategy_events (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_strategy_events_strategy_timestamp
    ON strategy_events (strategy_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_strategy_events_tx_hash ON strategy_events (tx_hash)
    WHERE tx_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_strategy_events_severity ON strategy_events (severity)
    WHERE severity IS NOT NULL;

-- strategy_versions indexes
CREATE INDEX IF NOT EXISTS idx_strategy_versions_strategy_id ON strategy_versions (strategy_id);
CREATE INDEX IF NOT EXISTS idx_strategy_versions_code_hash ON strategy_versions (code_hash);
CREATE INDEX IF NOT EXISTS idx_strategy_versions_created_at ON strategy_versions (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_strategy_versions_strategy_created
    ON strategy_versions (strategy_id, created_at DESC);

-- alerts indexes
CREATE INDEX IF NOT EXISTS idx_alerts_strategy_id ON alerts (strategy_id);
CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts (status);
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts (severity);
CREATE INDEX IF NOT EXISTS idx_alerts_condition ON alerts (condition);
CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_strategy_status
    ON alerts (strategy_id, status);
CREATE INDEX IF NOT EXISTS idx_alerts_expires_at ON alerts (expires_at)
    WHERE expires_at IS NOT NULL;

-- operator_cards indexes
CREATE INDEX IF NOT EXISTS idx_operator_cards_strategy_id ON operator_cards (strategy_id);
CREATE INDEX IF NOT EXISTS idx_operator_cards_event_type ON operator_cards (event_type);
CREATE INDEX IF NOT EXISTS idx_operator_cards_severity ON operator_cards (severity);
CREATE INDEX IF NOT EXISTS idx_operator_cards_created_at ON operator_cards (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_operator_cards_reason ON operator_cards (reason)
    WHERE reason IS NOT NULL;

-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for strategies table
DROP TRIGGER IF EXISTS trigger_strategies_updated_at ON strategies;
CREATE TRIGGER trigger_strategies_updated_at
    BEFORE UPDATE ON strategies
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger for alerts table
DROP TRIGGER IF EXISTS trigger_alerts_updated_at ON alerts;
CREATE TRIGGER trigger_alerts_updated_at
    BEFORE UPDATE ON alerts
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger for operator_cards table
DROP TRIGGER IF EXISTS trigger_operator_cards_updated_at ON operator_cards;
CREATE TRIGGER trigger_operator_cards_updated_at
    BEFORE UPDATE ON operator_cards
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Function to calculate state checksum
CREATE OR REPLACE FUNCTION calculate_state_checksum()
RETURNS TRIGGER AS $$
BEGIN
    NEW.checksum = encode(digest(NEW.state_data::text, 'sha256'), 'hex');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for state checksum
DROP TRIGGER IF EXISTS trigger_v2_strategy_state_checksum ON v2_strategy_state;
CREATE TRIGGER trigger_v2_strategy_state_checksum
    BEFORE INSERT OR UPDATE OF state_data ON v2_strategy_state
    FOR EACH ROW
    EXECUTE FUNCTION calculate_state_checksum();

-- ============================================================================
-- FUNCTIONS
-- ============================================================================

-- Function for CAS (Compare-And-Swap) state update
-- Single-row-per-agent model: updates only if version matches.
-- Returns true if update succeeded, false if version conflict.
CREATE OR REPLACE FUNCTION cas_update_state(
    p_strategy_id UUID,
    p_expected_version BIGINT,
    p_new_state JSONB
)
RETURNS BOOLEAN AS $$
DECLARE
    v_updated INTEGER;
BEGIN
    UPDATE v2_strategy_state
    SET state_data = p_new_state,
        version = version + 1,
        updated_at = NOW()
    WHERE strategy_id = p_strategy_id
      AND version = p_expected_version;

    GET DIAGNOSTICS v_updated = ROW_COUNT;
    RETURN v_updated > 0;
END;
$$ LANGUAGE plpgsql;

-- Function to get state with version
CREATE OR REPLACE FUNCTION get_latest_state(p_strategy_id UUID)
RETURNS TABLE (
    version BIGINT,
    state_data JSONB,
    schema_version INTEGER,
    created_at TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT ss.version, ss.state_data, ss.schema_version, ss.created_at
    FROM v2_strategy_state ss
    WHERE ss.strategy_id = p_strategy_id;
END;
$$ LANGUAGE plpgsql;

-- Function to get event timeline with pagination
CREATE OR REPLACE FUNCTION get_event_timeline(
    p_strategy_id UUID,
    p_event_type event_type DEFAULT NULL,
    p_limit INTEGER DEFAULT 50,
    p_offset INTEGER DEFAULT 0
)
RETURNS TABLE (
    event_id UUID,
    event_type event_type,
    event_data JSONB,
    description TEXT,
    tx_hash VARCHAR(66),
    timestamp TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT se.id, se.event_type, se.event_data, se.description, se.tx_hash, se.timestamp
    FROM strategy_events se
    WHERE se.strategy_id = p_strategy_id
      AND (p_event_type IS NULL OR se.event_type = p_event_type)
    ORDER BY se.timestamp DESC
    LIMIT p_limit
    OFFSET p_offset;
END;
$$ LANGUAGE plpgsql;

-- Function to get active operator card for strategy
CREATE OR REPLACE FUNCTION get_active_operator_card(p_strategy_id UUID)
RETURNS TABLE (
    card_id UUID,
    event_type card_event_type,
    reason stuck_reason,
    severity severity_level,
    risk_description TEXT,
    context_json JSONB,
    position_summary JSONB,
    suggested_actions JSONB,
    available_actions operator_action[],
    auto_remediation_action operator_action,
    auto_remediation_scheduled_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        oc.id,
        oc.event_type,
        oc.reason,
        oc.severity,
        oc.risk_description,
        oc.context_json,
        oc.position_summary,
        oc.suggested_actions,
        oc.available_actions,
        oc.auto_remediation_action,
        oc.auto_remediation_scheduled_at,
        oc.created_at
    FROM operator_cards oc
    WHERE oc.strategy_id = p_strategy_id
      AND oc.is_active = TRUE;
END;
$$ LANGUAGE plpgsql;

-- Function to resolve operator card
CREATE OR REPLACE FUNCTION resolve_operator_card(
    p_card_id UUID,
    p_action_taken operator_action,
    p_action_by VARCHAR(255),
    p_action_result JSONB DEFAULT NULL
)
RETURNS BOOLEAN AS $$
DECLARE
    v_updated INTEGER;
BEGIN
    UPDATE operator_cards
    SET is_active = FALSE,
        action_taken = p_action_taken,
        action_taken_at = NOW(),
        action_taken_by = p_action_by,
        action_result = p_action_result,
        resolved_at = NOW()
    WHERE id = p_card_id
      AND is_active = TRUE;

    GET DIAGNOSTICS v_updated = ROW_COUNT;
    RETURN v_updated > 0;
END;
$$ LANGUAGE plpgsql;

-- Function to cleanup old events (retention policy)
CREATE OR REPLACE FUNCTION cleanup_old_events(
    p_retention_days INTEGER DEFAULT 90
)
RETURNS INTEGER AS $$
DECLARE
    v_deleted INTEGER;
BEGIN
    DELETE FROM strategy_events
    WHERE timestamp < NOW() - (p_retention_days || ' days')::INTERVAL;

    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RETURN v_deleted;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- VIEWS
-- ============================================================================

-- View for strategy overview (dashboard summary)
CREATE OR REPLACE VIEW strategy_overview AS
SELECT
    s.id,
    s.name,
    s.chain,
    s.protocol,
    s.status,
    s.config_json,
    s.tags,
    s.created_at,
    s.updated_at,
    s.last_active_at,
    ss.version as state_version,
    ss.schema_version,
    (SELECT COUNT(*) FROM strategy_events se WHERE se.strategy_id = s.id) as event_count,
    (SELECT MAX(timestamp) FROM strategy_events se WHERE se.strategy_id = s.id) as last_event_at,
    (SELECT COUNT(*) FROM alerts a WHERE a.strategy_id = s.id AND a.status = 'PENDING') as pending_alerts,
    CASE WHEN oc.id IS NOT NULL THEN TRUE ELSE FALSE END as has_active_card
FROM strategies s
LEFT JOIN v2_strategy_state ss ON s.id = ss.strategy_id
LEFT JOIN operator_cards oc ON s.id = oc.strategy_id AND oc.is_active = TRUE;

-- View for recent events across all strategies
CREATE OR REPLACE VIEW recent_events AS
SELECT
    se.id as event_id,
    se.strategy_id,
    s.name as strategy_name,
    se.event_type,
    se.description,
    se.severity,
    se.tx_hash,
    se.timestamp
FROM strategy_events se
JOIN strategies s ON se.strategy_id = s.id
ORDER BY se.timestamp DESC
LIMIT 1000;

-- View for active alerts
CREATE OR REPLACE VIEW active_alerts AS
SELECT
    a.id as alert_id,
    a.strategy_id,
    s.name as strategy_name,
    a.condition,
    a.severity,
    a.title,
    a.message,
    a.status,
    a.escalation_level,
    a.created_at,
    a.expires_at
FROM alerts a
JOIN strategies s ON a.strategy_id = s.id
WHERE a.status IN ('PENDING', 'SENT', 'ESCALATED')
ORDER BY
    CASE a.severity
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3
        WHEN 'LOW' THEN 4
    END,
    a.created_at DESC;

-- View for strategies requiring attention
CREATE OR REPLACE VIEW strategies_attention_required AS
SELECT
    s.id,
    s.name,
    s.chain,
    s.status,
    oc.event_type as card_event_type,
    oc.reason as stuck_reason,
    oc.severity as card_severity,
    oc.risk_description,
    oc.auto_remediation_scheduled_at,
    oc.created_at as card_created_at,
    (SELECT COUNT(*) FROM alerts a
     WHERE a.strategy_id = s.id AND a.status IN ('PENDING', 'ESCALATED')) as alert_count
FROM strategies s
JOIN operator_cards oc ON s.id = oc.strategy_id AND oc.is_active = TRUE
ORDER BY
    CASE oc.severity
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3
        WHEN 'LOW' THEN 4
    END,
    oc.created_at ASC;

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE strategies IS 'Core strategy metadata including name, chain, protocol, and operational status';
COMMENT ON TABLE v2_strategy_state IS 'Single row per strategy state storage with CAS semantics via version field';
COMMENT ON TABLE strategy_events IS 'Chronological event timeline for audit and debugging';
COMMENT ON TABLE strategy_versions IS 'Code and configuration version tracking for deployments and rollbacks';
COMMENT ON TABLE alerts IS 'Alert instances with status tracking and escalation support';
COMMENT ON TABLE operator_cards IS 'Operator action cards providing structured, actionable information for strategy issues';

COMMENT ON FUNCTION cas_update_state IS 'Atomic Compare-And-Swap state update - returns false if version conflict';
COMMENT ON FUNCTION get_latest_state IS 'Get state for a strategy';
COMMENT ON FUNCTION get_event_timeline IS 'Get paginated event timeline with optional type filtering';
COMMENT ON FUNCTION get_active_operator_card IS 'Get the active operator card for a strategy if one exists';
COMMENT ON FUNCTION resolve_operator_card IS 'Mark an operator card as resolved with action details';
COMMENT ON FUNCTION cleanup_old_events IS 'Remove events older than retention period (default 90 days)';
