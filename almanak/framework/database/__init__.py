"""Database package for strategy state persistence.

This package provides PostgreSQL schema definitions and database utilities
for the Almanak Strategy Framework.

Schema Overview:
- strategies: Core strategy metadata and status
- strategy_state: Versioned state storage with CAS support
- strategy_events: Chronological event timeline
- strategy_versions: Code and config version tracking
- alerts: Alert instances and status
- operator_cards: Operator action cards for strategy issues
"""
