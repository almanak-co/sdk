"""Multi-position dual-range LP demo strategy.

A reference template for multi-position strategies on the same pool. Opens
two concentrated-liquidity positions (narrow + wide range) on Uniswap V3,
one per ``decide()`` iteration, via an explicit phase machine.

See ``blueprints/04-strategy-layer.md`` §Multi-position dispatch for the
design contract, and ``strategies/accounting/lp_dual/`` for a more
elaborate accounting-fixture sibling.
"""

from .strategy import MultiLPDualRangeStrategy

__all__ = ["MultiLPDualRangeStrategy"]
