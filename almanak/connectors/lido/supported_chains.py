"""Lido strategy-side chain coverage.

Declares the chains on which the Lido liquid-staking connector is alive. See
``almanak.connectors._strategy_base.supported_chains_registry`` for the
aggregator that derives
:data:`almanak.framework.execution.config.SUPPORTED_PROTOCOLS`.
"""

from __future__ import annotations

# Lido liquid staking -- ethereum only.
#
# VIB-6231: this used to declare {ethereum, arbitrum, optimism, polygon},
# mirroring ``LIDO_ADDRESSES`` in ``adapter.py``. But that table only carries
# ``wsteth`` on the three L2s -- the bridged token. ``steth`` and
# ``withdrawal_queue`` exist on ethereum only, so STAKE (stETH.submit) and
# UNSTAKE (WithdrawalQueue.requestWithdrawals) are not merely unsupported off
# mainnet, they are impossible. ``LidoCompiler.chains`` and the manifest's
# ``strategy_chains`` both correctly say ``{ethereum}``.
#
# Because this module is the sole input to
# ``almanak.framework.execution.config.SUPPORTED_PROTOCOLS``, the wider set
# made config validation ACCEPT ``protocols={"arbitrum": ["lido"]}`` and the
# compiler then refuse it:
#
#     LidoCompiler only supported on ethereum, got: arbitrum
#
# "Where the bridged token exists" is not "where the protocol is alive". Chain
# coverage for the *asset* stays in ``LIDO_ADDRESSES``, which valuation and
# token resolution read; this declaration is about what a strategy may
# configure.
SUPPORTED_CHAINS_BY_PROTOCOL: dict[str, frozenset[str]] = {
    "lido": frozenset({"ethereum"}),
}
