"""Uniswap V4 chain support in the verified and candidate shape `ax` reports.

A chain declared with production execution is verified, as the promoted chains of
the other pool connectors are; any other declared chain, and each candidate chain
the resolver accepts, is a candidate.
"""

from almanak.connectors.support import RunMode, candidate_uniswap_support, uniswap_support


def verified_support():
    return tuple(entry for entry in uniswap_support() if RunMode.PRODUCTION in entry.modes)


def candidate_support():
    return candidate_uniswap_support()
