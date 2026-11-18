"""Underlying-token admin entitlement and XCP EMA for the January 2024 revision."""

from typing import Annotated

from pydantic import Field

from almanak.connectors.curve.crypto_integer import PRECISION, unsigned
from almanak.connectors.curve.crypto_prices import exponential
from almanak.connectors.curve.domain import Uint256
from almanak.models import Model, UInt


class TokenAccounting(Model):
    virtual_admin_shares: Uint256
    last_claim_time: UInt
    cached_xcp_oracle: Uint256
    last_xcp: Uint256
    xcp_timestamp: UInt
    xcp_ma_time: Annotated[Uint256, Field(gt=0)]

    def oracle(self, timestamp, *, last_xcp=None):
        if timestamp < self.xcp_timestamp:
            raise ValueError("TriCrypto XCP oracle cannot consume future state")
        if timestamp == self.xcp_timestamp:
            return self.cached_xcp_oracle
        last = self.last_xcp if last_xcp is None else last_xcp
        alpha = exponential(
            -(unsigned((timestamp - self.xcp_timestamp) * PRECISION) // self.xcp_ma_time)
        )
        return unsigned(last * (PRECISION - alpha) + self.cached_xcp_oracle * alpha) // PRECISION

    def advance(self, timestamp, last_xcp, *, proportional=False):
        cached = self.oracle(timestamp, last_xcp=last_xcp if proportional else None)
        return self.model_copy(
            update={
                "cached_xcp_oracle": cached,
                "xcp_timestamp": timestamp,
                "last_xcp": last_xcp,
            }
        )
