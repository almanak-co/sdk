"""Unit tests for protocol-specific lending APY providers."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.core.enums import Chain
from almanak.framework.backtesting.exceptions import DataSourceUnavailableError
from almanak.framework.backtesting.pnl.providers.lending.aave_v3_apy import (
    AaveV3APYProvider,
    AaveV3ClientConfig,
)
from almanak.framework.backtesting.pnl.providers.lending.morpho_apy import (
    BORROWER_SIDE as MORPHO_BORROWER_SIDE,
)
from almanak.framework.backtesting.pnl.providers.lending.morpho_apy import (
    LENDER_SIDE as MORPHO_LENDER_SIDE,
)
from almanak.framework.backtesting.pnl.providers.lending.morpho_apy import (
    MorphoBlueAPYProvider,
)
from almanak.framework.backtesting.pnl.providers.lending.spark_apy import (
    BORROWER_SIDE as SPARK_BORROWER_SIDE,
)
from almanak.framework.backtesting.pnl.providers.lending.spark_apy import (
    LENDER_SIDE as SPARK_LENDER_SIDE,
)
from almanak.framework.backtesting.pnl.providers.lending.spark_apy import (
    SparkAPYProvider,
)
from almanak.framework.backtesting.pnl.providers.subgraph_client import (
    SubgraphQueryError,
)
from almanak.framework.backtesting.pnl.types import DataConfidence


class StubSubgraphClient:
    """Tiny async subgraph client for APY provider tests."""

    def __init__(
        self,
        *,
        query_response: dict[str, Any] | None = None,
        paginated_response: list[dict[str, Any]] | None = None,
        query_error: Exception | None = None,
        pagination_error: Exception | None = None,
    ) -> None:
        self.query_response = query_response or {}
        self.paginated_response = paginated_response or []
        self.query_error = query_error
        self.pagination_error = pagination_error
        self.query_calls: list[dict[str, Any]] = []
        self.pagination_calls: list[dict[str, Any]] = []
        self.closed = False

    async def query(self, **kwargs: Any) -> dict[str, Any]:
        self.query_calls.append(kwargs)
        if self.query_error is not None:
            raise self.query_error
        return self.query_response

    async def query_with_pagination(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.pagination_calls.append(kwargs)
        if self.pagination_error is not None:
            raise self.pagination_error
        return self.paginated_response

    async def close(self) -> None:
        self.closed = True


def _fallback_overflow_error() -> DataSourceUnavailableError:
    return DataSourceUnavailableError(
        data_type="apy",
        identifier="market",
        remediation="Narrow the APY query window.",
        message="APY pagination window too large",
    )


class TestAaveV3ProtocolAPYProvider:
    """Characterization tests for Aave V3 APY history fetching."""

    @pytest.mark.asyncio
    async def test_successful_history_uses_reserve_and_high_confidence(self) -> None:
        supply_ray = str(int(Decimal("0.031") * Decimal("1e27")))
        borrow_ray = str(int(Decimal("0.052") * Decimal("1e27")))
        client = StubSubgraphClient(
            query_response={"reserves": [{"id": "reserve-usdc"}]},
            paginated_response=[
                {
                    "timestamp": "1704067200",
                    "liquidityRate": supply_ray,
                    "variableBorrowRate": borrow_ray,
                }
            ],
        )
        provider = AaveV3APYProvider(client=client)

        results = await provider.get_apy(
            "aave_v3",
            "usdc",
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 2, tzinfo=UTC),
        )

        assert len(results) == 1
        assert results[0].supply_apy == Decimal("0.031")
        assert results[0].borrow_apy == Decimal("0.052")
        assert results[0].source_info.confidence is DataConfidence.HIGH
        assert results[0].source_info.source == "aave_v3_subgraph"
        assert client.query_calls[0]["variables"] == {"symbol": "USDC"}
        assert client.pagination_calls[0]["variables"]["reserveId"] == "reserve-usdc"

    @pytest.mark.asyncio
    async def test_empty_history_returns_low_confidence_fallback(self) -> None:
        client = StubSubgraphClient(query_response={"reserves": [{"id": "reserve-dai"}]})
        provider = AaveV3APYProvider(client=client)

        results = await provider.get_apy(
            "aave_v3",
            "DAI",
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 2, tzinfo=UTC),
        )

        assert len(results) == 2
        assert {result.source_info.source for result in results} == {"fallback"}
        assert all(result.source_info.confidence is DataConfidence.LOW for result in results)

    @pytest.mark.asyncio
    async def test_unsupported_chain_returns_fallback_without_querying(self) -> None:
        client = StubSubgraphClient()
        provider = AaveV3APYProvider(
            config=AaveV3ClientConfig(chain=Chain.SOLANA),
            client=client,
        )

        results = await provider.get_apy(
            "aave_v3",
            "USDC",
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 1, tzinfo=UTC),
        )

        assert len(results) == 1
        assert results[0].source_info.source == "fallback"
        assert results[0].source_info.confidence is DataConfidence.LOW
        assert client.query_calls == []
        assert client.pagination_calls == []

    @pytest.mark.asyncio
    async def test_pagination_overflow_stays_loud(self) -> None:
        client = StubSubgraphClient(
            query_response={"reserves": [{"id": "reserve-usdc"}]},
            pagination_error=_fallback_overflow_error(),
        )
        provider = AaveV3APYProvider(client=client)

        with pytest.raises(DataSourceUnavailableError, match="pagination window"):
            await provider.get_apy(
                "aave_v3",
                "USDC",
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 12, 31, tzinfo=UTC),
            )


@pytest.mark.parametrize(
    ("provider_factory", "protocol", "source", "lender_side", "borrower_side"),
    [
        (
            lambda client: SparkAPYProvider(client=client),
            "spark",
            "spark_subgraph",
            SPARK_LENDER_SIDE,
            SPARK_BORROWER_SIDE,
        ),
        (
            lambda client: MorphoBlueAPYProvider(client=client),
            "morpho_blue",
            "morpho_blue_subgraph",
            MORPHO_LENDER_SIDE,
            MORPHO_BORROWER_SIDE,
        ),
    ],
)
class TestMessariProtocolAPYProviders:
    """Characterization tests for Messari-schema lending APY providers."""

    @pytest.mark.asyncio
    async def test_successful_direct_market_history_is_high_confidence(
        self,
        provider_factory: Any,
        protocol: str,
        source: str,
        lender_side: str,
        borrower_side: str,
    ) -> None:
        client = StubSubgraphClient(
            paginated_response=[
                {
                    "timestamp": "1704067200",
                    "rates": [
                        {"side": lender_side, "rate": "3.25"},
                        {"side": borrower_side, "rate": "5.75"},
                    ],
                }
            ],
        )
        provider = provider_factory(client)

        results = await provider.get_apy(
            protocol,
            "0x1234567890abcdef123456",
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 2, tzinfo=UTC),
        )

        assert len(results) == 1
        assert results[0].supply_apy == Decimal("0.0325")
        assert results[0].borrow_apy == Decimal("0.0575")
        assert results[0].source_info.source == source
        assert results[0].source_info.confidence is DataConfidence.HIGH
        assert client.query_calls == []

    @pytest.mark.asyncio
    async def test_unknown_symbol_returns_low_confidence_fallback(
        self,
        provider_factory: Any,
        protocol: str,
        source: str,
        lender_side: str,
        borrower_side: str,
    ) -> None:
        client = StubSubgraphClient(query_response={"markets": []})
        provider = provider_factory(client)

        results = await provider.get_apy(
            protocol,
            "DAI",
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 2, tzinfo=UTC),
        )

        assert len(results) == 2
        assert {result.source_info.source for result in results} == {"fallback"}
        assert all(result.source_info.confidence is DataConfidence.LOW for result in results)
        assert client.pagination_calls == []

    @pytest.mark.asyncio
    async def test_market_lookup_transport_failure_returns_fallback(
        self,
        provider_factory: Any,
        protocol: str,
        source: str,
        lender_side: str,
        borrower_side: str,
    ) -> None:
        client = StubSubgraphClient(query_error=RuntimeError("subgraph transport down"))
        provider = provider_factory(client)

        results = await provider.get_apy(
            protocol,
            "DAI",
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 2, tzinfo=UTC),
        )

        assert len(results) == 2
        assert {result.source_info.source for result in results} == {"fallback"}
        assert all(result.source_info.confidence is DataConfidence.LOW for result in results)
        assert client.pagination_calls == []

    @pytest.mark.asyncio
    async def test_subgraph_query_error_returns_fallback(
        self,
        provider_factory: Any,
        protocol: str,
        source: str,
        lender_side: str,
        borrower_side: str,
    ) -> None:
        client = StubSubgraphClient(
            query_response={"markets": [{"id": "market-dai"}]},
            pagination_error=SubgraphQueryError("subgraph failed"),
        )
        provider = provider_factory(client)

        results = await provider.get_apy(
            protocol,
            "DAI",
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 1, tzinfo=UTC),
        )

        assert len(results) == 1
        assert results[0].source_info.source == "fallback"
        assert results[0].source_info.confidence is DataConfidence.LOW

    @pytest.mark.asyncio
    async def test_pagination_overflow_stays_loud(
        self,
        provider_factory: Any,
        protocol: str,
        source: str,
        lender_side: str,
        borrower_side: str,
    ) -> None:
        client = StubSubgraphClient(pagination_error=_fallback_overflow_error())
        provider = provider_factory(client)

        with pytest.raises(DataSourceUnavailableError, match="pagination window"):
            await provider.get_apy(
                protocol,
                "0x1234567890abcdef123456",
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 12, 31, tzinfo=UTC),
            )
