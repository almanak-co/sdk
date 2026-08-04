"""Gateway-owned Chainlink round-reader accuracy and work-bound tests."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from almanak.integrations.chainlink.catalog import CATALOG
from almanak.integrations.chainlink.codec import RoundData
from almanak.integrations.chainlink.gateway.history import (
    ChainlinkHistoryReader,
    ChainlinkHistoryUnavailable,
    HistoricalPricePoint,
)


def _round(round_id: int, timestamp: int, answer: int | None = None) -> RoundData:
    return RoundData(
        round_id=round_id,
        answer=answer if answer is not None else timestamp * 100_000_000,
        started_at=timestamp,
        updated_at=timestamp,
        answered_in_round=round_id,
    )


def _encoded_round(round_data: RoundData) -> bytes:
    return b"".join(
        (
            round_data.round_id.to_bytes(32, "big"),
            round_data.answer.to_bytes(32, "big", signed=True),
            round_data.started_at.to_bytes(32, "big"),
            round_data.updated_at.to_bytes(32, "big"),
            round_data.answered_in_round.to_bytes(32, "big"),
        )
    )


class FixtureReader(ChainlinkHistoryReader):
    def __init__(self, rounds: dict[int, RoundData], **kwargs) -> None:
        super().__init__(chain="ethereum", rpc_url="http://gateway-owned.invalid", **kwargs)
        self.rounds = rounds
        feed = CATALOG.feed_for_token("ethereum", "ETH")
        assert feed is not None
        self._feed_decimals[feed.address.lower()] = feed.decimals

    async def _latest_round(self, address: str) -> RoundData:
        return self.rounds[max(self.rounds)]

    async def _round(
        self,
        address: str,
        round_id: int,
        *,
        confirm_absence: bool = False,
    ) -> RoundData | None:
        return self.rounds.get(round_id)


@pytest.mark.asyncio
async def test_history_returns_interval_plus_one_prior_observation() -> None:
    reader = FixtureReader({index: _round(index, index * 100) for index in range(1, 7)})
    points = await reader.get_history(token="ETH", start_ts=250, end_ts=551, max_points=20)
    assert [point.timestamp for point in points] == [200, 300, 400, 500]
    assert [point.observation_id for point in points] == [2, 3, 4, 5]
    assert points[-1].price == Decimal("500")


@pytest.mark.asyncio
async def test_history_downsamples_deterministically_to_caller_bound() -> None:
    reader = FixtureReader({index: _round(index, index * 100) for index in range(1, 11)})
    points = await reader.get_history(token="ETH", start_ts=150, end_ts=1_001, max_points=3)
    assert len(points) == 3
    assert points[0].timestamp == 100
    assert points[-1].timestamp == 1_000


@pytest.mark.asyncio
async def test_large_window_is_binary_searched_and_materialization_bounded() -> None:
    reader = FixtureReader(
        {index: _round(index, index * 100) for index in range(1, 101)},
        max_scanned_rounds=5,
        batch_size=5,
    )
    page = await reader.get_history_page(token="ETH", start_ts=100, end_ts=20_000, max_points=100)
    assert len(page.points) == 5
    assert page.points[0].timestamp == 100
    assert page.points[-1].timestamp == 10_000
    assert page.truncated is True
    assert page.recommended_split_ts == 10_050


@pytest.mark.asyncio
async def test_history_crosses_proxy_phase_without_scanning_uint80_gap() -> None:
    def round_id(phase: int, sequence: int) -> int:
        return (phase << 64) | sequence

    rounds = {
        round_id(1, 1): _round(round_id(1, 1), 100),
        round_id(1, 2): _round(round_id(1, 2), 200),
        round_id(1, 3): _round(round_id(1, 3), 300),
        round_id(2, 1): _round(round_id(2, 1), 400),
        round_id(2, 2): _round(round_id(2, 2), 500),
        round_id(2, 3): _round(round_id(2, 3), 600),
    }
    points = await FixtureReader(rounds).get_history(token="ETH", start_ts=250, end_ts=550, max_points=10)
    assert [point.timestamp for point in points] == [200, 300, 400, 500]


@pytest.mark.asyncio
async def test_quiet_window_uses_prior_observation_from_current_phase() -> None:
    def round_id(phase: int, sequence: int) -> int:
        return (phase << 64) | sequence

    rounds = {
        round_id(1, 1): _round(round_id(1, 1), 100),
        round_id(1, 2): _round(round_id(1, 2), 200),
        round_id(2, 1): _round(round_id(2, 1), 1_000),
        round_id(2, 2): _round(round_id(2, 2), 2_000),
        round_id(2, 3): _round(round_id(2, 3), 3_000),
    }

    page = await FixtureReader(rounds).get_history_page(
        token="ETH",
        start_ts=2_100,
        end_ts=2_900,
        max_points=10,
    )

    assert [(point.timestamp, point.observation_id) for point in page.points] == [(2_000, round_id(2, 2))]
    assert page.truncated is False


@pytest.mark.asyncio
async def test_quiet_window_skips_carried_immediate_predecessor() -> None:
    carried = RoundData(
        round_id=2,
        answer=2_000 * 100_000_000,
        started_at=2_000,
        updated_at=2_000,
        answered_in_round=1,
    )
    reader = FixtureReader(
        {
            1: _round(1, 1_000),
            2: carried,
            3: _round(3, 3_000),
        }
    )

    page = await reader.get_history_page(token="ETH", start_ts=2_100, end_ts=2_900, max_points=10)

    assert [(point.timestamp, point.observation_id) for point in page.points] == [(1_000, 1)]
    assert page.truncated is False


@pytest.mark.asyncio
async def test_prior_point_does_not_consume_range_materialization_capacity() -> None:
    reader = FixtureReader(
        {index: _round(index, index * 100) for index in range(1, 7)},
        max_scanned_rounds=5,
        batch_size=5,
    )

    page = await reader.get_history_page(token="ETH", start_ts=150, end_ts=700, max_points=100)

    assert [point.timestamp for point in page.points] == [100, 200, 300, 400, 500, 600]
    assert page.truncated is False
    assert page.recommended_split_ts == 0


@pytest.mark.asyncio
async def test_unreadable_search_gap_fails_closed_instead_of_claiming_complete_history() -> None:
    rounds = {index: _round(index, index * 100) for index in range(1, 201)}
    for index in range(60, 141):
        del rounds[index]

    with pytest.raises(ChainlinkHistoryUnavailable, match="no readable evidence"):
        await FixtureReader(rounds).get_history_page(
            token="ETH",
            start_ts=5_000,
            end_ts=19_000,
            max_points=200,
        )


@pytest.mark.asyncio
async def test_latest_is_provider_exact_and_decimal_scaled() -> None:
    reader = FixtureReader({1: _round(1, 100, 312_345_000_000)})
    point = await reader.get_latest(token="ETH")
    assert point == HistoricalPricePoint(timestamp=100, price=Decimal("3123.45"), observation_id=1)


@pytest.mark.asyncio
async def test_unknown_feed_is_an_honest_miss() -> None:
    reader = FixtureReader({1: _round(1, 100)})
    with pytest.raises(ChainlinkHistoryUnavailable, match="no direct Chainlink USD feed"):
        await reader.get_history(token="NOPE", start_ts=1, end_ts=2, max_points=1)


def test_downsample_preserves_endpoints() -> None:
    points = [HistoricalPricePoint(index, Decimal(index), index) for index in range(10)]
    sampled = ChainlinkHistoryReader._downsample(points, 4)
    assert sampled[0] is points[0]
    assert sampled[-1] is points[-1]
    assert len(sampled) == 4


@pytest.mark.asyncio
async def test_round_absence_confirmation_is_opt_in_and_transport_failure_retries() -> None:
    reader = ChainlinkHistoryReader(
        chain="ethereum",
        rpc_url="http://gateway-owned.invalid",
        rpc_retries=2,
    )

    class ContractLogicError(Exception):
        pass

    with patch.object(reader, "_eth_call", new_callable=AsyncMock, side_effect=ContractLogicError("revert")) as call:
        assert await reader._round("0xfeed", 1) is None
        assert call.await_count == 1

    with patch.object(reader, "_eth_call", new_callable=AsyncMock, side_effect=ContractLogicError("revert")) as call:
        assert await reader._round("0xfeed", 1, confirm_absence=True) is None
        assert call.await_count == 2

    recovered = _round(1, 100)
    with patch.object(
        reader,
        "_eth_call",
        new_callable=AsyncMock,
        side_effect=[ContractLogicError("revert"), _encoded_round(recovered)],
    ) as call:
        assert await reader._round("0xfeed", 1, confirm_absence=True) == recovered
        assert call.await_count == 2

    with patch.object(reader, "_eth_call", new_callable=AsyncMock, side_effect=TimeoutError("slow")) as call:
        with pytest.raises(ChainlinkHistoryUnavailable, match="bounded retries"):
            await reader._round("0xfeed", 1)
        assert call.await_count == 3


@pytest.mark.asyncio
async def test_present_carried_round_is_not_misclassified_as_absent() -> None:
    reader = ChainlinkHistoryReader(chain="ethereum", rpc_url="http://gateway-owned.invalid", rpc_retries=0)
    carried = RoundData(round_id=5, answer=100_000_000, started_at=100, updated_at=100, answered_in_round=4)
    with patch.object(reader, "_eth_call", new_callable=AsyncMock, return_value=_encoded_round(carried)):
        assert await reader._round("0xfeed", 5) == carried
    assert reader._price_points([carried], prior=None, start_ts=1, end_ts=200, decimals=8) == []


@pytest.mark.asyncio
async def test_carried_latest_round_is_searchable_but_not_publishable() -> None:
    reader = ChainlinkHistoryReader(chain="ethereum", rpc_url="http://gateway-owned.invalid", rpc_retries=0)
    carried = RoundData(round_id=5, answer=100_000_000, started_at=100, updated_at=100, answered_in_round=4)
    with patch.object(reader, "_required_call", new_callable=AsyncMock, return_value=_encoded_round(carried)):
        assert await reader._latest_round("0xfeed") == carried
        with pytest.raises(ChainlinkHistoryUnavailable, match="latest round is invalid"):
            await reader.get_latest(token="ETH")


@pytest.mark.asyncio
async def test_carried_latest_round_anchors_history_without_entering_series() -> None:
    carried = RoundData(round_id=4, answer=300_000_000, started_at=300, updated_at=300, answered_in_round=3)
    reader = FixtureReader(
        {
            1: _round(1, 100),
            2: _round(2, 200),
            3: _round(3, 300),
            4: carried,
        }
    )
    points = await reader.get_history(token="ETH", start_ts=1, end_ts=400, max_points=10)
    assert [point.observation_id for point in points] == [1, 2, 3]


@pytest.mark.parametrize("answer", [0, -1])
def test_non_positive_rounds_never_enter_history(answer: int) -> None:
    invalid = _round(1, 100, answer)
    assert (
        ChainlinkHistoryReader._price_points(
            [invalid],
            prior=None,
            start_ts=1,
            end_ts=200,
            decimals=8,
        )
        == []
    )


@pytest.mark.asyncio
async def test_history_wall_clock_budget_fails_closed() -> None:
    reader = FixtureReader({1: _round(1, 100)}, history_timeout_seconds=0.001)

    async def slow_latest(_address: str) -> RoundData:
        await asyncio.sleep(1)
        return _round(1, 100)

    with patch.object(reader, "_latest_round", side_effect=slow_latest):
        with pytest.raises(ChainlinkHistoryUnavailable, match="wall-clock budget"):
            await reader.get_history(token="ETH", start_ts=1, end_ts=200, max_points=10)


@pytest.mark.asyncio
async def test_history_decimals_mismatch_fails_closed() -> None:
    reader = ChainlinkHistoryReader(chain="ethereum", rpc_url="http://gateway-owned.invalid", rpc_retries=0)
    encoded = (18).to_bytes(32, "big")
    with patch.object(reader, "_eth_call", new_callable=AsyncMock, return_value=encoded):
        with pytest.raises(ChainlinkHistoryUnavailable, match="decimals mismatch"):
            await reader._verified_decimals("0xfeed", 8)


@pytest.mark.asyncio
async def test_probe_near_sequence_never_escapes_current_search_window() -> None:
    reader = FixtureReader({})
    observed: list[int] = []

    async def recording_round(_address: str, round_id: int) -> RoundData | None:
        observed.append(round_id)
        return None

    with patch.object(reader, "_round", side_effect=recording_round):
        assert (
            await reader._probe_near_sequence(
                "0xfeed",
                phase=0,
                sequence=9,
                minimum=8,
                maximum=9,
                phased=False,
            )
            is None
        )
    assert observed and set(observed) <= {8, 9}


@pytest.mark.asyncio
async def test_timestamp_search_clamps_probes_below_shrinking_high() -> None:
    reader = FixtureReader({index: _round(index, index * 100) for index in range(1, 17)})
    probed_rounds: list[int] = []
    windows: list[tuple[int, int, list[int]]] = []
    original_probe = reader._probe_near_sequence

    async def recording_round(_address: str, round_id: int) -> RoundData | None:
        probed_rounds.append(round_id)
        return reader.rounds.get(round_id)

    async def recording_probe(*args, **kwargs):
        before = len(probed_rounds)
        result = await original_probe(*args, **kwargs)
        windows.append((kwargs["minimum"], kwargs["maximum"], probed_rounds[before:]))
        return result

    with (
        patch.object(reader, "_round", side_effect=recording_round),
        patch.object(
            reader,
            "_probe_near_sequence",
            side_effect=recording_probe,
        ),
    ):
        sequence = await reader._first_sequence_at_or_after(
            "0xfeed",
            phase=0,
            maximum=16,
            timestamp=550,
            phased=False,
        )

    assert sequence == 6
    assert windows
    assert min(maximum for _minimum, maximum, _calls in windows) < 16
    for minimum, maximum, calls in windows:
        assert calls
        assert all(minimum <= round_id <= maximum for round_id in calls)


@pytest.mark.asyncio
async def test_timestamp_search_rejects_probe_without_bounded_progress() -> None:
    reader = FixtureReader({index: _round(index, index * 100) for index in range(1, 9)})
    outside_window = (9, _round(9, 900))
    with patch.object(reader, "_probe_near_sequence", new_callable=AsyncMock, return_value=outside_window):
        with pytest.raises(ChainlinkHistoryUnavailable, match="made no bounded progress"):
            await reader._first_sequence_at_or_after(
                "0xfeed",
                phase=0,
                maximum=8,
                timestamp=500,
                phased=False,
            )
