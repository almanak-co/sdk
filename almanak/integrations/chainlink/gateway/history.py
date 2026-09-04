"""Gateway-owned Chainlink historical round reader."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from decimal import Decimal

from almanak.core.chains import ChainRegistry
from almanak.core.retry import RetryPolicy
from almanak.gateway.utils.ssl_context import build_ssl_context
from almanak.integrations._base import OracleDataUnavailable
from almanak.integrations.chainlink.catalog import CATALOG
from almanak.integrations.chainlink.codec import (
    DECIMALS_SELECTOR,
    LATEST_ROUND_DATA_SELECTOR,
    RoundData,
    decode_round_data,
    decode_uint8,
    encode_get_round_data,
)

logger = logging.getLogger(__name__)

DEFAULT_MAX_SCANNED_ROUNDS = 50_000
DEFAULT_BATCH_SIZE = 100
DEFAULT_MAX_PHASES = 64
_MAX_BINARY_SEARCH_PROBES = 80
_MAX_GAP_PROBES = 16
DEFAULT_REQUEST_TIMEOUT_SECONDS = 10.0
DEFAULT_HISTORY_TIMEOUT_SECONDS = 45.0
DEFAULT_RPC_RETRIES = 2
DEFAULT_RPC_CONCURRENCY = 10


class ChainlinkHistoryUnavailable(OracleDataUnavailable):
    """A historical Chainlink request could not produce measured data."""


@dataclass(frozen=True)
class HistoricalPricePoint:
    timestamp: int
    price: Decimal
    observation_id: int


@dataclass(frozen=True)
class HistoricalPricePage:
    """Bounded history result with an explicit completeness signal."""

    points: list[HistoricalPricePoint]
    truncated: bool = False
    recommended_split_ts: int = 0


class ChainlinkHistoryReader:
    """Read historical AggregatorV3 rounds over one gateway-owned RPC client.

    Work is bounded by ``max_scanned_rounds`` and ``batch_size``. Round ids are
    traversed by proxy phase, so an aggregator upgrade does not create a
    practically infinite numeric gap at the phase boundary.
    """

    def __init__(
        self,
        *,
        chain: str,
        rpc_url: str,
        max_scanned_rounds: int = DEFAULT_MAX_SCANNED_ROUNDS,
        batch_size: int = DEFAULT_BATCH_SIZE,
        request_timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
        history_timeout_seconds: float = DEFAULT_HISTORY_TIMEOUT_SECONDS,
        rpc_retries: int = DEFAULT_RPC_RETRIES,
    ) -> None:
        descriptor = ChainRegistry.try_resolve(chain)
        normalized = descriptor.name if descriptor is not None else chain.strip().lower()
        if not CATALOG.supports_chain(normalized):
            raise ValueError(f"Chainlink has no feed catalogue for chain {chain!r}")
        if not rpc_url:
            raise ValueError(f"No RPC URL configured for Chainlink history on {normalized}")
        if (
            max_scanned_rounds <= 0
            or batch_size <= 0
            or request_timeout_seconds <= 0
            or history_timeout_seconds <= 0
            or rpc_retries < 0
        ):
            raise ValueError("Chainlink history work bounds must be positive")
        self.chain = normalized
        self._rpc_url = rpc_url
        self._max_scanned_rounds = max_scanned_rounds
        self._batch_size = min(batch_size, max_scanned_rounds)
        self._request_timeout_seconds = request_timeout_seconds
        self._history_timeout_seconds = history_timeout_seconds
        self._rpc_retry_policy = RetryPolicy(
            max_attempts=rpc_retries + 1,
            initial_delay_seconds=0.1,
        )
        self._rpc_semaphore = asyncio.Semaphore(min(DEFAULT_RPC_CONCURRENCY, self._batch_size))
        self._web3: object | None = None
        self._web3_lock = asyncio.Lock()
        self._feed_decimals: dict[str, int] = {}

    async def _client(self):
        if self._web3 is not None:
            return self._web3
        async with self._web3_lock:
            if self._web3 is None:
                from web3 import AsyncHTTPProvider, AsyncWeb3

                self._web3 = AsyncWeb3(
                    AsyncHTTPProvider(
                        self._rpc_url,
                        request_kwargs={"ssl": build_ssl_context(), "timeout": self._request_timeout_seconds},
                    )
                )
        return self._web3

    async def _eth_call(self, address: str, calldata: str) -> bytes:
        web3 = await self._client()
        checksum = web3.to_checksum_address(address)
        result = await web3.eth.call({"to": checksum, "data": calldata})
        return bytes(result)

    async def _latest_round(self, address: str) -> RoundData:
        raw = await self._required_call(address, LATEST_ROUND_DATA_SELECTOR, operation="latest round")
        try:
            round_data = decode_round_data(raw)
        except ValueError as exc:
            raise ChainlinkHistoryUnavailable(f"latest round malformed for feed {address}") from exc
        if not self._searchable(round_data):
            raise ChainlinkHistoryUnavailable(f"latest round is not searchable for feed {address}")
        return round_data

    async def _round(
        self,
        address: str,
        round_id: int,
        *,
        confirm_absence: bool = False,
    ) -> RoundData | None:
        calldata = encode_get_round_data(round_id)
        transport_failures = 0
        absence_seen = False
        while True:
            try:
                async with self._rpc_semaphore:
                    raw = await self._eth_call(address, calldata)
                return decode_round_data(raw)
            except Exception as exc:
                if self._is_absent_round_error(exc):
                    # Boundary searches deliberately use an absent round as
                    # their termination signal, so confirming every absence
                    # would multiply their latency. Value-returning reads opt
                    # in because a transient revert there would drop data.
                    if confirm_absence and not absence_seen:
                        absence_seen = True
                        await asyncio.sleep(0.1)
                        continue
                    return None
                attempt_number = transport_failures + 1
                if self._rpc_retry_policy.can_retry(attempt_number):
                    await asyncio.sleep(self._rpc_retry_policy.delay_for_attempt(attempt_number))
                    transport_failures += 1
                    continue
                raise ChainlinkHistoryUnavailable(
                    f"round {round_id} unavailable for feed {address} after bounded retries"
                ) from exc

    @staticmethod
    def _valid_observation(round_data: RoundData) -> bool:
        return (
            round_data.round_id > 0
            and round_data.answer > 0
            and round_data.updated_at > 0
            and round_data.answered_in_round >= round_data.round_id
        )

    @staticmethod
    def _searchable(round_data: RoundData) -> bool:
        """Whether a present round carries a timestamp usable for search."""
        return round_data.round_id > 0 and round_data.updated_at > 0

    @staticmethod
    def _is_absent_round_error(exc: Exception) -> bool:
        """Classify only revert-shaped getRoundData failures as absent rounds."""
        name = type(exc).__name__.lower()
        detail = " ".join(str(arg) for arg in exc.args).lower()
        return (
            "contractlogicerror" in name
            or "contractcustomerror" in name
            or "execution reverted" in detail
            or "no data present" in detail
        )

    async def _required_call(self, address: str, calldata: str, *, operation: str) -> bytes:
        for attempt in self._rpc_retry_policy.attempt_numbers:
            try:
                async with self._rpc_semaphore:
                    return await self._eth_call(address, calldata)
            except Exception as exc:
                if self._rpc_retry_policy.can_retry(attempt):
                    await asyncio.sleep(self._rpc_retry_policy.delay_for_attempt(attempt))
                    continue
                raise ChainlinkHistoryUnavailable(
                    f"{operation} unavailable for feed {address} after bounded retries"
                ) from exc
        raise AssertionError("unreachable")

    async def _verified_decimals(self, address: str, expected: int) -> int:
        cached = self._feed_decimals.get(address.lower())
        if cached is not None:
            return cached
        raw = await self._required_call(address, DECIMALS_SELECTOR, operation="feed decimals")
        try:
            measured = decode_uint8(raw)
        except ValueError as exc:
            raise ChainlinkHistoryUnavailable(f"feed decimals malformed for {address}") from exc
        if measured != expected:
            raise ChainlinkHistoryUnavailable(
                f"feed decimals mismatch for {address}: catalogue={expected}, on-chain={measured}"
            )
        self._feed_decimals[address.lower()] = measured
        return measured

    async def _phase_max_sequence(self, address: str, phase: int) -> int | None:
        """Find the last contiguous sequence number in an earlier proxy phase."""
        if phase <= 0:
            return None

        def composed(sequence: int) -> int:
            return (phase << 64) | sequence

        if await self._round(address, composed(1)) is None:
            return None
        low, high = 1, 2
        # This is an O(log n) boundary probe, not a round scan.  Do not bind it
        # to max_scanned_rounds: a long-lived feed phase can legitimately have
        # more observations than one request is allowed to materialize.
        for _ in range(64):
            if await self._round(address, composed(high)) is None:
                break
            low, high = high, high * 2
        else:
            raise ChainlinkHistoryUnavailable(f"could not bound phase {phase} for feed {address}")
        while low + 1 < high:
            middle = (low + high) // 2
            if await self._round(address, composed(middle)) is None:
                high = middle
            else:
                low = middle
        return low

    async def get_latest(self, *, token: str) -> HistoricalPricePoint:
        """Return the latest measured direct-USD observation for ``token``."""
        spec = CATALOG.feed_for_token(self.chain, token)
        if spec is None:
            raise ChainlinkHistoryUnavailable(f"no direct Chainlink USD feed for {token} on {self.chain}")
        round_data = await self._latest_round(spec.address)
        if not self._valid_observation(round_data):
            raise ChainlinkHistoryUnavailable(f"latest round is invalid for feed {spec.address}")
        decimals = await self._verified_decimals(spec.address, spec.decimals)
        return HistoricalPricePoint(
            timestamp=round_data.updated_at,
            price=Decimal(round_data.answer) / Decimal(10**decimals),
            observation_id=round_data.round_id,
        )

    async def _round_batch(self, address: str, round_ids: list[int]) -> list[RoundData | None]:
        return list(
            await asyncio.gather(*(self._round(address, round_id, confirm_absence=True) for round_id in round_ids))
        )

    @staticmethod
    def _round_id(phase: int, sequence: int, *, phased: bool) -> int:
        return ((phase << 64) | sequence) if phased else sequence

    async def _probe_near_sequence(
        self,
        address: str,
        *,
        phase: int,
        sequence: int,
        minimum: int,
        maximum: int,
        phased: bool,
    ) -> tuple[int, RoundData] | None:
        """Find a valid observation near a binary-search midpoint.

        Aggregator phases are normally contiguous, but defensive gap handling
        prevents one missing round from invalidating timestamp discovery.
        """
        candidates = [sequence]
        for offset in range(1, _MAX_GAP_PROBES + 1):
            candidates.extend((sequence - offset, sequence + offset))
        for candidate in candidates:
            if candidate < minimum or candidate > maximum:
                continue
            value = await self._round(address, self._round_id(phase, candidate, phased=phased))
            if value is not None and self._searchable(value):
                return candidate, value
        return None

    async def _first_sequence_at_or_after(
        self,
        address: str,
        *,
        phase: int,
        maximum: int,
        timestamp: int,
        phased: bool,
    ) -> int:
        """Binary-search the first sequence whose update time reaches a target."""
        low, high = 1, maximum + 1
        for _ in range(_MAX_BINARY_SEARCH_PROBES):
            if low >= high:
                return low
            middle = (low + high) // 2
            current_high = high
            probe = await self._probe_near_sequence(
                address,
                phase=phase,
                sequence=middle,
                minimum=low,
                maximum=min(maximum, high - 1),
                phased=phased,
            )
            if probe is None:
                raise ChainlinkHistoryUnavailable(
                    f"oracle timestamp search found no readable evidence near sequence {middle}"
                )
            sequence, value = probe
            if not low <= sequence < current_high:
                raise ChainlinkHistoryUnavailable("oracle timestamp search made no bounded progress")
            if value.updated_at >= timestamp:
                high = sequence
            else:
                low = sequence + 1
        raise ChainlinkHistoryUnavailable("oracle timestamp search exceeded its probe bound")

    @staticmethod
    def _sample_ranges(
        ranges: list[tuple[int, int, int]],
        *,
        limit: int,
        phased: bool,
    ) -> list[int]:
        """Uniformly sample conceptual phase ranges without materializing them."""
        lengths = [high - low + 1 for _phase, low, high in ranges]
        total = sum(lengths)
        if total <= 0:
            return []
        count = min(total, limit)
        if count == 1:
            positions = [total - 1]
        else:
            positions = sorted({round(index * (total - 1) / (count - 1)) for index in range(count)})
        output: list[int] = []
        range_index = 0
        offset = 0
        for position in positions:
            while position >= offset + lengths[range_index]:
                offset += lengths[range_index]
                range_index += 1
            phase, low, _high = ranges[range_index]
            output.append(ChainlinkHistoryReader._round_id(phase, low + position - offset, phased=phased))
        return output

    async def _history_ranges(
        self,
        address: str,
        *,
        latest: RoundData,
        start_ts: int,
        end_ts: int,
    ) -> tuple[list[tuple[int, int, int]], RoundData | None, bool]:
        """Discover timestamp-overlapping phase ranges, oldest first."""
        latest_phase = latest.round_id >> 64
        phased = latest_phase > 0
        phase = latest_phase if phased else 0
        latest_sequence = latest.round_id & ((1 << 64) - 1) if phased else latest.round_id
        descending: list[tuple[int, int, int]] = []
        previous_phase_point: RoundData | None = None

        for phase_count in range(DEFAULT_MAX_PHASES):
            maximum, last = await self._phase_boundary(
                address,
                phase=phase,
                phase_count=phase_count,
                latest_sequence=latest_sequence,
                latest=latest,
                phased=phased,
            )
            if maximum is None or last is None:
                break
            first = await self._round(
                address,
                self._round_id(phase, 1, phased=phased),
                confirm_absence=True,
            )
            if first is None:
                raise ChainlinkHistoryUnavailable(f"phase {phase} has no readable first observation")
            if last.updated_at < start_ts:
                previous_phase_point = await self._nearest_valid_prior(
                    address,
                    phase=phase,
                    start_sequence=maximum,
                    start_ts=start_ts,
                    phased=phased,
                )
                break
            overlap, prior_sequence = await self._timestamp_overlap_range(
                address,
                phase=phase,
                maximum=maximum,
                first=first,
                start_ts=start_ts,
                end_ts=end_ts,
                phased=phased,
            )
            if overlap is not None:
                descending.append(overlap)
            elif prior_sequence is not None:
                previous_phase_point = await self._nearest_valid_prior(
                    address,
                    phase=phase,
                    start_sequence=prior_sequence,
                    start_ts=start_ts,
                    phased=phased,
                )
                break
            if not phased or phase <= 1:
                break
            phase -= 1
        else:
            raise ChainlinkHistoryUnavailable(f"history exceeds the {DEFAULT_MAX_PHASES}-phase safety bound")

        return list(reversed(descending)), previous_phase_point, phased

    async def _phase_boundary(
        self,
        address: str,
        *,
        phase: int,
        phase_count: int,
        latest_sequence: int,
        latest: RoundData,
        phased: bool,
    ) -> tuple[int | None, RoundData | None]:
        if phase_count == 0:
            return latest_sequence, latest
        if not phased or phase <= 0:
            return None, None
        maximum = await self._phase_max_sequence(address, phase)
        if maximum is None:
            return None, None
        last = await self._round(
            address,
            self._round_id(phase, maximum, phased=True),
            confirm_absence=True,
        )
        return maximum, last

    async def _timestamp_overlap_range(
        self,
        address: str,
        *,
        phase: int,
        maximum: int,
        first: RoundData,
        start_ts: int,
        end_ts: int,
        phased: bool,
    ) -> tuple[tuple[int, int, int] | None, int | None]:
        if first.updated_at >= end_ts:
            return None, None
        low = await self._first_sequence_at_or_after(
            address,
            phase=phase,
            maximum=maximum,
            timestamp=start_ts,
            phased=phased,
        )
        end_sequence = await self._first_sequence_at_or_after(
            address,
            phase=phase,
            maximum=maximum,
            timestamp=end_ts,
            phased=phased,
        )
        high = min(maximum, end_sequence - 1)
        if low <= high:
            return (phase, low, high), None
        return None, low - 1 if low > 1 else None

    async def _prior_observation(
        self,
        address: str,
        *,
        ranges: list[tuple[int, int, int]],
        fallback: RoundData | None,
        start_ts: int,
        phased: bool,
    ) -> RoundData | None:
        if not ranges:
            return fallback
        earliest_phase, earliest_low, _ = ranges[0]
        if earliest_low > 1:
            return await self._nearest_valid_prior(
                address,
                phase=earliest_phase,
                start_sequence=earliest_low - 1,
                start_ts=start_ts,
                phased=phased,
            )
        if not phased or earliest_phase <= 1:
            return fallback
        previous_max = await self._phase_max_sequence(address, earliest_phase - 1)
        if previous_max is None:
            return fallback
        return await self._nearest_valid_prior(
            address,
            phase=earliest_phase - 1,
            start_sequence=previous_max,
            start_ts=start_ts,
            phased=True,
        )

    async def _nearest_valid_prior(
        self,
        address: str,
        *,
        phase: int,
        start_sequence: int,
        start_ts: int,
        phased: bool,
    ) -> RoundData:
        """Return the nearest bounded, publishable observation before ``start_ts``."""
        minimum = max(1, start_sequence - _MAX_GAP_PROBES)
        for sequence in range(start_sequence, minimum - 1, -1):
            round_data = await self._round(
                address,
                self._round_id(phase, sequence, phased=phased),
                confirm_absence=True,
            )
            if round_data is not None and self._valid_observation(round_data) and round_data.updated_at < start_ts:
                return round_data
        raise ChainlinkHistoryUnavailable(
            f"no valid prior observation within {_MAX_GAP_PROBES + 1} rounds "
            f"of sequence {start_sequence} in phase {phase}"
        )

    async def _materialize_ranges(
        self,
        address: str,
        *,
        ranges: list[tuple[int, int, int]],
        max_points: int,
        phased: bool,
    ) -> list[RoundData | None]:
        limit = self._materialization_limit(max_points)
        round_ids = self._sample_ranges(ranges, limit=limit, phased=phased)
        rounds: list[RoundData | None] = []
        for offset in range(0, len(round_ids), self._batch_size):
            rounds.extend(await self._round_batch(address, round_ids[offset : offset + self._batch_size]))
        return rounds

    def _materialization_limit(self, max_points: int) -> int:
        return min(self._max_scanned_rounds, max(max_points * 2, self._batch_size))

    @staticmethod
    def _price_points(
        rounds: list[RoundData | None],
        *,
        prior: RoundData | None,
        start_ts: int,
        end_ts: int,
        decimals: int,
    ) -> list[HistoricalPricePoint]:
        selected = [
            round_data
            for round_data in rounds
            if round_data is not None
            and ChainlinkHistoryReader._valid_observation(round_data)
            and start_ts <= round_data.updated_at < end_ts
        ]
        if prior is not None and ChainlinkHistoryReader._valid_observation(prior) and prior.updated_at < start_ts:
            selected.insert(0, prior)
        scale = Decimal(10**decimals)
        return [
            HistoricalPricePoint(
                timestamp=round_data.updated_at,
                price=Decimal(round_data.answer) / scale,
                observation_id=round_data.round_id,
            )
            for round_data in selected
        ]

    async def get_history(
        self,
        *,
        token: str,
        start_ts: int,
        end_ts: int,
        max_points: int,
    ) -> list[HistoricalPricePoint]:
        """Compatibility list view of :meth:`get_history_page`."""
        return (
            await self.get_history_page(token=token, start_ts=start_ts, end_ts=end_ts, max_points=max_points)
        ).points

    async def get_history_page(
        self,
        *,
        token: str,
        start_ts: int,
        end_ts: int,
        max_points: int,
    ) -> HistoricalPricePage:
        """Return observations in ``[start_ts, end_ts)`` plus one prior point.

        The prior point makes an at-or-before lookup at ``start_ts`` honest even
        when no oracle update lands exactly on the interval boundary.
        """
        if start_ts <= 0 or end_ts <= start_ts:
            raise ValueError("Chainlink history requires a positive increasing interval")
        if max_points <= 0:
            raise ValueError("Chainlink history max_points must be positive")
        spec = CATALOG.feed_for_token(self.chain, token)
        if spec is None:
            raise ChainlinkHistoryUnavailable(f"no direct Chainlink USD feed for {token} on {self.chain}")

        try:
            async with asyncio.timeout(self._history_timeout_seconds):
                decimals = await self._verified_decimals(spec.address, spec.decimals)
                latest = await self._latest_round(spec.address)
                ranges, fallback_prior, phased = await self._history_ranges(
                    spec.address,
                    latest=latest,
                    start_ts=start_ts,
                    end_ts=end_ts,
                )
                prior = await self._prior_observation(
                    spec.address,
                    ranges=ranges,
                    fallback=fallback_prior,
                    start_ts=start_ts,
                    phased=phased,
                )
                range_candidate_count = sum(high - low + 1 for _phase, low, high in ranges)
                rounds = await self._materialize_ranges(
                    spec.address,
                    ranges=ranges,
                    max_points=max_points,
                    phased=phased,
                )
                points = self._price_points(
                    rounds,
                    prior=prior,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    decimals=decimals,
                )
        except TimeoutError as exc:
            raise ChainlinkHistoryUnavailable(
                f"Chainlink history exceeded {self._history_timeout_seconds:g}s wall-clock budget"
            ) from exc
        if not points:
            raise ChainlinkHistoryUnavailable(f"no Chainlink observations for {token} in requested interval")

        ordered = sorted({point.observation_id: point for point in points}.values(), key=lambda point: point.timestamp)
        materialization_limit = self._materialization_limit(max_points)
        truncated = range_candidate_count > materialization_limit or len(ordered) > max_points
        return HistoricalPricePage(
            points=self._downsample(ordered, max_points),
            truncated=truncated,
            recommended_split_ts=(start_ts + end_ts) // 2 if truncated else 0,
        )

    @staticmethod
    def _downsample(points: list[HistoricalPricePoint], max_points: int) -> list[HistoricalPricePoint]:
        if len(points) <= max_points:
            return points
        if max_points == 1:
            return [points[-1]]
        last = len(points) - 1
        indexes = [round(index * last / (max_points - 1)) for index in range(max_points)]
        return [points[index] for index in indexes]

    async def close(self) -> None:
        web3 = self._web3
        self._web3 = None
        if web3 is None:
            return
        provider = getattr(web3, "provider", None)
        disconnect = getattr(provider, "disconnect", None)
        if disconnect is not None:
            result = disconnect()
            if asyncio.iscoroutine(result):
                await result


__all__ = [
    "ChainlinkHistoryReader",
    "ChainlinkHistoryUnavailable",
    "HistoricalPricePoint",
    "HistoricalPricePage",
]
