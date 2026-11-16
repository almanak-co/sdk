"""Time, capability and inventory contracts without protocol or VM assumptions."""

from datetime import UTC, datetime, timedelta

from pydantic_core import core_schema

from almanak.connectors.identifiers import ConnectorId as ConnectorId
from almanak.connectors.identifiers import Name as Name
from almanak.models import Chain, Model


class UnixTime(int):
    def __new__(cls, value):
        if isinstance(value, datetime):
            if value.utcoffset() is None or value.microsecond:
                raise ValueError("time requires an explicit timezone and whole seconds")
            value = int(value.timestamp())
        if type(value) not in (int, cls) or not 0 <= value <= 253402300799:
            raise ValueError("time must be UTC Unix seconds in the supported datetime range")
        return int.__new__(cls, value)

    def timestamp(self) -> int:
        """The Unix seconds themselves, so code written for a datetime field also reads this one."""
        return int(self)

    def datetime(self) -> datetime:
        """The same instant as a UTC datetime."""
        return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(seconds=int(self))

    @classmethod
    def __get_pydantic_core_schema__(cls, source, handler):
        return core_schema.no_info_plain_validator_function(
            cls,
            json_schema_input_schema=core_schema.int_schema(ge=0, le=253402300799),
            serialization=core_schema.plain_serializer_function_ser_schema(int),
        )


class AvailabilityFailure(Model):
    connector: ConnectorId
    chain: Chain
    requested_at: UnixTime
    backtest_start_at: UnixTime | None
    action: str | None
    reason: str


class AvailabilityError(ValueError):
    def __init__(self, failure: AvailabilityFailure):
        self.failure = failure
        super().__init__(
            f"{failure.reason}: {failure.connector}/{failure.chain.chain_id} "
            f"requested={failure.requested_at} earliest={failure.backtest_start_at}"
        )


class Capability(Model):
    connector: ConnectorId
    chain: Chain
    model: Name
    actions: tuple[Name, ...]
    backtest_start_at: UnixTime | None = None

    def require(self, time: UnixTime, action: str | None = None):
        if action is not None and action not in self.actions:
            self.refuse(time, action, "unsupported backtest action")
        if self.backtest_start_at is not None and time < self.backtest_start_at:
            self.refuse(time, action, "backtest_before_start")

    def refuse(self, time, action, reason):
        raise AvailabilityError(
            AvailabilityFailure(
                connector=self.connector,
                chain=self.chain,
                requested_at=time,
                backtest_start_at=self.backtest_start_at,
                action=action,
                reason=reason,
            )
        )
