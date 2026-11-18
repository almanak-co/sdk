"""Versioned display values and blocks. These models confer no execution authority."""

from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from almanak.caip import AssetType
from almanak.configuration.composed import Parameters as ComposedParameters
from almanak.connectors.aave_v3.domain import Target as AaveV3Target
from almanak.connectors.aave_v3.models import Parameters as AaveV3Parameters
from almanak.connectors.aave_v4.models import LendingConfig, SupplyTarget
from almanak.connectors.aerodrome.models import Parameters as AerodromeParameters
from almanak.connectors.aerodrome.models import Target as AerodromeTarget
from almanak.connectors.aerodrome_slipstream.models import Parameters as SlipstreamParameters
from almanak.connectors.aerodrome_slipstream.models import Target as SlipstreamTarget
from almanak.connectors.compound_v3.domain import Target as CompoundTarget
from almanak.connectors.compound_v3.models import Parameters as CompoundParameters
from almanak.connectors.curve.models import Parameters as CurveParameters
from almanak.connectors.curve.models import Target as CurveTarget
from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget as GMXExchangeTarget
from almanak.connectors.gmx_v2.models import Parameters as GMXParameters
from almanak.connectors.gmx_v2.models import Target as GMXTarget
from almanak.connectors.hyperliquid.models import Parameters as HyperliquidParameters
from almanak.connectors.hyperliquid.models import Target as HyperliquidTarget
from almanak.connectors.morpho_blue.domain import Target as MorphoTarget
from almanak.connectors.morpho_blue.models import Parameters as MorphoParameters
from almanak.connectors.pancakeswap_v3.models import Parameters as PancakeParameters
from almanak.connectors.pancakeswap_v3.models import Target as PancakeTarget
from almanak.connectors.sparklend.domain import Target as SparkTarget
from almanak.connectors.sparklend.models import Parameters as SparkParameters
from almanak.connectors.uniswap_v3.models import Parameters as V3ExchangeParameters
from almanak.connectors.uniswap_v3.models import Target as V3ExchangeTarget
from almanak.connectors.uniswap_v4.models import (
    Config as ExchangeConfig,
)
from almanak.connectors.uniswap_v4.models import (
    Target as ExchangeTarget,
)
from almanak.models import Account, Block, Chain, Model
from almanak.strategy_api import Memory

Key = Annotated[str, Field(pattern=r"^[a-zA-Z0-9_:-]{1,100}$")]
Label = Annotated[str, Field(min_length=1, max_length=200)]
TextValue = Annotated[str, Field(max_length=2000)]
ShortLabel = Annotated[str, Field(min_length=1, max_length=60)]
Quantity = Annotated[str, Field(pattern=r"^-?(0|[1-9][0-9]{0,99})$")]


class Scope(Model):
    account: Label
    fund: Account
    target: (
        SupplyTarget
        | AaveV3Target
        | SparkTarget
        | CompoundTarget
        | MorphoTarget
        | ExchangeTarget
        | V3ExchangeTarget
        | PancakeTarget
        | SlipstreamTarget
        | AerodromeTarget
        | CurveTarget
        | GMXTarget
        | GMXExchangeTarget
        | HyperliquidTarget
        | None
    ) = None
    payer: Account | None = None

    @model_validator(mode="after")
    def identity(self) -> Self:
        if self.target and (self.target.account != self.account or self.target.fund != self.fund):
            raise ValueError("value target must retain its named fund scope")
        if self.payer and self.payer.chain != self.fund.chain:
            raise ValueError("gas payer must retain chain/environment scope")
        return self


Unit = Literal[
    "base_units", "shares", "count", "seconds", "ratio", "percent", "usd", "index", "price"
]
Evidence = Literal["recorded", "configured", "author_derived", "projected"]


class Value(Model):
    id: Key
    amount: Quantity | None
    unit: Unit
    asset: AssetType | None = None
    decimals: Annotated[int, Field(ge=0, le=255)] | None = None
    scope: Scope | None = None
    evidence: Evidence
    block: Block | None = None
    reason: TextValue = ""
    inputs: Annotated[tuple[Key, ...], Field(max_length=32)] = ()
    # Where a fiat figure's price came from (oracle, pool, feed and its observation). Required
    # for `usd`: a dashboard never assumes a price.
    price_source: TextValue = ""
    # Display label only, never identity: a token symbol for base_units ("USDC"), or the
    # quote-per-base pair of a price ("USDC per WETH"). The asset field stays the identity.
    symbol: Annotated[str, Field(min_length=1, max_length=40)] | None = None
    # The asset a price is quoted in: two quotes are two scales, so prices compare by it.
    quote: AssetType | None = None

    @model_validator(mode="after")
    def identity(self) -> Self:
        if self.unit in ("base_units", "shares"):
            self.financial_identity()
        elif (self.asset is not None and self.unit != "price") or (
            self.decimals is not None and not self.fixed_point
        ):
            raise ValueError(
                "non-asset units cannot carry asset precision (a price may name its priced asset)"
            )
        if self.unit in ("usd", "price") and not self.price_source:
            raise ValueError(
                f"{self.unit} values require price_source: name the oracle, pool or feed the"
                " price came from"
            )
        if self.unit not in ("usd", "price") and self.price_source:
            raise ValueError("price_source is only for usd and price values")
        if (self.unit == "price") != (self.quote is not None):
            raise ValueError("a price names the asset it is quoted in, and only a price has one")
        if self.symbol is not None and self.unit not in ("base_units", "price"):
            raise ValueError(
                "symbol labels token amounts (base_units) and prices only; put other names in"
                " the block label"
            )
        if self.amount is None and not self.reason:
            raise ValueError("missing values require an explicit reason")
        if self.block and self.scope and self.block.chain != self.scope_chain:
            raise ValueError("observation block has a different value scope")
        if self.evidence in ("author_derived", "projected") and not self.reason:
            raise ValueError("derived and projected values require a calculation explanation")
        return self

    @property
    def fixed_point(self) -> bool:
        return self.unit in ("ratio", "percent", "usd", "index", "price")

    @property
    def scope_chain(self) -> Chain | None:
        if self.scope is None:
            return None
        return self.scope.fund.chain

    def financial_identity(self):
        if self.asset is None or self.scope is None:
            raise ValueError("financial values require asset and fund scope")
        chain = self.scope_chain
        if chain is None or self.asset.chain != chain.chain_id:
            raise ValueError("asset has a different fund chain")
        if self.unit == "shares" and (self.scope.target is None or self.decimals is not None):
            raise ValueError("shares require a position target and cannot use token decimals")
        target_assets = (
            self.scope.target.accounting_assets
            if isinstance(self.scope.target, GMXExchangeTarget)
            else (*self.scope.target.assets, self.scope.target.pool.shares_asset)
            if isinstance(self.scope.target, (AerodromeTarget, CurveTarget))
            else (
                self.scope.target.assets
                if isinstance(
                    self.scope.target,
                    (
                        ExchangeTarget,
                        V3ExchangeTarget,
                        PancakeTarget,
                        SlipstreamTarget,
                        GMXTarget,
                        HyperliquidTarget,
                    ),
                )
                else (self.scope.target.asset,)
                if self.scope.target
                else ()
            )
        )
        if self.scope.target and self.asset not in target_assets:
            raise ValueError("value asset differs from its position target")


class Ref(Model):
    ref: Key


Cell = TextValue | Ref


class Column(Model):
    key: Key
    label: Label
    kind: Literal["text", "value"] = "text"


class Dataset(Model):
    columns: Annotated[tuple[Column, ...], Field(min_length=1, max_length=16)]
    rows: Annotated[tuple[dict[Key, Cell], ...], Field(max_length=500)]
    coverage: TextValue
    truncated: bool = False

    @model_validator(mode="after")
    def cells(self) -> Self:
        columns = {column.key: column for column in self.columns}
        if len(columns) != len(self.columns):
            raise ValueError("dataset columns must be unique")
        for row in self.rows:
            validate_row(row, columns)
        return self


def validate_row(row, columns):
    if set(row) != set(columns):
        raise ValueError("row must provide exactly the declared columns")
    for key, cell in row.items():
        if isinstance(cell, Ref) != (columns[key].kind == "value"):
            raise ValueError("financial columns require value references")


class Text(Model):
    kind: Literal["text"] = "text"
    id: Key
    text: TextValue


CalloutTone = Literal["info", "warning", "danger", "success"]


class Callout(Model):
    """A short titled note: a risk, an assumption or what the dashboard does not model."""

    kind: Literal["callout"] = "callout"
    id: Key
    tone: CalloutTone = "info"
    title: Label
    text: TextValue


class State(Model):
    id: Key
    label: ShortLabel
    detail: Annotated[str, Field(max_length=240)] = ""
    tone: Literal["neutral", "warning", "danger", "success"] = "neutral"
    terminal: bool = False


class Transition(Model):
    source: Key
    target: Key
    label: Annotated[str, Field(min_length=1, max_length=90)]
    tone: Literal["neutral", "warning", "danger", "success", "info"] = "neutral"


class Steps(Model):
    """A strategy's lifecycle as states and labelled transitions, listed in reading order.

    A transition to the next listed state reads as the flow; any other (a branch, a loop back,
    an exit) is drawn under its source state.
    """

    kind: Literal["steps"] = "steps"
    id: Key
    title: Label
    states: Annotated[tuple[State, ...], Field(min_length=1, max_length=12)]
    transitions: Annotated[tuple[Transition, ...], Field(max_length=24)] = ()

    @model_validator(mode="after")
    def graph(self) -> Self:
        ids = [state.id for state in self.states]
        if len(set(ids)) != len(ids):
            raise ValueError(f"steps {self.id}: state ids must be unique")
        for edge in self.transitions:
            missing = {edge.source, edge.target} - set(ids)
            if missing:
                raise ValueError(
                    f"steps {self.id}: transition {edge.label!r} names unknown state"
                    f" {sorted(missing)}; declare it in states"
                )
        return self


class Metric(Model):
    kind: Literal["metric"] = "metric"
    id: Key
    label: Label
    value: Value


class Table(Model):
    kind: Literal["table"] = "table"
    id: Key
    title: Label
    data: Dataset


class Point(Model):
    label: Label
    value: Value


class BarChart(Model):
    kind: Literal["bar"] = "bar"
    id: Key
    title: Label
    points: Annotated[tuple[Point, ...], Field(max_length=100)]
    caption: TextValue


class Sample(Model):
    time: Annotated[int, Field(ge=0, le=9007199254740991)]
    # A Ref cites a snapshot fact by id instead of repeating it (recorded price history).
    value: Value | Ref


Tone = Literal["neutral", "warning", "danger", "success"]
AxisPosition = Annotated[int, Field(ge=-9007199254740991, le=9007199254740991)]


class Move(Model):
    """One sample on a price-move axis: `move_bps` from today's price, e.g. -2500 for -25%."""

    move_bps: Annotated[int, Field(ge=-10_000, le=1_000_000)]
    value: Value | Ref


class Reference(Model):
    """A labelled horizontal line at a value comparable with the chart's samples."""

    label: Label
    value: Value
    tone: Tone = "neutral"


class Band(Model):
    """A labelled horizontal range; an open side extends to the chart's edge."""

    label: Label
    low: Value | None = None
    high: Value | None = None
    tone: Tone = "neutral"

    @model_validator(mode="after")
    def bounded(self) -> Self:
        if self.low is None and self.high is None:
            raise ValueError("a band needs a low or a high bound")
        return self


class Annotation(Model):
    """A labelled mark on the line at `at`: a sample time, or `move_bps` on a move axis."""

    label: Label
    at: AxisPosition
    tone: Tone = "neutral"
    series: Key | None = None


Panel = Literal["main", "lower"]
LineStyle = Literal["solid", "dashed"]
XAxis = Literal["time", "price_move"]


# Bounds on each branch, so the JSON schema states them as array item counts (minItems/maxItems)
# that a browser validator applies, not as a string length on the union.
SeriesPoints = (
    Annotated[tuple[Sample, ...], Field(min_length=1, max_length=500)]
    | Annotated[tuple[Move, ...], Field(min_length=1, max_length=500)]
)
ChartPoints = (
    Annotated[tuple[Sample, ...], Field(max_length=500)]
    | Annotated[tuple[Move, ...], Field(max_length=500)]
)


class Series(Model):
    """One named line. Series in a panel share its y scale, so their values must be comparable;
    the `lower` panel (e.g. an indicator under a price) has its own scale on the same x axis."""

    id: Key
    label: ShortLabel
    points: SeriesPoints
    panel: Panel = "main"
    style: LineStyle = "solid"


class Marker(Model):
    """A data-driven event on a series at `at` (a trade, a recentre, a scheduled buy)."""

    at: AxisPosition
    label: Annotated[str, Field(min_length=1, max_length=24)]
    tone: Tone = "neutral"
    series: Key | None = None


class LineChart(Model):
    kind: Literal["line"] = "line"
    id: Key
    title: Label
    x_axis: XAxis = "time"
    points: ChartPoints = ()
    caption: TextValue
    series: Annotated[tuple[Series, ...], Field(max_length=6)] = ()
    references: Annotated[tuple[Reference, ...], Field(max_length=8)] = ()
    bands: Annotated[tuple[Band, ...], Field(max_length=8)] = ()
    annotations: Annotated[tuple[Annotation, ...], Field(max_length=8)] = ()
    markers: Annotated[tuple[Marker, ...], Field(max_length=200)] = ()
    # Labels of the references and bands drawn on the lower panel: an indicator's 30/70
    # lines belong there. Empty means every mark is on the main panel.
    lower_marks: Annotated[tuple[Label, ...], Field(max_length=16)] = ()
    # What each axis measures, in words ("value change, % of position", "WETH price move from
    # today", "RSI 14"). Without them the renderer names a price axis by its symbol and a
    # price-move axis generically; the dates of a time axis speak for themselves.
    x_label: ShortLabel | None = None
    y_label: ShortLabel | None = None
    lower_label: ShortLabel | None = None

    @model_validator(mode="after")
    def chronological(self) -> Self:
        if bool(self.points) == bool(self.series):
            raise ValueError(
                f"line {self.id}: give either points (one line) or series (several lines)"
            )
        sample = Move if self.x_axis == "price_move" else Sample
        for name, points in self.lines():
            if any(not isinstance(point, sample) for point in points):
                raise ValueError(
                    f"line {self.id}: {name} samples must match x_axis={self.x_axis!r}"
                    f" ({sample.__name__})"
                )
            positions = [chart_position(point) for point in points]
            if positions != sorted(positions):
                raise ValueError(f"line {self.id}: {name} samples must be ordered along the x axis")
        ids = [series.id for series in self.series]
        if len(set(ids)) != len(ids):
            raise ValueError(f"line {self.id}: series ids must be unique")
        marks: tuple[Marker | Annotation, ...] = (*self.markers, *self.annotations)
        for item in marks:
            if item.series is not None and item.series not in ids:
                raise ValueError(f"line {self.id}: mark {item.label!r} names unknown series")
        if self.series and self.series[0].panel != "main":
            raise ValueError(f"line {self.id}: the first series must be on the main panel")
        self.lower_marks_drawn()
        return self

    def lower_marks_drawn(self):
        if self.lower_marks and not any(series.panel == "lower" for series in self.series):
            raise ValueError(f"line {self.id}: lower_marks need a series on the lower panel")
        labels = {r.label for r in self.references} | {b.label for b in self.bands}
        unknown = [label for label in self.lower_marks if label not in labels]
        if unknown:
            raise ValueError(f"line {self.id}: lower_marks {unknown} name no reference or band")

    def lines(self) -> tuple[tuple[str, tuple], ...]:
        if self.series:
            return tuple((f"series {s.id}", s.points) for s in self.series)
        return (("points", self.points),)

    def panel_values(self, facts: dict | None = None) -> dict[str, list[Value]]:
        """Each panel's values; a cited fact (Ref) is resolved from `facts`, or left out
        when no facts are given."""

        def resolved(points):
            for point in points:
                if not isinstance(point.value, Ref):
                    yield point.value
                elif facts is not None:
                    if point.value.ref not in facts:
                        raise ValueError(f"line {self.id} cites an unknown snapshot fact")
                    yield facts[point.value.ref]

        panels: dict[str, list[Value]] = {"main": [], "lower": []}
        if self.series:
            for series in self.series:
                panels[series.panel].extend(resolved(series.points))
        else:
            panels["main"].extend(resolved(self.points))
        for mark in (
            *((r.label, r.value) for r in self.references),
            *((b.label, v) for b in self.bands for v in (b.low, b.high) if v is not None),
        ):
            panels["lower" if mark[0] in self.lower_marks else "main"].append(mark[1])
        return panels


def chart_position(point) -> int:
    return point.move_bps if isinstance(point, Move) else point.time


BlockType = Annotated[
    Text | Callout | Steps | Metric | Table | BarChart | LineChart, Field(discriminator="kind")
]


class Group(Model):
    kind: Literal["group"] = "group"
    id: Key
    title: Label
    blocks: Annotated[tuple[BlockType, ...], Field(min_length=1, max_length=16)]
    # Detail the reader opens on demand: drawn folded under its title.
    collapsed: bool = False


DashboardBlock = Annotated[
    Text | Callout | Steps | Metric | Table | BarChart | LineChart | Group,
    Field(discriminator="kind"),
]


class Dashboard(Model):
    title: Label
    description: TextValue = ""
    blocks: Annotated[tuple[DashboardBlock, ...], Field(min_length=1, max_length=32)]

    @model_validator(mode="after")
    def identifiers(self) -> Self:
        blocks = list(self.blocks)
        for block in self.blocks:
            if isinstance(block, Group):
                blocks.extend(block.blocks)
        if len(blocks) > 64:
            raise ValueError(f"dashboard allows at most 64 blocks, got {len(blocks)}")
        seen, repeated = set(), []
        for block in blocks:
            if block.id in seen:
                repeated.append(block.id)
            seen.add(block.id)
        if repeated:
            names = ", ".join(sorted(set(repeated)))
            raise ValueError(f"dashboard block IDs must be unique; repeated: {names}")
        return self


class Position(Model):
    allocation: Label
    target: SupplyTarget | AaveV3Target | SparkTarget | CompoundTarget | MorphoTarget
    owned_shares: Value


class ExchangePosition(Model):
    allocation: Label
    target: (
        ExchangeTarget
        | V3ExchangeTarget
        | PancakeTarget
        | SlipstreamTarget
        | AerodromeTarget
        | CurveTarget
        | GMXTarget
        | GMXExchangeTarget
        | HyperliquidTarget
    )
    inventory: tuple[Value, ...]


# A decimal reading for charts: plain digits, an optional point, no exponent.
DecimalText = Annotated[str, Field(pattern=r"^-?[0-9]{1,60}(\.[0-9]{1,30})?$")]


class HistoryRow(Model):
    """One retained observation for charts: when it was read and decimal readings named by the
    connector family (e.g. `price`, `range_lower`, `position0`, `wallet1`, `value`)."""

    time: Annotated[int, Field(ge=0)]
    block: Annotated[int, Field(ge=0)]
    fields: Annotated[dict[Key, DecimalText], Field(max_length=32)]


class PriceSample(Model):
    time: Annotated[int, Field(ge=0)]
    value: DecimalText


class StrategyPrices(Model):
    """A price series the strategy declared, as its latest decision read it: the closes its
    indicators were computed from, in `quote` per `asset`."""

    asset: AssetType
    quote: AssetType | None = None
    label: Label | None = None
    timeframe: Annotated[str, Field(max_length=16)] | None = None
    points: Annotated[tuple[PriceSample, ...], Field(max_length=500)]


class TradeAmount(Model):
    asset: AssetType
    symbol: ShortLabel | None = None
    # Signed, in token units: negative left the fund, positive arrived.
    amount: DecimalText


class Trade(Model):
    """One committed transaction that moved the fund's tokens (approvals are left out)."""

    time: Annotated[int, Field(ge=0)]
    allocation: Label
    # The intent it carried out (`swap_exact_input`, `exit_exchange`, `open_liquidity`, ...).
    action: Label
    amounts: Annotated[tuple[TradeAmount, ...], Field(max_length=8)] = ()


class Context(Model):
    config: (
        LendingConfig
        | AaveV3Parameters
        | SparkParameters
        | CompoundParameters
        | MorphoParameters
        | ExchangeConfig
        | PancakeParameters
        | SlipstreamParameters
        | AerodromeParameters
        | CurveParameters
        | GMXParameters
        | HyperliquidParameters
        | V3ExchangeParameters
        | ComposedParameters
    )
    memory: Memory
    positions: tuple[Position | ExchangePosition, ...]
    values: Annotated[dict[Key, Value], Field(max_length=5000)]
    datasets: Annotated[dict[Key, Dataset], Field(max_length=16)]
    # Readings at each decision, oldest first: what charts draw. Empty until the run has observed.
    history: Annotated[tuple[HistoryRow, ...], Field(max_length=500)] = ()
    # The strategy's declared price series at its latest decision: a full window from the start.
    prices: Annotated[tuple[StrategyPrices, ...], Field(max_length=8)] = ()
    # Committed transactions, oldest first: the strategy's trades, for markers on a chart.
    trades: Annotated[tuple[Trade, ...], Field(max_length=500)] = ()
    mode: Literal["live"] = "live"


class Snapshot(Model):
    deployment_id: Label
    strategy_hash: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")] | None
    config_hash: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    record_hash: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    captured_at: Annotated[int, Field(ge=0)]
    mode: Literal["running", "teardown", "closed"]
    initialized: bool
    chains: tuple[Chain, ...]
    fork_origin: Block | None
    pending_hash: str | None
    reserved_attempt: str | None
    undelivered_outcomes: Annotated[int, Field(ge=0)]
    latest_observation: Annotated[int, Field(ge=0)] | None
    notices: tuple[TextValue, ...]
    values: Annotated[dict[Key, Value], Field(max_length=5000)]

    @model_validator(mode="after")
    def fact_identity(self) -> Self:
        if any(key != value.id for key, value in self.values.items()):
            raise ValueError("snapshot fact keys must match their identifiers")
        return self


class Response(Model):
    protocol_version: Literal[1] = 1
    snapshot: Snapshot
    status: Literal["ready", "absent", "unavailable"]
    document: Dashboard | None
    diagnostic: TextValue = ""

    @model_validator(mode="after")
    def content(self) -> Self:
        if (self.status == "ready") != (self.document is not None):
            raise ValueError("ready response requires a document")
        if self.document:
            validate_document(self.document, self.snapshot.values)
        if len(self.model_dump_json().encode()) > 2 * 1024 * 1024:
            raise ValueError("dashboard response exceeds its byte limit")
        return self


def validate_document(document: Dashboard, values: dict[str, Value]):
    for block in document.blocks:
        if isinstance(block, Group):
            for child in block.blocks:
                validate_block(child, values)
        else:
            validate_block(block, values)


def validate_block(block, values):
    if isinstance(block, Metric):
        validate_value(block.value, values)
    elif isinstance(block, Table):
        for row in block.data.rows:
            for cell in row.values():
                if isinstance(cell, Ref) and cell.ref not in values:
                    raise ValueError("dataset references an unknown snapshot fact")
    elif isinstance(block, BarChart):
        validate_values([p.value for p in block.points], values, "chart")
    elif isinstance(block, LineChart):
        for panel, panel_values in block.panel_values(values).items():
            if panel_values:
                validate_values(panel_values, values, f"line {block.id} {panel} panel")


def validate_value(value, values):
    if value.id in values:
        if value != values[value.id]:
            raise ValueError("dashboard changed a snapshot fact")
    elif value.evidence not in ("author_derived", "projected"):
        raise ValueError(
            f"value {value.id}: a dashboard cannot assert a new {value.evidence} fact;"
            " use evidence='projected' or 'author_derived' with a reason"
        )
    if any(key not in values for key in value.inputs):
        raise ValueError("calculation references unknown inputs")


def validate_values(chart_values, values, where):
    units = set()
    for value in chart_values:
        validate_value(value, values)
        position = value.scope.target if value.scope and value.unit == "shares" else None
        units.add(
            (
                value.unit,
                value.asset,
                None if value.fixed_point else value.decimals,
                position,
                value.scope_chain,
                value.quote,
            )
        )
    if len(units) > 1:
        raise ValueError(
            f"{where}: values must share one unit, asset and environment"
            " (put a different unit on the lower panel or in another chart)"
        )
