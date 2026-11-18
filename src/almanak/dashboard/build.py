"""Authoring helpers: build dashboard blocks from plain numbers in a few calls.

Every helper returns protocol models, so the result validates exactly like hand-built blocks.
Numbers go in as Decimal, int, float or str and are stored as exact fixed-point quantities.
"""

from collections.abc import Iterable, Mapping, Sequence
from decimal import ROUND_DOWN, Decimal
from fractions import Fraction

from almanak.dashboard.models import (
    Annotation,
    Band,
    BarChart,
    Callout,
    CalloutTone,
    Dashboard,
    Evidence,
    Group,
    LineChart,
    LineStyle,
    Marker,
    Metric,
    Move,
    Panel,
    Point,
    Ref,
    Reference,
    Sample,
    Series,
    State,
    Steps,
    Tone,
    Transition,
    Unit,
    Value,
    XAxis,
)
from almanak.models import Block

Number = Decimal | int | float | str


def quantity(amount: Number, places: int, rounding=ROUND_DOWN) -> str:
    """`amount` as an integer count of 10^-places units, e.g. quantity("2.555", 2) == "255"."""
    scaled = Decimal(str(amount)) * Decimal(10) ** places
    return str(int(scaled.to_integral_value(rounding=rounding)))


def value(
    id: str,
    amount: Number | None,
    unit: Unit,
    *,
    reason: str,
    places: int | None = None,
    evidence: Evidence = "projected",
    block: Block | None = None,
    inputs: Iterable[str] = (),
    asset=None,
    scope=None,
    price_source: str = "",
    symbol: str | None = None,
    quote=None,
) -> Value:
    """One value. `places` is the fixed-point precision (ratio, percent, usd) or the token
    decimals (base_units); `amount` is the human number, never pre-scaled."""
    if places is None:
        places = {"ratio": 4, "percent": 2, "usd": 2, "index": 2, "price": 8}.get(unit, 0)
    # A derived value names its fund; the position target (with protocol integers past what a
    # browser number holds exactly) belongs to the recorded facts it was derived from.
    if evidence != "recorded" and scope is not None and getattr(scope, "target", None) is not None:
        scope = scope.model_copy(update={"target": None})
    return Value(
        id=id,
        amount=None if amount is None else quantity(amount, places),
        unit=unit,
        asset=asset,
        decimals=places
        if unit in ("ratio", "percent", "usd", "index", "price", "base_units")
        else None,
        scope=scope,
        evidence=evidence,
        block=block,
        reason=reason,
        inputs=tuple(inputs),
        price_source=price_source,
        symbol=symbol,
        quote=quote,
    )


def ratio(id: str, amount: Number, reason: str, *, places: int = 2, **kwargs) -> Value:
    return value(id, amount, "ratio", reason=reason, places=places, **kwargs)


def percent(id: str, amount: Number, reason: str, *, places: int = 1, **kwargs) -> Value:
    """`amount` in percent: percent("drop", -21.6, ...) shows -21.6%."""
    return value(id, amount, "percent", reason=reason, places=places, **kwargs)


def usd(id: str, amount: Number, reason: str, *, price_source: str, places: int = 2, **kwargs):
    """A dollar figure. `price_source` names the oracle, pool or feed and its observation."""
    return value(
        id, amount, "usd", reason=reason, places=places, price_source=price_source, **kwargs
    )


def index(id: str, amount: Number, reason: str, *, places: int = 1, **kwargs) -> Value:
    """An indicator reading (RSI 31.4, a z-score): shown as a bare number, no unit."""
    return value(id, amount, "index", reason=reason, places=places, **kwargs)


def price(
    id: str,
    amount: Number,
    reason: str,
    *,
    price_source: str,
    symbol: str | None,
    quote,
    asset=None,
    **kwargs,
) -> Value:
    """A price of `asset` in the `quote` asset, labelled `symbol` ("USDC per WETH")."""
    return value(
        id,
        amount,
        "price",
        reason=reason,
        price_source=price_source,
        symbol=symbol,
        quote=quote,
        asset=asset,
        **kwargs,
    )


def token(
    id: str, amount: Number, reason: str, *, asset, decimals: int, symbol: str, scope, **kwargs
) -> Value:
    """A token amount in human units (33.33 USDC), with its asset identity and fund scope.
    The display rounds; the value stays exact."""
    return value(
        id,
        amount,
        "base_units",
        reason=reason,
        places=decimals,
        asset=asset,
        scope=scope,
        symbol=symbol,
        **kwargs,
    )


def count(id: str, amount: int, reason: str, **kwargs) -> Value:
    return value(id, amount, "count", reason=reason, places=0, **kwargs)


def metric(id: str, label: str, of: Value) -> Metric:
    return Metric(id=id, label=label, value=of)


def series(
    id: str,
    label: str,
    points: Iterable[tuple[int, Value]],
    *,
    x_axis: XAxis = "time",
    panel: Panel = "main",
    style: LineStyle = "solid",
) -> Series:
    """A named line from (x, value) pairs: x is a unix time, or bps from today on a
    price_move axis."""
    samples: tuple[Sample, ...] | tuple[Move, ...]
    if x_axis == "time":
        points = thin(list(points))
    if x_axis == "price_move":
        samples = tuple(Move(move_bps=at, value=cite(v)) for at, v in points)
    else:
        samples = tuple(Sample(time=at, value=cite(v)) for at, v in points)
    return Series(id=id, label=label, points=samples, panel=panel, style=style)


# A chart is a few hundred pixels wide: more points add bytes, not detail. Each point repeats
# its value's metadata, so a long series is the bulk of a dashboard's size.
MAX_SERIES_POINTS = 150


def _amount(value: Value | Ref) -> Fraction | None:
    """The reading at its declared precision, exactly: amounts are integers scaled by `decimals`
    (a Decimal operation would round past its context's 28 digits). A cited fact has no amount
    here."""
    if isinstance(value, Ref) or value.amount is None:
        return None
    return Fraction(int(value.amount), 10 ** (value.decimals or 0))


def _gap(value: Value | Ref) -> bool:
    return isinstance(value, Value) and value.amount is None


def _thinned(points: Sequence[tuple[int, Value | Ref]], size: int) -> set[int]:
    """Per bucket of `size` points, its lowest and highest reading and first gap (or, when it has
    neither, its first point); and the latest."""
    kept: set[int] = {len(points) - 1}
    for start in range(0, len(points), size):
        bucket = range(start, min(start + size, len(points)))
        read = [(amount, i) for i in bucket if (amount := _amount(points[i][1])) is not None]
        gap = next((i for i in bucket if _gap(points[i][1])), None)
        if gap is not None:
            kept.add(gap)
        if read:
            kept.update((min(read)[1], max(read)[1]))
        elif gap is None:
            kept.add(start)
    return kept


def thin[Reading: (Value, Value | Ref)](
    points: list[tuple[int, Reading]], limit: int = MAX_SERIES_POINTS
) -> list[tuple[int, Reading]]:
    """At most `limit` points of a time series, in order. Each bucket keeps its lowest and highest
    reading (spikes survive) and one unavailable reading (its gap); a bucket of cited facts keeps
    its first point. The latest point is always kept. Buckets widen until the series fits."""
    if limit < 4:
        # A bucket's low, high and gap plus the latest point need four places.
        raise ValueError("thin needs a limit of at least 4")
    if len(points) <= limit:
        return points
    size = -(-len(points) // ((limit - 1) // 2))
    kept = _thinned(points, size)
    while len(kept) > limit:
        size *= 2
        kept = _thinned(points, size)
    return [points[i] for i in sorted(kept)]


def cite(of: Value) -> Value | Ref:
    """A recorded fact is cited by id: the snapshot already carries it, so a chart of recorded
    prices does not repeat each one."""
    return Ref(ref=of.id) if of.evidence == "recorded" else of


def reference(label: str, of: Value, tone: Tone = "neutral") -> Reference:
    return Reference(label=label, value=of, tone=tone)


def band(
    label: str, *, low: Value | None = None, high: Value | None = None, tone: Tone = "neutral"
) -> Band:
    return Band(label=label, low=low, high=high, tone=tone)


def marker(at: int, label: str, tone: Tone = "neutral", series: str | None = None) -> Marker:
    return Marker(at=at, label=label, tone=tone, series=series)


def annotation(
    at: int, label: str, tone: Tone = "neutral", series: str | None = None
) -> Annotation:
    return Annotation(at=at, label=label, tone=tone, series=series)


def line_chart(
    id: str,
    title: str,
    lines: Iterable[Series],
    *,
    caption: str,
    x_axis: XAxis = "time",
    references: Iterable[Reference] = (),
    bands: Iterable[Band] = (),
    markers: Iterable[Marker] = (),
    annotations: Iterable[Annotation] = (),
    lower_marks: Iterable[str] = (),
    x_label: str | None = None,
    y_label: str | None = None,
    lower_label: str | None = None,
) -> LineChart:
    """A chart of one or more series. Put an indicator with its own scale (RSI under a price)
    on panel="lower", and name its reference lines in `lower_marks`."""
    return LineChart(
        id=id,
        title=title,
        x_axis=x_axis,
        series=tuple(lines),
        caption=caption,
        references=tuple(references),
        bands=tuple(bands),
        markers=tuple(markers),
        annotations=tuple(annotations),
        lower_marks=tuple(lower_marks),
        x_label=x_label,
        y_label=y_label,
        lower_label=lower_label,
    )


def bar_chart(id: str, title: str, bars: Iterable[tuple[str, Value]], *, caption: str):
    return BarChart(
        id=id,
        title=title,
        points=tuple(Point(label=label, value=v) for label, v in bars),
        caption=caption,
    )


def callout(id: str, title: str, text: str, tone: CalloutTone = "info") -> Callout:
    return Callout(id=id, title=title, text=text, tone=tone)


StateSpec = tuple | Mapping
EdgeSpec = tuple | Mapping


def steps(
    id: str,
    title: str,
    states: Iterable[StateSpec],
    transitions: Iterable[EdgeSpec] = (),
) -> Steps:
    """The lifecycle in reading order.

    states: ("id", "Label") or ("id", "Label", "detail") or dicts with tone/terminal.
    transitions: ("from", "to", "label") or ("from", "to", "label", "tone") or dicts.
    A transition to the next listed state is the main flow; others are branches.
    """

    def state(spec):
        if isinstance(spec, Mapping):
            return State(**spec)
        return State(**dict(zip(("id", "label", "detail", "tone"), spec, strict=False)))

    def edge(spec):
        if isinstance(spec, Mapping):
            return Transition(**spec)
        return Transition(**dict(zip(("source", "target", "label", "tone"), spec, strict=False)))

    return Steps(
        id=id,
        title=title,
        states=tuple(state(s) for s in states),
        transitions=tuple(edge(t) for t in transitions),
    )


def group(id: str, title: str, *blocks, collapsed: bool = False) -> Group:
    return Group(id=id, title=title, blocks=tuple(blocks), collapsed=collapsed)


def dashboard(title: str, *blocks, description: str = "") -> Dashboard:
    flat: list = []
    for item in blocks:
        flat.extend(item if isinstance(item, (list, tuple)) else (item,))
    return Dashboard(title=title, description=description, blocks=tuple(flat))


def history(context, field: str) -> list[tuple[int, Decimal]]:
    """`(unix time, reading)` for one field of the readings kept at each decision, oldest first,
    for `series(...)`. The fields come from the connector family (`price`, `value`, `hold`,
    `range_lower`, `health`, `debt`, `size_usd`, ...; a composed strategy prefixes each part's
    by its allocation, `liquidity:price`); docs/dashboards.md lists them."""
    rows = context.history
    return [(row.time, Decimal(row.fields[field])) for row in rows if field in row.fields]


def prices(context, asset=None) -> list[tuple[int, Decimal]]:
    """`(unix time, close)` of a price series the strategy declared (the first, or `asset`'s), as
    its latest decision read it: the whole window from the first decision, and the closes its
    indicators were computed from. Its label (`"USDC per WETH"`) is on `context.prices`."""
    for row in context.prices:
        if asset is None or str(row.asset) == str(asset):
            return [(point.time, Decimal(point.value)) for point in row.points]
    return []


def trades(context, allocation: str | None = None):
    """The committed transactions that moved the fund's tokens, oldest first, as
    `(unix time, action, {symbol or asset: signed amount})` for markers: `action` is the intent
    carried out (`swap_exact_input` buys, `exit_exchange` sells, `open_liquidity`, ...)."""
    return [
        (
            trade.time,
            trade.action,
            {
                (amount.symbol or str(amount.asset)): Decimal(amount.amount)
                for amount in trade.amounts
            },
        )
        for trade in context.trades
        if allocation is None or trade.allocation == allocation
    ]


def seconds(duration) -> int:
    """A config duration ("7d", "2h", "300s", 60) in seconds, read as config.yaml reads it."""
    from almanak.configuration.values import seconds as config_seconds

    return config_seconds(duration)


def number(of: Value) -> Decimal:
    """A value's amount as a Decimal in its display unit (e.g. a ratio fact 7800 / 10^4)."""
    if of.amount is None:
        raise ValueError(f"value {of.id} has no amount: {of.reason}")
    return Decimal(of.amount) / Decimal(10) ** (of.decimals or 0)
