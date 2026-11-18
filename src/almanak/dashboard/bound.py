"""A drawn dashboard kept within its size budget, whatever the hook built.

`b.series` already thins a time series, but a hook can build `Series` or a table from
`context.history` itself. Those grow with every decision until the document no longer fits the
worker's output limit, and the dashboard then fails on every draw. Bounding the result instead
keeps it drawable.
"""

from almanak.dashboard.build import MAX_SERIES_POINTS, thin
from almanak.dashboard.models import Dashboard, Group, LineChart, Sample, Series, Table

# The newest rows a table keeps; the renderer notes that the history is truncated.
MAX_TABLE_ROWS = 200


def _thinned(points):
    if len(points) <= MAX_SERIES_POINTS or not all(isinstance(p, Sample) for p in points):
        return points
    return tuple(Sample(time=t, value=v) for t, v in thin([(p.time, p.value) for p in points]))


def _series(series: Series) -> Series:
    return series.model_copy(update={"points": _thinned(series.points)})


def _block(block):
    if isinstance(block, LineChart) and block.x_axis == "time":
        return block.model_copy(
            update={
                "points": _thinned(block.points),
                "series": tuple(_series(s) for s in block.series),
            }
        )
    if isinstance(block, Table) and len(block.data.rows) > MAX_TABLE_ROWS:
        data = block.data.model_copy(
            update={"rows": block.data.rows[-MAX_TABLE_ROWS:], "truncated": True}
        )
        return block.model_copy(update={"data": data})
    if isinstance(block, Group):
        return block.model_copy(update={"blocks": tuple(_block(b) for b in block.blocks)})
    return block


def bounded(dashboard: Dashboard) -> Dashboard:
    """`dashboard` with each time line thinned to MAX_SERIES_POINTS and each table cut to its
    newest MAX_TABLE_ROWS rows."""
    return dashboard.model_copy(update={"blocks": tuple(_block(b) for b in dashboard.blocks)})
