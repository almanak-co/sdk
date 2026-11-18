"""A composed decision merges each part's readings under its allocation."""

from types import SimpleNamespace

from pydantic import BaseModel, ConfigDict

from almanak.connectors.composed import lifecycle
from almanak.connectors.composed.connector import ComposedProjection
from almanak.connectors.uniswap_v3.connector import UniswapProjection
from almanak.dashboard.models import HistoryRow
from tests.test_dashboard_history import MINTED

LP, COLLATERAL, LOAN = (SimpleNamespace(allocation=name) for name in ("lp", "collateral", "loan"))


class View(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    owned: tuple = ()
    baseline: tuple = ()
    closure: tuple = ()


def item(target, **fields):
    return SimpleNamespace(target=target, **fields)


def lending_history(state, observed):
    (owned,) = state.owned
    return HistoryRow(time=1_000, block=10, fields={"debt": str(len(owned.target.allocation))})


def compose(monkeypatch, histories):
    views = {
        "uniswap_v3": View(
            owned=(item(LP),),
            baseline=(
                item(LP, wallet0=SimpleNamespace(amount=0), wallet1=SimpleNamespace(amount=10**9)),
            ),
        ),
        "aave_v3": View(owned=(item(COLLATERAL), item(LOAN))),
        "gmx_v2": View(),
    }
    monkeypatch.setattr(lifecycle, "component_state", lambda state, identity: views[identity])
    monkeypatch.setattr(
        "almanak.connectors.composition.connector_from_wire",
        lambda wire: SimpleNamespace(projection=histories[wire["connector"]]),
    )
    return SimpleNamespace(config=SimpleNamespace(parameter_components=tuple(views)))


def observed():
    pool = SimpleNamespace(
        **{**MINTED.pool.__dict__, "block": SimpleNamespace(timestamp=2_000, number=20)}
    )
    return (
        item(COLLATERAL),
        item(LOAN),
        SimpleNamespace(**{**MINTED.__dict__, "pool": pool, "target": LP}),
    )


def test_parts_with_a_history_merge_under_their_allocation_at_the_latest_block(monkeypatch):
    lending = SimpleNamespace(history=staticmethod(lending_history))
    state = compose(
        monkeypatch,
        {"uniswap_v3": UniswapProjection, "aave_v3": lending, "gmx_v2": SimpleNamespace()},
    )
    row = ComposedProjection.history(state, observed())
    assert (row.time, row.block) == (2_000, 20)
    assert row.fields["collateral:debt"] == "10" and row.fields["loan:debt"] == "4"
    assert {"lp:price", "lp:range_lower", "lp:value", "lp:hold"} <= set(row.fields)
    assert row.fields["lp:hold"] == "1000"


def test_parts_without_a_history_hook_leave_only_the_readable_parts(monkeypatch):
    bare = SimpleNamespace()
    state = compose(monkeypatch, {"uniswap_v3": UniswapProjection, "aave_v3": bare, "gmx_v2": bare})
    row = ComposedProjection.history(state, observed())
    assert row.fields and all(key.startswith("lp:") for key in row.fields)
    state = compose(monkeypatch, {"uniswap_v3": bare, "aave_v3": bare, "gmx_v2": bare})
    assert ComposedProjection.history(state, observed()) is None
    assert ComposedProjection.history(state, None) is None
