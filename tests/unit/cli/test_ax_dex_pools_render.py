"""`ax dex-pools` table rendering (VIB-6599).

The renderer carries the safety-critical distinction: THREE different empties
that mean different things, and must never print the same line. Venues the
caller's own floor excluded, an absence the provider can vouch for, and a view
too truncated to vouch for anything.
"""

from __future__ import annotations

import click
import pytest
from click.testing import CliRunner

from almanak.framework.agent_tools.schemas import ToolResponse, ToolResponseStatus
from almanak.framework.cli.ax import _format_usd_cell, _render_dex_pools_table


def _response(pools: list[dict], **data) -> ToolResponse:
    payload = {
        "pools": pools,
        "count": len(pools),
        "unfiltered_count": len(pools),
        "complete": True,
        "product_distinct_dex_id": True,
        "source": "coingecko_onchain",
    }
    payload.update(data)
    return ToolResponse(status=ToolResponseStatus.SUCCESS, data=payload)


def _render(response: ToolResponse) -> str:
    """Render inside a Click context so styling/echo behave as in the CLI."""
    runner = CliRunner()

    @click.command()
    def _cmd():
        _render_dex_pools_table(response, token="BTT", chain="ethereum")

    return runner.invoke(_cmd, color=False).output


_A_POOL = {
    "pool_address": "0x2d0ba902badaa82592f0e1c04c71d66cea21d921",
    "dex_id": "uniswap_v2",
    "name": "BTT / WETH",
    "reserve_usd": "211219.19",
    "volume_24h_usd": "84.35",
}


def test_renders_venue_rows_with_both_money_columns():
    out = _render(_response([_A_POOL]))

    assert "uniswap_v2" in out
    assert "BTT / WETH" in out
    assert "$211,219" in out
    assert "$84" in out  # the column that distinguishes a live venue from a dead one
    assert "0x2d0ba902badaa82592f0e1c04c71d66cea21d921" in out


def test_empty_and_complete_reports_absence():
    out = _render(_response([], unfiltered_count=0))
    assert "no DEX venues found" in out
    assert "COULD NOT VERIFY" not in out


def test_empty_because_of_the_floor_says_so_and_never_claims_absence():
    """Venues exist; the caller's --min-liquidity excluded them. Printing the
    absence line here is how an operator concludes a tradeable token is
    untradeable — and the machine-readable half of this same bug classified it
    as authoritative NOT_FOUND."""
    out = _render(_response([], unfiltered_count=6, min_liquidity_usd="1000000"))

    assert "6 venue(s) found" in out
    assert "--min-liquidity" in out
    assert "no DEX venues found" not in out


def test_empty_and_truncated_says_could_not_verify():
    out = _render(_response([], unfiltered_count=0, complete=False))

    assert "COULD NOT VERIFY" in out
    assert "could not verify" in out  # the hint, spelled for a human report
    assert "no DEX venues found" not in out


def test_the_three_empties_never_render_the_same_line():
    """Guard against a future edit collapsing them back together."""
    absent = _render(_response([], unfiltered_count=0))
    filtered = _render(_response([], unfiltered_count=6, min_liquidity_usd="1000000"))
    unverified = _render(_response([], unfiltered_count=0, complete=False))

    assert len({absent, filtered, unverified}) == 3


def test_truncated_window_with_venues_warns_about_depth_not_existence():
    out = _render(_response([_A_POOL], complete=False))

    assert "truncated" in out
    assert "deepest venue SHOWN" in out
    # Existence IS answered — the rows are right there, so no verify warning.
    assert "COULD NOT VERIFY" not in out


def test_non_product_distinct_provider_warns_before_pinning_a_protocol():
    out = _render(_response([_A_POOL], product_distinct_dex_id=False))
    assert "not product-distinct" in out
    assert "aerodrome" in out


@pytest.mark.parametrize(
    ("value", "expected"),
    [("211219.19", "$211,219"), ("0", "$0"), ("", "?"), ("not-a-number", "?")],
)
def test_unmeasured_money_renders_as_question_mark_never_zero(value: str, expected: str):
    """Empty != Zero at the last mile: `""` is unmeasured and must not print as
    `$0`, which would read as a measured-empty venue."""
    assert _format_usd_cell(value) == expected
