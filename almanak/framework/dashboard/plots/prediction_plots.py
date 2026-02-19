"""Prediction market plots for strategy dashboards.

This module provides visualization components for prediction market strategies including:
- Binary outcome position visualization
- Probability over time charts
- Market outcome comparison

These plots are designed for prediction markets like Polymarket.

Example:
    from almanak.framework.dashboard.plots.prediction_plots import (
        plot_prediction_position,
        plot_probability_over_time,
        plot_market_outcomes,
    )

    # Position overview
    fig = plot_prediction_position(
        yes_shares=100,
        no_shares=0,
        yes_price=0.65,
        no_price=0.35,
        cost_basis=60,
    )
    st.plotly_chart(fig)
"""

from dataclasses import dataclass

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from almanak.framework.dashboard.plots.base import (
    PlotConfig,
    apply_theme,
    create_empty_figure,
    format_usd,
    get_default_config,
    hex_to_rgba,
)


@dataclass
class PredictionPosition:
    """Prediction market position data.

    Attributes:
        market_id: Market identifier
        market_question: The question being predicted
        yes_shares: Number of YES shares held
        no_shares: Number of NO shares held
        yes_price: Current YES price (0-1)
        no_price: Current NO price (0-1)
        cost_basis: Total cost basis in USD
        current_value: Current position value in USD
        unrealized_pnl: Unrealized profit/loss
    """

    market_id: str
    market_question: str
    yes_shares: float
    no_shares: float
    yes_price: float
    no_price: float
    cost_basis: float
    current_value: float | None = None
    unrealized_pnl: float | None = None


def plot_prediction_position(
    yes_shares: float,
    no_shares: float,
    yes_price: float,
    no_price: float,
    cost_basis: float | None = None,
    market_question: str = "",
    title: str = "Position Overview",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot prediction market position with outcome probabilities.

    Shows current position, prices, and potential payouts for both outcomes.

    Args:
        yes_shares: Number of YES shares held
        no_shares: Number of NO shares held
        yes_price: Current YES price (0-1)
        no_price: Current NO price (0-1)
        cost_basis: Total cost invested
        market_question: The prediction question
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with position visualization
    """
    config = config or get_default_config()
    colors = config.colors

    # Calculate values
    current_value = yes_shares * yes_price + no_shares * no_price
    potential_yes_payout = yes_shares * 1.0  # $1 per share if YES wins
    potential_no_payout = no_shares * 1.0  # $1 per share if NO wins

    if cost_basis is not None:
        unrealized_pnl = current_value - cost_basis
        pnl_pct = (unrealized_pnl / cost_basis * 100) if cost_basis > 0 else 0
    else:
        unrealized_pnl = None
        pnl_pct = None

    # Create subplots
    fig = make_subplots(
        rows=1,
        cols=2,
        specs=[[{"type": "indicator"}, {"type": "bar"}]],
        column_widths=[0.4, 0.6],
    )

    # Probability gauge (weighted by position)
    if yes_shares > 0 or no_shares > 0:
        # Show probability of the outcome you're betting on
        if yes_shares >= no_shares:
            prob_value = yes_price
            prob_label = "YES Probability"
        else:
            prob_value = no_price
            prob_label = "NO Probability"
    else:
        prob_value = yes_price
        prob_label = "YES Probability"

    fig.add_trace(
        go.Indicator(
            mode="gauge+number",
            value=prob_value * 100,
            number={"suffix": "%"},
            title={"text": prob_label},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": colors.primary},
                "steps": [
                    {"range": [0, 33], "color": hex_to_rgba(colors.danger, 0.2)},  # Low probability zone
                    {"range": [33, 66], "color": hex_to_rgba(colors.warning, 0.2)},  # Medium probability zone
                    {"range": [66, 100], "color": hex_to_rgba(colors.success, 0.2)},  # High probability zone
                ],
            },
        ),
        row=1,
        col=1,
    )

    # Payout comparison
    fig.add_trace(
        go.Bar(
            x=["If YES Wins", "If NO Wins", "Current Value"],
            y=[potential_yes_payout, potential_no_payout, current_value],
            marker_color=[colors.success, colors.danger, colors.primary],
            text=[format_usd(potential_yes_payout), format_usd(potential_no_payout), format_usd(current_value)],
            textposition="outside",
            showlegend=False,
        ),
        row=1,
        col=2,
    )

    # Add cost basis line if provided
    if cost_basis is not None:
        fig.add_hline(
            y=cost_basis,
            line_dash="dash",
            line_color=colors.neutral,
            annotation_text=f"Cost Basis: {format_usd(cost_basis)}",
            row=1,
            col=2,
        )

    # Build title
    full_title = title
    if market_question:
        # Truncate long questions
        q = market_question[:50] + "..." if len(market_question) > 50 else market_question
        full_title = f"{title}<br><span style='font-size:12px'>{q}</span>"

    # Add PnL annotation
    if unrealized_pnl is not None:
        pnl_color = colors.profit if unrealized_pnl >= 0 else colors.loss
        fig.add_annotation(
            x=0.5,
            y=-0.15,
            xref="paper",
            yref="paper",
            text=f"<b>Unrealized PnL:</b> {format_usd(unrealized_pnl)} ({pnl_pct:+.1f}%)",
            showarrow=False,
            font={"size": 14, "color": pnl_color},
        )

    fig.update_layout(
        title={"text": full_title, "font": {"size": config.title_font_size}},
        height=350,
        margin={"b": 60},
    )

    return apply_theme(fig, config)


def plot_probability_over_time(
    probability_data: pd.DataFrame | list[dict],
    time_column: str = "timestamp",
    yes_column: str = "yes_price",
    no_column: str = "no_price",
    show_both: bool = True,
    market_question: str = "",
    title: str = "Probability Over Time",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot outcome probabilities over time.

    Args:
        probability_data: DataFrame or list with probability history
            Expected columns: timestamp, yes_price, no_price
        time_column: Name of time column
        yes_column: Name of YES probability column
        no_column: Name of NO probability column
        show_both: Whether to show both YES and NO lines
        market_question: The prediction question
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with probability history
    """
    config = config or get_default_config()
    colors = config.colors

    # Convert to DataFrame
    if isinstance(probability_data, list):
        if not probability_data:
            return create_empty_figure("No probability data", config)
        df = pd.DataFrame(probability_data)
    else:
        df = probability_data.copy()

    if df.empty:
        return create_empty_figure("No probability data", config)

    # Normalize column names
    time_col = time_column if time_column in df.columns else "timestamp"
    yes_col = yes_column if yes_column in df.columns else "yes_price"
    no_col = no_column if no_column in df.columns else "no_price"

    if time_col not in df.columns:
        return create_empty_figure("Invalid probability data format", config)

    # Ensure datetime
    if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
        df[time_col] = pd.to_datetime(df[time_col])

    df = df.sort_values(time_col)

    fig = go.Figure()

    # YES probability line
    if yes_col in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df[time_col],
                y=df[yes_col] * 100,
                mode="lines",
                name="YES",
                line={"color": colors.success, "width": config.line_width},
                fill="tozeroy",
                fillcolor=f"rgba({int(colors.success[1:3], 16)}, {int(colors.success[3:5], 16)}, {int(colors.success[5:7], 16)}, 0.1)",
            )
        )

    # NO probability line
    if show_both and no_col in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df[time_col],
                y=df[no_col] * 100,
                mode="lines",
                name="NO",
                line={"color": colors.danger, "width": config.line_width},
            )
        )

    # 50% reference line
    fig.add_hline(
        y=50,
        line_dash="dash",
        line_color=colors.neutral,
        annotation_text="50%",
    )

    # Build title
    full_title = title
    if market_question:
        q = market_question[:60] + "..." if len(market_question) > 60 else market_question
        full_title = f"{title}<br><span style='font-size:12px'>{q}</span>"

    fig.update_layout(
        title={"text": full_title, "font": {"size": config.title_font_size}},
        xaxis_title="Time",
        yaxis_title="Probability (%)",
        yaxis={"range": [0, 100]},
        hovermode="x unified",
    )

    return apply_theme(fig, config)


def plot_market_outcomes(
    markets: list[dict],
    title: str = "Market Outcomes",
    sort_by: str = "probability",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot comparison of multiple prediction markets.

    Args:
        markets: List of market dicts with keys:
            - question: Market question
            - yes_price: Current YES probability
            - position: "YES", "NO", or None
            - pnl: Optional PnL value
        title: Chart title
        sort_by: Sort markets by "probability" or "pnl"
        config: Plot configuration

    Returns:
        Plotly figure with market comparison
    """
    config = config or get_default_config()
    colors = config.colors

    if not markets:
        return create_empty_figure("No market data", config)

    # Sort markets
    if sort_by == "probability":
        markets = sorted(markets, key=lambda x: x.get("yes_price", 0), reverse=True)
    elif sort_by == "pnl":
        markets = sorted(markets, key=lambda x: x.get("pnl", 0) or 0, reverse=True)

    # Prepare data
    questions = []
    yes_probs = []
    bar_colors = []
    border_colors = []

    for m in markets:
        q = m.get("question", "Unknown")
        if len(q) > 40:
            q = q[:40] + "..."
        questions.append(q)

        yes_prob = m.get("yes_price", 0.5) * 100
        yes_probs.append(yes_prob)

        position = m.get("position")
        if position == "YES":
            bar_colors.append(colors.success)
            border_colors.append(colors.success)
        elif position == "NO":
            bar_colors.append(colors.danger)
            border_colors.append(colors.danger)
        else:
            bar_colors.append(colors.neutral)
            border_colors.append(colors.neutral)

    fig = go.Figure()

    # Horizontal bars for YES probability
    fig.add_trace(
        go.Bar(
            y=questions,
            x=yes_probs,
            orientation="h",
            marker={
                "color": bar_colors,
                "line": {"color": border_colors, "width": 2},
            },
            text=[f"{p:.0f}%" for p in yes_probs],
            textposition="auto",
            hovertemplate="%{y}<br>YES: %{x:.1f}%<extra></extra>",
        )
    )

    # 50% line
    fig.add_vline(
        x=50,
        line_dash="dash",
        line_color=colors.neutral,
    )

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        xaxis_title="YES Probability (%)",
        xaxis={"range": [0, 100]},
        yaxis={"autorange": "reversed"},  # Top-to-bottom
        height=max(300, len(markets) * 40),  # Scale height with number of markets
        margin={"l": 200},  # Room for long questions
    )

    return apply_theme(fig, config)


def plot_prediction_pnl_breakdown(
    trades: list[dict],
    title: str = "PnL by Trade",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot PnL breakdown from prediction market trades.

    Args:
        trades: List of trade dicts with keys:
            - market: Market name/question
            - outcome: "YES" or "NO"
            - shares: Number of shares
            - entry_price: Price paid per share
            - current_price: Current price (or exit price)
            - pnl: Profit/loss
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with PnL breakdown
    """
    config = config or get_default_config()
    colors = config.colors

    if not trades:
        return create_empty_figure("No trade data", config)

    # Prepare data
    labels = []
    pnls = []
    bar_colors = []

    for t in trades:
        market = t.get("market", "Unknown")[:30]
        outcome = t.get("outcome", "")
        pnl = t.get("pnl", 0)

        labels.append(f"{market} ({outcome})")
        pnls.append(pnl)
        bar_colors.append(colors.profit if pnl >= 0 else colors.loss)

    fig = go.Figure(
        go.Bar(
            x=labels,
            y=pnls,
            marker_color=bar_colors,
            text=[format_usd(p) for p in pnls],
            textposition="outside",
        )
    )

    # Zero line
    fig.add_hline(y=0, line_dash="dash", line_color=colors.neutral)

    # Total annotation
    total_pnl = sum(pnls)
    total_color = colors.profit if total_pnl >= 0 else colors.loss
    fig.add_annotation(
        x=1,
        y=1.05,
        xref="paper",
        yref="paper",
        text=f"<b>Total PnL: {format_usd(total_pnl)}</b>",
        showarrow=False,
        font={"size": 14, "color": total_color},
        xanchor="right",
    )

    fig.update_layout(
        title={"text": title, "font": {"size": config.title_font_size}},
        xaxis_title="Trade",
        yaxis_title="PnL (USD)",
        xaxis={"tickangle": -45},
    )

    return apply_theme(fig, config)


def plot_arbitrage_opportunity(
    yes_price: float,
    no_price: float,
    market_question: str = "",
    title: str = "Arbitrage Analysis",
    config: PlotConfig | None = None,
) -> go.Figure:
    """Plot arbitrage opportunity analysis for a prediction market.

    Shows whether YES + NO prices create an arbitrage opportunity.

    Args:
        yes_price: Current YES price (0-1)
        no_price: Current NO price (0-1)
        market_question: The prediction question
        title: Chart title
        config: Plot configuration

    Returns:
        Plotly figure with arbitrage analysis
    """
    config = config or get_default_config()
    colors = config.colors

    total_cost = yes_price + no_price
    guaranteed_payout = 1.0  # Always receive $1

    if total_cost == 0:
        # No prices available - no arbitrage analysis possible
        profit: float = 0
        profit_pct: float = 0
        opportunity_type = "NO DATA"
        opportunity_color = colors.neutral
    elif total_cost < 1:
        profit = guaranteed_payout - total_cost
        profit_pct = (profit / total_cost) * 100
        opportunity_type = "BUY BOTH"
        opportunity_color = colors.success
    elif total_cost > 1:
        # Can't directly profit by selling (need existing positions)
        profit = total_cost - guaranteed_payout
        profit_pct = (profit / guaranteed_payout) * 100
        opportunity_type = "OVERPRICED"
        opportunity_color = colors.warning
    else:
        profit = 0
        profit_pct = 0
        opportunity_type = "FAIR"
        opportunity_color = colors.neutral

    fig = go.Figure()

    # Stacked bar showing cost breakdown
    fig.add_trace(
        go.Bar(
            x=["Cost"],
            y=[yes_price * 100],
            name=f"YES ({yes_price * 100:.1f}%)",
            marker_color=colors.success,
            text=[f"{yes_price * 100:.1f}%"],
            textposition="inside",
        )
    )
    fig.add_trace(
        go.Bar(
            x=["Cost"],
            y=[no_price * 100],
            name=f"NO ({no_price * 100:.1f}%)",
            marker_color=colors.danger,
            text=[f"{no_price * 100:.1f}%"],
            textposition="inside",
        )
    )

    # Payout bar
    fig.add_trace(
        go.Bar(
            x=["Payout"],
            y=[100],
            name="Guaranteed Payout",
            marker_color=colors.primary,
            text=["$1.00"],
            textposition="inside",
        )
    )

    # 100% line
    fig.add_hline(
        y=100,
        line_dash="dash",
        line_color=colors.neutral,
        annotation_text="100%",
    )

    # Opportunity annotation
    fig.add_annotation(
        x=0.5,
        y=1.15,
        xref="paper",
        yref="paper",
        text=f"<b>{opportunity_type}</b><br>Total Cost: {total_cost * 100:.1f}% | Profit: {profit_pct:.2f}%",
        showarrow=False,
        font={"size": 14, "color": opportunity_color},
    )

    # Build title
    full_title = title
    if market_question:
        q = market_question[:50] + "..." if len(market_question) > 50 else market_question
        full_title = f"{title}<br><span style='font-size:12px'>{q}</span>"

    fig.update_layout(
        title={"text": full_title, "font": {"size": config.title_font_size}},
        yaxis_title="Percentage",
        barmode="stack",
        height=400,
    )

    return apply_theme(fig, config)
