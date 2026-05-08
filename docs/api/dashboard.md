# Dashboards

Strategy dashboards are Streamlit pages loaded by the hosted platform's
dashboard image and by `almanak dashboard` locally. Both call your
`render_custom_dashboard()` with the same arguments.

## Anatomy of a dashboard

If a built-in template renderer fits your strategy, call it. The renderer
already includes the audit sections (PnL, cost stack, trade tape) — don't
call them again.

```python
import streamlit as st
from almanak.framework.dashboard.templates import get_bollinger_config, render_ta_dashboard

def render_custom_dashboard(strategy_id, strategy_config, api_client, session_state):
    st.title("BB Swap cbETH/WETH (Base)")
    config = get_bollinger_config(period=20, std_dev=1.0)
    render_ta_dashboard(strategy_id, strategy_config, session_state, config)
```

If no template fits, hand-roll Streamlit and wire the audit primitives
yourself (this is what `almanak strat new` scaffolds):

```python
import streamlit as st
from almanak.framework.dashboard import (
    render_pnl_section, render_cost_stack_section, render_trade_tape_section,
)

def render_custom_dashboard(strategy_id, strategy_config, api_client, session_state):
    st.title("My Custom Strategy")
    render_pnl_section(strategy_id)
    # your indicator / position / performance UI
    render_cost_stack_section(strategy_id)
    render_trade_tape_section(strategy_id)
```

## Audit primitives

::: almanak.framework.dashboard
    options:
      members:
        - render_pnl_section
        - render_cost_stack_section
        - render_trade_tape_section

## Template renderers

Pre-built sections for common strategy types. Each renderer is paired with
factory configs that adapt the rendering to a specific protocol or
indicator. Use these to fill the middle of the dashboard instead of
hand-rolling indicator/position/performance UI.

### Technical analysis (RSI, MACD, Bollinger, …)

::: almanak.framework.dashboard.templates.ta_dashboard

### Liquidity provision

::: almanak.framework.dashboard.templates.lp_dashboard

### Lending

::: almanak.framework.dashboard.templates.lending_dashboard

### Perpetuals

::: almanak.framework.dashboard.templates.perp_dashboard

### Prediction markets

::: almanak.framework.dashboard.templates.prediction_dashboard
