"""Readable sections over retained connector reports, without changing their facts."""

import html
import json
from datetime import UTC, datetime

TABLES = ("Activity", "Liquidity positions")


def scope(target):
    fund = target.get("fund", {})
    return {
        "allocation": target.get("allocation"),
        "account": target.get("account"),
        "chain": fund.get("chain", {}).get("chain_id"),
        "fund": fund.get("address"),
    }


def position(row):
    source = row.get("payload", row)
    target = source.get("target", {})
    ignored = {
        "target",
        "transactions",
        "last_observation",
        "ownership",
        "owned",
        "valuation_source",
        "block",
        "wallet",
    }
    values = {key: value for key, value in source.items() if key not in ignored}
    owned = source.get("owned", source.get("ownership", {}))
    values.update({key: value for key, value in owned.items() if key != "target"})
    return {**scope(target), **values}


def activity(report):
    rows = []
    for entry in report.get("activity", []):
        rows.append({**entry, "scope": scope(entry.get("scope") or {})})
    for item in [*report.get("positions", []), *report.get("debts", [])]:
        for transaction in item.get("transactions", []):
            row = {"allocation": item["target"]["allocation"], **transaction}
            pool = item["target"].get("pool", {})
            for index in (0, 1):
                key = f"delta{index}"
                if key in row and f"currency{index}" in pool:
                    row[key] = {"asset": pool[f"currency{index}"], "amount": row[key]}
            rows.append(row)
    return rows


def payers(report):
    rows = []
    for payer in report.get("gas_payers", []):
        submitter = payer.get("submitter", {})
        rows.append(
            {
                "submitter": submitter.get("address"),
                "chain": submitter.get("chain", {}).get("chain_id"),
                **{k: v for k, v in payer.items() if k not in ("submitter", "nonces")},
                "recorded_transactions": len(payer.get("nonces", [])),
            }
        )
    return rows


def percent(value):
    return None if value is None else f"{value:+.2f}%"


def duration(seconds):
    if seconds is None:
        return None
    days, rest = divmod(int(seconds), 86400)
    hours, minutes = rest // 3600, rest % 3600 // 60
    parts = [f"{days}d" if days else "", f"{hours}h" if hours else "", f"{minutes}m"]
    return " ".join(part for part in parts[: 2 if days else 3] if part) or "0m"


def moment(time):
    if time is None:
        return "open"
    return datetime.fromtimestamp(time, UTC).strftime("%Y-%m-%d %H:%M UTC")


def valued(report, key):
    """One quote amount of a valued run, named with its quote."""
    value = (report.get("performance") or {}).get(key)
    return None if value is None else f"{value} {report.get('valuation_quote') or ''}".strip()


def performance(report):
    """A valued run's result in its quote, and what fees, swaps and price contributed."""
    facts = report.get("performance")
    if not isinstance(facts, dict):
        return {}
    metrics = report.get("metrics") or {}
    drawdown = metrics.get("max_drawdown_pct")
    window = duration(facts.get("window_seconds"))
    rows = {
        "valued in": report.get("valuation_quote"),
        "start value": valued(report, "initial_value"),
        "end value": valued(report, "final_value"),
        "net result": valued(report, "net_value"),
        "net return": percent(metrics.get("total_return_pct")),
        "fees earned": valued(report, "fees_value"),
        "swap costs": valued(report, "swap_cost_value"),
        "price effect": valued(report, "price_effect_value"),
        "largest drawdown": None if drawdown is None else f"{abs(drawdown):.2f}%",
        "time in market": f"{duration(facts.get('time_in_market_seconds'))} of {window}",
        "time in range": duration(facts.get("time_in_range_seconds")),
        "swaps": facts.get("swaps"),
    }
    return {key: value for key, value in rows.items() if value is not None}


def liquidity_positions(report):
    quote = report.get("valuation_quote") or ""
    rows = []
    for held in (report.get("performance") or {}).get("positions", []):
        rows.append(
            {
                "allocation": held.get("allocation"),
                "opened": moment(held.get("opened_at")),
                "closed": moment(held.get("closed_at")),
                "price range": " to ".join(held.get("price_range", [])) + f" {quote}",
                "deposited": held.get("deposited"),
                "withdrawn": held.get("withdrawn"),
                "fees": held.get("fees"),
                "fees value": f"{held.get('fees_value')} {quote}",
                "held": duration(held.get("held_seconds")),
                "in range": duration(held.get("in_range_seconds")),
                "price at close": (held.get("price_at_close") or "open").replace("_", " "),
            }
        )
    return rows


def overview(report):
    pending = report.get("pending_hash") or report.get("reserved_attempt") or report.get("pending")
    accounting = report.get("transaction_accounting_complete", report.get("accounting_complete"))
    headline = {
        "Net return": percent((report.get("metrics") or {}).get("total_return_pct")),
        "Net result": valued(report, "net_value"),
    }
    return {
        **{key: value for key, value in headline.items() if value is not None},
        "Lifecycle": report.get("mode", "not_started"),
        "Obligations": "clear"
        if report.get("mode") == "closed" or report.get("closed")
        else "remaining or unverified",
        "Pending execution": "retained work" if pending else "none recorded",
        "Accounting": "complete"
        if accounting is True
        else "incomplete"
        if accounting is False
        else "not measured",
    }


def sections(report):
    portfolio = report.get("portfolio") or {}
    positions = report.get("positions", portfolio.get("positions", []))
    selected = {
        "Performance": performance(report),
        "Liquidity positions": liquidity_positions(report),
        "Positions": [position(row) for row in positions],
        "Debt": [position(row) for row in report.get("debts", [])],
        "Balances": portfolio.get("balances", report.get("balances", report.get("funds", []))),
        "Gas payer costs": payers(report),
        "Chain availability": report.get("chains", report.get("blocked_chains", [])),
        "Accounting limitations": report.get("accounting_issues", []),
        "Activity": activity(report),
        "Simulation assumptions": report.get("assumptions", {}),
        "Next commands": report.get("next_commands", []),
    }
    return {key: value for key, value in selected.items() if value}


def table(rows, metadata):
    if not rows:
        return ""
    keys = list(dict.fromkeys(key for row in rows for key in row))
    from almanak.reports import value_html

    heading = "".join("<th>" + html.escape(key.replace("_", " ")) + "</th>" for key in keys)
    body = "".join(
        "<tr>"
        + "".join("<td>" + value_html(row.get(key), metadata) + "</td>" for key in keys)
        + "</tr>"
        for row in rows
    )
    return (
        '<div class="table-scroll"><table><thead><tr>'
        + heading
        + "</tr></thead><tbody>"
        + body
        + "</tbody></table></div>"
    )


def custom_html(custom):
    if custom is None or custom.document is None:
        return "", "'none'"
    from almanak.dashboard.preview import csp_hash, render_html

    document = render_html(custom)
    script = document.rsplit("<script>", 1)[1].split("</script>", 1)[0]
    frame = (
        '<section><h2>Strategy dashboard</h2><iframe sandbox="allow-scripts" srcdoc="'
        + html.escape(document, quote=True)
        + '"></iframe></section>'
    )
    return frame, csp_hash(script)


def render(report, *, custom=None, diagnostic=None):
    from almanak.reports import value_html

    metadata = report.get("asset_metadata", {})
    cards = "".join(
        '<div class="metric"><span>'
        + html.escape(key)
        + "</span><strong>"
        + html.escape(str(value))
        + "</strong></div>"
        for key, value in overview(report).items()
    )
    body = ""
    for key, value in sections(report).items():
        content = table(value, metadata) if key in TABLES else value_html(value, metadata)
        body += "<section><h2>" + html.escape(key) + "</h2>" + content + "</section>"
    custom_section, script_policy = custom_html(custom)
    notice = "<aside>" + html.escape(diagnostic) + "</aside>" if diagnostic else ""
    evidence = html.escape(json.dumps(report, indent=2))
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="Content-Security-Policy"
content="default-src 'none'; style-src 'unsafe-inline'; script-src {script_policy};
frame-src about:">
<title>Strategy report</title><style>
body{{font:15px/1.5 system-ui,sans-serif;
color:#183044;
background:#f3f6f8;
margin:0}}

main{{max-width:1120px;
margin:auto;
padding:40px 24px}}
h1{{font-size:32px;
margin:8px 0}}

h2{{font-size:19px;
margin:0 0 20px}}
p,aside{{color:#526879}}
.source-path{{overflow-wrap:anywhere}}

.overview{{display:grid;
grid-template-columns:repeat(4,1fr);
gap:12px;
margin:28px 0}}

.metric{{background:#163e45;
color:#fff;
padding:18px;
border-radius:8px}}

.metric span{{display:block;
font-size:12px;
color:#bfd6db}}
.metric strong{{font-size:20px}}

section,details{{background:#fff;
border:1px solid #dce4e8;
border-radius:8px;
padding:22px;
margin:18px 0}}

dl{{display:grid;
grid-template-columns:200px minmax(0,1fr);
gap:9px 16px}}

dl dl{{display:block}}

dt{{color:#526879;
text-transform:capitalize}}
dd{{margin:0;
overflow-wrap:anywhere}}

ol{{padding-left:22px}}
li{{padding:8px 0}}
li+li{{border-top:1px solid #dce4e8;
margin-top:18px}}

.missing{{color:#995513}}
table{{border-collapse:collapse;
width:100%;
font-size:13px}}

th,td{{text-align:left;
vertical-align:top;
padding:10px;
border-bottom:1px solid #dce4e8;
overflow-wrap:anywhere}}

td{{min-width:8ch}}
th{{text-transform:capitalize;
white-space:nowrap;
color:#526879}}
.table-scroll{{overflow:auto}}
iframe{{width:100%;
height:1100px;
border:0}}

summary{{cursor:pointer;
font-weight:600}}
pre{{white-space:pre-wrap;
overflow-wrap:anywhere;
font-size:12px}}

.asset{{display:block;
color:#526879;
font-size:11px}}
.quantity{{font-variant-numeric:tabular-nums}}

@media(max-width:700px){{main{{padding:20px 12px}}
.overview{{grid-template-columns:1fr 1fr}}
dl{{display:block}}
dt{{margin-top:10px}}
section{{padding:16px}}
}}

</style></head><body><main><h1>Strategy report</h1><p>{html.escape(report["evidence"])}</p>
<div class="overview">{cards}</div>{notice}{body}{custom_section}
<details><summary>Retained evidence and execution scope</summary><pre>{evidence}</pre></details>
<p class="source-path">{html.escape(report["source"])}</p></main></body></html>"""
