/* Shared renderer. All author content is inserted as text or validated values. */
(() => {
  "use strict";
  const node = (tag, className = "", text = "") => {
    const element = document.createElement(tag);
    element.className = className;
    element.textContent = text;
    return element;
  };
  const views = new WeakMap();
  const date = time => time === null ? "No retained observation" : new Date(time * 1000).toISOString().replace("T", " ").replace(".000Z", " UTC");
  function exact(value) {
    if (value.amount === null) return "Unavailable";
    let amount = value.amount;
    const negative = amount.startsWith("-");
    if (negative) amount = amount.slice(1);
    if (["base_units", "ratio", "percent", "usd", "index", "price"].includes(value.unit) && value.decimals !== null) {
      amount = amount.padStart(value.decimals + 1, "0");
      if (value.decimals) amount = amount.slice(0, -value.decimals) + "." + amount.slice(-value.decimals);
    }
    const [integer, fraction] = amount.split(".");
    const tail = fraction?.replace(/0+$/, "");
    return (negative ? "-" : "") + (value.unit === "usd" ? "$" : "") + integer.replace(/\B(?=(\d{3})+(?!\d))/g, ",") + (tail ? "." + tail : "");
  }
  function unit(value) {
    if (value.unit === "base_units") return value.symbol || (value.decimals === null ? "asset base units" : "asset units");
    if (value.unit === "price") return value.symbol || "price";
    return {shares: "raw accounting units", count: "count", seconds: "seconds", ratio: "", percent: "%", usd: "USD", index: ""}[value.unit];
  }
  // Token amounts and prices read at display precision; the exact quantity stays in the
  // evidence drawer and the data view.
  function shown(value) {
    const full = exact(value);
    if (!["base_units", "price"].includes(value.unit) || value.decimals === null || value.amount === null) return full;
    const n = Number(full.replaceAll(",", ""));
    if (!Number.isFinite(n)) return full;
    const size = Math.abs(n);
    // Dust left by rounding reads as such; its exact amount stays in the evidence drawer.
    if (value.unit === "base_units" && size > 0 && size < 1e-6) return (n < 0 ? "-" : "") + "<0.000001";
    const digits = size >= 1 || size === 0 ? 2 : Math.min(value.decimals, Math.max(2, 3 - Math.floor(Math.log10(size))));
    return n.toLocaleString("en-US", {minimumFractionDigits: 0, maximumFractionDigits: digits});
  }
  function evidence(value) {
    const parts = [value.evidence.replaceAll("_", " "), value.reason];
    if (value.asset) parts.push("Asset: " + value.asset);
    if (value.price_source) parts.push("Price source: " + value.price_source);
    if (value.scope) {
      parts.push("Fund: " + value.scope.account + " / " + value.scope.fund.address);
      parts.push("Environment: " + value.scope.fund.chain.environment);
      if (value.scope.target) parts.push("Position target: " + JSON.stringify(value.scope.target, null, 2));
      if (value.scope.payer) parts.push("Gas payer: " + value.scope.payer.address);
    }
    if (shown(value) !== exact(value)) parts.push("Exact: " + exact(value) + (unit(value) ? " " + unit(value) : ""));
    if (value.block) parts.push("Observed: " + date(value.block.timestamp) + " / block " + value.block.number);
    return parts.filter(Boolean).join("\n");
  }
  function valueView(value, large = false) {
    const root = node("div", "value-wrap");
    // A percentage reads as one figure ("-10.1%"); other units label the number.
    const inline = value.unit === "percent" && value.amount !== null;
    const number = node("span", large ? "metric-number" : "quantity", shown(value) + (inline ? "%" : ""));
    if (shown(value).length > 18) number.classList.add("long-number");
    root.append(number);
    if (!inline) root.append(node("span", "unit", unit(value)));
    const details = node("details", "evidence");
    details.append(node("summary", "", "Evidence"), node("pre", "", evidence(value)));
    root.append(details);
    return root;
  }
  // A fork test runs on a local fork of the chain (an anvil environment), with or without a recorded fork origin.
  const forked = snapshot => !!snapshot.fork_origin || snapshot.chains.some(c => c.environment.startsWith("anvil:"));
  function metadata(snapshot) {
    const wrap = node("section", "snapshot");
    wrap.setAttribute("aria-label", "Retained deployment status");
    const header = node("div", "snapshot-line");
    // A fork test runs the strategy on a fork of the chain: the same dashboard, drawn from the test's state.
    const fork = snapshot.fork_origin;
    if (fork) header.append(node("span", "badge", "FORK TEST"), node("span", "", "Drawn from the fork test"), node("span", "muted", "Forked at block " + fork.number + " · " + date(fork.timestamp)));
    else if (forked(snapshot)) header.append(node("span", "badge", "FORK TEST"), node("span", "", "Drawn from the fork test"), node("span", "muted", "Snapshot captured " + date(snapshot.captured_at)));
    else header.append(node("span", "badge", "RECORDED EVIDENCE"), node("span", "", "Recorded mode: " + snapshot.mode), node("span", "muted", "Snapshot captured " + date(snapshot.captured_at)));
    wrap.append(header);
    const facts = node("div", "snapshot-facts");
    facts.append(node("span", "", "Last observation: " + date(snapshot.latest_observation)), node("span", "mono", snapshot.chains.map(c => c.chain_id).join(" / ")));
    wrap.append(facts);
    // Work in flight is routine between decisions: said plainly, with its identity in the details.
    const inFlight = snapshot.pending_hash || snapshot.reserved_attempt;
    if (inFlight) facts.append(node("span", "muted", "A transaction is in flight; figures may change once it settles."));
    else if (snapshot.undelivered_outcomes) facts.append(node("span", "muted", "The latest transaction is still being recorded."));
    if (!snapshot.initialized) wrap.append(node("p", "warning", "Initial balances have not been observed."));
    const details = node("details", "snapshot-details");
    details.append(node("summary", "", "Data coverage and snapshot identity"));
    snapshot.notices.forEach(notice => details.append(node("p", "", notice)));
    details.append(node("pre", "", "Deployment: " + snapshot.deployment_id + "\nSource: " + snapshot.strategy_hash + "\nRecord: " + snapshot.record_hash + "\nConfiguration: " + snapshot.config_hash + (inFlight ? "\nPending transaction: " + inFlight : "") + (snapshot.undelivered_outcomes ? "\nUndelivered outcomes: " + snapshot.undelivered_outcomes : "") + "\nFork origin: " + (snapshot.fork_origin ? snapshot.fork_origin.chain.chain_id + " / block " + snapshot.fork_origin.number : "See retained chain environments") + "\n" + snapshot.chains.map(c => c.environment).join("\n")));
    wrap.append(details);
    return wrap;
  }
  function metric(block) {
    const card = node("article", "metric card");
    card.append(node("h3", "eyebrow", block.label), valueView(block.value, true));
    if (block.value.amount === null) card.append(node("p", "muted", block.value.reason));
    return card;
  }
  function compare(a, b, facts) {
    if (typeof a === "string") return a.localeCompare(b);
    const first = facts[a.ref], second = facts[b.ref];
    if (first.amount === null) return second.amount === null ? 0 : 1;
    if (second.amount === null) return -1;
    const chainOf = v => v.scope?.fund.chain;
    const scopeA = [first.unit, first.asset, chainOf(first), first.unit === "shares" ? first.scope?.target : null].map(v => JSON.stringify(v)).join(":");
    const scopeB = [second.unit, second.asset, chainOf(second), second.unit === "shares" ? second.scope?.target : null].map(v => JSON.stringify(v)).join(":");
    if (scopeA !== scopeB) return scopeA.localeCompare(scopeB);
    // Integer wire quantities stay exact during sorting, including values beyond 2^53.
    const scale = Math.max(first.decimals || 0, second.decimals || 0);
    const x = BigInt(first.amount) * 10n ** BigInt(scale - (first.decimals || 0));
    const y = BigInt(second.amount) * 10n ** BigInt(scale - (second.decimals || 0));
    return x < y ? -1 : x > y ? 1 : 0;
  }
  function table(block, facts, view) {
    const section = node("section", "table-section card");
    const head = node("div", "section-heading");
    const filter = node("input", "filter");
    filter.value = view.filter || "";
    filter.type = "search"; filter.placeholder = "Filter rows";
    filter.setAttribute("aria-label", "Filter " + block.title);
    head.append(node("h2", "", block.title), filter); section.append(head);
    const scroll = node("div", "table-scroll"); scroll.tabIndex = 0;
    scroll.setAttribute("role", "region"); scroll.setAttribute("aria-label", block.title + " table");
    const grid = node("table"); const thead = node("thead"); const heading = node("tr");
    const body = node("tbody"); let selected = block.data.columns.some(c => c.key === view.selected) ? view.selected : null; let direction = view.direction || 1;
    const count = node("p", "caption"); count.setAttribute("aria-live", "polite");
    const headers = [];
    function draw() {
      view.filter = filter.value; view.selected = selected; view.direction = direction;
      body.replaceChildren();
      const query = filter.value.toLowerCase();
      const rows = block.data.rows.filter(row => Object.values(row).some(v => (typeof v === "string" ? v : exact(facts[v.ref])).toLowerCase().includes(query)));
      if (selected) rows.sort((a, b) => direction * compare(a[selected], b[selected], facts));
      for (const row of rows) {
        const tr = node("tr");
        for (const column of block.data.columns) {
          const td = node("td"); const value = row[column.key];
          if (typeof value === "string") td.textContent = value;
          else td.append(valueView(facts[value.ref]));
          tr.append(td);
        }
        body.append(tr);
      }
      count.textContent = rows.length + " of " + block.data.rows.length + " rows. " + block.data.coverage + (block.data.truncated ? " History is truncated." : "");
    }
    for (const column of block.data.columns) {
      const th = node("th"); th.scope = "col"; headers.push(th);
      if (selected === column.key) th.setAttribute("aria-sort", direction === 1 ? "ascending" : "descending");
      const button = node("button", "sort", column.label);
      button.addEventListener("click", () => {
        direction = selected === column.key ? -direction : 1; selected = column.key;
        headers.forEach(h => h.removeAttribute("aria-sort"));
        th.setAttribute("aria-sort", direction === 1 ? "ascending" : "descending"); draw();
      });
      th.append(button); heading.append(th);
    }
    thead.append(heading); grid.append(thead, body); scroll.append(grid);
    section.append(scroll, count); filter.addEventListener("input", draw); draw(); return section;
  }
  const numeric = value => value.amount === null ? null : Number(value.amount) / 10 ** (value.decimals || 0);
  function bars(block) {
    const card = node("article", "chart card"); card.append(node("h3", "", block.title));
    const amounts = block.points.map(p => numeric(p.value)).filter(v => v !== null);
    const maximum = Math.max(1e-100, ...amounts); const minimum = Math.min(0, ...amounts);
    for (const point of block.points) {
      const row = node("div", "bar-row");
      row.append(node("span", "bar-label", point.label), node("span", "quantity", shown(point.value) + (unit(point.value) && ["base_units", "price"].includes(point.value.unit) ? " " + unit(point.value) : "")));
      if (point.value.amount !== null) {
        const meter = node("meter"); meter.min = minimum; meter.max = maximum; meter.value = numeric(point.value);
        meter.setAttribute("aria-label", point.label + ": " + exact(point.value) + " " + unit(point.value)); row.append(meter);
      }
      card.append(row);
    }
    card.append(node("p", "caption", block.caption));
    if (block.points.length) card.append(node("p", "asset-label", block.points[0].value.asset || unit(block.points[0].value)));
    card.append(chartEvidence(block.points));
    return card;
  }
  function chartEvidence(points) {
    const details = node("details", "evidence");
    details.append(node("summary", "", "View exact values and evidence"));
    for (const point of points) {
      details.append(node("p", "", point.label || date(point.time)), valueView(point.value));
    }
    return details;
  }
  const tones = ["neutral", "warning", "danger", "success", "info"];
  const tone = name => "tone-" + (tones.includes(name) ? name : "neutral");
  const position = (block, point) => block.x_axis === "price_move" ? point.move_bps : point.time;
  const moveLabel = bps => (bps > 0 ? "+" : "") + (bps / 100).toFixed(0) + "%";
  // A run of minutes or hours (a fork test) labels its ticks by the clock, a longer one by the day.
  // Clock ticks for a short span, with the date as well when the span crosses midnight (UTC).
  const timeLabel = (span, first) => {
    const day = time => new Date(time * 1000).toISOString().slice(0, 10);
    const crosses = day(first) !== day(first + span);
    return time => {
      const iso = new Date(time * 1000).toISOString();
      const clock = span < 600 ? iso.slice(11, 19) : iso.slice(11, 16);
      if (span >= 2 * 86400) return iso.slice(5, 10);
      return crosses ? iso.slice(5, 10) + " " + clock : clock;
    };
  };
  const axisNumber = (value, digits) => value.toLocaleString("en-US", {minimumFractionDigits: digits, maximumFractionDigits: digits});
  // Enough decimals that the axis's two ends read apart: a 0.0201 price over a 0.0171 stop
  // reads "0.02" twice at two decimals, which draws a moving price as a flat one.
  const axisDigits = (lo, hi) => {
    const range = hi - lo;
    if (range >= 20) return 0;
    if (range >= 2) return 1;
    const scale = range > 0 ? range : Math.max(Math.abs(lo), Math.abs(hi));
    if (!(scale > 0) || !Number.isFinite(scale)) return 2;
    return Math.min(20, Math.max(2, Math.ceil(-Math.log10(scale)) + 1));
  };
  const svgNS = "http://www.w3.org/2000/svg";
  function lines(block) {
    if (block.series?.length) return block.series;
    return [{id: "_", label: "", points: block.points, panel: "main", style: "solid"}];
  }
  function line(block) {
    const card = node("article", "chart card"); card.append(node("h3", "", block.title));
    const series = lines(block);
    const valid = series.flatMap(s => s.points).filter(p => p.value.amount !== null);
    if (!valid.length) { card.append(node("p", "muted", "No measured history")); return card; }
    const svg = document.createElementNS(svgNS, "svg");
    const references = block.references || [], bands = block.bands || [], annotations = block.annotations || [], markers = block.markers || [];
    const lowerMarks = new Set(block.lower_marks || []);
    const hasLower = series.some(s => s.panel === "lower");
    // Every chart names its values and times: a lone line without them reads as a shape, not a measure.
    const decorated = true;
    // Axis titles: the author's words, else a price axis named by its symbol and a move axis
    // generically; a time axis's dates speak for themselves.
    const firstValue = panel => series.find(s => s.panel === panel)?.points.find(p => p.value.amount !== null)?.value;
    const sampleMain = firstValue("main");
    const yTitle = block.y_label || (["price", "base_units"].includes(sampleMain?.unit) && sampleMain?.symbol) || null;
    const lowerTitle = hasLower ? (block.lower_label || series.find(s => s.panel === "lower")?.label || null) : null;
    const xTitle = block.x_label || (block.x_axis === "price_move" ? "price move from today" : null);
    const W = 600, R = 10;
    let L = decorated ? 40 : 10;
    const axis = decorated ? 22 : 0, xTitleH = decorated && xTitle ? 16 : 0;
    const top = decorated && yTitle ? 26 : 12, gap = lowerTitle ? 28 : 16;
    const mainH = decorated ? 190 : 170, lowerH = hasLower ? 84 : 0;
    const H = top + mainH + (hasLower ? gap + lowerH : 0) + axis + xTitleH + (decorated ? 4 : 10);
    const frames = {main: {top, bottom: top + mainH}};
    if (hasLower) frames.lower = {top: top + mainH + gap, bottom: top + mainH + gap + lowerH};
    svg.setAttribute("viewBox", "0 0 " + W + " " + H);
    svg.setAttribute("role", "img"); svg.setAttribute("aria-label", block.title + ". Exact samples in data view.");
    const add = (tag, attributes, className = "", text = null) => {
      const element = document.createElementNS(svgNS, tag);
      for (const [key, value] of Object.entries(attributes)) element.setAttribute(key, String(value));
      if (className) element.setAttribute("class", className);
      if (text !== null) element.textContent = text;
      svg.append(element); return element;
    };
    const panelOf = label => lowerMarks.has(label) && hasLower ? "lower" : "main";
    const all = series.flatMap(s => s.points);
    const positions = all.map(p => position(block, p));
    const first = Math.min(...positions), span = Math.max(1, Math.max(...positions) - first);
    const x = at => L + (W - L - R) * (at - first) / span;
    const scales = {}, yLabels = [], yRules = [];
    for (const panel of Object.keys(frames)) {
      // The data sets the axis; reference lines and band edges are added so they stay
      // visible, but a band never fills the whole panel: one covering most of the axis
      // widens it until the band reads as a range inside a wider chart (40% of the axis).
      const ys = series.filter(s => s.panel === panel).flatMap(s => s.points).filter(p => p.value.amount !== null).map(p => numeric(p.value));
      for (const r of references) if (panelOf(r.label) === panel && r.value.amount !== null) ys.push(numeric(r.value));
      const panelBands = bands.filter(b => panelOf(b.label) === panel);
      for (const b of panelBands) for (const v of [b.low, b.high]) if (v && v.amount !== null) ys.push(numeric(v));
      let rawLo = Math.min(...ys), rawHi = Math.max(...ys);
      for (const b of panelBands) {
        if (!b.low || !b.high || b.low.amount === null || b.high.amount === null) continue;
        const bLo = Math.min(numeric(b.low), numeric(b.high)), bHi = Math.max(numeric(b.low), numeric(b.high));
        if (bHi - bLo > 0.6 * (rawHi - rawLo)) {
          const centre = (bLo + bHi) / 2, half = 1.25 * (bHi - bLo);
          rawLo = Math.min(rawLo, centre - half); rawHi = Math.max(rawHi, centre + half);
        }
      }
      let lo = rawLo, hi = rawHi;
      if (decorated) { const pad = (hi - lo || Math.abs(hi) || 1) * 0.06; lo -= pad; hi += pad; }
      const {top, bottom} = frames[panel];
      scales[panel] = v => hi === lo ? (top + bottom) / 2 : bottom - (bottom - top) * (v - lo) / (hi - lo);
      if (decorated) {
        const digits = axisDigits(rawLo, rawHi);
        const sample = series.find(s => s.panel === panel)?.points.find(p => p.value.amount !== null)?.value;
        const suffix = sample?.unit === "percent" ? "%" : "";
        for (const v of [rawLo, rawHi]) yLabels.push([scales[panel](v) + 3, axisNumber(v, digits) + suffix]);
        if (panel === "lower") yRules.push(top - gap + 4);
        const title = panel === "main" ? yTitle : lowerTitle;
        if (title) add("text", {x: 4, y: top - 8, "text-anchor": "start"}, "axis-title", title);
      }
    }
    // The value labels sit left of the plot, which starts wherever the widest one ends.
    L = Math.max(L, ...yLabels.map(([, text]) => 12 + 6 * text.length));
    for (const [y, text] of yLabels) add("text", {x: L - 6, y, "text-anchor": "end"}, "axis-label", text);
    for (const y of yRules) add("line", {x1: L, x2: W - R, y1: y, y2: y}, "zero");
    for (const band of bands) {
      const panel = panelOf(band.label), y = scales[panel], f = frames[panel];
      const top = band.high && band.high.amount !== null ? y(numeric(band.high)) : f.top;
      const bottom = band.low && band.low.amount !== null ? y(numeric(band.low)) : f.bottom;
      const yTop = Math.max(f.top, Math.min(top, bottom)), yBottom = Math.min(f.bottom, Math.max(top, bottom));
      add("rect", {x: L, y: yTop, width: W - L - R, height: Math.max(0, yBottom - yTop)}, "band " + tone(band.tone));
      add("text", {x: L + 4, y: yBottom - 4}, "mark-label " + tone(band.tone), band.label);
    }
    // A label sits above its line; one that would overlap a label already placed moves below
    // its line, then further down, so near-equal references stay readable.
    const placed = [];
    for (const reference of references) {
      if (reference.value.amount === null) continue;
      const at = scales[panelOf(reference.label)](numeric(reference.value));
      add("line", {x1: L, x2: W - R, y1: at, y2: at}, "reference " + tone(reference.tone));
      let labelY = at - 4;
      const clashes = candidate => placed.some(p => Math.abs(p - candidate) < 11);
      if (clashes(labelY)) labelY = at + 12;
      while (clashes(labelY)) labelY += 11;
      placed.push(labelY);
      add("text", {x: W - R - 2, y: labelY, "text-anchor": "end"}, "mark-label " + tone(reference.tone), reference.label);
    }
    const baseline = H - 8 - xTitleH;
    if (xTitleH) add("text", {x: L + (W - L - R) / 2, y: H - 6, "text-anchor": "middle"}, "axis-title", xTitle);
    if (block.x_axis === "price_move") {
      const step = span > 6000 ? 2000 : 1000;
      for (let at = Math.ceil(first / step) * step; at <= first + span; at += step) {
        // A tick on the right edge would be cut in half: it ends there instead.
        const anchor = at >= first + span ? "end" : "middle";
        add("text", {x: x(at), y: baseline, "text-anchor": anchor}, "axis-label", moveLabel(at));
      }
      if (first <= 0 && first + span >= 0) add("line", {x1: x(0), x2: x(0), y1: frames.main.top, y2: frames.main.bottom}, "zero");
    } else if (decorated) {
      const tickLabel = timeLabel(span, first);
      for (let i = 0; i <= 4; i++) {
        const at = first + span * i / 4;
        add("text", {x: x(at), y: baseline, "text-anchor": i === 0 ? "start" : i === 4 ? "end" : "middle"}, "axis-label", tickLabel(at));
      }
    }
    const plotted = {};
    series.forEach((s, index) => {
      const y = scales[s.panel]; let d = ""; let pen = false; const points = [];
      // A sample with no measured neighbour draws no line, so it gets a dot.
      let run = [];
      const dotLone = () => { if (run.length === 1) add("circle", {cx: run[0].x, cy: run[0].y, r: 3}, "series s" + (index % 6 + 1)); run = []; };
      for (const sample of s.points) {
        if (sample.value.amount === null) { pen = false; dotLone(); continue; }
        const point = {x: x(position(block, sample)), y: y(numeric(sample.value))};
        d += (pen ? "L" : "M") + point.x.toFixed(2) + " " + point.y.toFixed(2); pen = true; points.push(point); run.push(point);
      }
      dotLone();
      add("path", {d, fill: "none"}, "series s" + (index % 6 + 1) + (s.style === "dashed" ? " dashed" : ""));
      plotted[s.id] = points;
    });
    const mainId = series[0].id;
    const interpolate = (id, px) => {
      const points = plotted[id] || [];
      if (!points.length) return null;
      if (px <= points[0].x) return points[0].y;
      for (let i = 1; i < points.length; i++) {
        if (px <= points[i].x) { const a = points[i - 1], b = points[i]; return a.y + (b.y - a.y) * (px - a.x) / (b.x - a.x || 1); }
      }
      return points.at(-1).y;
    };
    const labelMarkers = markers.length <= 24;
    markers.forEach(marker => {
      const px = x(marker.at), py = interpolate(marker.series || mainId, px);
      if (py === null || px < L || px > W - R) return;
      add("circle", {cx: px, cy: py, r: 3.5}, "marker " + tone(marker.tone));
      if (labelMarkers) add("text", {x: px, y: py - 7, "text-anchor": "middle"}, "mark-label marker-label " + tone(marker.tone), marker.label);
    });
    annotations.forEach((annotation, index) => {
      const px = x(annotation.at), py = interpolate(annotation.series || mainId, px);
      if (py === null || px < L || px > W - R) return;
      add("circle", {cx: px, cy: py, r: 4}, "annotation " + tone(annotation.tone));
      const above = index % 2 === 0;
      add("text", {x: px + 6, y: above ? py - 8 : py + 14}, "mark-label " + tone(annotation.tone), annotation.label);
    });
    hover(card, svg, add, {block, series, markers, x, L, W, R, H, frames, scales, title: yTitle});
    card.append(svg);
    if (series.length > 1 || markers.length) {
      const legend = node("div", "legend");
      series.forEach((s, index) => {
        if (!s.label) return;
        const item = node("span", "legend-item"); item.append(node("i", "swatch s" + (index % 6 + 1) + (s.style === "dashed" ? " dashed" : "")), document.createTextNode(s.label));
        legend.append(item);
      });
      const counts = new Map();
      for (const m of markers) counts.set(m.label + "|" + m.tone, (counts.get(m.label + "|" + m.tone) || 0) + 1);
      for (const [key, count] of counts) {
        const [label, markerTone] = key.split("|");
        const item = node("span", "legend-item"); item.append(node("i", "dot " + tone(markerTone)), document.createTextNode(label + " × " + count));
        legend.append(item);
      }
      card.append(legend);
    }
    card.append(node("p", "caption", block.caption));
    const evidencePoints = series.flatMap(s => s.points.map(p => ({
      label: (s.label ? s.label + " · " : "") + (block.x_axis === "price_move" ? moveLabel(p.move_bps) + " price move" : date(p.time)),
      value: p.value,
    })));
    card.append(chartEvidence(evidencePoints));
    return card;
  }
  // Pointing at (or touching) a line chart snaps a guide to the nearest reading and lists each
  // series' value there with the markers at that point, so a trade reads with its time and price.
  function hover(card, svg, add, {block, series, markers, x, L, W, R, H, frames, scales, title}) {
    const stops = new Set(markers.map(m => m.at));
    for (const s of series) for (const p of s.points) if (p.value.amount !== null) stops.add(position(block, p));
    const at = [...stops].sort((a, b) => a - b);
    const top = frames.main.top, bottom = (frames.lower || frames.main).bottom;
    const guide = add("line", {x1: 0, x2: 0, y1: top, y2: bottom}, "crosshair");
    const dots = series.map((s, index) => add("circle", {cx: 0, cy: 0, r: 3.5}, "crosshair-dot s" + (index % 6 + 1)));
    const area = add("rect", {x: L, y: top, width: W - L - R, height: bottom - top}, "hover-area");
    area.setAttribute("aria-hidden", "true");
    const tip = node("div", "chart-tip"); tip.hidden = true; card.append(tip);
    const nearest = (points, target) => {
      let best = null;
      for (const p of points) {
        if (p.value.amount === null) continue;
        const distance = Math.abs(position(block, p) - target);
        if (!best || distance < best.distance) best = {p, distance};
      }
      return best?.p || null;
    };
    const reading = value => {
      const name = unit(value);
      return shown(value) + (name === "%" ? "%" : name ? " " + name : "");
    };
    const hide = () => { tip.hidden = true; svg.classList.remove("hovering"); };
    const show = event => {
      const box = svg.getBoundingClientRect();
      if (!box.width) return;
      const pointer = (event.clientX - box.left) * W / box.width;
      let target = at[0];
      for (const candidate of at) if (Math.abs(x(candidate) - pointer) < Math.abs(x(target) - pointer)) target = candidate;
      const px = x(target);
      guide.setAttribute("x1", String(px)); guide.setAttribute("x2", String(px));
      tip.replaceChildren(node("strong", "", block.x_axis === "price_move" ? moveLabel(target) + " price move" : date(target)));
      for (const marker of markers.filter(m => m.at === target)) {
        const row = node("div", "tip-row"); row.append(node("i", "dot " + tone(marker.tone)), node("span", "", marker.label)); tip.append(row);
      }
      series.forEach((s, index) => {
        const point = nearest(s.points, target);
        if (!point) { dots[index].setAttribute("r", "0"); return; }
        dots[index].setAttribute("r", "3.5");
        dots[index].setAttribute("cx", String(x(position(block, point))));
        dots[index].setAttribute("cy", String(scales[s.panel](numeric(point.value))));
        const row = node("div", "tip-row");
        row.append(node("i", "swatch s" + (index % 6 + 1) + (s.style === "dashed" ? " dashed" : "")));
        if (series.length > 1) row.append(node("span", "muted", (s.label || title || block.title) + ":"));
        row.append(node("span", "quantity", reading(point.value)));
        // A marker between readings shows the nearest one: say when that reading was taken.
        const taken = position(block, point);
        if (taken !== target) row.append(node("span", "muted", "(" + (block.x_axis === "price_move" ? moveLabel(taken) + " price move" : date(taken)) + ")"));
        tip.append(row);
      });
      tip.hidden = false; svg.classList.add("hovering");
      // Beside the guide, on the side with more room, and never past the card's edges.
      const frame = card.getBoundingClientRect(), width = tip.offsetWidth;
      const anchor = box.left - frame.left + px * box.width / W;
      const left = px > (L + W - R) / 2 ? anchor - 10 - width : anchor + 10;
      tip.style.left = Math.max(4, Math.min(left, frame.width - width - 4)) + "px";
      tip.style.top = (box.top - frame.top + top * box.height / H) + "px";
    };
    area.addEventListener("pointermove", show);
    area.addEventListener("pointerdown", show);
    area.addEventListener("pointerleave", hide);
  }
  function callout(block) {
    const box = node("aside", "callout " + tone(block.tone));
    box.append(node("strong", "callout-title", block.title), node("p", "", block.text));
    return box;
  }
  function steps(block) {
    const card = node("article", "steps card"); card.append(node("h3", "", block.title));
    const list = node("ol", "flow");
    const order = block.states.map(s => s.id);
    block.states.forEach((state, index) => {
      const item = node("li", "state " + tone(state.tone) + (state.terminal ? " terminal" : ""));
      const head = node("div", "state-head");
      head.append(node("span", "state-index", String(index + 1)), node("strong", "", state.label));
      item.append(head);
      if (state.detail) item.append(node("p", "state-detail", state.detail));
      const out = block.transitions.filter(t => t.source === state.id);
      const next = order[index + 1];
      const branches = out.filter(t => t.target !== next);
      if (branches.length) {
        const ul = node("ul", "branches");
        for (const t of branches) {
          const target = block.states.find(s => s.id === t.target);
          const li = node("li", tone(t.tone));
          li.append(node("span", "branch-label", t.label), node("span", "branch-arrow", t.target === state.id ? "↻ repeats" : "→ " + target.label));
          ul.append(li);
        }
        item.append(ul);
      }
      list.append(item);
      const forward = out.find(t => t.target === next);
      if (next && forward) {
        const edge = node("li", "edge " + tone(forward.tone));
        edge.setAttribute("aria-hidden", "false");
        edge.append(node("span", "edge-arrow", "↓"), node("span", "edge-label", forward.label));
        list.append(edge);
      } else if (next) {
        list.append(node("li", "edge gap"));
      }
    });
    card.append(list);
    return card;
  }
  function renderBlock(block, facts, view) {
    let element;
    if (block.kind === "metric") element = metric(block);
    else if (block.kind === "text") element = node("p", "author-note", block.text);
    else if (block.kind === "callout") element = callout(block);
    else if (block.kind === "steps") element = steps(block);
    else if (block.kind === "table") element = table(block, facts, view[block.id] ||= {});
    else if (block.kind === "bar") element = bars(block);
    else if (block.kind === "line") element = line(block);
    else {
      const grid = node("div", "grid");
      for (const child of block.blocks) grid.append(renderBlock(child, facts, view));
      if (block.collapsed) {
        element = node("details", "group detail");
        const summary = node("summary", "group-title"); summary.append(node("h2", "", block.title));
        element.append(summary, grid);
      } else {
        element = node("section", "group"); element.append(node("h2", "group-title", block.title), grid);
      }
    }
    element.id = "block-" + block.id; return element;
  }
  // Host theme: CSS custom properties the stylesheet reads. Colours and font stacks only; layout stays the renderer's.
  const themeTokens = Object.freeze([
    "scheme", "ink", "muted", "line", "line-soft", "paper", "surface", "surface-muted",
    "accent", "accent-ink", "wash", "warning-paper", "warning-line", "warning-ink", "danger-ink", "success-ink",
    "info-ink", "danger-paper", "success-paper", "info-paper",
    "series-1", "series-2", "series-3", "series-4", "series-5", "series-6",
    "font", "heading-font", "heading-weight", "mono-font",
  ]);
  function applyTheme(documentElement, theme) {
    for (const token of themeTokens) documentElement.style.removeProperty("--" + token);
    if (theme === null || typeof theme !== "object") return;
    for (const token of themeTokens) {
      const value = theme[token];
      if (typeof value === "string" && value.length > 0 && value.length <= 256) documentElement.style.setProperty("--" + token, value);
    }
  }
  function mount(root, response, schema, options = {}) {
    applyTheme(root.ownerDocument.documentElement, options?.theme);
    root.replaceChildren();
    try { AlmanakDashboardValidation.validate(response, schema); response = AlmanakDashboardValidation.resolve(response); }
    catch { root.append(node("p", "warning", "Dashboard unavailable: invalid or unsupported document.")); return; }
    const scope = response.snapshot.deployment_id + ":" + response.snapshot.strategy_hash;
    if (views.get(root)?.scope !== scope) views.set(root, {scope, blocks: Object.create(null)});
    const view = views.get(root).blocks;
    const content = node("div", "content"); root.append(content);
    const intro = node("div", "intro");
    intro.append(node("p", "eyebrow", forked(response.snapshot) ? "STRATEGY / FORK TEST" : "STRATEGY / RECORDED EVIDENCE"), node("h1", "", response.document?.title || "Strategy dashboard"), node("p", "description", response.document?.description || "Retained deployment evidence"));
    content.append(intro, metadata(response.snapshot));
    const navigation = node("nav", "tabs"); navigation.setAttribute("aria-label", "Dashboard sections");
    for (const item of response.document?.blocks || []) {
      if (!["group", "table"].includes(item.kind) || item.collapsed) continue;
      const link = node("a", "", item.title); link.href = "#block-" + item.id; navigation.append(link);
      // A fragment link in an embedded srcdoc frame resolves against the host page's URL, so following it
      // would navigate the frame away and leave it blank; scroll to the section in place instead.
      link.addEventListener("click", event => {
        event.preventDefault();
        for (const other of navigation.children) other.removeAttribute("aria-current");
        link.setAttribute("aria-current", "location");
        root.ownerDocument.getElementById("block-" + item.id)?.scrollIntoView({block: "start"});
      });
    }
    navigation.firstElementChild?.setAttribute("aria-current", "location");
    content.append(navigation);
    if (response.document) {
      // Metrics outside a group share a grid with their neighbours, as a group's would.
      let loose = null;
      for (const block of response.document.blocks) {
        let element;
        try { element = renderBlock(block, response.snapshot.values, view); }
        catch { element = node("p", "warning", "This block could not be rendered: " + block.id); }
        if (block.kind !== "metric") { loose = null; content.append(element); continue; }
        if (!loose) { loose = node("div", "grid loose"); content.append(loose); }
        loose.append(element);
      }
    } else content.append(node("p", "warning", response.diagnostic || "This strategy has no custom dashboard."));
    content.append(node("footer", "", "ALMANAK / Evidence first. Snapshot data stays in this browser."));
  }
  globalThis.AlmanakDashboard = {mount, exact, compare, themeTokens, axisDigits};
})();
