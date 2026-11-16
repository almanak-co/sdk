/* The bundled schema is local. Validation never resolves URLs or expressions. */
(() => {
  "use strict";
  const own = (object, key) => Object.prototype.hasOwnProperty.call(object, key);
  const fail = (message) => { throw new Error(message); };
  function matches(value, rule, root, depth) {
    try { check(value, rule, root, depth); return true; } catch { return false; }
  }
  function check(value, rule, root, depth = 0) {
    if (depth > 40) fail("Document nesting exceeds the limit");
    if (rule.$ref) {
      if (!rule.$ref.startsWith("#/$defs/")) fail("External schema references are unsupported");
      return check(value, root.$defs[rule.$ref.slice(8)], root, depth + 1);
    }
    if (rule.anyOf && !rule.anyOf.some(r => matches(value, r, root, depth + 1))) fail("Invalid union value");
    if (rule.oneOf && rule.oneOf.filter(r => matches(value, r, root, depth + 1)).length !== 1) fail("Invalid block kind");
    if (own(rule, "const") && value !== rule.const) fail("Unsupported protocol or block kind");
    if (rule.enum && !rule.enum.includes(value)) fail("Unsupported option");
    if (rule.type === "null" && value !== null) fail("Expected null");
    if (rule.type === "boolean" && typeof value !== "boolean") fail("Expected boolean");
    if (rule.type === "string") {
      if (typeof value !== "string") fail("Expected text");
      if (value.length < (rule.minLength || 0) || value.length > (rule.maxLength ?? Infinity)) fail("Text exceeds limits");
      if (rule.pattern && !new RegExp(rule.pattern, "u").test(value)) fail("Invalid identifier or quantity");
    }
    if (rule.type === "integer" || rule.type === "number") {
      if (typeof value !== "number" || !Number.isFinite(value)) fail("Expected finite number");
      if (rule.type === "integer" && !Number.isSafeInteger(value)) fail("Expected exact integer");
      if (value < (rule.minimum ?? -Infinity) || value > (rule.maximum ?? Infinity)) fail("Number outside limits");
      if (value <= (rule.exclusiveMinimum ?? -Infinity) || value >= (rule.exclusiveMaximum ?? Infinity)) fail("Number outside limits");
    }
    if (rule.type === "array") {
      if (!Array.isArray(value)) fail("Expected array");
      if (value.length < (rule.minItems || 0) || value.length > (rule.maxItems ?? Infinity)) fail("Array exceeds limits");
      value.forEach(item => check(item, rule.items, root, depth + 1));
    }
    if (rule.type === "object") checkObject(value, rule, root, depth);
  }
  function checkObject(value, rule, root, depth) {
    if (!value || typeof value !== "object" || Array.isArray(value)) fail("Expected object");
    if (Object.keys(value).length > (rule.maxProperties ?? Infinity)) fail("Object exceeds limits");
    for (const key of rule.required || []) if (!own(value, key)) fail("Missing required field");
    for (const [key, entry] of Object.entries(value)) {
      if (["__proto__", "constructor", "prototype"].includes(key)) fail("Reserved field name");
      if (rule.propertyNames) check(key, rule.propertyNames, root, depth + 1);
      if (own(rule.properties || {}, key)) check(entry, rule.properties[key], root, depth + 1);
      else if (rule.additionalProperties === false) fail("Unknown field");
      else if (rule.additionalProperties && typeof rule.additionalProperties === "object") check(entry, rule.additionalProperties, root, depth + 1);
    }
  }
  function canonical(value) {
    if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
    if (value && typeof value === "object") return `{${Object.keys(value).sort().map(k => `${JSON.stringify(k)}:${canonical(value[k])}`).join(",")}}`;
    return JSON.stringify(value);
  }
  const chainOf = value => value.scope?.fund.chain;
  function validateValue(value, facts) {
    if (own(facts, value.id)) {
      if (canonical(value) !== canonical(facts[value.id])) fail("Changed snapshot fact");
    } else if (!["author_derived", "projected"].includes(value.evidence)) fail("Unrecognized recorded fact");
    if (value.inputs.some(key => !own(facts, key))) fail("Unknown calculation input");
    if (value.amount === null && !value.reason) fail("Missing value requires a reason");
    if (["author_derived", "projected"].includes(value.evidence) && !value.reason) fail("Missing calculation explanation");
    if (["base_units", "shares"].includes(value.unit) && (!value.asset || !value.scope)) fail("Missing financial scope");
    if (value.unit === "shares" && (!value.scope?.target || value.decimals !== null)) fail("Invalid share scope");
    if (value.scope && value.asset && !value.asset.startsWith(chainOf(value).chain_id + "/")) fail("Mismatched asset chain");
    if (["usd", "price"].includes(value.unit) && !value.price_source) fail("Price value without a price source");
    if ((value.unit === "price") !== (value.quote != null)) fail("A price names its quote asset, and only a price has one");
    if (value.symbol != null && !["base_units", "price"].includes(value.unit)) fail("Symbol on a non-token value");
  }
  function validateTable(data, facts) {
    const columns = data.columns.map(c => c.key);
    if (new Set(columns).size !== columns.length) fail("Duplicate columns");
    for (const row of data.rows) {
      if (canonical(Object.keys(row).sort()) !== canonical([...columns].sort())) fail("Mismatched table row");
      for (const column of data.columns) {
        const value = row[column.key];
        if (column.kind === "value") {
          if (!value || typeof value !== "object" || !own(facts, value.ref)) fail("Unknown table fact");
        } else if (typeof value !== "string") fail("Expected display text");
      }
    }
  }
  function comparable(value) {
    return canonical([value.unit, value.asset, ["ratio", "percent", "usd", "index", "price"].includes(value.unit) ? null : value.decimals, chainOf(value),
      value.unit === "shares" ? value.scope?.target : null, value.quote ?? null]);
  }
  function validate(response, schema) {
    if (new TextEncoder().encode(JSON.stringify(response)).length > 2 * 1024 * 1024) fail("Response exceeds byte limit");
    check(response, schema, schema);
    if (response.protocol_version !== 1) fail("Unsupported dashboard protocol");
    if ((response.status === "ready") !== (response.document !== null)) fail("Invalid response state");
    const facts = response.snapshot.values;
    for (const [key, value] of Object.entries(facts)) {
      if (key !== value.id) fail("Invalid snapshot fact identity");
      validateValue(value, facts);
    }
    if (!response.document) return;
    const ids = new Set();
    function block(item) {
      if (ids.has(item.id) || ids.size >= 64) fail("Invalid block IDs or count");
      ids.add(item.id);
      if (item.kind === "group") item.blocks.forEach(block);
      if (item.kind === "metric") validateValue(item.value, facts);
      if (item.kind === "table") validateTable(item.data, facts);
      if (item.kind === "bar") {
        item.points.forEach(p => validateValue(p.value, facts));
        if (new Set(item.points.map(p => comparable(p.value))).size > 1) fail("Incomparable chart units");
      }
      if (item.kind === "line") validateLine(item, facts);
      if (item.kind === "steps") {
        const states = new Set(item.states.map(s => s.id));
        if (states.size !== item.states.length) fail("Duplicate states");
        if (item.transitions.some(t => !states.has(t.source) || !states.has(t.target))) fail("Unknown state");
      }
    }
    response.document.blocks.forEach(block);
  }
  function validateLine(item, facts) {
    const series = item.series?.length ? item.series : [{id: "_", panel: "main", points: item.points}];
    if (!item.series?.length === !item.points.length) fail("Line needs points or series");
    const ids = new Set(series.map(s => s.id));
    if (ids.size !== series.length) fail("Duplicate series");
    const at = p => item.x_axis === "price_move" ? p.move_bps : p.time;
    const lower = new Set(item.lower_marks || []);
    if (lower.size && !series.some(s => s.panel === "lower")) fail("Lower marks without a lower series");
    const marks = new Set([...(item.references || []), ...(item.bands || [])].map(m => m.label));
    if ([...lower].some(label => !marks.has(label))) fail("Lower mark names no reference or band");
    const panels = {main: [], lower: []};
    for (const s of series) {
      const values = s.points.map(p => cited(p.value, facts));
      values.forEach(v => validateValue(v, facts));
      if (s.points.some(p => typeof at(p) !== "number")) fail("Sample does not match the axis");
      if (s.points.some((p, i) => i && at(p) < at(s.points[i - 1]))) fail("Unordered history");
      panels[s.panel].push(...values);
    }
    for (const r of item.references || []) { validateValue(r.value, facts); panels[lower.has(r.label) ? "lower" : "main"].push(r.value); }
    for (const b of item.bands || []) for (const v of [b.low, b.high]) if (v) { validateValue(v, facts); panels[lower.has(b.label) ? "lower" : "main"].push(v); }
    for (const values of Object.values(panels)) if (new Set(values.map(comparable)).size > 1) fail("Incomparable chart units");
    for (const m of [...(item.markers || []), ...(item.annotations || [])]) if (m.series != null && !ids.has(m.series)) fail("Unknown series");
  }
  // A chart sample may cite a snapshot fact ({ref}) instead of repeating it.
  function cited(value, facts) {
    if (value && typeof value === "object" && "ref" in value && !("id" in value)) {
      if (!own(facts, value.ref)) fail("Unknown chart fact");
      return facts[value.ref];
    }
    return value;
  }
  // The response with every cited chart fact replaced by the fact itself, for rendering.
  function resolve(response) {
    const facts = response.snapshot.values;
    const copy = JSON.parse(JSON.stringify(response));
    const line = item => {
      for (const s of item.series?.length ? item.series : [{points: item.points}])
        for (const p of s.points) p.value = cited(p.value, facts);
    };
    const visit = item => item.kind === "group" ? item.blocks.forEach(visit) : item.kind === "line" && line(item);
    copy.document?.blocks.forEach(visit);
    return copy;
  }
  globalThis.AlmanakDashboardValidation = {validate, canonical, resolve};
})();
