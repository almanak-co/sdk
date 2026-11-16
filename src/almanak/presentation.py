"""Human terminal output and structured automation output share retained facts."""

import json
import sys


def text_lines(value, metadata, depth=0):
    from almanak.report_values import quantity

    indent = "  " * depth
    if isinstance(value, dict):
        amount = quantity(value, metadata)
        if amount is not None:
            return [indent + amount[1] + " [" + amount[0] + "]"]
        return mapping_lines(value, metadata, depth)
    if isinstance(value, list):
        lines = []
        for entry in value:
            lines.extend(text_lines(entry, metadata, depth))
        return lines
    return [indent + ("not measured" if value is None else str(value))]


def mapping_lines(value, metadata, depth):
    indent = "  " * depth
    lines = []
    for key, item in value.items():
        label = str(key).replace("_", " ")
        if isinstance(item, (dict, list)):
            lines.append(indent + label + ":")
            lines.extend(text_lines(item, metadata, depth + 1))
        else:
            displayed = "not measured" if item is None else str(item)
            lines.append(indent + label + ": " + displayed)
    return lines


def render_text(value):
    from almanak.report_layout import overview, sections

    metadata = value.get("asset_metadata", {})
    # A status nests its run's facts under `recorded`, and one read from the workspace's
    # files without a supervisor also names its `evidence`; a report is only the facts.
    if "recorded" in value:
        facts = value["recorded"]
        value = {**value, "recorded": {**overview(facts), **sections(facts)}}
    elif "evidence" in value:
        value = {"evidence": value["evidence"], **overview(value), **sections(value)}
    return "\n".join(text_lines(value, metadata))


def emit(value, args=None):
    selected = getattr(args, "format", None) or ("text" if sys.stdout.isatty() else "json")
    print(render_text(value) if selected == "text" else json.dumps(value, indent=2), flush=True)
