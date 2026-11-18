"""Generate the CLI reference pages from the argparse tree.

One page per top-level product plus an index; lifecycle commands that share the
retained-run options are grouped on one page. Output is gitignored and rebuilt by
`make docs-cli`.
"""

import argparse
import sys
from pathlib import Path

from almanak.cli_arguments import build_parser

ROOT = Path(__file__).resolve().parents[2]
LIFECYCLE = ("status", "pause", "resume", "teardown", "shutdown")
LIFECYCLE_PAGE = "lifecycle"


def subcommands(parser):
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            helps = {choice.dest: choice.help for choice in action._choices_actions}
            return [(name, sub, helps.get(name)) for name, sub in action.choices.items()]
    return []


def type_name(action):
    if action.type is None:
        return ""
    return getattr(action.type, "__name__", str(action.type))


def option_rows(parser):
    rows = []
    for action in parser._actions:
        if action.help == argparse.SUPPRESS or isinstance(
            action, (argparse._HelpAction, argparse._SubParsersAction)
        ):
            continue
        flags = ", ".join(f"`{flag}`" for flag in action.option_strings) or f"`{action.dest}`"
        details = []
        if type_name(action):
            details.append(type_name(action))
        if action.choices:
            details.append("one of " + ", ".join(f"`{c}`" for c in action.choices))
        if action.default not in (None, argparse.SUPPRESS, False):
            details.append(f"default `{action.default}`")
        if action.required:
            details.append("required")
        rows.append((flags, "; ".join(details), action.help or ""))
    return rows


def exclusive_groups(parser):
    groups = []
    for group in parser._mutually_exclusive_groups:
        flags = ", ".join(f"`{a.option_strings[0]}`" for a in group._group_actions)
        groups.append(("Exactly one of " if group.required else "At most one of ") + flags)
    return groups


def render_command(parser, path, heading_level):
    parser.prog = " ".join(path)
    lines = ["#" * heading_level + " " + " ".join(path), ""]
    if parser.description:
        lines += [parser.description, ""]
    lines += ["```text", parser.format_usage().strip(), "```", ""]
    rows = option_rows(parser)
    if rows:
        lines += ["| Option | Details | Description |", "|---|---|---|"]
        lines += [f"| {flags} | {details} | {text} |" for flags, details, text in rows]
        lines.append("")
    for group in exclusive_groups(parser):
        lines += [group + ".", ""]
    for name, sub, help_text in subcommands(parser):
        if help_text:
            sub.description = help_text
        lines += render_command(sub, [*path, name], heading_level + 1)
    return lines


def pages(parser):
    """Map page stem to Markdown lines; the index lists every product."""
    root = " ".join([parser.prog])
    products = subcommands(parser)
    result: dict[str, list[str]] = {}
    index = [f"# {root}", "", parser.description or "", ""]
    index += ["```text", parser.format_usage().strip(), "```", ""]
    index += ["| Command | Description | Page |", "|---|---|---|"]
    for name, sub, help_text in products:
        stem = LIFECYCLE_PAGE if name in LIFECYCLE else name
        page = f"{parser.prog}-{stem}"
        index.append(f"| `{parser.prog} {name}` | {help_text or ''} | [{page}]({page}.md) |")
        if stem == LIFECYCLE_PAGE:
            lines = result.setdefault(
                page,
                [
                    f"# {parser.prog} lifecycle commands",
                    "",
                    "Retained-run commands that share the `--run` workspace argument.",
                    "",
                ],
            )
            sub.description = help_text
            lines += render_command(sub, [parser.prog, name], 2)
        else:
            sub.description = help_text
            result[page] = render_command(sub, [parser.prog, name], 1)
    index.append("")
    result[parser.prog] = index
    return result


def unexpected_pages(output, rendered):
    return sorted(p.stem for p in output.glob("*.md") if p.stem not in rendered)


def write_pages(output, rendered):
    output.mkdir(parents=True, exist_ok=True)
    for stem in unexpected_pages(output, rendered):
        (output / f"{stem}.md").unlink()
    for stem, lines in rendered.items():
        (output / f"{stem}.md").write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return sorted(rendered)


def main(argv=None):
    arguments = argparse.ArgumentParser(description=__doc__)
    arguments.add_argument("--output", type=Path, default=ROOT / "docs/cli")
    arguments.add_argument("--check", action="store_true", help="exit 1 when existing pages differ")
    args = arguments.parse_args(argv)
    rendered = pages(build_parser())
    if args.check:
        stale = [
            stem
            for stem, lines in rendered.items()
            if not (args.output / f"{stem}.md").exists()
            or (args.output / f"{stem}.md").read_text(encoding="utf-8")
            != "\n".join(lines).rstrip() + "\n"
        ]
        stale += unexpected_pages(args.output, rendered) if args.output.is_dir() else []
        if stale:
            print("stale CLI pages: " + ", ".join(sorted(stale)))
            return 1
        print(f"OK: {len(rendered)} CLI pages are current")
        return 0
    names = write_pages(args.output, rendered)
    print(f"Wrote {len(names)} CLI pages to {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
