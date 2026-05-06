"""Documented and unit-tested precedence rule.

One rule applied across every config surface, in order of decreasing
priority:

    CLI flag > env var > config.json > pyproject.toml > decorator default
        > hardcoded default

When two channels both supply a value, the higher-priority channel wins.
The precedence is enforced at the config-service boundary, not at the
read site.
"""

PRECEDENCE_ORDER: tuple[str, ...] = (
    "cli_flag",
    "env_var",
    "config_json",
    "pyproject_toml",
    "decorator_default",
    "hardcoded_default",
)

__all__ = ["PRECEDENCE_ORDER"]
