from __future__ import annotations

from pathlib import Path


def test_runner_and_strategy_do_not_reach_gateway_database_url_directly() -> None:
    root = Path(__file__).resolve().parents[3]
    production_files = [
        *Path(root, "almanak/framework/runner").rglob("*.py"),
        *Path(root, "almanak/framework/strategies").rglob("*.py"),
    ]
    forbidden = (
        "asyncpg",
        "database_url",
        "GatewaySettings().database_url",
        "ALMANAK_GATEWAY_DATABASE_URL",
    )

    hits: list[str] = []
    for path in production_files:
        text = path.read_text()
        for pattern in forbidden:
            if pattern in text:
                hits.append(f"{path.relative_to(root)} contains {pattern}")

    assert hits == []
