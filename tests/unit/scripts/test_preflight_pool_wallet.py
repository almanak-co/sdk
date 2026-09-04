"""Fail-closed tests for Mainnet Intent durable-wallet planning."""

from __future__ import annotations

import importlib.util
import json
import os
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from eth_account import Account

REPO = Path(__file__).resolve().parents[3]


def _load(name: str):
    path = REPO / "qa_lab" / name
    spec = importlib.util.spec_from_file_location(name.removesuffix(".py"), path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _pool(tmp_path: Path, *, funded: bool = False, backup: bool = True) -> tuple[Path, object]:
    account = Account.create()
    path = tmp_path / "pool.json"
    path.write_text(
        json.dumps(
            {
                "bootstrap": [
                    {
                        "user_consent_at": "2026-08-15T00:00:00Z",
                        "backup_confirmed_at": "2026-08-15T00:01:00Z" if backup else None,
                    }
                ],
                "wallets": [
                    {
                        "index": 7,
                        "address": account.address,
                        "private_key": account.key.hex(),
                        "funded": funded,
                        "role": "test",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    os.chmod(path, 0o600)
    return path, account


def _plan(module, pool: Path) -> dict:
    return module.build_plan(
        pool_path=pool,
        cell_id="intent.aave_v3.arbitrum.SUPPLY.mainnet.eoa",
        pool_index=7,
        native="0.0001",
        tokens=["USDC:3"],
        trading_cap_usd="3",
        gas_budget_usd="1",
        total_wallet_cap_usd="5",
        wallet_value=lambda _address, _chain: Decimal("0.25"),
        price=lambda symbol, _chain: Decimal("2000") if symbol == "ETH" else Decimal("1"),
    )


def test_plan_is_sanitized_capped_and_digest_bound(tmp_path: Path) -> None:
    module = _load("preflight_pool_wallet.py")
    pool, account = _pool(tmp_path)

    plan = _plan(module, pool)

    assert plan["wallet"] == account.address
    assert plan["projected_wallet_usd"] == "3.4500"
    assert plan["gates"]["funding_executed"] is False
    assert "private_key" not in json.dumps(plan)
    assert module.verify_plan_digest(plan) == plan["approval_digest"]
    plan["funding"]["tokens"] = ["USDC:4"]
    with pytest.raises(ValueError, match="digest"):
        module.verify_plan_digest(plan)


def test_chain_env_supports_service_account_without_checkout_env(tmp_path: Path, monkeypatch) -> None:
    module = _load("chains.py")
    monkeypatch.setattr(module, "ENV_FILE", tmp_path / "absent.env")
    monkeypatch.setenv("ARBITRUM_RPC_URL", "https://operator.invalid/arbitrum")

    env = module.load_env()

    assert env["ARBITRUM_RPC_URL"] == "https://operator.invalid/arbitrum"


def test_process_env_overrides_checkout_env(tmp_path: Path, monkeypatch) -> None:
    module = _load("chains.py")
    env_file = tmp_path / ".env"
    env_file.write_text("ARBITRUM_RPC_URL=https://checkout.invalid/arbitrum\n", encoding="utf-8")
    monkeypatch.setattr(module, "ENV_FILE", env_file)
    monkeypatch.setenv("ARBITRUM_RPC_URL", "https://operator.invalid/arbitrum")

    env = module.load_env()

    assert env["ARBITRUM_RPC_URL"] == "https://operator.invalid/arbitrum"


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda path, _account: os.chmod(path, 0o644), "0600"),
        (
            lambda path, _account: _rewrite(path, lambda data: data["wallets"][0].update(funded=True)),
            "already allocated",
        ),
        (
            lambda path, _account: _rewrite(path, lambda data: data["bootstrap"][0].update(backup_confirmed_at=None)),
            "backup confirmation",
        ),
        (
            lambda path, _account: _rewrite(
                path, lambda data: data["wallets"][0].update(private_key=Account.create().key.hex())
            ),
            "does not match",
        ),
    ],
)
def test_invalid_pool_fails_closed(tmp_path: Path, mutation, message: str) -> None:
    module = _load("preflight_pool_wallet.py")
    pool, account = _pool(tmp_path)
    mutation(pool, account)

    with pytest.raises(ValueError, match=message):
        _plan(module, pool)


def _rewrite(path: Path, mutate) -> None:
    data = json.loads(path.read_text(encoding="utf-8"))
    mutate(data)
    path.write_text(json.dumps(data), encoding="utf-8")
    os.chmod(path, 0o600)


def test_budget_and_projected_wallet_cap_fail_closed(tmp_path: Path) -> None:
    module = _load("preflight_pool_wallet.py")
    pool, _account = _pool(tmp_path)
    kwargs = {
        "pool_path": pool,
        "cell_id": "intent.aave_v3.arbitrum.SUPPLY.mainnet.eoa",
        "pool_index": 7,
        "native": "0",
        "tokens": ["USDC:3"],
        "trading_cap_usd": "3",
        "gas_budget_usd": "3",
        "total_wallet_cap_usd": "5",
        "wallet_value": lambda _address, _chain: Decimal("0"),
        "price": lambda _symbol, _chain: Decimal("1"),
    }
    with pytest.raises(ValueError, match="trading cap plus gas budget"):
        module.build_plan(**kwargs)

    kwargs.update(
        trading_cap_usd="2",
        gas_budget_usd="1",
        total_wallet_cap_usd="3",
        wallet_value=lambda *_: Decimal("1"),
    )
    with pytest.raises(ValueError, match="projected whole-wallet value"):
        module.build_plan(**kwargs)


def test_funder_rejects_parameter_drift_from_approved_plan(tmp_path: Path) -> None:
    preflight = _load("preflight_pool_wallet.py")
    funder = _load("fund_pool_wallet.py")
    pool, _account = _pool(tmp_path)
    plan = _plan(preflight, pool)
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    args = SimpleNamespace(
        approved_plan=plan_path,
        approved_plan_sha256=plan["approval_digest"],
        token=["USDC:3"],
        chain="arbitrum",
        intent_cell_id="intent.aave_v3.arbitrum.SUPPLY.mainnet.eoa",
        pool_index=7,
        native="0.0001",
        trading_cap_usd="3",
        gas_budget_usd="1",
        total_wallet_cap_usd="5",
        leg="aave-supply",
    )

    funder._validate_approved_plan(args)
    args.token = ["USDC:3.01"]
    with pytest.raises(SystemExit, match="differ from"):
        funder._validate_approved_plan(args)
