# 快速入门

本指南将引导您完成 Almanak SDK 的安装、创建您的第一个策略，以及在本地 Anvil 分叉上运行 -- 无需钱包或 API 密钥。

## 前提条件

- **Python 3.12+**
- **uv**（Python 包管理器）：

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

- **Foundry**（提供用于本地分叉测试的 Anvil）：

```bash
curl -L https://foundry.paradigm.xyz | bash
foundryup
```

## 安装

```bash
pipx install almanak
```

或者使用 [uv](https://docs.astral.sh/uv/)：

```bash
uv tool install almanak
```

这会全局安装 `almanak` CLI。每个创建的策略也会在自己的 `.venv/` 中将 `almanak` 作为本地依赖 -- 这种双重安装模式是标准做法（与 CrewAI、Dagster 等相同）。

**使用 AI 编程代理？** 一个命令即可让它学会 SDK：

```bash
almanak agent install
```

这会自动检测您的平台（Claude Code、Codex、Cursor、Copilot 以及 [其他 6 个](agent-skills.md)）并安装策略构建器技能。

## 1. 获取策略

### 选项 A：复制一个可运行的演示（推荐初学者使用）

```bash
almanak strat demo
```

这会显示一个交互式菜单，包含 13 个可运行的演示策略。选择一个，它会被复制到您当前的目录中，随时可以运行。您也可以跳过菜单：

```bash
almanak strat demo --name uniswap_rsi
```

### 选项 B：从模板创建

```bash
almanak strat new
```

按照交互式提示选择模板、链和名称。这将创建一个**独立的 Python 项目**，包含：

- `strategy.py` - 您的策略实现，包含 `decide()` 方法
- `config.json` - 链、协议和参数配置
- `pyproject.toml` - 依赖和 `[tool.almanak]` 元数据
- `uv.lock` - 锁定的依赖（由 `uv sync` 创建）
- `.venv/` - 每个策略的虚拟环境（由 `uv sync` 创建）
- `.env` - 环境变量（稍后填入您的密钥）
- `.gitignore` - Git 忽略规则
- `.python-version` - Python 版本固定（3.12）
- `__init__.py` - 包导出
- `tests/` - 测试脚手架
- `AGENTS.md` - AI 代理指南

脚手架会自动运行 `uv sync` 来安装依赖。要添加额外的包：

```bash
uv add pandas-ta          # 更新 pyproject.toml + uv.lock + .venv/
uv run pytest tests/ -v   # 在策略的 venv 中运行测试
```

## 2. 在本地 Anvil 分叉上运行

测试策略的最快方式 -- 无需钱包密钥、无需真实资金、零风险：

```bash
cd my_strategy
almanak strat run --network anvil --once
```

此命令会自动：

1. **启动 Anvil 分叉** -- 分叉 `config.json` 中指定的链（默认使用免费公共 RPC）
2. **使用默认 Anvil 钱包** -- 无需 `ALMANAK_PRIVATE_KEY`
3. **启动网关**侧车在后台运行
4. **为您的钱包充值** `anvil_funding` 中列出的代币（见下方）
5. **运行一次迭代** 您的策略的 `decide()` 方法

### Anvil 上的钱包充值

在 `config.json` 中添加 `anvil_funding` 块，以便在分叉启动时自动为您的钱包充值：

```json
{
    "anvil_funding": {
        "ETH": 10,
        "USDC": 10000,
        "WETH": 5
    }
}
```

原生代币（ETH、AVAX 等）通过 `anvil_setBalance` 充值。ERC-20 代币通过存储槽操作充值。每次分叉启动时都会自动执行。

### 更好的 RPC 性能（可选）

免费公共 RPC 可用但有速率限制。如需更快的分叉速度，请在 `.env` 中设置 Alchemy 密钥：

```bash
ALCHEMY_API_KEY=your_alchemy_key
```

这会自动为所有支持的链构建 RPC URL。任何提供商都可以 -- 请参阅[环境变量](environment-variables.md)了解完整的优先级顺序。

## 3. 在主网运行

!!! warning
    主网执行使用**真实资金**。请从小额开始，并使用专用钱包。

要在实际链上运行，您需要在 `.env` 中设置钱包私钥：

```bash
# .env
ALMANAK_PRIVATE_KEY=0xYOUR_PRIVATE_KEY

# RPC 访问（选择一个）
ALCHEMY_API_KEY=your_alchemy_key
# 或者: RPC_URL=https://your-rpc-provider.com/v1/your-key
```

然后不使用 `--network anvil` 标志运行：

```bash
almanak strat run --once
```

!!! tip
    先使用 `--dry-run` 测试，模拟运行但不提交交易：

    ```bash
    almanak strat run --dry-run --once
    ```

请参阅[环境变量](environment-variables.md)了解完整的配置选项列表，包括协议特定的 API 密钥。

## 策略结构

策略实现 `decide()` 方法，该方法接收 `MarketSnapshot` 并返回 `Intent`：

```python
from decimal import Decimal
from almanak import IntentStrategy, Intent, MarketSnapshot

class MyStrategy(IntentStrategy):
    def decide(self, market: MarketSnapshot) -> Intent | None:
        price = market.price("ETH")
        balance = market.balance("USDC")

        if price < Decimal("2000") and balance.balance_usd > Decimal("500"):
            return Intent.swap(
                from_token="USDC",
                to_token="ETH",
                amount_usd=Decimal("500"),
            )
        return Intent.hold(reason="没有机会")
```

## 可用意图

| 意图 | 描述 |
|--------|-------------|
| `SwapIntent` | DEX 上的代币交换 |
| `HoldIntent` | 无操作，等待下一轮 |
| `LPOpenIntent` | 开设流动性头寸 |
| `LPCloseIntent` | 关闭流动性头寸 |
| `BorrowIntent` | 从借贷协议借款 |
| `RepayIntent` | 偿还借款资产 |
| `SupplyIntent` | 向借贷协议提供资产 |
| `WithdrawIntent` | 从借贷协议提取资产 |
| `StakeIntent` | 质押代币 |
| `UnstakeIntent` | 取消质押代币 |
| `PerpOpenIntent` | 开设永续合约头寸 |
| `PerpCloseIntent` | 关闭永续合约头寸 |
| `FlashLoanIntent` | 闪电贷操作 |
| `CollectFeesIntent` | 收取 LP 费用 |
| `PredictionBuyIntent` | 购买预测市场份额 |
| `PredictionSellIntent` | 出售预测市场份额 |
| `PredictionRedeemIntent` | 赎回预测市场收益 |
| `VaultDepositIntent` | 存入金库 |
| `VaultRedeemIntent` | 从金库赎回 |
| `WrapNativeIntent` | 包装原生代币（例如 ETH 转 WETH） |
| `UnwrapNativeIntent` | 解包原生代币（例如 WETH 转 ETH） |
| `BridgeIntent` | 跨链桥接代币 |
| `EnsureBalanceIntent` | 元意图，解析为 `BridgeIntent` 或 `HoldIntent`，确保目标链上的最低代币余额 |

## 生成权限清单（Safe 钱包）

通过带有 Zodiac Roles 限制的 Safe 钱包部署策略时，代理需要一组明确的合约权限。SDK 可以通过检查策略的 intent 编译到哪些合约和函数选择器来自动生成此清单：

```bash
# 从策略目录运行
almanak strat permissions

# 指定目录
almanak strat permissions -d strategies/demo/uniswap_rsi

# 覆盖链
almanak strat permissions --chain base

# 写入文件
almanak strat permissions -o permissions.json
```

该命令从 `@almanak_strategy` 装饰器读取 `supported_protocols` 和 `intent_types`，通过真实编译器编译合成 intent，并提取所需的最小合约地址和函数选择器集合。输出为 JSON 清单，可应用于 Zodiac Roles 模块。如果策略支持多条链，输出为 JSON 数组，每条链一个清单；使用 `--chain` 可生成单条链的清单。

!!! note "仅用于 Safe/Zodiac 部署"
    权限清单仅在通过带有 Zodiac Roles 的 Safe 钱包运行时需要。本地 Anvil 测试或直接密钥执行不需要权限。

## 下一步

- [环境变量](environment-variables.md) - 所有配置选项
- [API 参考](api/index.md) - 完整的 Python API 文档
- [CLI 参考](cli/almanak.md) - 所有 CLI 命令
- [网关 API](gateway/api-reference.md) - 网关 gRPC 服务
