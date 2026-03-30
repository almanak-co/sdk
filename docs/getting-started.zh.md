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

!!! info "上线前注意事项"
    - 首次实际执行前，务必先运行 `--dry-run --once` 以验证 intent 编译，而不提交交易。
    - 如果交换因 "Too little received" 而回滚，请将 `amount_usd=` 改为 `amount=`（代币单位）。
      `amount_usd=` 依赖网关价格预言机进行 USD 到代币的转换，可能与 DEX 价格有偏差。
    - 从小额开始，监控前几次迭代，并记录您的实例 ID 以便使用 `--id` 恢复。

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
| `Intent.bridge()` | 跨链桥接代币（工厂方法，返回复合意图） |
| `Intent.ensure_balance()` | 确保目标链上的最低代币余额（工厂方法，解析为桥接或持有） |

## 状态持久化（有状态策略必需）

框架会在每次迭代后自动持久化运行器级别的元数据（迭代计数、错误计数器）。但是，**策略特定的状态** -- 头寸 ID、交易次数、阶段跟踪、冷却计时器 -- 只有在您实现两个钩子时才会保存：

```python
from typing import Any
from decimal import Decimal

class MyStrategy(IntentStrategy):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._position_id: int | None = None
        self._trades_today: int = 0

    def get_persistent_state(self) -> dict[str, Any]:
        """返回要保存的状态。每次迭代后调用。"""
        return {
            "position_id": self._position_id,
            "trades_today": self._trades_today,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """在启动时恢复状态。恢复运行时调用。"""
        self._position_id = state.get("position_id")
        self._trades_today = state.get("trades_today", 0)
```

如果没有这些钩子，您的策略在重启时将丢失所有内部状态。这对于 LP 策略尤其危险，因为丢失 `position_id` 意味着策略无法关闭自己的头寸。

!!! warning "没有持久化会丢失什么"
    如果您将状态存储在实例变量中（例如 `self._position_id`）但没有实现 `get_persistent_state()` 和 `load_persistent_state()`，该状态会在进程停止时丢失。重启后，您的策略将从零开始，不记得任何已开启的头寸、已完成的交易或内部阶段。

!!! tip "提示"
    - 在 `load_persistent_state()` 中使用带默认值的 `.get()` 进行防御性编程，这样旧的状态字典不会因缺少键而崩溃。
    - 将 `Decimal` 值存储为字符串（`str(amount)`）并在读取时解析（`Decimal(state["amount"])`），以确保 JSON 往返安全。
    - `on_intent_executed()` 回调是在交易后更新状态的最佳位置（例如存储新的头寸 ID），然后 `get_persistent_state()` 会在保存时获取它。

## 策略拆卸（必需）

每个策略都必须实现拆卸功能，以便运营者可以安全地关闭头寸。没有拆卸功能，关闭请求会被静默忽略，头寸将保持开启状态。`almanak strat new` 模板包含存根 -- 在构建策略时填写它们。

```python
class MyStrategy(IntentStrategy):
    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        """查询链上状态并返回已开启的头寸。"""
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary
        # ... 返回包含您头寸的 TeardownPositionSummary

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """返回有序的 intent 列表以平仓所有头寸。"""
        from almanak.framework.teardown import TeardownMode
        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")
        return [Intent.swap(from_token="WETH", to_token="USDC", amount="all", max_slippage=max_slippage)]
```

如果您的策略持有多种头寸类型，请按以下顺序关闭：**永续合约 -> 借款 -> 供应 -> LP -> 代币**。参阅[拆卸 CLI](cli/strat-teardown.md)了解运营者如何触发拆卸。

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

!!! note "回测 CLI"
    与 `almanak strat run` 从当前目录自动发现策略不同，
    回测命令需要明确的策略名称：`almanak strat backtest pnl -s my_strategy`。
    使用 `--list-strategies` 查看可用的策略。

## 下一步

- [环境变量](environment-variables.md) - 所有配置选项
- [API 参考](api/index.md) - 完整的 Python API 文档
- [CLI 参考](cli/almanak.md) - 所有 CLI 命令
- [网关 API](gateway/api-reference.md) - 网关 gRPC 服务

## 想让 LLM 来做决策？

SDK 还支持**代理策略**，其中 LLM 使用 Almanak 的 29 个内置工具自主决定
要做什么。您无需在 Python 中编写 `decide()` 逻辑，而是编写系统提示词，
让 LLM 基于市场数据进行推理。

这种方式需要**您自己的 LLM API 密钥**（OpenAI、Anthropic 或任何
兼容 OpenAI 的提供商）。

| | 确定性策略（本指南） | 代理策略 |
|---|---|---|
| **您编写的** | Python `decide()` 方法 | 系统提示词 + 策略 |
| **决策者** | 您的代码 | LLM（GPT-4、Claude 等） |
| **需要** | 仅 SDK | SDK + LLM API 密钥 |
| **最适合** | 已知规则、量化信号 | 复杂推理、多步计划 |

两种路径共享相同的网关、连接器和执行管道。

**开始使用：** [代理交易指南](agentic/index.md)
