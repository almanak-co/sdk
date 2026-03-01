![Almanak](assets/logo-dark.svg){ .hero-logo }

<div style="text-align: center">

<p><strong>面向量化交易者的生产级 DeFi 策略框架</strong></p>

<p>
  <a href="https://pypi.org/project/almanak/"><img src="https://img.shields.io/pypi/v/almanak?style=flat-square&color=blue" alt="PyPI version"></a>
  <a href="https://pypi.org/project/almanak/"><img src="https://img.shields.io/pypi/pyversions/almanak?style=flat-square" alt="Python 3.12+"></a>
  <a href="https://github.com/almanak-co/almanak-sdk/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-green?style=flat-square" alt="License: Apache-2.0"></a>
  <a href="https://discord.gg/yuCMvQv3rN"><img src="https://img.shields.io/badge/Discord-join-5865F2?style=flat-square&logo=discord&logoColor=white" alt="Discord"></a>
  <a href="https://x.com/Almanak__"><img src="https://img.shields.io/badge/Twitter-follow-1DA1F2?style=flat-square&logo=x&logoColor=white" alt="Twitter"></a>
</p>

<p>
  <a href="/">English</a> |
  <a href="/zh/">中文</a> |
  <a href="/fr/">Français</a> |
  <a href="/es/">Español</a>
</p>

</div>

---

Almanak SDK 提供了一个全面的框架，用于开发、测试和部署自主 DeFi 代理。基于意图驱动架构构建，策略以高级意图表达，代码量最少。

## 功能特性

- **意图驱动架构** - 以高级意图（Swap、LP、Borrow 等）表达交易逻辑。框架自动处理编译和执行。
- **三级状态管理** - 自动持久化，支持 HOT/WARM/COLD 三级存储以确保可靠性。
- **全面的回测系统** - PnL 模拟、Anvil 分叉上的模拟交易和参数扫描。
- **多链支持** - Ethereum、Arbitrum、Optimism、Base、Avalanche、Polygon、BSC、Sonic、Plasma、Blast、Mantle、Berachain 等。
- **协议集成** - Uniswap V3、Aave V3、Morpho Blue、GMX V2、Pendle、Polymarket、Kraken 等。
- **非托管设计** - 通过 Safe 智能账户完全控制您的资金。
- **生产就绪** - 内置告警、卡顿检测、紧急管理和金丝雀部署。

## 安装

```bash
pip install almanak
```

## 快速开始

```bash
# 从模板创建新策略
almanak strat new

# 在本地 Anvil 分叉上运行 -- 无需钱包或 API 密钥
cd my_strategy
almanak strat run --network anvil --once
```

Anvil 分叉测试是推荐的起点。SDK 会自动启动本地分叉，使用默认的已充值钱包，零配置即可运行您的策略。请参阅[快速入门](getting-started.md)获取完整教程。

## 编写策略

策略实现 `decide()` 方法，该方法接收一个 `MarketSnapshot` 并返回一个 `Intent`（或 `None` 跳过本轮）：

```python
from decimal import Decimal
from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot

class MyStrategy(IntentStrategy):
    """一个简单的均值回归策略。"""

    def decide(self, market: MarketSnapshot) -> Intent | None:
        eth_price = market.price("ETH")
        usdc = market.balance("USDC")

        if eth_price < Decimal("2000") and usdc.balance_usd > Decimal("500"):
            return Intent.swap(
                from_token="USDC",
                to_token="ETH",
                amount_usd=Decimal("500"),
            )
        return Intent.hold(reason="等待更好的条件")
```

## 架构

```text
almanak/
  framework/           # V2 策略框架
    strategies/        # IntentStrategy 基类
    intents/           # 意图词汇表和编译器
    state/             # 三级状态管理
    execution/         # 交易编排
    backtesting/       # PnL、模拟交易、参数扫描
    connectors/        # 协议适配器
    data/              # 价格预言机、指标
    alerting/          # Slack/Telegram 通知
    services/          # 卡顿检测、紧急管理
  gateway/             # gRPC 网关侧车
  transaction_builder/ # 底层交易构建
  core/                # 枚举、模型、工具
  cli/                 # 命令行界面
```

所有策略通过**网关专用架构**运行以确保安全。网关侧车持有所有密钥并暴露受控的 gRPC API。策略容器没有密钥，也没有直接的互联网访问权限。

## 反馈与功能请求

有想法、发现了 bug 或想要请求新功能？请前往我们的 [Discord](https://discord.gg/yuCMvQv3rN) 并在相应频道发帖。我们会积极监控那里的反馈，并据此制定 SDK 路线图。

## 下一步

- [快速入门](getting-started.md) - 安装和首个策略教程
- [CLI 参考](cli/almanak.md) - 所有 CLI 命令
- [API 参考](api/index.md) - 完整的 Python API 文档
- [网关](gateway/api-reference.md) - 网关 gRPC API
