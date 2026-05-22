# 环境变量

所有策略通过**网关侧车**运行（由 `almanak strat run` 自动启动）。网关持有密钥，提供 RPC 访问，并执行交易。

在您的策略目录中创建 `.env` 文件，填入以下变量。

---

## 必需

在运行任何策略之前必须设置这些变量。

| 变量 | 描述 | 示例 |
|----------|-------------|---------|
| `ALMANAK_PRIVATE_KEY` | 用于签署交易和派生钱包地址的钱包私钥 | `0x4c0883a6...` |

### RPC 访问（推荐；未设置时使用免费公共 RPC）

| 变量 | 优先级 | 描述 | 示例 |
|----------|----------|-------------|---------|
| `ALMANAK_{CHAIN}_RPC_URL` | 1（最高） | 带 ALMANAK 前缀的按链 RPC URL | `https://arb-mainnet.infura.io/v3/KEY` |
| `{CHAIN}_RPC_URL` | 2 | 按链 RPC URL（如 `ARBITRUM_RPC_URL`） | `https://arb-mainnet.infura.io/v3/KEY` |
| `ALMANAK_RPC_URL` | 3 | 所有链的通用 RPC URL | `https://your-rpc.com/v1/KEY` |
| `RPC_URL` | 4 | 基本通用 RPC URL | `https://your-rpc.com/v1/KEY` |
| `ALCHEMY_API_KEY` | 5（备用） | Alchemy API 密钥 -- 按链自动构建 URL | `abc123def456` |
| `TENDERLY_API_KEY_{CHAIN}` | 6（备用） | 按链的 Tenderly API 密钥（如 `TENDERLY_API_KEY_ARBITRUM`） | `abc123...` |

任何提供商都可以：Infura、QuickNode、自托管、Alchemy 等。`ALCHEMY_API_KEY` 是可选的备用方案，会自动为所有支持的链构建 URL。如果未设置任何变量，网关将回退到免费公共 RPC（有速率限制，尽力而为）。

!!! warning
    切勿提交私钥。开发时请使用专用测试钱包。

**注意：** 网关还接受 `ALMANAK_GATEWAY_PRIVATE_KEY`（带有自己的前缀）。如果设置了，它将优先使用。否则，网关将回退到 `ALMANAK_PRIVATE_KEY` -- 因此您只需要一个变量。

---

## 可选 API 密钥

根据您的策略使用的协议和功能设置这些变量。

| 变量 | 何时需要 | 获取密钥 |
|----------|-------------|-----------|
| `ENSO_API_KEY` | 通过 Enso Finance 聚合器进行交换路由 | [enso.finance](https://enso.finance/) |
| `COINGECKO_API_KEY` | 改善价格数据的速率限制（无密钥也可使用） | [coingecko.com/en/api](https://www.coingecko.com/en/api) |
| `ALMANAK_API_KEY` | Almanak 平台身份验证 | [app.almanak.co](https://app.almanak.co/) |
| `THEGRAPH_API_KEY` | 使用子图数据进行回测（DEX 交易量、借贷 APY） | [thegraph.com/studio](https://thegraph.com/studio/) |

---

## 协议特定

仅在您的策略使用这些特定协议时需要。

### Kraken

| 变量 | 描述 |
|----------|-------------|
| `KRAKEN_API_KEY` | Kraken API 密钥（[获取凭证](https://www.kraken.com/u/security/api)） |
| `KRAKEN_API_SECRET` | Kraken API 密钥秘密 |

### Polymarket

| 变量 | 描述 |
|----------|-------------|
| `POLYMARKET_WALLET_ADDRESS` | Polymarket 钱包地址 |
| `POLYMARKET_PRIVATE_KEY` | Polymarket 签名密钥 |
| `POLYMARKET_API_KEY` | CLOB API 密钥 |
| `POLYMARKET_SECRET` | HMAC 秘密 |
| `POLYMARKET_PASSPHRASE` | API 密码短语 |

### Pendle

| 变量 | 描述 |
|----------|-------------|
| `ALMANAK_GATEWAY_PENDLE_API_KEY` | Pendle 协议 API 密钥 |

### Solana

| 变量 | 描述 |
|----------|-------------|
| `SOLANA_PRIVATE_KEY` | Ed25519 密钥对，base58 格式（或 64 字符十六进制种子）。Solana 策略必需。 |
| `SOLANA_RPC_URL` | Solana RPC 端点。默认为 `https://api.mainnet-beta.solana.com`（有速率限制）。生产环境请使用 Helius、QuickNode 或 Triton。 |
| `JUPITER_API_KEY` | Jupiter 聚合器 API 密钥。未设置时使用免费层级。 |

---

## 网关身份验证与安全

托管（Almanak Infra）部署的关键依赖项。每个变量在网关启动时读取一次，更改需要重启。

| 变量 | 描述 |
|----------|-------------|
| `ALMANAK_GATEWAY_AUTH_TOKEN` | gRPC 身份验证的共享密钥令牌。设置后，客户端必须在元数据中提供此令牌才能访问服务。**托管部署上必须设置。** |
| `ALMANAK_GATEWAY_ALLOW_INSECURE` | 设置为 `true` 时，允许网关在未设置 `ALMANAK_GATEWAY_AUTH_TOKEN` 的情况下启动。默认 `false`（网关拒绝启动）。**仅限本地开发** — 切勿在托管部署上设置。 |
| `ALMANAK_GATEWAY_OPERATOR_TOKEN` | `DashboardService` 上变更类 RPC（`PreviewReconcile`、`ApplyReconcile`、`RefreshRegistryFromChain`）所需的二次因子令牌（VIB-4493 第一阶段）。调用方必须在 `x-operator-token` 元数据头中发送相同的值，同时还要带上常规的身份验证令牌。未设置（默认）时，这些处理程序回退到仅 auth-token 验证 — 对单用户 / 本地部署是安全的。 |

!!! danger "未设置这些变量的托管部署不安全"
    在托管网关上省略 `ALMANAK_GATEWAY_AUTH_TOKEN`（或启用 `ALMANAK_GATEWAY_ALLOW_INSECURE=true`）会将每个 gRPC 服务暴露给未经身份验证的调用方 — 包括对交易进行签名和提交的 `ExecutionService`。请将两者都视为生产环境的密钥。

---

## 手动价格覆盖

针对真实预言机源无法定价的代币（如新兴链上的长尾代币）的最后兜底方案。

| 变量 | 描述 |
|----------|-------------|
| `ALMANAK_GATEWAY_ENABLE_MANUAL_PRICE_OVERRIDES` | 启用 `ManualPriceOverrideSource` 回退源。默认 `false`。默认关闭，因为设置错误的环境变量可能将错误价格输入到滑点 / 拆仓决策中。 |
| `ALMANAK_PRICE_OVERRIDE_<TOKEN>` | 每个代币的 USD 覆盖价格。仅在所有真实预言机源都未能为该代币定价时才会查阅。示例：`ALMANAK_PRICE_OVERRIDE_W0G=0.012`。 |

两者都需要设置：启用标志打开数据源；每个代币的变量提供价格。

---

## Tenderly 模拟

当模拟器设置为 `"tenderly"`（或自动选择）时，由 `SimulationService.SimulateBundle` 使用。三个变量必须同时设置 — 任何一个为空都会禁用 Tenderly 并在可用时回退到 Alchemy 模拟。

| 变量 | 描述 |
|----------|-------------|
| `ALMANAK_GATEWAY_TENDERLY_ACCOUNT_SLUG` | Tenderly 账户标识（仪表板 URL 中的 `<account>` 段）。 |
| `ALMANAK_GATEWAY_TENDERLY_PROJECT_SLUG` | 账户中的 Tenderly 项目标识。 |
| `ALMANAK_GATEWAY_TENDERLY_ACCESS_KEY` | 具有模拟权限的 Tenderly 访问密钥（[账户设置 → 访问密钥](https://dashboard.tenderly.co/account/authorization)）。 |

---

## 投资组合数据源（多提供商）

配置网关的投资组合估值源。由 `IntegrationService.GetWalletPortfolio` / `GetWalletPositions` 用于跨链聚合余额和 DeFi 持仓。

| 变量 | 描述 |
|----------|-------------|
| `ALMANAK_GATEWAY_PORTFOLIO_API_KEY` | 单提供商 API 密钥（旧版单提供商路径）。 |
| `ALMANAK_GATEWAY_PORTFOLIO_API_PROVIDER` | 单提供商名称。默认 `zerion`。 |
| `ALMANAK_GATEWAY_PORTFOLIO_PROVIDERS` | 多提供商覆盖。按优先顺序以逗号分隔的提供商名称（如 `zerion,moralis`）。设置后，优先于单提供商密钥。每个提供商从 `{NAME}_API_KEY` 读取自己的 API 密钥（如 `ZERION_API_KEY`、`MORALIS_API_KEY`）。 |

---

## Safe 钱包

用于通过 Gnosis Safe 多签执行的策略。

| 变量 | 描述 |
|----------|-------------|
| `ALMANAK_GATEWAY_SAFE_ADDRESS` | Safe 钱包地址 |
| `ALMANAK_GATEWAY_SAFE_MODE` | `direct`（Anvil/阈值为1）或 `zodiac`（生产环境） |
| `ALMANAK_GATEWAY_ZODIAC_ROLES_ADDRESS` | Zodiac Roles 模块地址（zodiac 模式） |
| `ALMANAK_GATEWAY_SIGNER_SERVICE_URL` | 远程签名服务 URL（zodiac 模式） |
| `ALMANAK_GATEWAY_SIGNER_SERVICE_JWT` | 远程签名 JWT（zodiac 模式） |

---

## 回测

### 归档 RPC URL

历史链上数据（Chainlink 价格、TWAP 计算）所必需。标准 RPC 节点不支持历史状态查询。请使用支持归档的提供商，如 Alchemy（付费）、QuickNode 或 Infura。

模式：`ARCHIVE_RPC_URL_{CHAIN}`（如 `ARCHIVE_RPC_URL_ARBITRUM`、`ARCHIVE_RPC_URL_ETHEREUM`、`ARCHIVE_RPC_URL_BASE`、`ARCHIVE_RPC_URL_OPTIMISM`、`ARCHIVE_RPC_URL_POLYGON`、`ARCHIVE_RPC_URL_AVALANCHE`）

### 区块浏览器 API 密钥

可选，用于历史 Gas 价格数据。模式：`{EXPLORER}_API_KEY`

| 变量 | 浏览器 |
|----------|----------|
| `ETHERSCAN_API_KEY` | [etherscan.io](https://etherscan.io/apis) |
| `ARBISCAN_API_KEY` | [arbiscan.io](https://arbiscan.io/apis) |
| `BASESCAN_API_KEY` | [basescan.org](https://basescan.org/apis) |
| `OPTIMISTIC_ETHERSCAN_API_KEY` | [optimistic.etherscan.io](https://optimistic.etherscan.io/apis) |
| `POLYGONSCAN_API_KEY` | [polygonscan.com](https://polygonscan.com/apis) |
| `SNOWTRACE_API_KEY` | [snowtrace.io](https://snowtrace.io/apis) |
| `BSCSCAN_API_KEY` | [bscscan.com](https://bscscan.com/apis) |

---

## 快速开始 `.env`

```bash
# 必需
ALMANAK_PRIVATE_KEY=0xYOUR_PRIVATE_KEY

# RPC 访问（选择一个）
RPC_URL=https://your-rpc-provider.com/v1/your-key
# ALCHEMY_API_KEY=your_alchemy_key  # 替代方案：按链自动构建 URL

# 推荐
ENSO_API_KEY=your_enso_key
COINGECKO_API_KEY=your_coingecko_key
```

所有其他网关和框架设置都有合理的默认值，不需要设置。请参阅 [`.env.example`](https://github.com/almanak-co/almanak-sdk/blob/main/.env.example) 了解高级选项的完整列表。
