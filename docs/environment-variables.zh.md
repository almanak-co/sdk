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
| `ALMANAK_API_KEY` | 平台功能：`strat push`、`strat pull`、部署 | [app.almanak.co](https://app.almanak.co/) |
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

所有其他网关和框架设置都有合理的默认值，不需要设置。请参阅 [`.env.example`](https://github.com/almanak-co/sdk/blob/main/.env.example) 了解高级选项的完整列表。
