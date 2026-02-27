# AI 代理技能

Almanak SDK 附带一个 **代理技能** -- 一个结构化的知识文件，教 AI 编程代理如何构建 DeFi 策略。将其安装到您的项目中，让您的 AI 助手理解 IntentStrategy API、意图词汇表、市场数据方法和 CLI 命令。

## 快速安装

最快的入门方式：

```bash
almanak agent install
```

这会自动检测您使用的 AI 编程工具（通过检查 `.claude/`、`.cursor/`、`.github/` 等），并将技能安装到每个平台的原生技能目录中。

## 特定平台说明

### Claude Code

Claude Code 从 `.claude/skills/` 子目录发现技能。

```bash
almanak agent install -p claude
```

**结果：** `.claude/skills/almanak-strategy-builder/SKILL.md`

---

### OpenAI Codex

Codex 从 `.codex/skills/` 子目录发现项目技能。

```bash
almanak agent install -p codex
```

**结果：** `.codex/skills/almanak-strategy-builder/SKILL.md`

---

### Cursor

Cursor 从 `.cursor/rules/` 读取自定义规则，使用 `.mdc` 文件和基于 glob 的作用域。

```bash
almanak agent install -p cursor
```

安装的文件包含 YAML 前置信息，在编辑 `strategy.py` 或 `config.json` 文件时激活技能：

```yaml
---
globs:
  - "**/strategy.py"
  - "**/config.json"
---
```

**结果：** `.cursor/rules/almanak-strategy-builder.mdc`

---

### GitHub Copilot

Copilot 从 `.github/instructions/` 读取作用域指令，使用 `applyTo` 前置信息。

```bash
almanak agent install -p copilot
```

安装的文件包含限定到策略文件的前置信息：

```yaml
---
applyTo: "**/strategy.py"
---
```

**结果：** `.github/instructions/almanak-strategy-builder.instructions.md`

---

### Windsurf

Windsurf 从 `.windsurf/rules/` 读取规则。

```bash
almanak agent install -p windsurf
```

**结果：** `.windsurf/rules/almanak-strategy-builder.md`

---

### Cline

Cline 从 `.clinerules/` 读取规则。

```bash
almanak agent install -p cline
```

**结果：** `.clinerules/almanak-strategy-builder.md`

---

### Roo Code

Roo Code 从 `.roo/rules/` 读取规则。

```bash
almanak agent install -p roo
```

**结果：** `.roo/rules/almanak-strategy-builder.md`

---

### Aider

Aider 从 `.aider/skills/` 子目录发现技能。

```bash
almanak agent install -p aider
```

**结果：** `.aider/skills/almanak-strategy-builder/SKILL.md`

---

### Amazon Q

Amazon Q 从 `.amazonq/rules/` 读取规则。

```bash
almanak agent install -p amazonq
```

**结果：** `.amazonq/rules/almanak-strategy-builder.md`

---

### OpenClaw

OpenClaw 从 `.openclaw/skills/` 子目录发现技能。也可通过 ClawHub 市场获取：`clawhub install almanak-strategy-builder`。

```bash
almanak agent install -p openclaw
```

**结果：** `.openclaw/skills/almanak-strategy-builder/SKILL.md`

---

## 安装所有平台

一次为所有支持的平台安装：

```bash
almanak agent install -p all
```

## 全局安装

安装到您的主目录（`~/`），这样技能在每个项目中都可用，无需逐项目设置：

```bash
almanak agent install -g
```

这会写入到 `~/.claude/skills/`、`~/.codex/skills/`、`~/.cursor/rules/` 等。仅支持项目级规则的平台（Copilot、Cline、Roo Code、Amazon Q）会自动跳过。

您也可以指定特定平台：

```bash
almanak agent install -g -p claude
```

检查或更新全局安装：

```bash
almanak agent status -g
almanak agent update -g
```

!!! note
    大多数平台允许本地（项目级）技能覆盖全局技能。如果两者都有，项目级技能优先。

## 通过 npx 安装（skills.sh）

如果您使用 [skills.sh](https://skills.sh) 注册表，可以直接从 GitHub 安装：

```bash
npx skills add almanak-co/almanak-sdk
```

这会从公共仓库发现 `almanak-strategy-builder` 技能，并为您检测到的平台安装。

## 管理已安装的技能

### 查看状态

查看哪些平台已安装该技能以及是否是最新版本：

```bash
almanak agent status
```

输出示例：

```text
Agent skill status (SDK v2.0.0):

  claude      up to date (v2.0.0)
  codex       not installed
  cursor      outdated (v1.9.0 -> v2.0.0)
  copilot     not installed
  ...

Installed: 1  Outdated: 1  Missing: 7
```

### 更新

升级 SDK 后（`pip install --upgrade almanak`），更新已安装的技能文件以匹配：

```bash
almanak agent update
```

这会扫描所有已安装的平台文件，并用当前 SDK 版本替换其内容。

### 试运行

预览 `install` 会做什么，但不写入任何文件：

```bash
almanak agent install --dry-run
```

### 自定义目录

安装到特定项目目录：

```bash
almanak agent install -d /path/to/my-project -p claude
```

## 逐策略指南

使用 `almanak strat new` 创建新策略时，会在策略目录内自动生成 `AGENTS.md` 文件。这个轻量级指南针对您选择的模板进行了定制 -- 它只列出与该特定策略相关的意图类型和模式。

```bash
almanak strat new --template mean_reversion --name my_rsi --chain arbitrum
# 在 strategy.py 和 config.json 旁边创建 my_rsi/AGENTS.md
```

## 技能教授内容

打包的技能涵盖：

| 章节 | 涵盖内容 |
|---------|---------------|
| 快速开始 | 安装、创建、运行 |
| 核心概念 | IntentStrategy、decide()、MarketSnapshot |
| 意图参考 | 所有意图类型及其签名和示例 |
| 市场数据 API | price()、balance()、rsi()、macd()、bollinger_bands() 及 10 多个指标 |
| 状态管理 | self.state 持久化、on_intent_executed 回调 |
| 配置 | config.json 格式、.env 密钥、anvil_funding |
| 代币解析 | get_token_resolver()、resolve_for_swap() |
| 回测 | PnL 模拟、模拟交易、参数扫描 |
| CLI 命令 | strat new/run/demo/backtest、gateway、agent |
| 链与协议 | 12 条链、10 个协议及枚举名称 |
| 常见模式 | 再平衡、告警、清仓、IntentSequence |
| 故障排除 | 常见错误及修复方法 |

## 直接阅读技能

查看或搜索原始技能内容：

```bash
# 打印文件路径
almanak docs agent-skill

# 输出完整内容
almanak docs agent-skill --dump

# 搜索特定主题
almanak docs agent-skill --dump | grep "Intent.swap"
```
