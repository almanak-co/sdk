# Security policy

## Reporting a vulnerability

If you find a security vulnerability in the Almanak SDK, report it privately.
Do not open a public GitHub issue.

Email: **security@almanak.co**

Include as much of the following as you can:

- A description of the vulnerability
- Steps to reproduce, or a proof of concept
- Affected versions and components
- Your assessment of the impact

## Response timeline

| Stage | Timeframe |
|---|---|
| Acknowledgment | Within 2 business days |
| Initial assessment | Within 5 business days |
| Fix for critical issues | Within 30 days |
| Public disclosure | After a fix is released and users have had time to upgrade |

## Scope

In scope:

- The `almanak` PyPI package: runtime, connectors, intents and backtesting
- The gateway services: execution gateway, historical gateway and read gateway
- Strategy isolation: the sandbox that runs author code
- Generated Safe and Zodiac Roles permissions and their verification
- The `almanak` command-line tool
- The documentation site (docs.almanak.co)
- Dependencies and binaries shipped inside the package

Out of scope:

- Your own strategy code and configuration
- Third-party DeFi protocols such as Uniswap or Aave
- Private keys, provider keys and operator files you manage
- RPC providers and external API services
- Social engineering

## Supported versions

| Version | Supported |
|---|---|
| 3.x | Yes |
| 2.x and earlier | No |

## Disclosure

We follow coordinated disclosure. We will:

1. Confirm the vulnerability and determine the affected versions
2. Develop and test a fix
3. Release a patched version
4. Credit the reporter, unless they ask to remain anonymous
5. Publish a security advisory on GitHub
