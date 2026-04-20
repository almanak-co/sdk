# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in the Almanak SDK, please report it responsibly. **Do not open a public GitHub issue.**

Email: **security@almanak.co**

Include as much of the following as possible:

- Description of the vulnerability
- Steps to reproduce or proof-of-concept
- Affected versions and components
- Potential impact assessment

## Response Timeline

| Stage | Timeframe |
|-------|-----------|
| Acknowledgment | Within 2 business days |
| Initial assessment | Within 5 business days |
| Fix for critical issues | Within 30 days |
| Public disclosure | After fix is released and users have time to upgrade |

## Scope

The following are in scope for security reports:

- `almanak` PyPI package (SDK framework, connectors, intents, backtesting)
- Gateway (gRPC sidecar)
- CLI (`almanak` command)
- Documentation site (docs.almanak.co)
- Bundled dependencies shipped with the package

## Out of Scope

The following are **not** in scope:

- Deployed user strategies (your own code)
- Third-party DeFi protocols (Uniswap, Aave, etc.)
- User-provided configuration or private keys
- RPC providers and external API services
- Social engineering attacks

## Supported Versions

| Version | Supported |
|---------|-----------|
| 2.x     | Yes       |
| < 2.0   | No        |

## Disclosure Policy

We follow coordinated disclosure. We will:

1. Confirm the vulnerability and determine affected versions
2. Develop and test a fix
3. Release a patched version
4. Credit the reporter (unless anonymity is requested)
5. Publish a security advisory on GitHub

## Recognition

We appreciate security researchers who help keep Almanak and its users safe. Reporters will be credited in the security advisory unless they prefer to remain anonymous.
