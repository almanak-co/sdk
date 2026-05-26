
# almanak ax

Execute DeFi actions directly from the command line.

    One-shot commands for swaps, balance checks, price queries, and more.
    No strategy files needed -- just run ``almanak ax <action>``.

    Auto-starts a gateway if none is running. Use --network to control
    mainnet vs Anvil, or connect to an existing gateway via --gateway-host/port.

    
    Network mode (Anvil vs Mainnet):
      --network anvil    Local Anvil fork (free, safe testing)
      --network mainnet  Real transactions (default)
      (omit)             Connects to existing gateway, or starts mainnet

    
    Safety model (TTY detection):
      - Interactive terminal: simulate -> preview -> confirm (unless --yes)
      - Non-interactive (piped/scripted): fails unless --yes is passed
      - --dry-run: simulate only, never submit

    
    Modes:
      Structured (default):
        almanak ax swap USDC ETH 100
        almanak ax balance USDC
        almanak ax price ETH

      Natural language (--natural / -n):
        almanak ax -n "swap 5 USDC to ETH on base"
        almanak ax -n "what's the price of ETH?"
        almanak ax -n "check my USDC balance"

    
    Examples:
        almanak ax balance USDC                    # Check USDC balance
        almanak ax price ETH                       # Get ETH price
        almanak ax swap USDC ETH 100 --dry-run     # Simulate a swap
        almanak ax swap USDC ETH 100               # Execute after confirmation
        almanak ax lp-info 123456                  # View LP position details
        almanak ax lp-close 123456                 # Close an LP position
        almanak ax pool WBTC WETH                  # Pool state (price, TVL, etc.)
        almanak ax bridge USDC 100 --from-chain arbitrum --to-chain base  # Bridge tokens
        almanak ax -n "swap 5 USDC to WETH on base"  # Natural language mode
        almanak ax --network anvil swap USDC ETH 100  # Auto-start Anvil gateway
    

## Usage

```
Usage: almanak ax [OPTIONS] COMMAND [ARGS]...
```

## Arguments


## Options

* `gateway_host`:
    * Type: STRING
    * Default: `127.0.0.1`
    * Usage: `--gateway-host`
    Gateway gRPC host.


* `gateway_port`:
    * Type: INT
    * Default: `50051`
    * Usage: `--gateway-port`
    Gateway gRPC port.


* `chain`:
    * Type: STRING
    * Default: `arbitrum`
    * Usage: `--chain
-c`
    Default chain (default: arbitrum).


* `wallet`:
    * Type: STRING
    * Default: `None`
    * Usage: `--wallet
-w`
    Wallet address. Auto-derived from ALMANAK_PRIVATE_KEY if not set.


* `max_trade_usd`:
    * Type: FLOAT
    * Default: `10000`
    * Usage: `--max-trade-usd`
    Max single trade size in USD (default: 10000).


* `dry_run`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--dry-run`
    Simulate only, do not submit transactions.


* `json_output`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--json`
    Output results as JSON instead of human-readable tables.


* `yes`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--yes
-y`
    Skip confirmation prompts (for non-interactive / AI agent use).


* `natural`:
    * Type: STRING
    * Default: `None`
    * Usage: `--natural
-n`
    Natural language mode: describe what you want in plain English (e.g. -n "swap 5 USDC to ETH").


* `network`:
    * Type: Choice(['mainnet', 'anvil'])
    * Default: `None`
    * Usage: `--network`
    Network mode. Auto-starts a gateway if none is running (default: mainnet).


* `help`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--help`
    Show this message and exit.


## CLI Help

```
Usage: almanak ax [OPTIONS] COMMAND [ARGS]...

  Execute DeFi actions directly from the command line.

  One-shot commands for swaps, balance checks, price queries, and more. No
  strategy files needed -- just run ``almanak ax <action>``.

  Auto-starts a gateway if none is running. Use --network to control mainnet
  vs Anvil, or connect to an existing gateway via --gateway-host/port.

  Network mode (Anvil vs Mainnet):
    --network anvil    Local Anvil fork (free, safe testing)
    --network mainnet  Real transactions (default)
    (omit)             Connects to existing gateway, or starts mainnet

  Safety model (TTY detection):
    - Interactive terminal: simulate -> preview -> confirm (unless --yes)
    - Non-interactive (piped/scripted): fails unless --yes is passed
    - --dry-run: simulate only, never submit

  Modes:
    Structured (default):
      almanak ax swap USDC ETH 100
      almanak ax balance USDC
      almanak ax price ETH

    Natural language (--natural / -n):     almanak ax -n "swap 5 USDC to ETH
    on base"     almanak ax -n "what's the price of ETH?"     almanak ax -n
    "check my USDC balance"

  Examples:
      almanak ax balance USDC                    # Check USDC balance
      almanak ax price ETH                       # Get ETH price
      almanak ax swap USDC ETH 100 --dry-run     # Simulate a swap
      almanak ax swap USDC ETH 100               # Execute after confirmation
      almanak ax lp-info 123456                  # View LP position details
      almanak ax lp-close 123456                 # Close an LP position
      almanak ax pool WBTC WETH                  # Pool state (price, TVL, etc.)
      almanak ax bridge USDC 100 --from-chain arbitrum --to-chain base  # Bridge tokens
      almanak ax -n "swap 5 USDC to WETH on base"  # Natural language mode
      almanak ax --network anvil swap USDC ETH 100  # Auto-start Anvil gateway

Options:
  --gateway-host TEXT        Gateway gRPC host.  [env var:
                             ALMANAK_GATEWAY_HOST, GATEWAY_HOST; default:
                             127.0.0.1]
  --gateway-port INTEGER     Gateway gRPC port.  [env var:
                             ALMANAK_GATEWAY_PORT, GATEWAY_PORT; default:
                             50051]
  -c, --chain TEXT           Default chain (default: arbitrum).
  -w, --wallet TEXT          Wallet address. Auto-derived from
                             ALMANAK_PRIVATE_KEY if not set.
  --max-trade-usd FLOAT      Max single trade size in USD (default: 10000).
  --dry-run                  Simulate only, do not submit transactions.
  --json                     Output results as JSON instead of human-readable
                             tables.
  -y, --yes                  Skip confirmation prompts (for non-interactive /
                             AI agent use).
  -n, --natural TEXT         Natural language mode: describe what you want in
                             plain English (e.g. -n "swap 5 USDC to ETH").
  --network [mainnet|anvil]  Network mode. Auto-starts a gateway if none is
                             running (default: mainnet).
  --help                     Show this message and exit.

Commands:
  balance           Get the balance of a token in your wallet.
  bridge            Bridge tokens from one chain to another.
  bundle-clear      Remove cached compiled bundles from disk.
  bundle-list       List compiled intent bundles cached on disk.
  lending-borrow    Borrow tokens from a lending protocol.
  lending-list      List a wallet's lending positions (account totals +...
  lending-repay     Repay a lending position.
  lending-supply    Supply tokens to a lending protocol.
  lending-withdraw  Withdraw supplied tokens from a lending protocol.
  lp-close          Close (fully withdraw) a liquidity position.
  lp-info           Get details about an existing LP position.
  lp-list           List all LP positions owned by your wallet on a chain.
  pool              Get details about a liquidity pool.
  portfolio         Aggregate snapshot: native + ERC20 balances, LP...
  positions         Position reconciliation commands (T24 / VIB-4210).
  price             Get the current USD price of a token.
  resolve           Resolve a token symbol or address to its metadata on...
  run               Run any tool from the catalog by name.
  swap              Swap tokens on a DEX.
  tools             List all available tools in the catalog.
  unwrap            Unwrap wrapped native tokens (e.g.
```

