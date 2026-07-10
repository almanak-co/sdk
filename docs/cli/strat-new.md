
# strat new

Create a new strategy from template.

    
    This scaffolds a strategy project with:
    - strategy.py: Main strategy with decide() method
    - config.json: Runtime configuration
    - tests/: Test scaffolding

    
    Templates:
      blank          Minimal starting point for custom implementations
      ta_swap        Technical analysis swap (RSI, Bollinger, or combined)
      dynamic_lp     Price-based LP range management
      lending_loop   Supply/borrow leverage loop (Aave V3)
      basis_trade    Spot+perp funding rate arbitrage (GMX V2)
      vault_yield    Vault deposit/redeem yield farming (MetaMorpho)
      copy_trader    Copy trading from leader wallets
      perps          Perpetual futures with TP/SL (GMX V2)
      multi_step     Atomic multi-step via IntentSequence
      staking        Liquid staking (Lido)
    

## Usage

```
Usage: almanak strat new [OPTIONS]
```

## Arguments


## Options

* `name`:
    * Type: STRING
    * Default: `None`
    * Usage: `--name
-n`
    Name for the new strategy. If not provided, will prompt interactively.


* `working_dir`:
    * Type: `Path`
    * Default: `None`
    * Usage: `--working-dir
-o`
    Output directory for the new strategy. Defaults to current directory.


* `template`:
    * Type: Choice(['blank', 'ta_swap', 'dynamic_lp', 'lending_loop', 'basis_trade', 'vault_yield', 'copy_trader', 'perps', 'multi_step', 'staking'])
    * Default: `blank`
    * Usage: `--template
-t`
    Strategy template to use (default: blank)


* `chain`:
    * Type: <almanak.framework.cli.chain_params.ChainChoice object at 0x7f02d5fac2c0>
    * Default: `arbitrum`
    * Usage: `--chain
-c`
    Target blockchain network (default: arbitrum)


* `protocol`:
    * Type: STRING
    * Default: `None`
    * Usage: `--protocol
-p`
    Protocol slug rendered into the scaffold (decorator metadata and the template's config protocol defaults), e.g. aerodrome_slipstream, morpho_blue, hyperliquid. Defaults to the template's canonical protocol.


* `help`:
    * Type: BOOL
    * Default: `False`
    * Usage: `--help`
    Show this message and exit.


## CLI Help

```
Usage: almanak strat new [OPTIONS]

  Create a new strategy from template.

  This scaffolds a strategy project with:
  - strategy.py: Main strategy with decide() method
  - config.json: Runtime configuration
  - tests/: Test scaffolding

  Templates:
    blank          Minimal starting point for custom implementations
    ta_swap        Technical analysis swap (RSI, Bollinger, or combined)
    dynamic_lp     Price-based LP range management
    lending_loop   Supply/borrow leverage loop (Aave V3)
    basis_trade    Spot+perp funding rate arbitrage (GMX V2)
    vault_yield    Vault deposit/redeem yield farming (MetaMorpho)
    copy_trader    Copy trading from leader wallets
    perps          Perpetual futures with TP/SL (GMX V2)
    multi_step     Atomic multi-step via IntentSequence
    staking        Liquid staking (Lido)

Options:
  -n, --name TEXT                 Name for the new strategy. If not provided,
                                  will prompt interactively.
  -o, --working-dir PATH          Output directory for the new strategy.
                                  Defaults to current directory.
  -t, --template [blank|ta_swap|dynamic_lp|lending_loop|basis_trade|vault_yield|copy_trader|perps|multi_step|staking]
                                  Strategy template to use (default: blank)
  -c, --chain [arbitrum|avalanche|base|berachain|blast|bsc|ethereum|hyperevm|linea|mantle|monad|optimism|plasma|polygon|robinhood|solana|sonic|xlayer|zerog]
                                  Target blockchain network (default:
                                  arbitrum)
  -p, --protocol TEXT             Protocol slug rendered into the scaffold
                                  (decorator metadata and the template's
                                  config protocol defaults), e.g.
                                  aerodrome_slipstream, morpho_blue,
                                  hyperliquid. Defaults to the template's
                                  canonical protocol.
  --help                          Show this message and exit.
```

