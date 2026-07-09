# Pool-analytics test fixtures (VIB-4727)

Recorded payload shapes used by the UAT-card D1/D2/D3 test pack
(`docs/internal/uat-cards/VIB-4727.md`). **No D1/D2/D3 test depends on a
live external API**; tests patch the gateway servicer's provider seam
(`_query_coingecko_onchain_pool`) with these fixtures.

CoinGecko Onchain is the sole external pool-analytics lane. The legacy
DefiLlama fixtures were deleted with the structurally-dead matcher lane —
the DefiLlama catalog keys pools by opaque UUIDs, never by
address, so the matcher could never return data.

| File | Provider | Use |
|---|---|---|
| `geckoterminal_arbitrum_univ3.json` | CoinGecko Onchain | D1.S1, D2.M1, D2.M4 — Antonis pool happy path |
| `geckoterminal_ethereum_univ3.json` | CoinGecko Onchain | D2.M1 — Ethereum chain-mapping branch |

The file names keep the historical `geckoterminal_` prefix — CoinGecko
acquired GeckoTerminal and the Onchain API serves the same payload shape.

Schema is intentionally trimmed to the fields the servicer reads
(`_parse_coingecko_onchain_pool`). Adding fields here is fine; removing
one the parser reads is a breaking change.
