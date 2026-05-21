# Pool-analytics test fixtures (VIB-4727)

Recorded payload shapes used by the UAT-card D1/D2/D3 test pack
(`docs/internal/uat-cards/VIB-4727.md`). **No D1/D2/D3 test depends on a
live external API**; tests patch the gateway servicer's provider seams
(`_query_defillama_pools`, `_query_geckoterminal_pool`) with these
fixtures.

| File | Provider | Use |
|---|---|---|
| `defillama_arbitrum_univ3.json` | DefiLlama | D1.S1, D1.S2, D2.M1, D2.M4 — Antonis pool happy path |
| `defillama_ethereum_univ3.json` | DefiLlama | D2.M1 — Ethereum chain-mapping branch |
| `defillama_wrong_chain_only.json` | DefiLlama | D3.F6 — only contains pools for a different chain, so the on-chain match fails deterministically |
| `geckoterminal_arbitrum_univ3.json` | GeckoTerminal | D2.M2 — fallback after DefiLlama raises |
| `geckoterminal_ethereum_univ3.json` | GeckoTerminal | D2.M1 — Ethereum branch fallback |

Schema is intentionally trimmed to the fields the servicer reads
(`_parse_llama_pool`, `_parse_gt_pool`). Adding fields here is fine;
removing one a parser reads is a breaking change.
