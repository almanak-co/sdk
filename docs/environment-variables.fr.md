# Variables d'environnement

Toutes les stratégies s'exécutent à travers le **sidecar passerelle** (démarré automatiquement par `almanak strat run`). La passerelle détient les secrets, fournit l'accès RPC et exécute les transactions.

Créez un fichier `.env` dans le répertoire de votre stratégie avec les variables ci-dessous.

---

## Obligatoires

Ces variables doivent être définies avant d'exécuter toute stratégie.

| Variable | Description | Exemple |
|----------|-------------|---------|
| `ALMANAK_PRIVATE_KEY` | Clé privée du portefeuille pour signer les transactions et dériver l'adresse du portefeuille | `0x4c0883a6...` |

### Accès RPC (recommandé ; RPCs publics gratuits utilisés si non défini)

| Variable | Priorité | Description | Exemple |
|----------|----------|-------------|---------|
| `ALMANAK_{CHAIN}_RPC_URL` | 1 (plus haute) | URL RPC par chaîne avec préfixe ALMANAK | `https://arb-mainnet.infura.io/v3/KEY` |
| `{CHAIN}_RPC_URL` | 2 | URL RPC par chaîne (ex : `ARBITRUM_RPC_URL`) | `https://arb-mainnet.infura.io/v3/KEY` |
| `ALMANAK_RPC_URL` | 3 | URL RPC générique pour toutes les chaînes | `https://your-rpc.com/v1/KEY` |
| `RPC_URL` | 4 | URL RPC générique simple | `https://your-rpc.com/v1/KEY` |
| `ALCHEMY_API_KEY` | 5 (secours) | Clé API Alchemy -- URLs construites automatiquement par chaîne | `abc123def456` |
| `TENDERLY_API_KEY_{CHAIN}` | 6 (secours) | Clé API Tenderly par chaîne (ex : `TENDERLY_API_KEY_ARBITRUM`) | `abc123...` |

N'importe quel fournisseur fonctionne : Infura, QuickNode, auto-hébergé, Alchemy, etc. `ALCHEMY_API_KEY` est un secours optionnel qui construit automatiquement les URLs pour toutes les chaînes supportées. Si rien n'est défini, la passerelle utilise les RPCs publics gratuits (limités en débit, best-effort).

!!! warning
    Ne commitez jamais de clés privées. Utilisez un portefeuille de test dédié pour le développement.

**Note :** La passerelle accepte aussi `ALMANAK_GATEWAY_PRIVATE_KEY` (avec son propre préfixe). Si définie, elle est prioritaire. Sinon, la passerelle utilise `ALMANAK_PRIVATE_KEY` -- vous n'avez besoin que d'une seule variable.

---

## Clés API optionnelles

Définissez-les selon les protocoles et fonctionnalités utilisés par votre stratégie.

| Variable | Quand c'est nécessaire | Obtenir une clé |
|----------|-------------|-----------|
| `ENSO_API_KEY` | Routage de swaps via l'agrégateur Enso Finance | [enso.finance](https://enso.finance/) |
| `COINGECKO_API_KEY` | Améliore les limites de débit pour les données de prix (fonctionne sans clé) | [coingecko.com/en/api](https://www.coingecko.com/en/api) |
| `ALMANAK_API_KEY` | Fonctionnalités de la plateforme : `strat push`, `strat pull`, déploiement | [app.almanak.co](https://app.almanak.co/) |
| `THEGRAPH_API_KEY` | Backtesting avec données de subgraphs (volumes DEX, APYs de prêt) | [thegraph.com/studio](https://thegraph.com/studio/) |

---

## Spécifiques aux protocoles

Nécessaires uniquement si votre stratégie utilise ces protocoles spécifiques.

### Kraken

| Variable | Description |
|----------|-------------|
| `KRAKEN_API_KEY` | Clé API Kraken ([obtenir des identifiants](https://www.kraken.com/u/security/api)) |
| `KRAKEN_API_SECRET` | Secret API Kraken |

### Polymarket

| Variable | Description |
|----------|-------------|
| `POLYMARKET_WALLET_ADDRESS` | Adresse du portefeuille Polymarket |
| `POLYMARKET_PRIVATE_KEY` | Clé de signature Polymarket |
| `POLYMARKET_API_KEY` | Clé API CLOB |
| `POLYMARKET_SECRET` | Secret HMAC |
| `POLYMARKET_PASSPHRASE` | Phrase secrète API |

### Pendle

| Variable | Description |
|----------|-------------|
| `ALMANAK_GATEWAY_PENDLE_API_KEY` | Clé API du protocole Pendle |

---

## Portefeuille Safe

Pour les stratégies qui s'exécutent via un multisig Gnosis Safe.

| Variable | Description |
|----------|-------------|
| `ALMANAK_GATEWAY_SAFE_ADDRESS` | Adresse du portefeuille Safe |
| `ALMANAK_GATEWAY_SAFE_MODE` | `direct` (Anvil/seuil-1) ou `zodiac` (production) |
| `ALMANAK_GATEWAY_ZODIAC_ROLES_ADDRESS` | Adresse du module Zodiac Roles (mode zodiac) |
| `ALMANAK_GATEWAY_SIGNER_SERVICE_URL` | URL du service de signature distant (mode zodiac) |
| `ALMANAK_GATEWAY_SIGNER_SERVICE_JWT` | JWT du signataire distant (mode zodiac) |

---

## Backtesting

### URLs RPC d'archive

Nécessaires pour les données historiques on-chain (prix Chainlink, calculs TWAP). Les nœuds RPC standard ne supportent pas les requêtes d'état historique. Utilisez des fournisseurs compatible archive comme Alchemy (payant), QuickNode ou Infura.

Modèle : `ARCHIVE_RPC_URL_{CHAIN}` (ex : `ARCHIVE_RPC_URL_ARBITRUM`, `ARCHIVE_RPC_URL_ETHEREUM`, `ARCHIVE_RPC_URL_BASE`, `ARCHIVE_RPC_URL_OPTIMISM`, `ARCHIVE_RPC_URL_POLYGON`, `ARCHIVE_RPC_URL_AVALANCHE`)

### Clés API d'explorateurs de blocs

Optionnelles, pour les données historiques de prix du gas. Modèle : `{EXPLORER}_API_KEY`

| Variable | Explorateur |
|----------|----------|
| `ETHERSCAN_API_KEY` | [etherscan.io](https://etherscan.io/apis) |
| `ARBISCAN_API_KEY` | [arbiscan.io](https://arbiscan.io/apis) |
| `BASESCAN_API_KEY` | [basescan.org](https://basescan.org/apis) |
| `OPTIMISTIC_ETHERSCAN_API_KEY` | [optimistic.etherscan.io](https://optimistic.etherscan.io/apis) |
| `POLYGONSCAN_API_KEY` | [polygonscan.com](https://polygonscan.com/apis) |
| `SNOWTRACE_API_KEY` | [snowtrace.io](https://snowtrace.io/apis) |
| `BSCSCAN_API_KEY` | [bscscan.com](https://bscscan.com/apis) |

---

## `.env` de démarrage rapide

```bash
# Obligatoire
ALMANAK_PRIVATE_KEY=0xYOUR_PRIVATE_KEY

# Accès RPC (choisissez un)
RPC_URL=https://your-rpc-provider.com/v1/your-key
# ALCHEMY_API_KEY=your_alchemy_key  # alternative : construit automatiquement les URLs par chaîne

# Recommandé
ENSO_API_KEY=your_enso_key
COINGECKO_API_KEY=your_coingecko_key
```

Tous les autres paramètres de la passerelle et du framework ont des valeurs par défaut raisonnables et n'ont pas besoin d'être définis. Consultez [`.env.example`](https://github.com/almanak-co/almanak-sdk/blob/main/.env.example) pour la liste complète des options avancées.
