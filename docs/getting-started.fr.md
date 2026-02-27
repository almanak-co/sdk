# Premiers pas

Ce guide vous accompagne dans l'installation du SDK Almanak, la création de votre première stratégie et son exécution locale sur un fork Anvil -- sans portefeuille ni clés API.

## Prérequis

- **Python 3.11+**
- **Foundry** (fournit Anvil pour les tests sur fork local) :

```bash
curl -L https://foundry.paradigm.xyz | bash
foundryup
```

## Installation

```bash
pip install almanak
```

Ou avec [uv](https://docs.astral.sh/uv/) :

```bash
uv pip install almanak
```

**Vous utilisez un agent de codage IA ?** Enseignez-lui le SDK en une commande :

```bash
almanak agent install
```

Cela détecte automatiquement votre plateforme (Claude Code, Codex, Cursor, Copilot et [6 autres](agent-skills.md)) et installe la compétence de construction de stratégies.

## 1. Obtenir une stratégie

### Option A : Copier une démo fonctionnelle (recommandé pour les débutants)

```bash
almanak strat demo
```

Cela affiche un menu interactif de 13 stratégies de démonstration fonctionnelles. Choisissez-en une et elle est copiée dans votre répertoire courant, prête à être exécutée. Vous pouvez aussi ignorer le menu :

```bash
almanak strat demo --name uniswap_rsi
```

### Option B : Créer à partir d'un modèle

```bash
almanak strat new
```

Suivez les invites interactives pour choisir un modèle, une chaîne et un nom. Cela crée un répertoire de stratégie contenant :

- `strategy.py` - Votre implémentation de stratégie avec la méthode `decide()`
- `config.json` - Configuration de la chaîne, du protocole et des paramètres
- `.env` - Variables d'environnement (remplissez vos clés plus tard)
- `__init__.py` - Exports du package
- `tests/` - Échafaudage de tests

## 2. Exécuter sur un fork Anvil local

Le moyen le plus rapide de tester votre stratégie -- pas de clés de portefeuille, pas de fonds réels, aucun risque :

```bash
cd my_strategy
almanak strat run --network anvil --once
```

Cette commande effectue automatiquement :

1. **Démarre un fork Anvil** de la chaîne spécifiée dans votre `config.json` (les RPC publics gratuits sont utilisés par défaut)
2. **Utilise un portefeuille Anvil par défaut** -- pas besoin de `ALMANAK_PRIVATE_KEY`
3. **Démarre la passerelle** sidecar en arrière-plan
4. **Finance votre portefeuille** avec les tokens listés dans `anvil_funding` (voir ci-dessous)
5. **Exécute une itération** de la méthode `decide()` de votre stratégie

### Financement du portefeuille sur Anvil

Ajoutez un bloc `anvil_funding` à votre `config.json` pour financer automatiquement votre portefeuille au démarrage du fork :

```json
{
    "strategy_id": "my_strategy",
    "chain": "arbitrum",
    "anvil_funding": {
        "ETH": 10,
        "USDC": 10000,
        "WETH": 5
    }
}
```

Les tokens natifs (ETH, AVAX, etc.) sont financés via `anvil_setBalance`. Les tokens ERC-20 sont financés par manipulation des slots de stockage. Cela se produit automatiquement à chaque démarrage du fork.

### Meilleures performances RPC (optionnel)

Les RPC publics gratuits fonctionnent mais sont limités en débit. Pour un forking plus rapide, définissez une clé Alchemy dans votre `.env` :

```bash
ALCHEMY_API_KEY=your_alchemy_key
```

Cela construit automatiquement les URLs RPC pour toutes les chaînes supportées. N'importe quel fournisseur fonctionne -- consultez [Variables d'environnement](environment-variables.md) pour l'ordre de priorité complet.

## 3. Exécuter sur le réseau principal

!!! warning
    L'exécution sur le réseau principal utilise des **fonds réels**. Commencez avec de petits montants et utilisez un portefeuille dédié.

Pour exécuter sur des chaînes en production, vous avez besoin d'une clé privée de portefeuille dans votre `.env` :

```bash
# .env
ALMANAK_PRIVATE_KEY=0xYOUR_PRIVATE_KEY

# Accès RPC (choisissez un)
ALCHEMY_API_KEY=your_alchemy_key
# ou : RPC_URL=https://your-rpc-provider.com/v1/your-key
```

Puis exécutez sans le flag `--network anvil` :

```bash
almanak strat run --once
```

!!! tip
    Testez d'abord avec `--dry-run` pour simuler sans soumettre de transactions :

    ```bash
    almanak strat run --dry-run --once
    ```

Consultez [Variables d'environnement](environment-variables.md) pour la liste complète des options de configuration, y compris les clés API spécifiques aux protocoles.

## Structure d'une stratégie

Une stratégie implémente la méthode `decide()`, qui reçoit un `MarketSnapshot` et retourne un `Intent` :

```python
from decimal import Decimal
from almanak import IntentStrategy, Intent, MarketSnapshot

class MyStrategy(IntentStrategy):
    def decide(self, market: MarketSnapshot) -> Intent | None:
        price = market.price("ETH")
        balance = market.balance("USDC")

        if price < Decimal("2000") and balance.balance_usd > Decimal("500"):
            return Intent.swap(
                from_token="USDC",
                to_token="ETH",
                amount_usd=Decimal("500"),
            )
        return Intent.hold(reason="Pas d'opportunité")
```

## Intentions disponibles

| Intention | Description |
|--------|-------------|
| `SwapIntent` | Échanges de tokens sur les DEX |
| `HoldIntent` | Pas d'action, attendre le prochain cycle |
| `LPOpenIntent` | Ouvrir une position de liquidité |
| `LPCloseIntent` | Fermer une position de liquidité |
| `BorrowIntent` | Emprunter auprès de protocoles de prêt |
| `RepayIntent` | Rembourser les actifs empruntés |
| `SupplyIntent` | Fournir aux protocoles de prêt |
| `WithdrawIntent` | Retirer des protocoles de prêt |
| `StakeIntent` | Staker des tokens |
| `UnstakeIntent` | Unstaker des tokens |
| `PerpOpenIntent` | Ouvrir une position de perpétuels |
| `PerpCloseIntent` | Fermer une position de perpétuels |
| `FlashLoanIntent` | Opérations de prêt flash |
| `CollectFeesIntent` | Collecter les frais LP |
| `PredictionBuyIntent` | Acheter des parts de marché prédictif |
| `PredictionSellIntent` | Vendre des parts de marché prédictif |
| `PredictionRedeemIntent` | Racheter les gains de marché prédictif |
| `VaultDepositIntent` | Déposer dans un coffre |
| `VaultRedeemIntent` | Racheter depuis un coffre |

## Prochaines étapes

- [Variables d'environnement](environment-variables.md) - Toutes les options de configuration
- [Référence API](api/index.md) - Documentation complète de l'API Python
- [Référence CLI](cli/almanak.md) - Toutes les commandes CLI
- [API Passerelle](gateway/api-reference.md) - Services gRPC de la passerelle
