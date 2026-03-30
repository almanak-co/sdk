# Premiers pas

Ce guide vous accompagne dans l'installation du SDK Almanak, la création de votre première stratégie et son exécution locale sur un fork Anvil -- sans portefeuille ni clés API.

## Prérequis

- **Python 3.12+**
- **uv** (gestionnaire de paquets Python) :

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

- **Foundry** (fournit Anvil pour les tests sur fork local) :

```bash
curl -L https://foundry.paradigm.xyz | bash
foundryup
```

## Installation

```bash
pipx install almanak
```

Ou avec [uv](https://docs.astral.sh/uv/) :

```bash
uv tool install almanak
```

Cela installe le CLI `almanak` globalement. Chaque stratégie créée a aussi `almanak` comme dépendance locale dans son propre `.venv/` -- ce modèle à double installation est standard (comme CrewAI, Dagster, etc.).

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

Suivez les invites interactives pour choisir un modèle, une chaîne et un nom. Cela crée un **projet Python autonome** contenant :

- `strategy.py` - Votre implémentation de stratégie avec la méthode `decide()`
- `config.json` - Configuration de la chaîne, du protocole et des paramètres
- `pyproject.toml` - Dépendances et métadonnées `[tool.almanak]`
- `uv.lock` - Dépendances verrouillées (créé par `uv sync`)
- `.venv/` - Environnement virtuel par stratégie (créé par `uv sync`)
- `.env` - Variables d'environnement (remplissez vos clés plus tard)
- `.gitignore` - Règles d'exclusion Git
- `.python-version` - Version Python épinglée (3.12)
- `__init__.py` - Exports du package
- `tests/` - Échafaudage de tests
- `AGENTS.md` - Guide pour agents IA

Le scaffold exécute `uv sync` automatiquement pour installer les dépendances. Pour ajouter des paquets supplémentaires :

```bash
uv add pandas-ta          # Met à jour pyproject.toml + uv.lock + .venv/
uv run pytest tests/ -v   # Exécuter les tests dans le venv de la stratégie
```

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

!!! info "Avant de passer en production"
    - Exécutez toujours `--dry-run --once` avant votre première exécution en production pour vérifier la
      compilation des intents sans soumettre de transactions.
    - Si les swaps échouent avec "Too little received", passez de `amount_usd=` à `amount=` (unités de
      tokens). `amount_usd=` repose sur l'oracle de prix de la passerelle pour la conversion USD-token,
      qui peut diverger du prix du DEX.
    - Commencez avec de petits montants, surveillez les premières itérations, et notez votre ID
      d'instance pour reprendre avec `--id`.

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
| `WrapNativeIntent` | Envelopper des tokens natifs (ex: ETH vers WETH) |
| `UnwrapNativeIntent` | Désenvelopper des tokens natifs (ex: WETH vers ETH) |
| `Intent.bridge()` | Transférer des tokens entre chaînes (methode factory retournant un intent composite) |
| `Intent.ensure_balance()` | Assurer un solde minimum de tokens sur la chaîne cible (methode factory se résolvant en bridge ou hold) |

## Persistance de l'état (requis pour les stratégies avec état)

Le framework persiste automatiquement les métadonnées du runner (compteurs d'itérations, compteurs d'erreurs) après chaque itération. Cependant, **l'état spécifique à la stratégie** -- IDs de positions, compteurs de trades, suivi de phase, timers de cooldown -- n'est sauvegardé que si vous implémentez deux hooks :

```python
from typing import Any
from decimal import Decimal

class MyStrategy(IntentStrategy):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._position_id: int | None = None
        self._trades_today: int = 0

    def get_persistent_state(self) -> dict[str, Any]:
        """Return state to save. Called after each iteration."""
        return {
            "position_id": self._position_id,
            "trades_today": self._trades_today,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Restore state on startup. Called when resuming a run."""
        self._position_id = state.get("position_id")
        self._trades_today = state.get("trades_today", 0)
```

Sans ces hooks, votre stratégie perdra tout son état interne au redémarrage. C'est particulièrement dangereux pour les stratégies LP où la perte du `position_id` signifie que la stratégie ne peut plus fermer ses propres positions.

!!! warning "Ce qui est perdu sans persistance"
    Si vous stockez l'état dans des variables d'instance (par ex. `self._position_id`) mais n'implémentez pas `get_persistent_state()` et `load_persistent_state()`, cet état est perdu quand le processus s'arrête. Au redémarrage, votre stratégie repart de zéro sans mémoire des positions ouvertes, des trades effectués ou de la phase interne.

!!! tip "Conseils"
    - Utilisez `.get()` avec des valeurs par défaut dans `load_persistent_state()` de manière défensive pour que les anciens dicts d'état ne plantent pas sur des clés manquantes.
    - Stockez les valeurs `Decimal` sous forme de chaînes (`str(amount)`) et parsez-les au chargement (`Decimal(state["amount"])`) pour un aller-retour JSON sûr.
    - Le callback `on_intent_executed()` est l'endroit naturel pour mettre à jour l'état après un trade (par ex. stocker un nouvel ID de position), et `get_persistent_state()` le récupère ensuite pour la sauvegarde.

## Démontage de la stratégie (requis)

Chaque stratégie doit implémenter le démontage pour que les opérateurs puissent fermer les positions en toute sécurité. Sans démontage, les demandes de fermeture sont silencieusement ignorées et les positions restent ouvertes. Les templates `almanak strat new` incluent des stubs -- remplissez-les au fur et à mesure que vous construisez votre stratégie.

```python
class MyStrategy(IntentStrategy):
    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Query on-chain state and return open positions."""
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary
        # ... return TeardownPositionSummary with your positions

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Return ordered intents to unwind all positions."""
        from almanak.framework.teardown import TeardownMode
        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.005")
        return [Intent.swap(from_token="WETH", to_token="USDC", amount="all", max_slippage=max_slippage)]
```

Si votre stratégie détient plusieurs types de positions, fermez-les dans l'ordre : **perpétuels -> emprunts -> fournitures -> LPs -> tokens**. Consultez le [CLI de démontage](cli/strat-teardown.md) pour savoir comment les opérateurs déclenchent le démontage.

## Génération du manifeste de permissions (portefeuilles Safe)

Lors du déploiement d'une stratégie via un portefeuille Safe avec des restrictions Zodiac Roles, l'agent a besoin d'un ensemble explicite de permissions de contrats. Le SDK peut générer ce manifeste automatiquement en inspectant quels contrats et sélecteurs de fonctions les intents de votre stratégie compilent :

```bash
# Depuis le répertoire de la stratégie
almanak strat permissions

# Répertoire explicite
almanak strat permissions -d strategies/demo/uniswap_rsi

# Spécifier la chaîne
almanak strat permissions --chain base

# Écrire dans un fichier
almanak strat permissions -o permissions.json
```

La commande lit `supported_protocols` et `intent_types` depuis votre décorateur `@almanak_strategy`, compile des intents synthétiques via le vrai compilateur, et extrait l'ensemble minimal d'adresses de contrats et de sélecteurs de fonctions nécessaires. La sortie est un manifeste JSON applicable à un module Zodiac Roles. Si la stratégie supporte plusieurs chaînes, la sortie est un tableau JSON avec un manifeste par chaîne ; utilisez `--chain` pour générer pour une seule chaîne.

!!! note "Uniquement pour les déploiements Safe/Zodiac"
    Les manifestes de permissions ne sont nécessaires que lors de l'exécution via un portefeuille Safe avec Zodiac Roles. Pour les tests Anvil locaux ou l'exécution par clé directe, aucune permission n'est requise.

!!! note "CLI de backtest"
    Contrairement à `almanak strat run` qui découvre automatiquement la stratégie depuis le répertoire courant,
    les commandes de backtest nécessitent un nom de stratégie explicite : `almanak strat backtest pnl -s my_strategy`.
    Utilisez `--list-strategies` pour voir les stratégies disponibles.

## Prochaines étapes

- [Variables d'environnement](environment-variables.md) - Toutes les options de configuration
- [Référence API](api/index.md) - Documentation complète de l'API Python
- [Référence CLI](cli/almanak.md) - Toutes les commandes CLI
- [API Passerelle](gateway/api-reference.md) - Services gRPC de la passerelle

## Vous voulez qu'un LLM prenne les décisions ?

Le SDK supporte aussi les **stratégies agentiques** où un LLM décide
de manière autonome quoi faire en utilisant les 29 outils intégrés d'Almanak. Au lieu
d'écrire la logique `decide()` en Python, vous écrivez un prompt système et laissez
le LLM raisonner sur les données de marché.

Cette approche nécessite **votre propre clé API LLM** (OpenAI, Anthropic ou tout
fournisseur compatible OpenAI).

| | Déterministe (ce guide) | Agentique |
|---|---|---|
| **Vous écrivez** | Méthode Python `decide()` | Prompt système + politique |
| **Décideur** | Votre code | LLM (GPT-4, Claude, etc.) |
| **Nécessite** | Juste le SDK | SDK + clé API LLM |
| **Idéal pour** | Règles connues, signaux quantitatifs | Raisonnement complexe, plans multi-étapes |

Les deux chemins partagent la même passerelle, les mêmes connecteurs et le même pipeline d'exécution.

**Pour commencer :** [Guide de trading agentique](agentic/index.md)
