![Almanak](assets/logo-dark.svg){ .hero-logo }

<div style="text-align: center">

<p><strong>Framework DeFi de production pour les Quants</strong></p>

<p>
  <a href="https://pypi.org/project/almanak/"><img src="https://img.shields.io/pypi/v/almanak?style=flat-square&color=blue" alt="PyPI version"></a>
  <a href="https://pypi.org/project/almanak/"><img src="https://img.shields.io/pypi/pyversions/almanak?style=flat-square" alt="Python 3.12+"></a>
  <a href="https://github.com/almanak-co/almanak-sdk/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-green?style=flat-square" alt="License: Apache-2.0"></a>
  <a href="https://discord.gg/yuCMvQv3rN"><img src="https://img.shields.io/badge/Discord-join-5865F2?style=flat-square&logo=discord&logoColor=white" alt="Discord"></a>
  <a href="https://x.com/almanak"><img src="https://img.shields.io/badge/Twitter-follow-1DA1F2?style=flat-square&logo=x&logoColor=white" alt="Twitter"></a>
</p>

<p>
  <a href="/">English</a> |
  <a href="/zh/">中文</a> |
  <a href="/fr/">Français</a> |
  <a href="/es/">Español</a>
</p>

</div>

---

Le SDK Almanak fournit un framework complet pour le développement, le test et le déploiement d'agents DeFi autonomes. Construit sur une architecture basée sur les intentions, les stratégies sont exprimées sous forme d'intentions de haut niveau avec un minimum de code.

## Fonctionnalités

- **Architecture basée sur les intentions** - Exprimez votre logique de trading sous forme d'intentions de haut niveau (Swap, LP, Borrow, etc.). Le framework gère la compilation et l'exécution.
- **Gestion d'état à trois niveaux** - Persistance automatique avec les niveaux HOT/WARM/COLD pour la fiabilité.
- **Backtesting complet** - Simulation PnL, paper trading sur des forks Anvil et balayage de paramètres.
- **Support multi-chaînes** - Ethereum, Arbitrum, Optimism, Base, Avalanche, Polygon, BSC, Sonic, Plasma, Blast, Mantle, Berachain, et plus encore.
- **Intégration de protocoles** - Uniswap V3, Aave V3, Morpho Blue, GMX V2, Pendle, Polymarket, Kraken, et plus encore.
- **Conception non-custodiale** - Contrôle total de vos fonds via des comptes intelligents Safe avec génération automatique du manifeste de permissions Zodiac Roles.
- **Prêt pour la production** - Alertes intégrées, détection de blocage, gestion d'urgence et déploiements canari.

## Installation

```bash
pipx install almanak
```

Le test sur fork Anvil (ci-dessous) nécessite [Foundry](https://book.getfoundry.sh/getting-started/installation) :

```bash
curl -L https://foundry.paradigm.xyz | bash
foundryup
```

## Démarrage rapide

```bash
# Créer une nouvelle stratégie (crée un projet Python autonome avec pyproject.toml, .venv/, uv.lock)
almanak strat new

# L'exécuter sur un fork Anvil local -- pas besoin de portefeuille ni de clés API
cd my_strategy
almanak strat run --network anvil --once
```

Chaque stratégie créée est un projet Python autonome avec son propre `pyproject.toml`, `.venv/` et `uv.lock`. Les mêmes fichiers gèrent à la fois le développement local et le build Docker cloud de la plateforme.

Le test sur fork Anvil est le point de départ recommandé. Le SDK démarre automatiquement un fork local, utilise un portefeuille pré-financé par défaut, et exécute votre stratégie sans aucune configuration. Consultez [Premiers pas](getting-started.md) pour le guide complet.

## Écrire une stratégie

Les stratégies implémentent la méthode `decide()`, qui reçoit un `MarketSnapshot` et retourne un `Intent` (ou `None` pour passer le cycle) :

```python
from decimal import Decimal
from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot

class MyStrategy(IntentStrategy):
    """Une stratégie simple de retour à la moyenne."""

    def decide(self, market: MarketSnapshot) -> Intent | None:
        eth_price = market.price("ETH")
        usdc = market.balance("USDC")

        if eth_price < Decimal("2000") and usdc.balance_usd > Decimal("500"):
            return Intent.swap(
                from_token="USDC",
                to_token="ETH",
                amount_usd=Decimal("500"),
            )
        return Intent.hold(reason="En attente de meilleures conditions")
```

## Architecture

```text
almanak/
  framework/           # Framework de stratégie V2
    strategies/        # Classe de base IntentStrategy
    intents/           # Vocabulaire d'intentions et compilateur
    state/             # Gestion d'état à trois niveaux
    execution/         # Orchestration des transactions
    backtesting/       # PnL, paper trading, balayage de paramètres
    connectors/        # Adaptateurs de protocoles
    data/              # Oracles de prix, indicateurs
    alerting/          # Notifications Slack/Telegram
    services/          # Détection de blocage, gestion d'urgence
  gateway/             # Sidecar gRPC de la passerelle
  transaction_builder/ # Construction de transactions bas niveau
  core/                # Énumérations, modèles, utilitaires
  cli/                 # Interface en ligne de commande
```

Toutes les stratégies s'exécutent à travers une **architecture passerelle exclusive** pour la sécurité. Le sidecar passerelle détient tous les secrets et expose une API gRPC contrôlée. Les conteneurs de stratégie n'ont pas de secrets et pas d'accès direct à Internet.

## Retours et demandes de fonctionnalités

Vous avez une idée, trouvé un bug ou souhaitez demander une fonctionnalité ? Rendez-vous sur notre [Discord](https://discord.gg/yuCMvQv3rN) et publiez dans le canal approprié. Nous surveillons activement les retours et les utilisons pour façonner la feuille de route du SDK.

## Prochaines étapes

- [Premiers pas](getting-started.md) - Installation et guide de votre première stratégie
- [Référence CLI](cli/almanak.md) - Toutes les commandes CLI
- [Référence API](api/index.md) - Documentation complète de l'API Python
- [Passerelle](gateway/api-reference.md) - API gRPC de la passerelle
