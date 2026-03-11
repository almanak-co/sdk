# Compétences d'agent

Le SDK Almanak est livré avec une **compétence d'agent** -- un fichier de connaissances structuré qui enseigne aux agents de codage IA comment construire des stratégies DeFi. Installez-la dans votre projet pour que votre assistant IA comprenne l'API IntentStrategy, le vocabulaire d'intentions, les méthodes de données de marché et les commandes CLI.

## Installation rapide

Le moyen le plus rapide pour commencer :

```bash
almanak agent install
```

Cela détecte automatiquement les outils de codage IA que vous utilisez (en vérifiant `.claude/`, `.cursor/`, `.github/`, etc.) et installe la compétence dans le répertoire natif de chaque plateforme.

## Instructions par plateforme

### Claude Code

Claude Code découvre les compétences depuis les sous-répertoires `.claude/skills/`.

```bash
almanak agent install -p claude
```

**Résultat :** `.claude/skills/almanak-strategy-builder/SKILL.md`

---

### OpenAI Codex

Codex découvre les compétences projet depuis les sous-répertoires `.codex/skills/`.

```bash
almanak agent install -p codex
```

**Résultat :** `.codex/skills/almanak-strategy-builder/SKILL.md`

---

### Cursor

Cursor lit les règles personnalisées depuis `.cursor/rules/` sous forme de fichiers `.mdc` avec un scope basé sur les globs.

```bash
almanak agent install -p cursor
```

Le fichier installé inclut un en-tête YAML qui active la compétence lors de l'édition des fichiers `strategy.py` ou `config.json` :

```yaml
---
globs:
  - "**/strategy.py"
  - "**/config.json"
---
```

**Résultat :** `.cursor/rules/almanak-strategy-builder.mdc`

---

### GitHub Copilot

Copilot lit les instructions scopées depuis `.github/instructions/` avec un en-tête `applyTo`.

```bash
almanak agent install -p copilot
```

Le fichier installé inclut un en-tête scopé aux fichiers de stratégie :

```yaml
---
applyTo: "**/strategy.py"
---
```

**Résultat :** `.github/instructions/almanak-strategy-builder.instructions.md`

---

### Windsurf

Windsurf lit les règles depuis `.windsurf/rules/`.

```bash
almanak agent install -p windsurf
```

**Résultat :** `.windsurf/rules/almanak-strategy-builder.md`

---

### Cline

Cline lit les règles depuis `.clinerules/`.

```bash
almanak agent install -p cline
```

**Résultat :** `.clinerules/almanak-strategy-builder.md`

---

### Roo Code

Roo Code lit les règles depuis `.roo/rules/`.

```bash
almanak agent install -p roo
```

**Résultat :** `.roo/rules/almanak-strategy-builder.md`

---

### Aider

Aider découvre les compétences depuis les sous-répertoires `.aider/skills/`.

```bash
almanak agent install -p aider
```

**Résultat :** `.aider/skills/almanak-strategy-builder/SKILL.md`

---

### Amazon Q

Amazon Q lit les règles depuis `.amazonq/rules/`.

```bash
almanak agent install -p amazonq
```

**Résultat :** `.amazonq/rules/almanak-strategy-builder.md`

---

### OpenClaw

OpenClaw decouvre les competences depuis les sous-repertoires `.openclaw/skills/`.

```bash
almanak agent install -p openclaw
```

**Resultat :** `.openclaw/skills/almanak-strategy-builder/SKILL.md`

---

## Installer pour toutes les plateformes

Pour installer sur toutes les plateformes supportées en une fois :

```bash
almanak agent install -p all
```

## Installation globale

Installez dans votre répertoire personnel (`~/`) pour que la compétence soit disponible dans chaque projet sans configuration par projet :

```bash
almanak agent install -g
```

Cela écrit dans `~/.claude/skills/`, `~/.codex/skills/`, `~/.cursor/rules/`, etc. Les plateformes qui ne supportent que les règles au niveau projet (Copilot, Cline, Roo Code, Amazon Q) sont automatiquement ignorées.

Vous pouvez aussi cibler des plateformes spécifiques :

```bash
almanak agent install -g -p claude
```

Pour vérifier ou mettre à jour les installations globales :

```bash
almanak agent status -g
almanak agent update -g
```

!!! note
    La plupart des plateformes permettent aux compétences locales (projet) de remplacer les globales. Si vous avez les deux, la compétence au niveau projet a la priorité.

## Installation via npx (skills.sh)

Si vous utilisez le registre [skills.sh](https://skills.sh), vous pouvez installer directement depuis GitHub :

```bash
npx skills add almanak-co/sdk
```

Cela découvre la compétence `almanak-strategy-builder` depuis le dépôt public et l'installe pour votre plateforme détectée.

## Gérer les compétences installées

### Vérifier le statut

Voir quelles plateformes ont la compétence installée et si elles sont à jour :

```bash
almanak agent status
```

Exemple de sortie :

```text
Agent skill status (SDK v2.0.0):

  claude      up to date (v2.0.0)
  codex       not installed
  cursor      outdated (v1.9.0 -> v2.0.0)
  copilot     not installed
  ...

Installed: 1  Outdated: 1  Missing: 7
```

### Mettre à jour

Après avoir mis à niveau le SDK (`pip install --upgrade almanak`), mettez à jour vos fichiers de compétences installés :

```bash
almanak agent update
```

Cela scanne tous les fichiers de plateformes installés et remplace leur contenu avec la version actuelle du SDK.

### Simulation

Prévisualisez ce que `install` ferait sans écrire de fichiers :

```bash
almanak agent install --dry-run
```

### Répertoire personnalisé

Installez dans un répertoire de projet spécifique :

```bash
almanak agent install -d /path/to/my-project -p claude
```

## Guides par stratégie

Lorsque vous créez une nouvelle stratégie avec `almanak strat new`, un fichier `AGENTS.md` est automatiquement généré dans le répertoire de la stratégie. Ce guide léger est adapté au modèle que vous avez choisi -- il ne liste que les types d'intentions et les patterns pertinents pour cette stratégie spécifique.

Chaque stratégie créée est un projet Python autonome avec `pyproject.toml`, `.venv/` et `uv.lock`, le `AGENTS.md` par stratégie documente donc aussi l'ajout de dépendances (`uv add`) et l'exécution de tests (`uv run pytest`).

```bash
almanak strat new --template mean_reversion --name my_rsi --chain arbitrum
# Crée my_rsi/AGENTS.md aux côtés de strategy.py, config.json, pyproject.toml, etc.
```

## Ce que la compétence enseigne

La compétence fournie couvre :

| Section | Ce qu'elle couvre |
|---------|---------------|
| Démarrage rapide | Installer, créer, exécuter |
| Concepts fondamentaux | IntentStrategy, decide(), MarketSnapshot |
| Référence des intentions | Tous les types d'intentions avec signatures et exemples |
| API de données de marché | price(), balance(), rsi(), macd(), bollinger_bands(), et 10+ indicateurs |
| Gestion d'état | Persistance self.state, callback on_intent_executed |
| Configuration | Format config.json, secrets .env, anvil_funding |
| Résolution de tokens | get_token_resolver(), resolve_for_swap() |
| Backtesting | Simulation PnL, paper trading, balayage de paramètres |
| Commandes CLI | strat new/run/demo/backtest, gateway, agent |
| Chaînes et protocoles | 12 chaînes, 10 protocoles avec noms d'énumérations |
| Patterns courants | Rééquilibrage, alertes, teardown, IntentSequence |
| Dépannage | Erreurs courantes et corrections |

## Lire la compétence directement

Pour afficher ou rechercher dans le contenu brut de la compétence :

```bash
# Afficher le chemin du fichier
almanak docs agent-skill

# Afficher le contenu complet
almanak docs agent-skill --dump

# Rechercher des sujets spécifiques
almanak docs agent-skill --dump | grep "Intent.swap"
```
