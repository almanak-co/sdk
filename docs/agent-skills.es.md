# Habilidades de agente

El SDK de Almanak incluye una **habilidad de agente** -- un archivo de conocimiento estructurado que enseña a los agentes de codificación IA cómo construir estrategias DeFi. Instálala en tu proyecto para que tu asistente IA entienda la API IntentStrategy, el vocabulario de intenciones, los métodos de datos de mercado y los comandos CLI.

## Instalación rápida

La forma más rápida de comenzar:

```bash
almanak agent install
```

Esto detecta automáticamente las herramientas de codificación IA que usas (verificando `.claude/`, `.cursor/`, `.github/`, etc.) e instala la habilidad en el directorio nativo de cada plataforma.

## Instrucciones por plataforma

### Claude Code

Claude Code descubre habilidades desde subdirectorios `.claude/skills/`.

```bash
almanak agent install -p claude
```

**Resultado:** `.claude/skills/almanak-strategy-builder/SKILL.md`

---

### OpenAI Codex

Codex descubre habilidades de proyecto desde subdirectorios `.codex/skills/`.

```bash
almanak agent install -p codex
```

**Resultado:** `.codex/skills/almanak-strategy-builder/SKILL.md`

---

### Cursor

Cursor lee reglas personalizadas desde `.cursor/rules/` como archivos `.mdc` con alcance basado en globs.

```bash
almanak agent install -p cursor
```

El archivo instalado incluye un encabezado YAML que activa la habilidad al editar archivos `strategy.py` o `config.json`:

```yaml
---
globs:
  - "**/strategy.py"
  - "**/config.json"
---
```

**Resultado:** `.cursor/rules/almanak-strategy-builder.mdc`

---

### GitHub Copilot

Copilot lee instrucciones con alcance desde `.github/instructions/` con encabezado `applyTo`.

```bash
almanak agent install -p copilot
```

El archivo instalado incluye un encabezado limitado a archivos de estrategia:

```yaml
---
applyTo: "**/strategy.py"
---
```

**Resultado:** `.github/instructions/almanak-strategy-builder.instructions.md`

---

### Windsurf

Windsurf lee reglas desde `.windsurf/rules/`.

```bash
almanak agent install -p windsurf
```

**Resultado:** `.windsurf/rules/almanak-strategy-builder.md`

---

### Cline

Cline lee reglas desde `.clinerules/`.

```bash
almanak agent install -p cline
```

**Resultado:** `.clinerules/almanak-strategy-builder.md`

---

### Roo Code

Roo Code lee reglas desde `.roo/rules/`.

```bash
almanak agent install -p roo
```

**Resultado:** `.roo/rules/almanak-strategy-builder.md`

---

### Aider

Aider descubre habilidades desde subdirectorios `.aider/skills/`.

```bash
almanak agent install -p aider
```

**Resultado:** `.aider/skills/almanak-strategy-builder/SKILL.md`

---

### Amazon Q

Amazon Q lee reglas desde `.amazonq/rules/`.

```bash
almanak agent install -p amazonq
```

**Resultado:** `.amazonq/rules/almanak-strategy-builder.md`

---

### OpenClaw

OpenClaw descubre habilidades desde subdirectorios `.openclaw/skills/`. También disponible a través del marketplace ClawHub: `clawhub install almanak-strategy-builder`.

```bash
almanak agent install -p openclaw
```

**Resultado:** `.openclaw/skills/almanak-strategy-builder/SKILL.md`

---

## Instalar en todas las plataformas

Para instalar en todas las plataformas soportadas a la vez:

```bash
almanak agent install -p all
```

## Instalación global

Instala en tu directorio personal (`~/`) para que la habilidad esté disponible en cada proyecto sin configuración por proyecto:

```bash
almanak agent install -g
```

Esto escribe en `~/.claude/skills/`, `~/.codex/skills/`, `~/.cursor/rules/`, etc. Las plataformas que solo soportan reglas a nivel de proyecto (Copilot, Cline, Roo Code, Amazon Q) se omiten automáticamente.

También puedes apuntar a plataformas específicas:

```bash
almanak agent install -g -p claude
```

Para verificar o actualizar instalaciones globales:

```bash
almanak agent status -g
almanak agent update -g
```

!!! note
    La mayoría de las plataformas permiten que las habilidades locales (proyecto) anulen las globales. Si tienes ambas, la habilidad a nivel de proyecto tiene prioridad.

## Instalar vía npx (skills.sh)

Si usas el registro [skills.sh](https://skills.sh), puedes instalar directamente desde GitHub:

```bash
npx skills add almanak-co/almanak-sdk
```

Esto descubre la habilidad `almanak-strategy-builder` desde el repositorio público y la instala para tu plataforma detectada.

## Gestionar habilidades instaladas

### Verificar estado

Ver qué plataformas tienen la habilidad instalada y si están actualizadas:

```bash
almanak agent status
```

Ejemplo de salida:

```text
Agent skill status (SDK v2.0.0):

  claude      up to date (v2.0.0)
  codex       not installed
  cursor      outdated (v1.9.0 -> v2.0.0)
  copilot     not installed
  ...

Installed: 1  Outdated: 1  Missing: 7
```

### Actualizar

Después de actualizar el SDK (`pip install --upgrade almanak`), actualiza los archivos de habilidades instalados:

```bash
almanak agent update
```

Esto escanea todos los archivos de plataformas instalados y reemplaza su contenido con la versión actual del SDK.

### Simulación

Previsualiza lo que `install` haría sin escribir ningún archivo:

```bash
almanak agent install --dry-run
```

### Directorio personalizado

Instala en un directorio de proyecto específico:

```bash
almanak agent install -d /path/to/my-project -p claude
```

## Guías por estrategia

Cuando creas una nueva estrategia con `almanak strat new`, se genera automáticamente un archivo `AGENTS.md` dentro del directorio de la estrategia. Esta guía ligera está adaptada a la plantilla que elegiste -- solo lista los tipos de intenciones y patrones relevantes para esa estrategia específica.

```bash
almanak strat new --template mean_reversion --name my_rsi --chain arbitrum
# Crea my_rsi/AGENTS.md junto a strategy.py y config.json
```

## Lo que enseña la habilidad

La habilidad incluida cubre:

| Sección | Lo que cubre |
|---------|---------------|
| Inicio rápido | Instalar, crear, ejecutar |
| Conceptos fundamentales | IntentStrategy, decide(), MarketSnapshot |
| Referencia de intenciones | Todos los tipos de intención con firmas y ejemplos |
| API de datos de mercado | price(), balance(), rsi(), macd(), bollinger_bands(), y 10+ indicadores |
| Gestión de estado | Persistencia self.state, callback on_intent_executed |
| Configuración | Formato config.json, secretos .env, anvil_funding |
| Resolución de tokens | get_token_resolver(), resolve_for_swap() |
| Backtesting | Simulación PnL, paper trading, barrido de parámetros |
| Comandos CLI | strat new/run/demo/backtest, gateway, agent |
| Cadenas y protocolos | 12 cadenas, 10 protocolos con nombres de enumeración |
| Patrones comunes | Rebalanceo, alertas, teardown, IntentSequence |
| Solución de problemas | Errores comunes y soluciones |

## Leer la habilidad directamente

Para ver o buscar en el contenido bruto de la habilidad:

```bash
# Imprimir la ruta del archivo
almanak docs agent-skill

# Mostrar el contenido completo
almanak docs agent-skill --dump

# Buscar temas específicos
almanak docs agent-skill --dump | grep "Intent.swap"
```
