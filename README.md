# Filter Engine

Sistema de alertas de investimentos em tempo real. Coleta dados de mercado via Yahoo Finance, processa regras de filtragem com Apache Flink e entrega notificações aos clientes via NATS JetStream.

```
Market Worker (Python)
       │
       ▼
NATS JetStream ──── Subscription Service
       │
       ▼
Apache Flink (Java)
       │
       ▼
user.<UUID>.notifications
       │
       ▼
Client (Python)
```

---

## Pré-requisitos

- Docker e Docker Compose
- Python 3.11+
- Java 17
- Maven 3.9+

---

## 1. NATS

O NATS roda via Docker Compose a partir da pasta `nats/`.

```bash
cd nats/
docker compose up -d
```

Verifique se subiu:

```bash
docker compose ps
```

> O NATS ficará disponível em `nats://localhost:4222`.

---

## 2. Market Worker

Serviço Python que se conecta ao Yahoo Finance via WebSocket e publica eventos de mercado no JetStream.

### Setup do ambiente

```bash
cd market-worker/
python -m venv .venv
source .venv/bin/activate        # Linux/macOS
# .venv\Scripts\activate         # Windows
pip install -r requirements.txt
```

### Rodar

```bash
python main.py
```

Variáveis de ambiente disponíveis:

| Variável   | Padrão                  | Descrição             |
|------------|-------------------------|-----------------------|
| `NATS_URL` | `nats://localhost:4222` | Endereço do NATS      |

---

## 3. Middleware (Flink)

Motor de processamento implementado em Java com Apache Flink. Consome eventos do mercado, aplica as regras das subscriptions e publica notificações.

### Requisitos

- Java 17
- Maven 3.9+

### Rodar

```bash
cd middleware/
mvn clean package exec:java
```

> Certifique-se de que o NATS já está rodando antes de iniciar o middleware, pois ele tentará se conectar ao JetStream na inicialização.

Variáveis de ambiente disponíveis:

| Variável   | Padrão                  | Descrição        |
|------------|-------------------------|------------------|
| `NATS_URL` | `nats://localhost:4222` | Endereço do NATS |

---

## 4. Client

Aplicação Python interativa que permite configurar filtros de investimento e receber notificações em tempo real.

### Setup do ambiente

```bash
cd client/
python -m venv .venv
source .venv/bin/activate        # Linux/macOS
# .venv\Scripts\activate         # Windows
pip install -r requirements.txt
```

### Rodar

```bash
python client.py
```

Ao iniciar, o client:

1. Gera um `UUID` único para a sessão.
2. Abre uma assinatura no tópico `user.<UUID>.notifications`.
3. Exibe um menu para configurar os filtros desejados.

### Filtros disponíveis

| Filtro        | Descrição                                              |
|---------------|--------------------------------------------------------|
| `sector`      | Setor do ativo (ex: `ENERGIA`, `CRYPTO`, `ANY`)        |
| `ticker`      | Ticker específico (ex: `PETR4.SA`, `BTC-USD`, `ANY`)  |
| `higherThan`  | Preço mínimo para notificar                            |
| `smallerThan` | Preço máximo para notificar                            |

> `ANY` em `sector` ou `ticker` significa que o filtro não será aplicado para aquele campo.

---

## Ordem de inicialização

```
1. NATS          → docker compose (nats/)
2. Market Worker → publica eventos de mercado
3. Middleware    → processa eventos e subscriptions
4. Client        → configura filtros e recebe notificações
```
