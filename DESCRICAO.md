# Invest Event Engine

## Descrição

O **Invest Event Engine** é uma plataforma distribuída de processamento de eventos em tempo real voltada para a identificação e distribuição de oportunidades de investimento.

O sistema recebe continuamente eventos provenientes do mercado financeiro, processa essas informações por meio de regras definidas pelos usuários e encaminha notificações apenas aos consumidores cujos critérios sejam satisfeitos.

A solução utiliza conceitos de **Event-Driven Architecture (EDA)**, **Publish/Subscribe**, **Stream Processing** e **Complex Event Processing (CEP)**, permitindo o tratamento de grandes volumes de eventos com baixa latência e elevada escalabilidade.

---

# Objetivos

* Processar eventos financeiros em tempo real.
* Permitir que usuários definam critérios de interesse.
* Realizar filtragem de conteúdo baseada em regras.
* Distribuir notificações de forma desacoplada.
* Suportar milhares de usuários simultaneamente.
* Garantir escalabilidade horizontal.
* Explorar conceitos modernos de sistemas distribuídos.

---

# Motivação

Investidores frequentemente precisam monitorar continuamente o mercado em busca de oportunidades específicas.

O sistema permite que cada usuário configure regras personalizadas, como:

* Setor econômico.
* Faixa de preço.
* Variação percentual.
* Condições temporais.
* Padrões de eventos.
* Combinações de critérios.

Quando uma oportunidade compatível é detectada, o sistema envia uma notificação em tempo real.

---

# Arquitetura Geral

```text
+--------------------+
| Market Workers     |
+--------------------+
          |
          v
+--------------------+
| NATS / JetStream   |
+--------------------+
          |
          v
+--------------------+
| Apache Flink       |
| Processing Engine  |
+--------------------+
          |
          v
+--------------------+
| Notification Bus   |
+--------------------+
          |
          v
user.{id}.notifications
          |
          v
+--------------------+
| Client Application |
+--------------------+
```

---

# Componentes

## Market Workers

Serviços responsáveis por coletar dados de mercado.

Responsabilidades:

* Consumir APIs externas.
* Monitorar ativos.
* Publicar eventos de investimento.

Exemplo:

```json
{
  "ticker": "PETR4",
  "setor": "energia",
  "preco": 32.50
}
```

---

## Subscription Service

Responsável pelo cadastro e atualização das regras dos usuários.

Exemplo:

```json
{
  "userId": 50,
  "setor": "energia",
  "precoMax": 40
}
```

As alterações são enviadas ao motor de processamento.

---

## Processing Engine

Implementado utilizando Apache Flink.

Responsabilidades:

* Receber eventos do mercado.
* Receber alterações de regras.
* Manter o estado das subscriptions.
* Realizar o matching entre eventos e usuários.
* Produzir notificações.

---

## Notification Bus

Após o processamento, as notificações são publicadas em tópicos específicos.

Exemplo:

```text
user.50.notifications
```

Esse modelo permite desacoplamento entre produtores e consumidores.

---

## Client

Aplicação responsável por consumir as notificações.

Pode ser implementada como:

* Aplicação web.
* Aplicação mobile.
* WebSocket.
* Serviço externo.

---

# Fluxos de Eventos

## Market Events

Representam oportunidades de investimento.

```json
{
  "ticker": "VALE3",
  "setor": "mineração",
  "preco": 58
}
```

---

## Subscription Events

Representam alterações nas regras dos usuários.

```json
{
  "userId": 10,
  "setor": "mineração",
  "precoMax": 60
}
```

---

# Processamento de Streams

O sistema utiliza processamento orientado a eventos.

As subscriptions são mantidas como estado distribuído no Apache Flink, permitindo:

* Atualizações em tempo real.
* Consulta eficiente das regras.
* Recuperação após falhas.
* Escalabilidade horizontal.

---

# Complex Event Processing

A arquitetura permite futura evolução para CEP.

Exemplos:

* Ativo caiu 10% em 30 minutos.
* Três quedas consecutivas em determinado setor.
* Correlação entre ativos.
* Tendências de mercado.
* Janelas temporais.

Exemplo de regra:

> Notificar quando uma ação do setor de energia apresentar queda superior a 10% em menos de 30 minutos.

---

# Escalabilidade

O processamento é distribuído entre múltiplos nós do cluster.

Características:

* Paralelismo configurável.
* Particionamento por chave.
* Processamento distribuído.
* Balanceamento de carga.
* Tolerância a falhas.
* Checkpoints automáticos.

---

# Tecnologias

* Java
* Apache Flink
* NATS
* JetStream
* Docker
* Kubernetes (opcional)

---

# Conceitos Envolvidos

* Event-Driven Architecture
* Publish/Subscribe
* Content-Based Filtering
* Stream Processing
* Complex Event Processing
* Stateful Processing
* Message-Oriented Middleware
* Sistemas Distribuídos
* Tolerância a Falhas
* Escalabilidade Horizontal

---

# Resultado Esperado

Desenvolver um middleware distribuído capaz de identificar oportunidades de investimento em tempo real, aplicar filtros personalizados dos usuários e distribuir notificações de maneira escalável, resiliente e desacoplada.
