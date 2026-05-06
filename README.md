# fb-decision-engine

Filtra scores do `fb-strategy-ml` e decide se abre trade baseado em RSI.

## Fluxo

```
strategies.evaluated (fb-strategy-ml)
  → fb-decision-engine
    → fetch RSI atual (15m, período 56)
    → se score >= 0.65 E RSI < 38 → trade.opportunity
```

## Variáveis de Ambiente

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `NATS_URL` | `nats://crypto-nats:4222` | Servidor NATS |
| `MIN_CONFIDENCE_SCORE` | `0.65` | Score mínimo para considerar sinal |
| `MAX_RSI_ENTRY` | `38` | RSI máximo para entrada (sobrevenda) |

## Deploy

```bash
docker run -e NATS_URL=nats://crypto-nats:4222 fb-decision-engine:latest
```
