"""
fb-decision-engine: Filtra scores do strategy-ml e decide se abre trade.

Fluxo:
  strategies.evaluated → para cada ativo:
    score >= MIN_CONFIDENCE_SCORE?
    → fetch OHLCV 15m → calcula RSI atual
    → RSI < MAX_RSI_ENTRY?
    → publica trade.opportunity
"""
import asyncio, logging, os, json, numpy as np, ccxt, nats
from nats.js.api import ConsumerConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("fb-decision-engine")

NATS_URL = os.getenv("NATS_URL", "nats://crypto-nats:4222")
MIN_CONFIDENCE_SCORE = float(os.getenv("MIN_CONFIDENCE_SCORE", "0.65"))
MAX_RSI_ENTRY = float(os.getenv("MAX_RSI_ENTRY", "38"))
SHORT_MIN_SCORE = float(os.getenv("SHORT_MIN_SCORE", "0.85"))
SHORT_MIN_RSI = float(os.getenv("SHORT_MIN_RSI", "65"))
RSI_PERIOD = 56
BTC_SMA_PERIOD = int(os.getenv("BTC_SMA_PERIOD", "50"))


class DecisionEngine:
    def __init__(self):
        self.nc = None
        self.js = None
        self.exchange = ccxt.binance({"enableRateLimit": True})

    async def connect_nats(self):
        self.nc = await nats.connect(NATS_URL)
        self.js = self.nc.jetstream()
        logger.info(f"NATS conectado: {NATS_URL}")

    def compute_rsi(self, closes):
        import pandas as pd
        delta = np.diff(closes)
        gain = np.maximum(delta, 0)
        loss = -np.minimum(delta, 0)
        avg_gain = pd.Series(gain).rolling(RSI_PERIOD).mean().values
        avg_loss = pd.Series(loss).rolling(RSI_PERIOD).mean().values
        rsi = 100 - 100 / (1 + avg_gain / (avg_loss + 1e-10))
        return float(rsi[-1])

    async def fetch_rsi(self, symbol):
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, "15m", limit=200)
            closes = [c[4] for c in ohlcv]
            if len(closes) < RSI_PERIOD + 1:
                return None
            return self.compute_rsi(closes)
        except Exception as e:
            logger.error(f"Erro RSI {symbol}: {e}")
            return None

    async def fetch_btc_trend(self):
        try:
            ohlcv = self.exchange.fetch_ohlcv("BTC/USDT", "1h", limit=BTC_SMA_PERIOD + 10)
            if not ohlcv or len(ohlcv) < BTC_SMA_PERIOD:
                return "neutral"
            closes = [c[4] for c in ohlcv]
            sma = sum(closes[-BTC_SMA_PERIOD:]) / BTC_SMA_PERIOD
            current = closes[-1]
            if current > sma * 1.01:
                return "bull"
            elif current < sma * 0.99:
                return "bear"
            return "neutral"
        except Exception as e:
            logger.error(f"Erro BTC trend: {e}")
            return "neutral"

    async def process_evaluations(self, msg):
        try:
            evaluations = json.loads(msg.data.decode())
            btc_trend = await self.fetch_btc_trend()
            logger.info(f"Analisando {len(evaluations)} avaliações [BTC: {btc_trend}]")
            opportunities = []

            for ev in evaluations:
                symbol = ev["symbol"]
                tier = ev.get("tier", "Unknown")
                strategies = ev.get("strategies", [])

                for strat in strategies:
                    score = strat["score"]
                    direction = strat.get("direction", "LONG")

                    # Filtro de regime: BTC trend bloqueia direção contrária
                    if btc_trend == "bull" and direction == "SHORT":
                        continue
                    if btc_trend == "bear" and direction == "LONG":
                        continue

                    if direction == "SHORT":
                        if score < SHORT_MIN_SCORE:
                            continue

                        rsi = await self.fetch_rsi(symbol)
                        if rsi is None:
                            logger.warning(f"  {symbol}: sem dados RSI para SHORT")
                            continue

                        if rsi < SHORT_MIN_RSI:
                            logger.info(f"  {symbol}: short_score={score:.4f} ok, mas RSI={rsi:.1f} < {SHORT_MIN_RSI} → ignora")
                            continue

                        logger.info(f"  {symbol}: SIGNAL SHORT → score={score:.4f} RSI={rsi:.1f} >= {SHORT_MIN_RSI}")
                        opportunities.append({
                            "symbol": symbol,
                            "tier": tier,
                            "strategy": strat["name"],
                            "score": score,
                            "rsi": round(rsi, 1),
                            "direction": "SHORT",
                            "timestamp": ev.get("timestamp", ""),
                        })
                    else:
                        if score < MIN_CONFIDENCE_SCORE:
                            continue

                        rsi = await self.fetch_rsi(symbol)
                        if rsi is None:
                            logger.warning(f"  {symbol}: sem dados RSI")
                            continue

                        if rsi >= MAX_RSI_ENTRY:
                            logger.info(f"  {symbol}: score={score:.4f} ok, mas RSI={rsi:.1f} >= {MAX_RSI_ENTRY} → ignora")
                            continue

                        logger.info(f"  {symbol}: SIGNAL LONG → score={score:.4f} RSI={rsi:.1f} < {MAX_RSI_ENTRY}")
                        opportunities.append({
                            "symbol": symbol,
                            "tier": tier,
                            "strategy": strat["name"],
                            "score": score,
                            "rsi": round(rsi, 1),
                            "direction": "LONG",
                            "timestamp": ev.get("timestamp", ""),
                        })

            if opportunities:
                payload = json.dumps(opportunities).encode()
                await self.js.publish("trade.opportunity", payload)
                logger.info(f"Publicadas {len(opportunities)} oportunidades em trade.opportunity")

            await msg.ack()
        except Exception as e:
            logger.error(f"Erro ao processar: {e}")

    async def run(self):
        await self.connect_nats()
        await self.js.subscribe("strategies.evaluated", durable="DECISION_ENGINE_WORKER",
                                 cb=self.process_evaluations, manual_ack=True,
                                 config=ConsumerConfig(ack_wait=30))
        logger.info(f"fb-decision-engine online (score>={MIN_CONFIDENCE_SCORE}, RSI<{MAX_RSI_ENTRY})")
        while True:
            if self.nc.is_closed:
                await self.connect_nats()
            await asyncio.sleep(10)


if __name__ == "__main__":
    engine = DecisionEngine()
    asyncio.run(engine.run())
