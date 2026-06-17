"""
fb-decision-engine: Filtra scores do strategy-ml e decide se abre trade.

Fluxo:
  strategies.evaluated → para cada ativo:
    score >= MIN_CONFIDENCE_SCORE?
    → fetch OHLCV 15m → calcula RSI atual
    → RSI < MAX_RSI_ENTRY?
    → publica trade.opportunity
"""
import asyncio, logging, os, json, numpy as np, ccxt, nats, psycopg2
from nats.js.api import ConsumerConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("fb-decision-engine")

NATS_URL = os.getenv("NATS_URL", "nats://crypto-nats:4222")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://crypto_admin:ZNG5z43LaSrk7FEmwu6CPtRUB2IVKdvY@crypto-postgres:5432/crypto_bot")
MIN_CONFIDENCE_SCORE = float(os.getenv("MIN_CONFIDENCE_SCORE", "0.65"))
MAX_RSI_ENTRY = float(os.getenv("MAX_RSI_ENTRY", "38"))
SHORT_MIN_SCORE = float(os.getenv("SHORT_MIN_SCORE", "0.85"))
SHORT_MIN_RSI = float(os.getenv("SHORT_MIN_RSI", "65"))
RSI_PERIOD = 56
BTC_SMA_PERIOD = int(os.getenv("BTC_SMA_PERIOD", "12"))
SHORT_ALLOWED_REGIMES = [r.strip().lower() for r in os.getenv("SHORT_ALLOWED_REGIMES", "bear,neutral").split(",") if r.strip()]
LONG_ALLOWED_REGIMES = [r.strip().lower() for r in os.getenv("LONG_ALLOWED_REGIMES", "bull").split(",") if r.strip()]
SHORT_ALLOWED_TIERS = [t.strip() for t in os.getenv("SHORT_ALLOWED_TIERS", "Major,Strong Alt,High Volatility").split(",") if t.strip()]
LONG_ALLOWED_TIERS = [t.strip() for t in os.getenv("LONG_ALLOWED_TIERS", "Major,Strong Alt,High Volatility").split(",") if t.strip()]


class DecisionEngine:
    def __init__(self):
        self.nc = None
        self.js = None
        self.exchange = ccxt.binance({"enableRateLimit": True})
        self.db_url = DATABASE_URL
        self.db_conn = None
        self.db_cursor = None

    def init_db(self):
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS evaluations_log (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    tier VARCHAR(30),
                    strategy VARCHAR(50),
                    direction VARCHAR(10),
                    score FLOAT,
                    rsi FLOAT,
                    btc_trend VARCHAR(10),
                    decision VARCHAR(30),
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            conn.commit()
            cur.close()
            conn.close()
            logger.info("Tabela evaluations_log inicializada.")
        except Exception as e:
            logger.error(f"Erro ao inicializar banco para evaluations_log: {e}")

    def get_adjusted_thresholds(self, symbol, direction, btc_trend):
        """
        Calcula limite de score ajustado com base no histórico de perdas neste regime.
        """
        min_score = SHORT_MIN_SCORE if direction == "SHORT" else MIN_CONFIDENCE_SCORE
        
        try:
            # Garante conexão ativa
            if self.db_conn is None or self.db_conn.closed != 0:
                self.db_conn = psycopg2.connect(self.db_url)
                self.db_conn.autocommit = True
                self.db_cursor = self.db_conn.cursor()
                
            self.db_cursor.execute("""
                SELECT pnl_pct, exit_reason FROM trade_log
                WHERE symbol = %s AND market_regime = %s AND status = 'CLOSED'
                ORDER BY updated_at DESC LIMIT 5
            """, (symbol, btc_trend))
            rows = self.db_cursor.fetchall()
            
            if rows:
                pnl_list = [float(r[0]) if r[0] is not None else 0.0 for r in rows]
                avg_pnl = sum(pnl_list) / len(pnl_list)
                losses = sum(1 for pnl in pnl_list if pnl < 0)
                win_rate = (len(pnl_list) - losses) / len(pnl_list)
                
                # Se média de PnL for negativa ou WR < 40%, aplica penalidade
                if avg_pnl < -0.5 or win_rate < 0.40:
                    penalty = 0.10 if direction == "LONG" else 0.05
                    min_score += penalty
                    logger.info(f"  [RISK PENALTY] {symbol} no regime {btc_trend}: avg_pnl={avg_pnl:.2f}%, WR={win_rate:.0%} → Score mínimo ajustado para {min_score:.2f}")
        except Exception as e:
            logger.error(f"Erro ao calcular limite ajustado para {symbol}: {e}")
            self.db_conn = None
            self.db_cursor = None
            
        return min(min_score, 0.95)

    def is_in_cooldown(self, symbol):
        """
        Verifica se o ativo está em cooldown progressivo devido a Stop Losses recentes.
        """
        cooldown_base = float(os.getenv("COOLDOWN_HOURS", "2.0"))
        if cooldown_base <= 0:
            return False
            
        try:
            import time
            if self.db_conn is None or self.db_conn.closed != 0:
                self.db_conn = psycopg2.connect(self.db_url)
                self.db_conn.autocommit = True
                self.db_cursor = self.db_conn.cursor()
                
            self.db_cursor.execute("""
                SELECT pnl_pct, EXTRACT(EPOCH FROM updated_at), exit_reason FROM trade_log
                WHERE symbol = %s AND status = 'CLOSED'
                ORDER BY updated_at DESC LIMIT 10
            """, (symbol,))
            rows = self.db_cursor.fetchall()
            
            if rows:
                consecutive_losses = 0
                last_exit_ts = None
                for r in rows:
                    pnl = float(r[0]) if r[0] is not None else 0.0
                    ts = float(r[1]) if r[1] is not None else 0.0
                    reason = r[2]
                    
                    if last_exit_ts is None:
                        last_exit_ts = ts
                        
                    if pnl < 0 or reason == 'STOP_LOSS':
                        consecutive_losses += 1
                    else:
                        break
                        
                if consecutive_losses > 0 and last_exit_ts is not None:
                    cooldown_h = cooldown_base * (2.0 ** (consecutive_losses - 1))
                    cooldown_h = min(cooldown_h, 48.0)
                    
                    elapsed = time.time() - last_exit_ts
                    if elapsed < cooldown_h * 3600:
                        logger.info(f"  [COOLDOWN] {symbol}: ativo após {consecutive_losses} loss(es) ({elapsed/3600:.1f}h < {cooldown_h:.1f}h)")
                        return True
        except Exception as e:
            logger.error(f"Erro ao verificar cooldown para {symbol}: {e}")
            self.db_conn = None
            self.db_cursor = None
            
        return False

    def log_evaluation(self, symbol, tier, strategy, direction, score, rsi, btc_trend, decision):
        try:
            if self.db_conn is None or self.db_conn.closed != 0:
                self.db_conn = psycopg2.connect(self.db_url)
                self.db_conn.autocommit = True
                self.db_cursor = self.db_conn.cursor()
                
            self.db_cursor.execute("""
                INSERT INTO evaluations_log (symbol, tier, strategy, direction, score, rsi, btc_trend, decision)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (symbol, tier, strategy, direction, score, rsi, btc_trend, decision))
            self.db_conn.commit()
        except Exception as e:
            logger.error(f"Erro ao salvar log de avaliacao no banco: {e}")
            self.db_conn = None
            self.db_cursor = None

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

                    # 1. Filtro de regime: verifica se a direção é permitida no regime atual do BTC
                    if direction == "LONG" and btc_trend not in LONG_ALLOWED_REGIMES:
                        dec_reason = "REJECTED_LATERAL" if btc_trend == "neutral" else "REJECTED_REGIME"
                        self.log_evaluation(symbol, tier, strat["name"], direction, score, None, btc_trend, dec_reason)
                        continue

                    if direction == "SHORT" and btc_trend not in SHORT_ALLOWED_REGIMES:
                        dec_reason = "REJECTED_LATERAL" if btc_trend == "neutral" else "REJECTED_REGIME"
                        self.log_evaluation(symbol, tier, strat["name"], direction, score, None, btc_trend, dec_reason)
                        continue

                    # 1.5 Filtro de Tier
                    if direction == "LONG" and tier not in LONG_ALLOWED_TIERS:
                        self.log_evaluation(symbol, tier, strat["name"], direction, score, None, btc_trend, "REJECTED_TIER")
                        continue

                    if direction == "SHORT" and tier not in SHORT_ALLOWED_TIERS:
                        self.log_evaluation(symbol, tier, strat["name"], direction, score, None, btc_trend, "REJECTED_TIER")
                        continue

                    # 2. Filtro de Cooldown progressivo de Stop Loss
                    if self.is_in_cooldown(symbol):
                        self.log_evaluation(symbol, tier, strat["name"], direction, score, None, btc_trend, "REJECTED_COOLDOWN")
                        continue

                    # 3. Calcula o limite de score ajustado com base na penalidade de risco
                    min_score_required = self.get_adjusted_thresholds(symbol, direction, btc_trend)

                    default_min_score = SHORT_MIN_SCORE if direction == "SHORT" else MIN_CONFIDENCE_SCORE
                    if score < default_min_score:
                        self.log_evaluation(symbol, tier, strat["name"], direction, score, None, btc_trend, "REJECTED_SCORE")
                        continue
                    
                    if score < min_score_required:
                        self.log_evaluation(symbol, tier, strat["name"], direction, score, None, btc_trend, "REJECTED_PENALTY")
                        continue

                    # 4. Filtro de RSI
                    rsi = await self.fetch_rsi(symbol)
                    if rsi is None:
                        logger.warning(f"  {symbol}: sem dados RSI")
                        self.log_evaluation(symbol, tier, strat["name"], direction, score, None, btc_trend, "REJECTED_NO_DATA")
                        continue

                    if direction == "SHORT":
                        if rsi < SHORT_MIN_RSI:
                            logger.info(f"  {symbol}: short_score={score:.4f} ok, mas RSI={rsi:.1f} < {SHORT_MIN_RSI} → ignora")
                            self.log_evaluation(symbol, tier, strat["name"], direction, score, rsi, btc_trend, "REJECTED_RSI")
                            continue

                        logger.info(f"  {symbol}: SIGNAL SHORT → score={score:.4f} RSI={rsi:.1f} >= {SHORT_MIN_RSI}")
                        opportunities.append({
                            "symbol": symbol,
                            "tier": tier,
                            "strategy": strat["name"],
                            "score": score,
                            "rsi": round(rsi, 1),
                            "direction": "SHORT",
                            "market_regime": btc_trend,
                            "timestamp": ev.get("timestamp", ""),
                        })
                        self.log_evaluation(symbol, tier, strat["name"], direction, score, rsi, btc_trend, "ACCEPTED")
                    else:
                        if rsi >= MAX_RSI_ENTRY:
                            logger.info(f"  {symbol}: score={score:.4f} ok, mas RSI={rsi:.1f} >= {MAX_RSI_ENTRY} → ignora")
                            self.log_evaluation(symbol, tier, strat["name"], direction, score, rsi, btc_trend, "REJECTED_RSI")
                            continue

                        logger.info(f"  {symbol}: SIGNAL LONG → score={score:.4f} RSI={rsi:.1f} < {MAX_RSI_ENTRY}")
                        opportunities.append({
                            "symbol": symbol,
                            "tier": tier,
                            "strategy": strat["name"],
                            "score": score,
                            "rsi": round(rsi, 1),
                            "direction": "LONG",
                            "market_regime": btc_trend,
                            "timestamp": ev.get("timestamp", ""),
                        })
                        self.log_evaluation(symbol, tier, strat["name"], direction, score, rsi, btc_trend, "ACCEPTED")

            if opportunities:
                payload = json.dumps(opportunities).encode()
                await self.js.publish("trade.opportunity", payload)
                logger.info(f"Publicadas {len(opportunities)} oportunidades em trade.opportunity")

            await msg.ack()
        except Exception as e:
            logger.error(f"Erro ao processar: {e}")

    async def run(self):
        await self.connect_nats()
        self.init_db()
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
