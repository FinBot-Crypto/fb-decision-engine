import time
import logging
import os
import json
import redis

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("decision-engine")

# Configurações via Ambiente
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
MIN_CONFIDENCE_SCORE = float(os.getenv("MIN_CONFIDENCE_SCORE", 0.75))

class DecisionEngine:
    def __init__(self):
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.pubsub = self.r.pubsub()

    def make_decision(self, message):
        """Escolhe as melhores estratégias baseadas no score de confiança."""
        evaluations = json.loads(message['data'])
        logger.info(f"Recebida avaliação de {len(evaluations)} ativos.")
        
        decisions = []
        
        for item in evaluations:
            symbol = item['symbol']
            strategies = item['strategies']
            
            # Filtra apenas estratégias acima do score mínimo
            valid_strategies = [s for s in strategies if s['score'] >= MIN_CONFIDENCE_SCORE]
            
            if valid_strategies:
                # Escolhe a melhor estratégia para aquele ativo
                best_strategy = max(valid_strategies, key=lambda x: x['score'])
                
                logger.info(f"DECISÃO: Operar {symbol} com estratégia {best_strategy['name']} (Score: {best_strategy['score']:.2f})")
                
                decisions.append({
                    "symbol": symbol,
                    "strategy": best_strategy['name'],
                    "confidence": best_strategy['score'],
                    "tier": best_strategy['tier'],
                    "timestamp": time.time()
                })

        if decisions:
            payload = json.dumps(decisions)
            self.r.set("decision:active_decisions", payload)
            self.r.publish("events:trade_decided", payload)
            logger.info(f"Publicadas {len(decisions)} decisões de trade.")

    def run(self):
        self.pubsub.subscribe(**{'events:strategies_evaluated': self.make_decision})
        logger.info("Decision Engine aguardando 'events:strategies_evaluated'...")
        
        for message in self.pubsub.listen():
            if message['type'] == 'message':
                pass

if __name__ == "__main__":
    engine = DecisionEngine()
    engine.run()
