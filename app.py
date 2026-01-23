from fastapi import FastAPI
from src.api.route_validator import router as route_validator_router
from src.models.route_models import init_db
from src.core.redis_client import RedisMicroservice
import os

app = FastAPI(title="Sales Microservice")

# Configuración Redis
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
# El queue name debe coincidir con lo que NestJS envía
redis_ms = RedisMicroservice(host=REDIS_HOST, port=REDIS_PORT, queue="SALES_SERVICE")

# Handler para datos de Gescom vía Redis
@redis_ms.on("process_gescom_data")
def handle_gescom_data(data):
    print(f"[*] Recibidos datos de Gescom vía Redis: {data}")
    # Aquí procesarías los datos recibidos
    return {"status": "processed", "received": len(data) if isinstance(data, list) else 1}

# Inicializar DB al arrancar
@app.on_event("startup")
def startup_event():
    init_db()
    redis_ms.start()

# Rutas de Salud
@app.get("/health")
def health():
    return {"status": "ok", "service": "sales"}

# Registrar Routers (Módulos)
app.include_router(route_validator_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
