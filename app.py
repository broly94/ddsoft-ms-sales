from fastapi import FastAPI
from src.api.route_validator import router as route_validator_router
from src.models.route_models import init_db

app = FastAPI(title="Sales Microservice")

# Inicializar DB al arrancar
@app.on_event("startup")
def startup_event():
    init_db()

# Rutas de Salud
@app.get("/health")
def health():
    return {"status": "ok", "service": "sales"}

# Registrar Routers (Módulos)
app.include_router(route_validator_router)

# Aquí puedes incluir mas routers en el futuro
# app.include_router(gescom_connector_router)
# app.include_router(sales_reports_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
