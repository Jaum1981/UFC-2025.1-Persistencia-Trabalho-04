from fastapi import FastAPI
from routes.biomaRoute import router as bioma_router
from routes.edificioRouter import router as edificio_router
from database import edificio_IBAMA_collection
import uvicorn

app = FastAPI(
    title="IBAMA API",
    description="API para upload e gerenciamento de dados do IBAMA",
    version="1.0.0"
)

@app.on_event("startup")
async def ensure_geo_index():
    # cria índice 2dsphere em 'location' se ainda não existir
    await edificio_IBAMA_collection.create_index(
        [("location", "2dsphere")],
        name="location_2dsphere"
    )

app.include_router(bioma_router)
app.include_router(edificio_router)