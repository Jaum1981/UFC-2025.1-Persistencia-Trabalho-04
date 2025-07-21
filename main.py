from fastapi import FastAPI
from routes.biomaRoute import router as bioma_router
from routes.edificioRouter import router as edificio_router
from routes.especimeRouter import router as especime_router
from routes.enquadramentoRouter import router as enquadramento_router
from routes.AutoInfracaoRouter import router as auto_infracao_router
from database import edificio_IBAMA_collection

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
app.include_router(especime_router)
app.include_router(enquadramento_router)
app.include_router(auto_infracao_router)