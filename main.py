from fastapi import FastAPI
from routes.biomaRoute import router as bioma_router
from routes.edificioRouter import router as edificio_router
from routes.especimeRouter import router as especime_router
from routes.enquadramentoRouter import router as enquadramento_router
from routes.AutoInfracaoRouter import router as auto_infracao_router
from routes.infratorRouter import router as infrator_router
from database import edificio_IBAMA_collection, auto_infracao_collection, enquadramento_collection, especime_collection, auto_infracao_collection, infrator_collection
from routes.ComplexRouter import router as complex_router

app = FastAPI(
    title="IBAMA API",
    description="API para upload e gerenciamento de dados do IBAMA",
    version="1.0.0"
)

@app.on_event("startup")
async def init_indexes():
    # --- Geo index ---
    await edificio_IBAMA_collection.create_index(
        [("location", "2dsphere")],
        name="location_2dsphere"
    )

    # --- Índices em auto_infracao para filtros e ordenação ---
    await auto_infracao_collection.create_index(
        [("dat_hora_auto_infracao", 1)], name="idx_auto_data"
    )
    await auto_infracao_collection.create_index(
        [("municipio", 1)], name="idx_auto_municipio"
    )
    await auto_infracao_collection.create_index(
        [("tipo_auto", "text")], name="idx_auto_tipo_auto"
    )

    # --- Índices para lookups rápidos ---
    await enquadramento_collection.create_index(
        [("seq_auto_infracao", 1)], name="idx_enq_seq_auto"
    )
    await especime_collection.create_index(
        [("seq_auto_infracao", 1)], name="idx_esp_seq_auto"
    )
    await infrator_collection.create_index(
        [("num_auto_infracao", 1)], name="idx_infrator_num_auto"
    )

app.include_router(bioma_router)
app.include_router(edificio_router)
app.include_router(especime_router)
app.include_router(enquadramento_router)
app.include_router(auto_infracao_router)
app.include_router(infrator_router)
app.include_router(complex_router)
