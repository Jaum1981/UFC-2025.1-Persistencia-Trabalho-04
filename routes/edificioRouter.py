from bson import ObjectId
from fastapi import APIRouter, HTTPException, UploadFile, File, Query
from database import edificio_IBAMA_collection
import pandas as pd
import io
import re

from models.edificio_IBAMA import Edf_Pub_Civil_IBAMACreate, Edf_Pub_Civil_IBAMAOut, PaginatedEdf_Pub_Civil_IBAMAResponse
from logs.logger import logger


router = APIRouter(prefix="/edf", tags=["Edf Pub Civil IBAMA"])

def dms_to_decimal(dms_str: str) -> float:
    s = re.sub(r'\s+', '', dms_str)
    sign = -1 if re.search(r'[SWsw]', s) else 1
    parts = re.split(r'[°\'"]+', s)
    deg, min_, sec = parts[0], parts[1], parts[2]
    return sign * (float(deg) + float(min_)/60 + float(sec)/3600)


@router.post("/upload", response_model=list[Edf_Pub_Civil_IBAMAOut])
async def upload_edf_csv(file: UploadFile = File(...)):
    logger.info(f"Iniciando upload de arquivo CSV: {file.filename}")
    
    df = pd.read_csv(
        io.BytesIO(await file.read()),
        dtype=str,
        keep_default_na=False,
        na_values=['']
    )
    
    logger.info(f"Arquivo CSV carregado com {len(df)} linhas")

    coluna_map = {
        "nome": "nome",
        "nomeabrev": "nomeabrev",
        "municip": "municipio",
        "estado": "estado",
        "situacaofisica": "situacao_fisica",
        "lat": "lat",
        "long": "long"
    }

    cols_existentes = [c for c in df.columns if c in coluna_map]
    df = df[cols_existentes].rename(columns={c: coluna_map[c] for c in cols_existentes})

    docs = []
    for i, row in df.iterrows():
        d = row.to_dict()
        try:
            #Converter DMS → decimal
            lat_dd = dms_to_decimal(d["lat"])
            long_dd = dms_to_decimal(d["long"])

            #Validar e construir o documento
            inst = Edf_Pub_Civil_IBAMACreate(**d)
            doc = inst.dict()
            doc["location"] = {
                "type": "Point",
                "coordinates": [long_dd, lat_dd]
            }
            docs.append(doc)
        except Exception as e:
            logger.warning(f"Erro linha {i+1}: {e}")
            continue

    if not docs:
        logger.error("Nenhum registro válido após tratamento do arquivo CSV")
        raise HTTPException(400, "Nenhum registro válido após tratamento.")

    logger.info(f"Processando inserção de {len(docs)} documentos válidos")
    res = await edificio_IBAMA_collection.insert_many(docs)
    
    logger.info(f"Upload concluído com sucesso: {len(docs)} edificios inseridos")
    return [
        Edf_Pub_Civil_IBAMAOut(
            **{**doc, "_id": str(res.inserted_ids[idx])} 
        )
        for idx, doc in enumerate(docs)
    ]

@router.get("/stats/edificios/municipio")
async def get_edificio_stats_municipio():
    """
    Total de edifícios por município.
    """
    logger.info("Gerando estatísticas de edifícios por município")
    try:
        pipeline = [
            {"$group": {
                "_id": "$municipio",
                "total_edificios": {"$sum": 1}
            }},
            {"$project": {
                "municipio": "$_id",
                "total_edificios": 1,
                "_id": 0
            }},
            {"$sort": {"total_edificios": -1}}
        ]
        stats = await edificio_IBAMA_collection.aggregate(pipeline).to_list(None)
        return {"estatisticas_por_municipio": stats}
    except Exception as e:
        logger.error(f"Erro ao gerar estatísticas de edifícios: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/nearby", response_model=Edf_Pub_Civil_IBAMAOut)
async def nearby(
    lat: float = Query(...),
    long: float = Query(...),
    max_distance: int = Query(10000, description="Distância máxima em metros")
):
    logger.info(f"Buscando edificio próximo - Lat: {lat}, Long: {long}, Distância máxima: {max_distance}m")
    
    try:
        query = {
            "location": {
                "$nearSphere": {
                    "$geometry": {"type": "Point", "coordinates": [long, lat]},
                    "$maxDistance": max_distance
                }
            }
        }
        docs = await edificio_IBAMA_collection.find(query).limit(1).to_list(1)
        if not docs:
            logger.warning(f"Nenhum edificio encontrado próximo às coordenadas {lat}, {long} dentro de {max_distance}m")
            raise HTTPException(404, "Nenhuma unidade próxima encontrada dentro da distância especificada.")
        
        doc = docs[0]
        # converte _id para string antes de criar o modelo
        doc["_id"] = str(doc["_id"])
        
        logger.info(f"Edificio encontrado: {doc.get('nome', 'N/A')} (ID: {doc['_id']})")
        return Edf_Pub_Civil_IBAMAOut(**doc)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro na consulta geoespacial: {e}")
        raise HTTPException(500, f"Erro na consulta geoespacial: {e}")
    
@router.get("/count_edificio")
async def count_edificio():
    logger.info("Contando total de edificios na coleção")
    try:
        count = await edificio_IBAMA_collection.count_documents({})
        logger.info(f"Total de edificios encontrados: {count}")
        return {"count": count}
    except Exception as e:
        logger.error(f"Erro ao contar documentos: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao contar documentos: {e}")
    
@router.get("/edificios", response_model=PaginatedEdf_Pub_Civil_IBAMAResponse)
async def get_edificios(page: int = Query(1, ge=1), page_size: int = Query(10, ge=1)):
    logger.info(f"Buscando edificios - Página: {page}, Tamanho: {page_size}")
    try:
        total = await edificio_IBAMA_collection.count_documents({})
        items = await edificio_IBAMA_collection.find().skip((page - 1) * page_size).limit(page_size).to_list(length=None)
        
        logger.info(f"Retornando {len(items)} edificios de um total de {total}")
        return PaginatedEdf_Pub_Civil_IBAMAResponse(total=total, page=page, size=page_size, items=items)
    except Exception as e:
        logger.error(f"Erro ao buscar documentos: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao buscar documentos: {e}")
    
@router.get("/edificio/{edificio_id}", response_model=Edf_Pub_Civil_IBAMAOut)
async def get_edificio(edificio_id: str):
    logger.info(f"Buscando edificio por ID: {edificio_id}")
    try:
        if not ObjectId.is_valid(edificio_id):
            logger.warning(f"ID inválido fornecido: {edificio_id}")
            raise HTTPException(status_code=400, detail="ID inválido")
        
        edificio = await edificio_IBAMA_collection.find_one({"_id": ObjectId(edificio_id)})
        if not edificio:
            logger.warning(f"Edificio não encontrado para ID: {edificio_id}")
            raise HTTPException(status_code=404, detail="Edifício não encontrado")
        
        edificio["_id"] = str(edificio["_id"])  # Converte _id para string
        logger.info(f"Edificio encontrado: {edificio.get('nome', 'N/A')} (ID: {edificio_id})")
        return Edf_Pub_Civil_IBAMAOut(**edificio)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar edifício por ID {edificio_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao buscar edifício: {e}")
