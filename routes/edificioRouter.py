from fastapi import APIRouter, HTTPException, UploadFile, File, Query
from database import edificio_IBAMA_collection
import pandas as pd
import io, re

from models.edificio_IBAMA import Edf_Pub_Civil_IBAMACreate, Edf_Pub_Civil_IBAMAOut


router = APIRouter(prefix="/edf", tags=["Edf Pub Civil IBAMA"])

def dms_to_decimal(dms_str: str) -> float:
    s = re.sub(r'\s+', '', dms_str)
    sign = -1 if re.search(r'[SWsw]', s) else 1
    parts = re.split(r'[°\'"]+', s)
    deg, min_, sec = parts[0], parts[1], parts[2]
    return sign * (float(deg) + float(min_)/60 + float(sec)/3600)


@router.post("/upload", response_model=list[Edf_Pub_Civil_IBAMAOut])
async def upload_edf_csv(file: UploadFile = File(...)):
    df = pd.read_csv(
        io.BytesIO(await file.read()),
        dtype=str,
        keep_default_na=False,
        na_values=['']
    )

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
            print(f"Erro linha {i+1}: {e}")
            continue

    if not docs:
        raise HTTPException(400, "Nenhum registro válido após tratamento.")

    res = await edificio_IBAMA_collection.insert_many(docs)
    return [
        Edf_Pub_Civil_IBAMAOut(
            **{**doc, "_id": str(res.inserted_ids[idx])} 
        )
        for idx, doc in enumerate(docs)
    ]




@router.get("/nearby", response_model=Edf_Pub_Civil_IBAMAOut)
async def nearby(
    lat: float = Query(...),
    long: float = Query(...),
    max_distance: int = Query(10000, description="Distância máxima em metros")
):
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
            raise HTTPException(404, "Nenhuma unidade próxima encontrada dentro da distância especificada.")
        doc = docs[0]
        # converte _id para string antes de criar o modelo
        doc["_id"] = str(doc["_id"])
        return Edf_Pub_Civil_IBAMAOut(**doc)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Erro na consulta geoespacial: {e}")
    
@router.get("/count_edificio")
async def count_edificio():
    try:
        count = await edificio_IBAMA_collection.count_documents({})
        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao contar documentos: {e}")
