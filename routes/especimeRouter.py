from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query, UploadFile, File
from models.especime import EspecimeCreate, EspecimeOut, PaginatedEspecimeResponse
from database import especime_collection
import pandas as pd
import io

router = APIRouter(prefix="/especime", tags=["Especime"])


@router.post("/upload", response_model=list[EspecimeOut])
async def upload_especime_csv(file: UploadFile = File(...)):
    try:
        df = pd.read_csv(
            io.BytesIO(await file.read()),
            sep=";",
            dtype=str,
            keep_default_na=False,
            na_values=['']
        )

        coluna_map = {
            "SEQ_AUTO_INFRACAO": "seq_auto_infracao",
            "NUM_AUTO_INFRACAO": "num_auto_infracao",
            "SEQ_ESPECIME": "seq_especime",
            "QUANTIDADE": "quantidade",
            "UNIDADE_MEDIDA": "unidade_medida",
            "CARACTERISTICA": "caracteristica",
            "TIPO": "tipo",
            "NOME_CIENTIFICO": "nome_cientifico",
            "NOME_POPULAR": "nome_popular"
        }

        cols_existentes = [col for col in df.columns if col in coluna_map]
        if not cols_existentes:
            raise HTTPException(status_code=400, detail="Nenhuma coluna válida encontrada no CSV.")

        df = df[cols_existentes].rename(columns=coluna_map)

        documentos = []
        for i, row in df.iterrows():
            try:
                doc_dict = row.to_dict()

                # Conversão de tipos
                doc_dict["seq_auto_infracao"] = int(doc_dict["seq_auto_infracao"])
                doc_dict["num_auto_infracao"] = int(doc_dict["num_auto_infracao"])
                doc_dict["seq_especime"] = int(doc_dict["seq_especime"])
                doc_dict["quantidade"] = int(doc_dict["quantidade"])

                especime = EspecimeCreate(**doc_dict)
                documentos.append(especime.dict())
            except Exception as e:
                print(f"Erro na linha {i+1}: {e}")
                continue

        if not documentos:
            raise HTTPException(400, "Nenhum registro válido encontrado.")

        res = await especime_collection.insert_many(documentos)
        return [
            EspecimeOut(**{**doc, "_id": str(res.inserted_ids[idx])})
            for idx, doc in enumerate(documentos)
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar arquivo: {e}")
    
@router.get("/especimes", response_model=PaginatedEspecimeResponse)
async def get_especimes(page: int = Query(1, ge=1), page_size: int = Query(10, ge=1, le=100)):
    try:
        skip = (page - 1) * page_size
        total = await especime_collection.count_documents({})
        especimes = await especime_collection.find({}).skip(skip).limit(page_size).to_list(length=page_size)

        def serialize(doc):
            doc["_id"] = str(doc["_id"])
            return doc

        items = [EspecimeOut(**serialize(doc)) for doc in especimes]

        return PaginatedEspecimeResponse(
            total=total,
            page=page,
            size=page_size,
            items=items
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar documentos: {e}")

@router.get("/count_especime")
async def count_especime():
    try:
        count = await especime_collection.count_documents({})
        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao contar documentos: {e}")
    
@router.get("/especime/{especime_id}", response_model=EspecimeOut)
async def get_especime(especime_id: str):
    try:
        especime = await especime_collection.find_one({"_id": ObjectId(especime_id)})
        if not especime:
            raise HTTPException(status_code=404, detail="Especime não encontrado.")
        especime["_id"] = str(especime["_id"])  # Converte _id para string
        return EspecimeOut(**especime)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar especime: {e}")
