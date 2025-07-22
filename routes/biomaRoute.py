from fastapi import APIRouter, HTTPException, UploadFile, File, Query
from database import bioma_collection
from models.bioma import BiomaCreate, BiomaOut, PaginatedBiomaResponse
from bson import ObjectId
import pandas as pd
import io
import numpy as np

router = APIRouter(prefix="/biomas", tags=["Biomas"])

COLUNA_BIOMAS = {
    "SEQ_AUTO_INFRACAO": "seq_auto_infracao",
    "NUM_AUTO_INFRACAO": "num_auto_infracao",
    "CD_SERIE_AUTO_INFRACAO": "cd_serie_auto_infracao",
    "BIOMA": "bioma",
    "ULTIMA_ATUALIZACAO_RELATORIO": "ultima_atualizacao"
}

def normalize_column_name(column_name: str) -> str:
    return column_name.strip().lower().replace(" ", "_")

@router.post("/upload/biomas")
async def upload_biomas(file: UploadFile = File(...)):
    try:
        df = pd.read_csv(
            io.BytesIO(await file.read()),
            sep=";",
            encoding="utf-8",
            dtype=str,  # Força tudo como string inicialmente
            keep_default_na=False,  # Não converte valores vazios para NaN
            na_values=['']  # Trata apenas strings vazias como NA
        )

        rename_map = {
            csv_col: model_col
            for csv_col, model_col in COLUNA_BIOMAS.items()
            if csv_col in df.columns
        }
        
        if not rename_map:
            available_columns = list(df.columns)
            raise HTTPException(
                status_code=400,
                detail=f"Nenhuma coluna válida encontrada no CSV. Colunas disponíveis: {available_columns}. Colunas esperadas: {list(COLUNA_BIOMAS.keys())}"
            )
            
        df = df[list(rename_map.keys())].rename(columns=rename_map)

        df = df.replace({np.nan: None, '': None})
        
        registros = []
        registros_com_erro = []
        
        for index, row in df.iterrows():
            try:
                registro_dict = row.to_dict()
                
                # O modelo Pydantic agora cuida da conversão de tipos
                bioma = BiomaCreate(**registro_dict)
                registros.append(bioma.model_dump())
                
            except Exception as e:
                erro_info = {
                    "linha": index + 1,
                    "dados": row.to_dict(),
                    "erro": str(e)
                }
                registros_com_erro.append(erro_info)
                print(f"Erro ao processar linha {index + 1}: {registro_dict} | Erro: {e}")
                continue
        
        if not registros:
            raise HTTPException(
                status_code=400, 
                detail=f"Nenhum registro válido foi encontrado no CSV. Total de erros: {len(registros_com_erro)}"
            )
        
        resultado = await bioma_collection.insert_many(registros)
        
        return {
            "message": "Upload realizado com sucesso!",
            "total_processados": len(df),
            "total_inseridos": len(resultado.inserted_ids),
            "total_erros": len(registros_com_erro),
            "detalhes_erros": registros_com_erro[:5] if registros_com_erro else [] 
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno do servidor: {str(e)}")


@router.get("/biomas", response_model=PaginatedBiomaResponse)
async def get_biomas(page: int = Query(1, ge=1), page_size: int = Query(10, ge=1)):
    try:
        skip = (page - 1) * page_size
        total = await bioma_collection.count_documents({})
        biomas = await bioma_collection.find({}).skip(skip).limit(page_size).to_list(length=page_size)

        def serialize(doc):
            doc["_id"] = str(doc["_id"])
            return doc

        items = [BiomaOut(**serialize(doc)) for doc in biomas]

        return PaginatedBiomaResponse(
            total=total,
            page=page,
            size=page_size,
            items=items
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar documentos: {e}")

@router.get("/biomas/{bioma_id}", response_model=BiomaOut)
async def obter_bioma(bioma_id: str):
    """Obter um bioma específico pelo ID"""
    try:
        if not ObjectId.is_valid(bioma_id):
            raise HTTPException(status_code=400, detail="ID inválido")
        
        bioma = await bioma_collection.find_one({"_id": ObjectId(bioma_id)})
        if not bioma:
            raise HTTPException(status_code=404, detail="Bioma não encontrado")
        
        bioma["_id"] = str(bioma["_id"])
        return BiomaOut(**bioma)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar bioma: {str(e)}")

@router.get("/biomas/stats/contagem")
async def contar_biomas():
    """Contar total de biomas na base"""
    try:
        total = await bioma_collection.count_documents({})
        return {"total_biomas": total}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao contar biomas: {str(e)}")