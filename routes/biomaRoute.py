from fastapi import APIRouter, HTTPException, UploadFile, File, Query
from database import bioma_collection
from models.bioma import BiomaCreate, BiomaOut, PaginatedBiomasResponse, PaginationMeta
from bson import ObjectId
import math
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
            "message": f"Upload realizado com sucesso!",
            "total_processados": len(df),
            "total_inseridos": len(resultado.inserted_ids),
            "total_erros": len(registros_com_erro),
            "detalhes_erros": registros_com_erro[:5] if registros_com_erro else [] 
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno do servidor: {str(e)}")


@router.get("/biomas", response_model=PaginatedBiomasResponse)
async def listar_biomas(
    skip: int = Query(0, ge=0, description="Número de registros a pular"),
    limit: int = Query(10, ge=1, le=200, description="Número de registros por página")
):
    """
    Listar biomas com metadados de paginação.
    """
    try:
        # 1. Contar o total de documentos na coleção (sem paginação)
        total_items = await bioma_collection.count_documents({})

        # 2. Calcular metadados da paginação
        total_pages = math.ceil(total_items / limit)
        current_page = (skip // limit) + 1

        # 3. Buscar os documentos da página atual
        cursor = bioma_collection.find({}).skip(skip).limit(limit)
        
        # Forma mais concisa de montar a lista
        biomas_list = [
            BiomaOut(**{**doc, "_id": str(doc["_id"])}) 
            async for doc in cursor
        ]

        # 4. Montar o objeto de resposta final
        return PaginatedBiomasResponse(
            meta=PaginationMeta(
                total_items=total_items,
                total_pages=total_pages,
                current_page=current_page,
                limit=limit
            ),
            data=biomas_list
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar biomas: {str(e)}")

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