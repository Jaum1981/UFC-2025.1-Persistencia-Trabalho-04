from fastapi import APIRouter, HTTPException, UploadFile, File, Query
from fastapi.responses import StreamingResponse
from database import bioma_collection
from models.bioma import BiomaCreate, BiomaOut, PaginatedBiomaResponse
from bson import ObjectId
import pandas as pd
import io
import math
import numpy as np
import matplotlib.pyplot as plt
from typing import Optional
from pydantic import BaseModel
from typing import List, Optional
from logs.logger import logger

class PaginationMeta(BaseModel):
    total_items: int
    total_pages: int
    current_page: int
    limit: int

class PaginatedBiomasResponse(BaseModel):
    meta: PaginationMeta
    data: List[BiomaOut]

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
    logger.info(f"Iniciando upload de arquivo CSV de biomas: {file.filename}")
    try:
        df = pd.read_csv(
            io.BytesIO(await file.read()),
            sep=";",
            encoding="utf-8",
            dtype=str,  # Força tudo como string inicialmente
            keep_default_na=False,  # Não converte valores vazios para NaN
            na_values=['']  # Trata apenas strings vazias como NA
        )
        
        logger.info(f"Arquivo CSV carregado com {len(df)} linhas")

        rename_map = {
            csv_col: model_col
            for csv_col, model_col in COLUNA_BIOMAS.items()
            if csv_col in df.columns
        }
        
        if not rename_map:
            available_columns = list(df.columns)
            logger.error(f"Nenhuma coluna válida encontrada. Disponíveis: {available_columns}")
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
                logger.warning(f"Erro ao processar linha {index + 1}: {registro_dict} | Erro: {e}")
                continue
        
        if not registros:
            logger.error(f"Nenhum registro válido encontrado. Total de erros: {len(registros_com_erro)}")
            raise HTTPException(
                status_code=400, 
                detail=f"Nenhum registro válido foi encontrado no CSV. Total de erros: {len(registros_com_erro)}"
            )
        
        logger.info(f"Processando inserção de {len(registros)} biomas válidos")
        resultado = await bioma_collection.insert_many(registros)
        
        logger.info(f"Upload concluído: {len(resultado.inserted_ids)} biomas inseridos com sucesso")
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
        logger.error(f"Erro interno no upload de biomas: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro interno do servidor: {str(e)}")


@router.get("/biomas", response_model=PaginatedBiomaResponse)
async def get_biomas(page: int = Query(1, ge=1), page_size: int = Query(10, ge=1)):
    logger.info(f"Buscando biomas - Página: {page}, Tamanho: {page_size}")
    try:
        skip = (page - 1) * page_size
        total = await bioma_collection.count_documents({})
        biomas = await bioma_collection.find({}).skip(skip).limit(page_size).to_list(length=page_size)

        def serialize(doc):
            doc["_id"] = str(doc["_id"])
            return doc

        items = [BiomaOut(**serialize(doc)) for doc in biomas]

        logger.info(f"Retornando {len(items)} biomas de um total de {total}")
        return PaginatedBiomaResponse(
            total=total,
            page=page,
            size=page_size,
            items=items
        )

    except Exception as e:
        logger.error(f"Erro ao buscar biomas: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao buscar documentos: {e}")

@router.get("/biomas/{bioma_id}", response_model=BiomaOut)
async def obter_bioma(bioma_id: str):
    """Obter um bioma específico pelo ID"""
    logger.info(f"Buscando bioma por ID: {bioma_id}")
    try:
        if not ObjectId.is_valid(bioma_id):
            logger.warning(f"ID inválido fornecido: {bioma_id}")
            raise HTTPException(status_code=400, detail="ID inválido")
        
        bioma = await bioma_collection.find_one({"_id": ObjectId(bioma_id)})
        if not bioma:
            logger.warning(f"Bioma não encontrado para ID: {bioma_id}")
            raise HTTPException(status_code=404, detail="Bioma não encontrado")
        
        bioma["_id"] = str(bioma["_id"])
        logger.info(f"Bioma encontrado: {bioma.get('bioma', 'N/A')} (ID: {bioma_id})")
        return BiomaOut(**bioma)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar bioma por ID {bioma_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao buscar bioma: {str(e)}")

@router.get("/biomas/stats/contagem")
async def contar_biomas():
    """Contar total de biomas na base"""
    logger.info("Contando total de biomas na coleção")
    try:
        total = await bioma_collection.count_documents({})
        logger.info(f"Total de biomas encontrados: {total}")
        return {"total_biomas": total}
    except Exception as e:
        logger.error(f"Erro ao contar biomas: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao contar biomas: {str(e)}")
    
@router.get("/biomas_report")
async def get_biomas_report():
    """
    Gera um relatório de insights sobre os biomas, incluindo um gráfico.
    """
    logger.info("Gerando relatório de biomas com gráfico")
    try:
        cursor = bioma_collection.find({})
        data = await cursor.to_list(length=None)
        
        if not data:
            logger.warning("Nenhum dado de bioma encontrado para gerar relatório")
            raise HTTPException(status_code=404, detail="Nenhum dado de bioma encontrado para gerar relatório.")

        df = pd.DataFrame(data)
        logger.info(f"Dados carregados para relatório: {len(df)} registros")

        # Insights: Contagem de infrações por bioma
        infracoes_por_bioma = df['bioma'].value_counts().reset_index()
        infracoes_por_bioma.columns = ['bioma', 'total_infracoes']

        # Gerar gráfico
        plt.figure(figsize=(10, 6))
        plt.bar(infracoes_por_bioma['bioma'], infracoes_por_bioma['total_infracoes'], color='skyblue')
        plt.xlabel('Bioma')
        plt.ylabel('Número de Infrações')
        plt.title('Número de Infrações por Bioma')
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        img_bytes = io.BytesIO()
        plt.savefig(img_bytes, format='png')
        img_bytes.seek(0)

        logger.info("Relatório de biomas gerado com sucesso")
        return StreamingResponse(img_bytes, media_type="image/png")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao gerar relatório de biomas: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao gerar relatório de insights: {str(e)}")
    finally:
        plt.close()


@router.get("/stats/summary")
async def get_bioma_stats():
    """
    Agregações e cálculos estatísticos simples.
    """
    logger.info("Gerando estatísticas resumidas de biomas")
    try:
        pipeline = [
            {
                "$group": {
                    "_id": "$bioma",
                    "total_infracoes": {"$sum": 1}
                }
            },
            {
                "$project": {
                    "bioma": "$_id",
                    "total_infracoes": 1,
                    "_id": 0
                }
            }
        ]
        
        stats = await bioma_collection.aggregate(pipeline).to_list(None)
        
        logger.info(f"Estatísticas geradas para {len(stats)} tipos de biomas")
        return {
            "estatisticas_por_bioma": stats
        }
    except Exception as e:
        logger.error(f"Erro ao gerar estatísticas de biomas: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/", response_model=PaginatedBiomasResponse)
async def listar_biomas(
    bioma: Optional[str] = Query(None, description="Filtrar por nome do bioma"),
    skip: int = Query(0, ge=0, description="Número de registros a pular"),
    limit: int = Query(10, ge=1, le=200, description="Número de registros por página")
):
    """
    Listar biomas com metadados de paginação e filtros.
    """
    logger.info(f"Listando biomas - Filtro: {bioma}, Skip: {skip}, Limit: {limit}")
    try:
        query = {}
        if bioma:
            query["bioma"] = {"$regex": bioma, "$options": "i"}
            logger.info(f"Aplicando filtro por bioma: {bioma}")

        total_items = await bioma_collection.count_documents(query)
        total_pages = math.ceil(total_items / limit)
        current_page = (skip // limit) + 1

        cursor = bioma_collection.find(query).skip(skip).limit(limit)
        
        biomas_list = [
            BiomaOut(**{**doc, "_id": str(doc["_id"])}) 
            async for doc in cursor
        ]

        logger.info(f"Retornando {len(biomas_list)} biomas (página {current_page} de {total_pages})")
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
        logger.error(f"Erro ao listar biomas: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao buscar biomas: {str(e)}")