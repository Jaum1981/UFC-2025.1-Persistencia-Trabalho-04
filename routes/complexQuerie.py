import traceback
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from database import auto_infracao_collection, especime_collection, enquadramento_collection, bioma_collection
import matplotlib.pyplot as plt
import pandas as pd
import io
from datetime import datetime, timedelta
import math
from logs.logger import logger
from typing import List, Dict, Any
from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional, List
from datetime import datetime
from database import auto_infracao_collection, bioma_collection
from logs.logger import logger

from models.auto_infracao import AutoInfracaoOut
from models.bioma import BiomaOut
from models.especime import EspecimeOut

router = APIRouter()

@router.get("/auto-infracao-enquadramento/{seq_auto_infracao}")
async def buscar_auto_infracao_com_enquadramento(seq_auto_infracao: int) -> Dict[str, Any]:
    """
    Busca um auto de infração e seus respectivos enquadramentos através do SEQ_AUTO_INFRACAO.
    
    Args:
        seq_auto_infracao: Sequencial do auto de infração
        
    Returns:
        Dicionário contendo os dados do auto de infração e lista de enquadramentos
    """
    try:
        logger.info(f"Buscando auto de infração e enquadramentos para SEQ_AUTO_INFRACAO: {seq_auto_infracao}")
        
        # Buscar o auto de infração
        auto_infracao = await auto_infracao_collection.find_one(
            {"seq_auto_infracao": seq_auto_infracao}
        )
        
        if not auto_infracao:
            raise HTTPException(
                status_code=404, 
                detail=f"Auto de infração com SEQ_AUTO_INFRACAO {seq_auto_infracao} não encontrado"
            )
        
        # Buscar todos os enquadramentos relacionados
        enquadramentos_cursor = enquadramento_collection.find(
            {"seq_auto_infracao": seq_auto_infracao}
        )
        enquadramentos = await enquadramentos_cursor.to_list(length=None)
        
        # Converter ObjectId para string nos resultados
        if auto_infracao.get("_id"):
            auto_infracao["_id"] = str(auto_infracao["_id"])
            
        for enquadramento in enquadramentos:
            if enquadramento.get("_id"):
                enquadramento["_id"] = str(enquadramento["_id"])
        
        # Retornar resultado estruturado
        resultado = {
            "seq_auto_infracao": seq_auto_infracao,
            "auto_infracao": auto_infracao,
            "enquadramentos": enquadramentos,
            "total_enquadramentos": len(enquadramentos)
        }
        
        logger.info(f"Consulta realizada com sucesso. Encontrados {len(enquadramentos)} enquadramentos")
        return resultado
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar auto de infração e enquadramentos: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Erro interno do servidor: {str(e)}"
        )

@router.get("/auto-infracao-enquadramento-multiplos")
async def buscar_multiplos_autos_com_enquadramento(
    seq_auto_infracoes: str = Query(..., description="Lista de SEQ_AUTO_INFRACAO separados por vírgula"),
    limite: int = Query(100, description="Número máximo de resultados", ge=1, le=1000)
) -> Dict[str, Any]:
    """
    Busca múltiplos autos de infração e seus respectivos enquadramentos.
    
    Args:
        seq_auto_infracoes: String com SEQ_AUTO_INFRACAO separados por vírgula (ex: "123,456,789")
        limite: Número máximo de resultados a retornar
        
    Returns:
        Dicionário contendo lista de autos de infração com seus enquadramentos
    """
    try:
        # Converter string em lista de inteiros
        try:
            seq_list = [int(seq.strip()) for seq in seq_auto_infracoes.split(",")]
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Formato inválido para seq_auto_infracoes. Use números separados por vírgula."
            )
        
        # Limitar o número de consultas
        if len(seq_list) > limite:
            seq_list = seq_list[:limite]
        
        logger.info(f"Buscando {len(seq_list)} autos de infração com enquadramentos")
        
        resultados = []
        
        for seq_auto_infracao in seq_list:
            # Buscar o auto de infração
            auto_infracao = await auto_infracao_collection.find_one(
                {"seq_auto_infracao": seq_auto_infracao}
            )
            
            if auto_infracao:
                # Buscar enquadramentos relacionados
                enquadramentos_cursor = enquadramento_collection.find(
                    {"seq_auto_infracao": seq_auto_infracao}
                )
                enquadramentos = await enquadramentos_cursor.to_list(length=None)
                
                # Converter ObjectId para string
                if auto_infracao.get("_id"):
                    auto_infracao["_id"] = str(auto_infracao["_id"])
                    
                for enquadramento in enquadramentos:
                    if enquadramento.get("_id"):
                        enquadramento["_id"] = str(enquadramento["_id"])
                
                resultado_item = {
                    "seq_auto_infracao": seq_auto_infracao,
                    "auto_infracao": auto_infracao,
                    "enquadramentos": enquadramentos,
                    "total_enquadramentos": len(enquadramentos)
                }
                resultados.append(resultado_item)
        
        # Retornar resultado estruturado
        response = {
            "total_encontrados": len(resultados),
            "total_solicitados": len(seq_list),
            "resultados": resultados
        }
        
        logger.info(f"Consulta múltipla realizada com sucesso. {len(resultados)} de {len(seq_list)} encontrados")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar múltiplos autos de infração: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Erro interno do servidor: {str(e)}"
        )

@router.get("/auto-infracao-enquadramento/agregacao/{seq_auto_infracao}")
async def buscar_auto_infracao_agregacao(seq_auto_infracao: int) -> Dict[str, Any]:
    """
    Busca um auto de infração com seus enquadramentos usando agregação MongoDB.
    Esta consulta é mais eficiente pois faz o join diretamente no banco de dados.
    
    Args:
        seq_auto_infracao: Sequencial do auto de infração
        
    Returns:
        Dicionário contendo os dados do auto de infração com enquadramentos agregados
    """
    try:
        logger.info(f"Buscando auto de infração com agregação para SEQ_AUTO_INFRACAO: {seq_auto_infracao}")
        
        # Pipeline de agregação que faz lookup com a coleção de enquadramentos
        pipeline = [
            {
                "$match": {
                    "seq_auto_infracao": seq_auto_infracao
                }
            },
            {
                "$lookup": {
                    "from": "enquadramento",
                    "localField": "seq_auto_infracao",
                    "foreignField": "seq_auto_infracao",
                    "as": "enquadramentos"
                }
            },
            {
                "$addFields": {
                    "total_enquadramentos": {"$size": "$enquadramentos"}
                }
            }
        ]
        
        # Executar a agregação
        cursor = auto_infracao_collection.aggregate(pipeline)
        resultado = await cursor.to_list(length=1)
        
        if not resultado:
            raise HTTPException(
                status_code=404, 
                detail=f"Auto de infração com SEQ_AUTO_INFRACAO {seq_auto_infracao} não encontrado"
            )
        
        # Converter ObjectId para string
        auto_infracao_completo = resultado[0]
        if auto_infracao_completo.get("_id"):
            auto_infracao_completo["_id"] = str(auto_infracao_completo["_id"])
            
        for enquadramento in auto_infracao_completo.get("enquadramentos", []):
            if enquadramento.get("_id"):
                enquadramento["_id"] = str(enquadramento["_id"])
        
        logger.info(f"Consulta com agregação realizada com sucesso. Encontrados {auto_infracao_completo.get('total_enquadramentos', 0)} enquadramentos")
        return auto_infracao_completo
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar auto de infração com agregação: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Erro interno do servidor: {str(e)}"
        )

@router.get("/auto-infracao-completo/{seq_auto_infracao}")
async def buscar_auto_completo(seq_auto_infracao: int) -> Dict[str, Any]:
    """
    Retorna um auto de infração com seus enquadramentos e espécimes relacionados.
    """
    try:
        pipeline = [
            {"$match": {"seq_auto_infracao": seq_auto_infracao}},
            {
                "$lookup": {
                    "from": "enquadramento",
                    "localField": "seq_auto_infracao",
                    "foreignField": "seq_auto_infracao",
                    "as": "enquadramentos"
                }
            },
            {
                "$lookup": {
                    "from": "especime",
                    "localField": "seq_auto_infracao",
                    "foreignField": "seq_auto_infracao",
                    "as": "especimes"
                }
            },
            {
                "$addFields": {
                    "total_enquadramentos": {"$size": "$enquadramentos"},
                    "total_especimes": {"$size": "$especimes"}
                }
            }
        ]

        resultado = await auto_infracao_collection.aggregate(pipeline).to_list(length=1)
        if not resultado:
            raise HTTPException(status_code=404, detail="Auto de infração não encontrado")

        auto = resultado[0]
        auto["_id"] = str(auto["_id"])
        for item in auto["enquadramentos"]:
            item["_id"] = str(item["_id"])
        for item in auto["especimes"]:
            item["_id"] = str(item["_id"])

        return auto
    except Exception as e:
        logger.error("Erro completo:\n" + traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

@router.get("/auto_infracao/detailed")
async def listagem_completa_auto(
    start_date: Optional[str] = Query(None, description="Data inicial ISO (YYYY-MM-DD)"),
    end_date:   Optional[str] = Query(None, description="Data final ISO (YYYY-MM-DD)"),
    municipio:  Optional[str] = Query(None, description="Busca por nome do município"),
    sort_by:    str = Query(
                    "dat_hora_auto_infracao",
                    regex="^(dat_hora_auto_infracao|val_auto_infracao)$",
                    description="Campo para ordenação: 'dat_hora_auto_infracao' ou 'val_auto_infracao'"
                 ),
    order:      str = Query("desc", regex="^(asc|desc)$"),
    page:       int = Query(1, ge=1),
    limit:      int = Query(10, ge=1, le=100)
):
    """
    Retorna autos de infração completos, com enquadramentos e espécies,
    aplicando filtros, ordenação e paginação.
    """
    try:
        logger.info("Iniciando listagem completa de autos de infração")

        # 1) Monta filtros
        match_stage: dict = {}
        if start_date or end_date:
            date_range: dict = {}
            if start_date:
                date_range["$gte"] = f"{start_date}T00:00:00"
            if end_date:
                date_range["$lte"] = f"{end_date}T23:59:59"
            match_stage["dat_hora_auto_infracao"] = date_range
            logger.debug(f"Filtro de data: {date_range!r}")
        if municipio:
            match_stage["municipio"] = municipio
            logger.debug(f"Filtro de município: {municipio!r}")

        # 2) Ordenação e paginação
        direction = 1 if order == "asc" else -1
        skip = (page - 1) * limit
        # Normaliza sort_by para o nome real do campo no MongoDB (minúsculas)
        sort_field = sort_by.lower()
        logger.debug(f"Sort: {sort_field} {order}, skip={skip}, limit={limit}")

        # 3) Pipeline de agregação com lookups limitados usando sub-pipeline
        pipeline: list[dict] = []
        if match_stage:
            pipeline.append({"$match": match_stage})

        pipeline += [
            {"$sort": {sort_field: direction}},
            {"$skip": skip},
            {"$limit": limit},

            # lookup limitado para enquadramentos
            {"$lookup": {
                "from": "enquadramento",
                "let": {"auto_id": "$seq_auto_infracao"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$seq_auto_infracao", "$$auto_id"]}}},
                    {"$limit": 100},
                    {"$project": {"_id": 0, "sq_enquadramento": 1, "tp_norma": 1, "nu_norma": 1}}
                ],
                "as": "enquadramentos"
            }},

            # lookup limitado para espécies
            {"$lookup": {
                "from": "especime",
                "let": {"auto_id": "$seq_auto_infracao"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$seq_auto_infracao", "$$auto_id"]}}},
                    {"$limit": 100},
                    {"$project": {"_id": 0, "seq_especime": 1, "quantidade": 1, "nome_popular": 1}}
                ],
                "as": "especies"
            }},

            # projeta campos principais
            {"$project": {
                "_id": 0,
                "seq_auto_infracao": 1,
                "dat_hora_auto_infracao": 1,
                "municipio": 1,
                "val_auto_infracao": 1,
                "enquadramentos": 1,
                "especies": 1
            }}
        ]
        logger.debug(f"Pipeline montado com {len(pipeline)} estágios")

        # 4) Conta total de documentos que casam com filtros
        count_pipeline = ([{"$match": match_stage}] if match_stage else []) + [{"$count": "total"}]
        count_result = await auto_infracao_collection.aggregate(count_pipeline).to_list(1)
        total = count_result[0]["total"] if count_result else 0
        logger.info(f"Total de autos compatíveis: {total}")

        # 5) Executa agregação com uso de disco
        cursor = auto_infracao_collection.aggregate(pipeline, allowDiskUse=True)
        results = await cursor.to_list(length=limit)
        logger.info(f"Retornados {len(results)} registros na página {page}")

        # 6) Converte objetos datetime para string
        def convert_datetime_to_string(obj):
            if isinstance(obj, dict):
                return {key: convert_datetime_to_string(value) for key, value in obj.items()}
            elif isinstance(obj, list):
                return [convert_datetime_to_string(item) for item in obj]
            elif isinstance(obj, datetime):
                return obj.isoformat()
            else:
                return obj

        results = convert_datetime_to_string(results)

        # 7) Retorna resposta
        return JSONResponse({
            "meta": {"page": page, "limit": limit, "total": total},
            "data": results
        })

    except Exception as e:
        logger.error(f"Erro na listagem completa de autos de infração: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Erro ao executar consulta completa de autos de infração.")

@router.get("/infractions-by-biome")
async def stats_infracoes_bioma(
    start_date: Optional[str] = Query(None, description="Data inicial ISO (YYYY-MM-DD)"),
    end_date:   Optional[str] = Query(None, description="Data final ISO (YYYY-MM-DD)"),
    bioma:      str = Query(..., description="Nome do bioma para filtrar"),
    sort_by:    str = Query(
                    "total_infracoes",
                    regex="^(total_infracoes|media_valor)$",
                    description="Campo para ordenação: 'total_infracoes' ou 'media_valor'"
                 ),
    order:      str = Query("desc", regex="^(asc|desc)$"),
    page:       int = Query(1, ge=1),
    limit:      int = Query(10, ge=1, le=100)
):
    """
    Estatísticas de autos de infração para um único bioma: total, média de valor e data da última atualização.
    """
    try:
        logger.info("Iniciando agregação de estatísticas por bioma")

        # 1) Filtro de data e bioma (obrigatório)
        match_stage: dict = {"ds_biomas_atingidos": bioma}  # usa o campo correto do auto_infracao
        if start_date or end_date:
            date_range: dict = {}
            if start_date:
                date_range["$gte"] = f"{start_date}T00:00:00"
            if end_date:
                date_range["$lte"] = f"{end_date}T23:59:59"
            match_stage["dat_hora_auto_infracao"] = date_range
            logger.debug(f"Filtro de data: {date_range}")
        logger.debug(f"Filtro de bioma: {bioma}")

        # 2) Ordenação e paginação
        sort_dir = 1 if order == "asc" else -1
        skip = (page - 1) * limit

        # 3) Pipeline principal
        pipeline: list[dict] = [
            {"$match": match_stage},
            {"$group": {
                "_id": "$ds_biomas_atingidos",  # agrupa pelo campo correto
                "total_infracoes": {"$sum": 1},
                "media_valor": {"$avg": {"$toDouble": {"$replaceAll": {"input": "$val_auto_infracao", "find": ",", "replacement": "."}}}}
            }},
            {"$lookup": {
                "from": "bioma",
                "let": {"nome_bioma": "$_id"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$bioma", "$$nome_bioma"]}}},
                    {"$sort": {"ultima_atualizacao_relatorio": -1}},
                    {"$limit": 1},
                    {"$project": {"_id": 0, "ultima_atualizacao": "$ultima_atualizacao_relatorio"}}
                ],
                "as": "bioma_info"
            }},
            {"$project": {
                "_id": 0,
                "bioma": "$_id",
                "total_infracoes": 1,
                "media_valor": 1,
                "ultima_atualizacao": {"$arrayElemAt": ["$bioma_info.ultima_atualizacao", 0]}
            }},
            {"$sort": {sort_by: sort_dir}},
            {"$skip": skip},
            {"$limit": limit}
        ]
        logger.debug(f"Pipeline: {pipeline}")

        # 4) Total de registros para paginação
        count_pipeline: list[dict] = [
            {"$match": match_stage},
            {"$group": {"_id": "$bioma"}},
            {"$count": "total"}
        ]
        count_result = await auto_infracao_collection.aggregate(count_pipeline).to_list(1)
        total = count_result[0]["total"] if count_result else 0

        # 5) Executa agregação
        cursor = auto_infracao_collection.aggregate(pipeline, allowDiskUse=True)
        results = await cursor.to_list(length=limit)
        logger.info(f"Consulta de estatísticas por bioma retornou {len(results)} registros")

        # 6) Converte objetos datetime para string
        def convert_datetime_to_string(obj):
            if isinstance(obj, dict):
                return {key: convert_datetime_to_string(value) for key, value in obj.items()}
            elif isinstance(obj, list):
                return [convert_datetime_to_string(item) for item in obj]
            elif isinstance(obj, datetime):
                return obj.isoformat()
            else:
                return obj

        results = convert_datetime_to_string(results)

        return JSONResponse({
            "meta": {"page": page, "limit": limit, "total": total},
            "data": results
        })

    except Exception as e:
        logger.error(f"Erro nas estatísticas por bioma: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Erro ao processar estatísticas por bioma.")

@router.get("/auto-infracao/biomas/especimes")
async def busca_especimes_por_bioma_em_auto_infracao(
        data_inicio: datetime = Query(..., description="Data inicial no formato YYYY-MM-DD"),
        data_fim: datetime = Query(..., description="Data final no formato YYYY-MM-DD"),
        bioma: str = Query(..., description="Nome do bioma filtrado"),
        skip:int = Query(1, ge=0, le=10),
        limit: int = Query(100, description="Número máximo", ge=1, len=1000)):
    try:
        logger.info(f"Buscando nomes populares pela bioma: {bioma}")

        filter = {
            "dat_hora_auto_infracao": {
                "$gte": data_inicio,
                "$lte": data_fim
            }
        }

        if bioma:
            filter["bioma"] = {"$regex": bioma, "$options": "i"}

        infra_docs = await auto_infracao_collection.find(filter).skip(skip).limit(limit).to_list(length=None)

        if not infra_docs:
            logger.warning(f"Nenhuma auto infração encontrada no bioma: {bioma}")
            raise HTTPException(status_code=404, detail="Nenhuma auto infração encontrada")

        results = []
        for infra in infra_docs:
            infra_model = AutoInfracaoOut(**infra)

            especimes = await especime_collection.find({
                "seq_auto_infracao": infra_model.seq_auto_infracao
            }).to_list(length=None)

            especimes_models = [EspecimeOut(**e) for e in especimes]

            results.append({
                "auto_infracao": infra_model.dict(),
                "especimes": [e.dict() for e in especimes_models]
            })

        logger.info(f"Encontradas {len(results)} autuações com espécimes")
        return results

    except HTTPException as e:
        logger.error(f"Erro ao buscar nomes populares das espécimes: {e}")
        raise HTTPException(status_code=500, detail=f"Erro interno no servidor: {e}")