from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional, List
from datetime import datetime
from database import auto_infracao_collection, bioma_collection
from logs.logger import logger

router = APIRouter(prefix="/complex", tags=["Complex Routes"])

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
            match_stage["DAT_HORA_AUTO_INFRACAO"] = date_range
            logger.debug(f"Filtro de data: {date_range!r}")
        if municipio:
            match_stage["MUNICIPIO"] = municipio
            logger.debug(f"Filtro de município: {municipio!r}")

        # 2) Ordenação e paginação
        direction = 1 if order == "asc" else -1
        skip = (page - 1) * limit
        # Normaliza sort_by para o nome real do campo no MongoDB (maiúsculas)
        sort_field = sort_by.upper()
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
                "let": {"auto_id": "$SEQ_AUTO_INFRACAO"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$SEQ_AUTO_INFRACAO", "$$auto_id"]}}},
                    {"$limit": 100},
                    {"$project": {"_id": 0, "SQ_ENQUADRAMENTO": 1, "TP_NORMA": 1, "NU_NORMA": 1}}
                ],
                "as": "enquadramentos"
            }},

            # lookup limitado para espécies
            {"$lookup": {
                "from": "especime",
                "let": {"auto_id": "$SEQ_AUTO_INFRACAO"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$SEQ_AUTO_INFRACAO", "$$auto_id"]}}},
                    {"$limit": 100},
                    {"$project": {"_id": 0, "SEQ_ESPECIME": 1, "QUANTIDADE": 1, "NOME_POPULAR": 1}}
                ],
                "as": "especies"
            }},

            # projeta campos principais
            {"$project": {
                "_id": 0,
                "SEQ_AUTO_INFRACAO": 1,
                "DAT_HORA_AUTO_INFRACAO": 1,
                "MUNICIPIO": 1,
                "VAL_AUTO_INFRACAO": 1,
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

        # 6) Retorna resposta
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
        match_stage: dict = {"DS_BIOMAS_ATINGIDOS": bioma}  # usa o campo correto do auto_infracao
        if start_date or end_date:
            date_range: dict = {}
            if start_date:
                date_range["$gte"] = f"{start_date}T00:00:00"
            if end_date:
                date_range["$lte"] = f"{end_date}T23:59:59"
            match_stage["DAT_HORA_AUTO_INFRACAO"] = date_range
            logger.debug(f"Filtro de data: {date_range}")
        logger.debug(f"Filtro de bioma: {bioma}")

        # 2) Ordenação e paginação
        sort_dir = 1 if order == "asc" else -1
        skip = (page - 1) * limit

        # 3) Pipeline principal
        pipeline: list[dict] = [
            {"$match": match_stage},
            {"$group": {
                "_id": "$DS_BIOMAS_ATINGIDOS",  # agrupa pelo campo correto
                "total_infracoes": {"$sum": 1},
                "media_valor": {"$avg": {"$toDouble": {"$replaceAll": {"input": "$VAL_AUTO_INFRACAO", "find": ",", "replacement": "."}}}}
            }},
            {"$lookup": {
                "from": "bioma",
                "let": {"nome_bioma": "$_id"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$BIOMA", "$$nome_bioma"]}}},
                    {"$sort": {"ULTIMA_ATUALIZACAO_RELATORIO": -1}},
                    {"$limit": 1},
                    {"$project": {"_id": 0, "ultima_atualizacao": "$ULTIMA_ATUALIZACAO_RELATORIO"}}
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
            {"$group": {"_id": "$BIOMA"}},
            {"$count": "total"}
        ]
        count_result = await auto_infracao_collection.aggregate(count_pipeline).to_list(1)
        total = count_result[0]["total"] if count_result else 0

        # 5) Executa agregação
        cursor = auto_infracao_collection.aggregate(pipeline, allowDiskUse=True)
        results = await cursor.to_list(length=limit)
        logger.info(f"Consulta de estatísticas por bioma retornou {len(results)} registros")

        return JSONResponse({
            "meta": {"page": page, "limit": limit, "total": total},
            "data": results
        })

    except Exception as e:
        logger.error(f"Erro nas estatísticas por bioma: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Erro ao processar estatísticas por bioma.")
