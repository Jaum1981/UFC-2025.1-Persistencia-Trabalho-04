import traceback
from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from database import auto_infracao_collection, especime_collection, enquadramento_collection
import matplotlib.pyplot as plt
import pandas as pd
import io
from datetime import datetime, timedelta
import math
from logs.logger import logger
from typing import List, Dict, Any

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