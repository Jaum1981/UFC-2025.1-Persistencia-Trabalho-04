from bson import ObjectId
from fastapi import APIRouter, HTTPException, UploadFile, File
from models.enquadramento import EnquadramentoCreate, EnquadramentoOut, PaginatedEnquadramentoResponse
from database import enquadramento_collection
import pandas as pd
import io
from datetime import datetime
import math
from logs.logger import logger
from typing import List

router = APIRouter(prefix="/enquadramento", tags=["Enquadramento"])

@router.post("/upload", response_model=list[EnquadramentoOut])
async def upload_enquadramento_csv(file: UploadFile = File(...)):
    logger.info(f"Iniciando upload de arquivo CSV de enquadramentos: {file.filename}")
    try:
        df = pd.read_csv(
            io.BytesIO(await file.read()),
            sep=";",
            dtype=str,
            keep_default_na=False,
            na_values=['']
        )
        
        logger.info(f"Arquivo CSV carregado com {len(df)} linhas")

        coluna_map = {
            "SEQ_AUTO_INFRACAO": "seq_auto_infracao",
            "NUM_AUTO_INFRACAO": "num_auto_infracao",
            "SQ_ENQUADRAMENTO": "sq_enquadramento",
            "ADMINISTRATIVO": "administrativo",
            "TP_NORMA": "tp_norma",
            "NU_NORMA": "nu_norma",
            "ULTIMA_ATUALIZACAO_RELATORIO": "ultima_atualizacao"
        }

        df = df[[col for col in coluna_map if col in df.columns]].rename(columns=coluna_map)

        documentos = []
        erros_processamento = 0
        for i, row in df.iterrows():
            try:
                doc_dict = row.to_dict()

                # Conversões seguras
                seq_str = str(doc_dict.get("seq_auto_infracao") or "").strip()
                doc_dict["seq_auto_infracao"] = int(seq_str) if seq_str.isdigit() else None

                sq_str = str(doc_dict.get("sq_enquadramento") or "").strip()
                doc_dict["sq_enquadramento"] = int(sq_str) if sq_str.isdigit() else None

                nu_str = str(doc_dict.get("nu_norma") or "").strip()
                doc_dict["nu_norma"] = int(nu_str) if nu_str.isdigit() else None

                data_str = str(doc_dict.get("ultima_atualizacao") or "").strip()
                doc_dict["ultima_atualizacao"] = (
                    datetime.strptime(data_str, "%Y-%m-%d %H:%M:%S") if data_str else None
                )

                # Verificação de campos obrigatórios
                campos_criticos = [
                    doc_dict["seq_auto_infracao"],
                    doc_dict["num_auto_infracao"],
                    doc_dict["sq_enquadramento"],
                    doc_dict["tp_norma"],
                    doc_dict["nu_norma"],
                    doc_dict["ultima_atualizacao"]
                ]
                if any(val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))) for val in campos_criticos):
                    raise ValueError("Valores obrigatórios ausentes ou inválidos.")

                enquadramento = EnquadramentoCreate(**doc_dict)
                documentos.append(enquadramento.dict())

            except Exception as e:
                erros_processamento += 1
                logger.warning(f"Erro na linha {i+1}: {e}")
                continue

        if not documentos:
            logger.error(f"Nenhum registro válido encontrado. Total de erros: {erros_processamento}")
            raise HTTPException(400, "Nenhum registro válido encontrado.")

        logger.info(f"Processando inserção de {len(documentos)} enquadramentos válidos")
        res = await enquadramento_collection.insert_many(documentos)
        
        logger.info(f"Upload concluído: {len(res.inserted_ids)} enquadramentos inseridos com sucesso")
        return [
            EnquadramentoOut(**{**doc, "_id": str(res.inserted_ids[idx])})
            for idx, doc in enumerate(documentos)
        ]

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro interno no upload de enquadramentos: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao processar arquivo: {e}")
    
@router.get("/stats/enquadramento/tipo_norma")
async def get_stats_enquadramento_tipo_norma():
    """
    Quantidade de enquadramentos por tipo de norma (Lei, Decreto etc.).
    """
    logger.info("Gerando estatísticas de enquadramentos por tipo de norma")
    try:
        pipeline = [
            {"$group": {
                "_id": "$tp_norma",
                "total": {"$sum": 1}
            }},
            {"$project": {
                "tipo_norma": "$_id",
                "total": 1,
                "_id": 0
            }},
            {"$sort": {"total": -1}}
        ]
        stats = await enquadramento_collection.aggregate(pipeline).to_list(None)
        return {"estatisticas_por_tipo_norma": stats}
    except Exception as e:
        logger.error(f"Erro ao gerar estatísticas de enquadramento: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/count_enquadramento")
async def count_enquadramento():
    logger.info("Contando total de enquadramentos na coleção")
    try:
        count = await enquadramento_collection.count_documents({})
        logger.info(f"Total de enquadramentos encontrados: {count}")
        return {"count": count}
    except Exception as e:
        logger.error(f"Erro ao contar enquadramentos: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao contar documentos: {e}")

@router.get("/enquadramentos", response_model=PaginatedEnquadramentoResponse)
async def get_enquadramentos(page: int = 1, page_size: int = 10):
    logger.info(f"Buscando enquadramentos - Página: {page}, Tamanho: {page_size}")
    try:
        total = await enquadramento_collection.count_documents({})
        items = await enquadramento_collection.find().skip((page - 1) * page_size).limit(page_size).to_list(length=None)
        
        logger.info(f"Retornando {len(items)} enquadramentos de um total de {total}")
        return PaginatedEnquadramentoResponse(total=total, page=page, size=page_size, items=items)
    except Exception as e:
        logger.error(f"Erro ao buscar enquadramentos: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao buscar documentos: {e}")


@router.get("/enquadramento/norma_and_adm", response_model=List[EnquadramentoOut])
async def get_enquadramento_by_norma_and_adm(tp_norma: str, administrativo: str, page: int = 1, page_size: int = 10):
    logger.info(f"Buscando enquadramentos por tipo de norma e administrativo: {tp_norma}, {administrativo}")
    try:

        filter = {
            "tp_norma": tp_norma,
            "administrativo": administrativo
        }

        docs = await enquadramento_collection.find(filter).skip((page - 1) * page_size).limit(page_size).to_list(length=None)

        if not docs:
            logger.warning(f"Enquadramentos não encontrados")
            raise HTTPException(status_code=404, detail="Enquadramentos não encontrados.")

        results = []
        for doc in docs:
            doc["_id"] = str(doc["_id"])
            results.append(EnquadramentoOut(**doc))

        logger.info(f"Enquadramentos encontrados")
        return results
    except Exception as e:
        logger.error(f"Erro ao interno ao buscar enquadramentos")
        raise HTTPException(status_code=500, detail=f"Erro ao buscar documento: {e}")

@router.get("/enquadramento/nu_norma", response_model=List[EnquadramentoOut])
async def get_enquadramento_by_nu_norma(nu_norma: int, page: int = 1, page_size: int = 10):
    logger.info(f"Buscando enquadramentos pelo número da norma: {nu_norma}")
    try:
        docs = await enquadramento_collection.find({"nu_norma": nu_norma}).skip((page - 1) * page_size).limit(page_size).to_list(length=None)

        if not docs:
            logger.warning(f"Enquadramentos não encontrados")
            raise HTTPException(status_code=404, detail="Enquadramentos não encontrados.")

        results = []
        for doc in docs:
            doc["_id"] = str(doc["_id"])
            results.append(EnquadramentoOut(**doc))

        logger.info(f"Enquadramentos encontrados")
        return results
    except Exception as e:
        logger.error(f"Erro ao interno ao buscar enquadramentos")
        raise HTTPException(status_code=500, detail=f"Erro ao buscar documento: {e}")

@router.get("/enquadramento/{enquadramento_id}", response_model=EnquadramentoOut)
async def get_enquadramento(enquadramento_id: str):
    logger.info(f"Buscando enquadramento por ID: {enquadramento_id}")
    try:
        if not ObjectId.is_valid(enquadramento_id):
            logger.warning(f"ID inválido fornecido: {enquadramento_id}")
            raise HTTPException(status_code=400, detail="ID inválido")
            
        doc = await enquadramento_collection.find_one({"_id": ObjectId(enquadramento_id)})
        if not doc:
            logger.warning(f"Enquadramento não encontrado para ID: {enquadramento_id}")
            raise HTTPException(status_code=404, detail="Enquadramento não encontrado.")
        
        doc["_id"] = str(doc["_id"])  # converte _id para string
        logger.info(f"Enquadramento encontrado (ID: {enquadramento_id})")
        return EnquadramentoOut(**doc)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar enquadramento por ID {enquadramento_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao buscar documento: {e}")