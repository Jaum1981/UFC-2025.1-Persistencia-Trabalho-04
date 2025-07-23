from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query, UploadFile, File
from models.especime import EspecimeCreate, EspecimeOut, PaginatedEspecimeResponse
from database import especime_collection
import pandas as pd
import io
from logs.logger import logger

router = APIRouter(prefix="/especime", tags=["Especime"])


@router.post("/upload", response_model=list[EspecimeOut])
async def upload_especime_csv(file: UploadFile = File(...)):
    logger.info(f"Iniciando upload de arquivo CSV de espécimes: {file.filename}")
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
            logger.error("Nenhuma coluna válida encontrada no CSV")
            raise HTTPException(status_code=400, detail="Nenhuma coluna válida encontrada no CSV.")

        df = df[cols_existentes].rename(columns=coluna_map)

        documentos = []
        erros_processamento = 0
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
                erros_processamento += 1
                logger.warning(f"Erro na linha {i+1}: {e}")
                continue

        if erros_processamento > 0:
            logger.warning(f"Total de {erros_processamento} erros durante o processamento")

        if not documentos:
            logger.error("Nenhum registro válido encontrado após processamento")
            raise HTTPException(400, "Nenhum registro válido encontrado.")

        logger.info(f"Processando inserção de {len(documentos)} espécimes válidos")
        res = await especime_collection.insert_many(documentos)
        
        logger.info(f"Upload concluído: {len(res.inserted_ids)} espécimes inseridos com sucesso")
        return [
            EspecimeOut(**{**doc, "_id": str(res.inserted_ids[idx])})
            for idx, doc in enumerate(documentos)
        ]

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro interno no upload de espécimes: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao processar arquivo: {e}")
    
@router.get("/especimes", response_model=PaginatedEspecimeResponse)
async def get_especimes(page: int = Query(1, ge=1), page_size: int = Query(10, ge=1, le=100)):
    logger.info(f"Buscando espécimes - Página: {page}, Tamanho: {page_size}")
    try:
        skip = (page - 1) * page_size
        total = await especime_collection.count_documents({})
        especimes = await especime_collection.find({}).skip(skip).limit(page_size).to_list(length=page_size)

        def serialize(doc):
            doc["_id"] = str(doc["_id"])
            return doc

        items = [EspecimeOut(**serialize(doc)) for doc in especimes]

        logger.info(f"Retornando {len(items)} espécimes de um total de {total}")
        return PaginatedEspecimeResponse(
            total=total,
            page=page,
            size=page_size,
            items=items
        )

    except Exception as e:
        logger.error(f"Erro ao buscar espécimes: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao buscar documentos: {e}")

@router.get("/count_especime")
async def count_especime():
    logger.info("Contando total de espécimes na coleção")
    try:
        count = await especime_collection.count_documents({})
        logger.info(f"Total de espécimes encontrados: {count}")
        return {"count": count}
    except Exception as e:
        logger.error(f"Erro ao contar espécimes: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao contar documentos: {e}")
    
@router.get("/especime/{especime_id}", response_model=EspecimeOut)
async def get_especime(especime_id: str):
    logger.info(f"Buscando espécime por ID: {especime_id}")
    try:
        if not ObjectId.is_valid(especime_id):
            logger.warning(f"ID inválido fornecido: {especime_id}")
            raise HTTPException(status_code=400, detail="ID inválido")
            
        especime = await especime_collection.find_one({"_id": ObjectId(especime_id)})
        if not especime:
            logger.warning(f"Espécime não encontrado para ID: {especime_id}")
            raise HTTPException(status_code=404, detail="Especime não encontrado.")
            
        especime["_id"] = str(especime["_id"])  # Converte _id para string
        logger.info(f"Espécime encontrado: {especime.get('nome_cientifico', 'N/A')} (ID: {especime_id})")
        return EspecimeOut(**especime)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar espécime por ID {especime_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao buscar especime: {e}")
