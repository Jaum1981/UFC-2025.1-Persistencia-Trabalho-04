from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from models.auto_infracao import AutoInfracaoCreate, AutoInfracaoOut, PaginatedAutoInfracaoResponse
from database import auto_infracao_collection
import matplotlib.pyplot as plt
import pandas as pd
import io
from datetime import datetime, timedelta
import math
from logs.logger import logger

router = APIRouter(prefix="/auto_infracao", tags=["Auto de Infração"])

@router.post("/upload", response_model=list[AutoInfracaoOut])
async def upload_auto_infracao_csv(file: UploadFile = File(...)):
    logger.info(f"Iniciando upload de arquivo CSV de autos de infração: {file.filename}")
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
            "TIPO_AUTO": "tipo_auto",
            "VAL_AUTO_INFRACAO": "val_auto_infracao",
            "MOTIVACAO_CONDUTA": "motivacao_conduta",
            "EFEITO_SAUDE_PUBLICA": "efeito_saude_publica",
            "DAT_HORA_AUTO_INFRACAO": "dat_hora_auto_infracao",
            "MUNICIPIO": "municipio",
            "NUM_LONGITUDE_AUTO": "num_longitude",
            "NUM_LATITUDE_AUTO": "num_latitude",
            "DS_BIOMAS_ATINGIDOS": "bioma"
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

                valor = str(doc_dict.get("val_auto_infracao") or "").replace(",", ".").strip()
                doc_dict["val_auto_infracao"] = float(valor) if valor else None

                data_str = str(doc_dict.get("dat_hora_auto_infracao") or "").strip()
                doc_dict["dat_hora_auto_infracao"] = (
                    datetime.strptime(data_str, "%Y-%m-%d %H:%M:%S") if data_str else None
                )

                lon = str(doc_dict.get("num_longitude") or "").replace(",", ".").strip()
                lat = str(doc_dict.get("num_latitude") or "").replace(",", ".").strip()
                doc_dict["num_longitude"] = float(lon) if lon else None
                doc_dict["num_latitude"] = float(lat) if lat else None

                # Verifica se campos obrigatórios estão presentes e válidos
                campos_criticos = [
                    doc_dict["seq_auto_infracao"],
                    doc_dict["val_auto_infracao"],
                    doc_dict["dat_hora_auto_infracao"],
                    doc_dict["num_longitude"],
                    doc_dict["num_latitude"]
                ]
                if any(
                    val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val)))
                    for val in campos_criticos
                ):
                    raise ValueError("Valores obrigatórios ausentes ou inválidos.")

                auto = AutoInfracaoCreate(**doc_dict)
                documentos.append(auto.dict())

            except Exception as e:
                erros_processamento += 1
                logger.warning(f"Erro na linha {i+1}: {e}")
                continue

        if erros_processamento > 0:
            logger.warning(f"Total de {erros_processamento} erros durante o processamento")

        if not documentos:
            logger.error("Nenhum registro válido encontrado após processamento")
            raise HTTPException(400, "Nenhum registro válido encontrado.")

        logger.info(f"Processando inserção de {len(documentos)} autos de infração válidos")
        res = await auto_infracao_collection.insert_many(documentos)
        
        logger.info(f"Upload concluído: {len(res.inserted_ids)} autos de infração inseridos com sucesso")
        return [
            AutoInfracaoOut(**{**doc, "_id": str(res.inserted_ids[idx])})
            for idx, doc in enumerate(documentos)
        ]

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro interno no upload de autos de infração: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao processar arquivo: {e}")
    
@router.get("/stats/auto_infracao/top_municipios")
async def get_top_municipios_auto_infracao():
    """
    Top 5 municípios com maior número de autos de infração.
    """
    logger.info("Gerando ranking de municípios com mais autos de infração")
    try:
        pipeline = [
            {"$group": {
                "_id": "$municipio",
                "total": {"$sum": 1}
            }},
            {"$sort": {"total": -1}},
            {"$limit": 5},
            {"$project": {
                "municipio": "$_id",
                "total": 1,
                "_id": 0
            }}
        ]
        stats = await auto_infracao_collection.aggregate(pipeline).to_list(None)
        return {"top_municipios": stats}
    except Exception as e:
        logger.error(f"Erro ao gerar ranking de municípios: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/count_auto_infracao")
async def count_auto_infracao():
    logger.info("Contando total de autos de infração na coleção")
    try:
        count = await auto_infracao_collection.count_documents({})
        logger.info(f"Total de autos de infração encontrados: {count}")
        return {"count": count}
    except Exception as e:
        logger.error(f"Erro ao contar autos de infração: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao contar documentos: {e}")

@router.get("/get_by_date", response_model=list[AutoInfracaoOut])
async def get_auto_infracao_by_date(data: str = Query(..., description="Formato: AAAA-MM-DD")):
    logger.info(f"Buscando autos de infração por data: {data}")
    try:
        # Converte a string para datetime (início do dia)
        data_inicio = datetime.strptime(data, "%Y-%m-%d")
        # Define o fim do dia (23h59m59s)
        data_fim = data_inicio + timedelta(days=1)

        documentos = await auto_infracao_collection.find({
            "dat_hora_auto_infracao": {
                "$gte": data_inicio,
                "$lt": data_fim
            }
        }).to_list(length=None)

        logger.info(f"Encontrados {len(documentos)} autos de infração para a data {data}")
        # Garante que o _id seja convertido para string se necessário
        return [AutoInfracaoOut(**{**doc, "_id": str(doc["_id"])}) for doc in documentos]

    except ValueError:
        logger.warning(f"Formato de data inválido fornecido: {data}")
        raise HTTPException(status_code=400, detail="Formato de data inválido. Use AAAA-MM-DD.")
    except Exception as e:
        logger.error(f"Erro ao buscar autos de infração por data {data}: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao buscar documentos: {e}")

@router.get("/auto_infracoes", response_model=PaginatedAutoInfracaoResponse)
async def get_auto_infracoes(page: int = 1, page_size: int = 10):
    logger.info(f"Buscando autos de infração - Página: {page}, Tamanho: {page_size}")
    try:
        total = await auto_infracao_collection.count_documents({})
        items = await auto_infracao_collection.find().skip((page - 1) * page_size).limit(page_size).to_list(length=None)
        
        logger.info(f"Retornando {len(items)} autos de infração de um total de {total}")
        return PaginatedAutoInfracaoResponse(total=total, page=page, size=page_size, items=items)
    except Exception as e:
        logger.error(f"Erro ao buscar autos de infração paginados: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao buscar documentos: {e}")
    
@router.get("/auto_infracaoget_by_id/{id}", response_model=AutoInfracaoOut)
async def get_auto_by_id(id: str):
    logger.info(f"Buscando auto de infração por ID: {id}")
    try:
        if not ObjectId.is_valid(id):
            logger.warning(f"ID inválido fornecido: {id}")
            raise HTTPException(status_code=400, detail="ID inválido")
            
        auto = await auto_infracao_collection.find_one({"_id": ObjectId(id)})
        if not auto:
            logger.warning(f"Auto de infração não encontrado para ID: {id}")
            raise HTTPException(status_code=404, detail="Auto de infração não encontrado")
            
        logger.info(f"Auto de infração encontrado: {auto.get('seq_auto_infracao', 'N/A')} (ID: {id})")
        return AutoInfracaoOut(**{**auto, "_id": str(auto["_id"])})
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar auto de infração por ID {id}: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao buscar por id: {e}")
    
@router.get("/auto_infracao_report")
async def get_auto_infracao_report():
    logger.info("Gerando relatório de distribuição dos efeitos à saúde pública")
    try:
        cursor = auto_infracao_collection.find({})
        data = await cursor.to_list(length=None)

        if not data:
            logger.warning("Nenhum dado encontrado para gerar o relatório")
            raise HTTPException(status_code=404, detail="Nenhum dado encontrado para gerar o relatório.")

        logger.info(f"Processando relatório com {len(data)} registros")
        df = pd.DataFrame(data)
        if "efeito_saude_publica" not in df.columns:
            logger.error("Coluna 'efeito_saude_publica' não encontrada nos dados")
            raise HTTPException(status_code=400, detail="Coluna 'efeito_saude_publica' não encontrada.")

        efeito_counts = df["efeito_saude_publica"].value_counts()
        logger.info(f"Distribuição dos efeitos: {dict(efeito_counts)}")

        plt.figure(figsize=(8, 6))
        efeito_counts.plot(kind="bar", color="skyblue")
        plt.title("Distribuição dos Efeitos à Saúde Pública")
        plt.xlabel("Efeito à Saúde Pública")
        plt.ylabel("Quantidade")
        plt.tight_layout()

        img_bytes = io.BytesIO()
        plt.savefig(img_bytes, format="png")
        img_bytes.seek(0)

        logger.info("Relatório gráfico gerado com sucesso")
        return StreamingResponse(img_bytes, media_type="image/png")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao gerar relatório: {e}")
        raise HTTPException(status_code=500, detail="Erro ao gerar relatório.")
    finally:
        plt.close()

@router.get("/auto_infracao/nearby")
async def get_nearby_auto_infracao(
    longitude: float = Query(..., description="Longitude do ponto de referência"),
    latitude: float = Query(..., description="Latitude do ponto de referência"),
    radius: int = Query(10000, description="Raio de busca em metros")
):
    logger.info(f"Buscando autos de infração próximos - Lat: {latitude}, Long: {longitude}, Raio: {radius}m")
    try:
        # Converte metros para graus (aproximadamente)
        # 1 grau ≈ 111,320 metros no equador
        radius_degrees = radius / 111320
        
        query = {
            "num_longitude": {
                "$gte": longitude - radius_degrees,
                "$lte": longitude + radius_degrees
            },
            "num_latitude": {
                "$gte": latitude - radius_degrees,
                "$lte": latitude + radius_degrees
            }
        }
        
        docs = await auto_infracao_collection.find(query).to_list(length=None)
        if not docs:
            logger.warning(f"Nenhum auto de infração encontrado próximo às coordenadas {latitude}, {longitude} dentro de {radius}m")
            raise HTTPException(404, "Nenhum auto de infração próximo encontrado dentro da distância especificada.")
        
        logger.info(f"Encontrados {len(docs)} autos de infração na área de busca inicial")
        
        def calculate_distance(lat1, lon1, lat2, lon2):
            # Fórmula de Haversine para calcular distância entre coordenadas
            R = 6371000  # Raio da Terra em metros
            phi1 = math.radians(lat1)
            phi2 = math.radians(lat2)
            delta_phi = math.radians(lat2 - lat1)
            delta_lambda = math.radians(lon2 - lon1)
            
            a = math.sin(delta_phi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2)**2
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
            
            return R * c
        
        results = []
        for doc in docs:
            if doc.get("num_latitude") and doc.get("num_longitude"):
                distance = calculate_distance(
                    latitude, longitude,
                    doc["num_latitude"], doc["num_longitude"]
                )
                if distance <= radius:
                    doc["distance"] = distance
                    doc["_id"] = str(doc["_id"])
                    results.append(doc)
        
        if not results:
            logger.warning(f"Nenhum auto de infração dentro do raio exato de {radius}m após cálculo de distância")
            raise HTTPException(404, "Nenhum auto de infração próximo encontrado dentro da distância especificada.")
        
        results.sort(key=lambda x: x["distance"])
        closest = results[0]
        closest_distance = closest.get("distance", 0)
        closest.pop("distance", None)  
        
        logger.info(f"Auto de infração mais próximo encontrado a {closest_distance:.2f}m de distância (ID: {closest['_id']})")
        return AutoInfracaoOut(**closest)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar autos de infração próximos: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao buscar infrações próximas: {e}")