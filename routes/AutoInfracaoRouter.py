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

router = APIRouter(prefix="/auto_infracao", tags=["Auto de Infração"])

@router.post("/upload", response_model=list[AutoInfracaoOut])
async def upload_auto_infracao_csv(file: UploadFile = File(...)):
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
                print(f"Erro na linha {i+1}: {e}")
                continue

        if not documentos:
            raise HTTPException(400, "Nenhum registro válido encontrado.")

        res = await auto_infracao_collection.insert_many(documentos)
        return [
            AutoInfracaoOut(**{**doc, "_id": str(res.inserted_ids[idx])})
            for idx, doc in enumerate(documentos)
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar arquivo: {e}")


@router.get("/count_auto_infracao")
async def count_auto_infracao():
    try:
        count = await auto_infracao_collection.count_documents({})
        return {"count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao contar documentos: {e}")

@router.get("/get_by_date", response_model=list[AutoInfracaoOut])
async def get_auto_infracao_by_date(data: str = Query(..., description="Formato: AAAA-MM-DD")):
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

        # Garante que o _id seja convertido para string se necessário
        return [AutoInfracaoOut(**{**doc, "_id": str(doc["_id"])}) for doc in documentos]

    except ValueError:
        raise HTTPException(status_code=400, detail="Formato de data inválido. Use AAAA-MM-DD.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar documentos: {e}")

@router.get("/auto_infracoes", response_model=PaginatedAutoInfracaoResponse)
async def get_auto_infracoes(page: int = 1, page_size: int = 10):
    try:
        total = await auto_infracao_collection.count_documents({})
        items = await auto_infracao_collection.find().skip((page - 1) * page_size).limit(page_size).to_list(length=None)
        return PaginatedAutoInfracaoResponse(total=total, page=page, size=page_size, items=items)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar documentos: {e}")
    
@router.get("/auto_infracaoget_by_id/{id}", response_model=AutoInfracaoOut)
async def get_auto_by_id(id: str):
    try:
        auto = await auto_infracao_collection.find_one({"_id": ObjectId(id)})
        if not auto:
            raise HTTPException(status_code=404, detail="Auto de infração não encontrado")
        return AutoInfracaoOut(**{**auto, "_id": str(auto["_id"])})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar por id: {e}")
    
@router.get("/auto_infracao_report")
async def get_auto_infracao_report():
    try:
        cursor = auto_infracao_collection.find({})
        data = await cursor.to_list(length=None)

        if not data:
            raise HTTPException(status_code=404, detail="Nenhum dado encontrado para gerar o relatório.")

        
        df = pd.DataFrame(data)
        if "efeito_saude_publica" not in df.columns:
            raise HTTPException(status_code=400, detail="Coluna 'efeito_saude_publica' não encontrada.")

        efeito_counts = df["efeito_saude_publica"].value_counts()

        plt.figure(figsize=(8, 6))
        efeito_counts.plot(kind="bar", color="skyblue")
        plt.title("Distribuição dos Efeitos à Saúde Pública")
        plt.xlabel("Efeito à Saúde Pública")
        plt.ylabel("Quantidade")
        plt.tight_layout()

        img_bytes = io.BytesIO()
        plt.savefig(img_bytes, format="png")
        img_bytes.seek(0)

        return StreamingResponse(img_bytes, media_type="image/png")
    except HTTPException:
        raise HTTPException(status_code=500, detail="Erro ao gerar relatório.")
    finally:
        plt.close()

@router.get("/auto_infracao/nearby")
async def get_nearby_auto_infracao(
    longitude: float = Query(..., description="Longitude do ponto de referência"),
    latitude: float = Query(..., description="Latitude do ponto de referência"),
    radius: int = Query(10000, description="Raio de busca em metros")
):
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
            raise HTTPException(404, "Nenhum auto de infração próximo encontrado dentro da distância especificada.")
        
        
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
            raise HTTPException(404, "Nenhum auto de infração próximo encontrado dentro da distância especificada.")
        
        results.sort(key=lambda x: x["distance"])
        closest = results[0]
        closest.pop("distance", None)  
        
        return AutoInfracaoOut(**closest)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar infrações próximas: {e}")