from fastapi import APIRouter, HTTPException, UploadFile, File
from models.enquadramento import EnquadramentoCreate, EnquadramentoOut
from database import enquadramento_collection
import pandas as pd
import io
from datetime import datetime
import math

router = APIRouter(prefix="/enquadramento", tags=["Enquadramento"])

@router.post("/upload", response_model=list[EnquadramentoOut])
async def upload_enquadramento_csv(file: UploadFile = File(...)):
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
            "NUM_AUTO_INFRACAO": "num_auto_infracao",
            "SQ_ENQUADRAMENTO": "sq_enquadramento",
            "ADMINISTRATIVO": "administrativo",
            "TP_NORMA": "tp_norma",
            "NU_NORMA": "nu_norma",
            "ULTIMA_ATUALIZACAO_RELATORIO": "ultima_atualizacao"
        }

        df = df[[col for col in coluna_map if col in df.columns]].rename(columns=coluna_map)

        documentos = []
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
                print(f"Erro na linha {i+1}: {e}")
                continue

        if not documentos:
            raise HTTPException(400, "Nenhum registro válido encontrado.")

        res = await enquadramento_collection.insert_many(documentos)
        return [
            EnquadramentoOut(**{**doc, "_id": str(res.inserted_ids[idx])})
            for idx, doc in enumerate(documentos)
        ]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar arquivo: {e}")
