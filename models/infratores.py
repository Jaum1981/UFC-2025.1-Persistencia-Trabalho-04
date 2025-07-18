from datetime import datetime
from typing import List, Optional
from bson import ObjectId
from pydantic import BaseModel, Field

class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v, *args, **kwargs):
        if not ObjectId.is_valid(v):
            raise ValueError("ID inválido")
        return str(v)
    
class Infrator(BaseModel):
    nome_infrator: str
    infracao_area: str
    municipio: str
    estado: str
    des_local_infracao: str
    historico_infracoes: List[str] = []
    dt_inicio_ato_inequivoco: datetime
    dt_fim_ato_inequivoco: datetime