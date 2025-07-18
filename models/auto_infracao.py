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
    
class AutoInfracao(BaseModel):
    seq_auto_infracao: int
    tipo_auto: str
    val_auto_infracao: float
    motivacao_conduta: str
    efeito_saude_publica: str
    dat_hora_auto_infracao: datetime
    municipio: str
    num_longitude: float
    num_latitude: float
    bioma: str

class AutoInfracaoCreate(AutoInfracao):
    pass

class AutoInfracaoOut(AutoInfracao):
    id: Optional[str] = Field(default=None, alias="_id")

    class Config:
        json_encoders = {
            ObjectId: str
        }
        populate_by_name = True