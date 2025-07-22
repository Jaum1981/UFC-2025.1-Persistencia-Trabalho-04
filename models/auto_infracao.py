from datetime import datetime
from typing import List
from bson import ObjectId
from pydantic import BaseModel, Field
from models.PyObjectId import PyObjectId
    
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
    id: PyObjectId = Field(..., alias="_id")

    class Config:
        json_encoders = {
            ObjectId: str
        }
        populate_by_name = True

class PaginatedAutoInfracaoResponse(BaseModel):
    total: int
    page: int
    size: int
    items: List[AutoInfracaoOut]

    class Config:
        json_encoders = {
            ObjectId: str
        }
        populate_by_name = True