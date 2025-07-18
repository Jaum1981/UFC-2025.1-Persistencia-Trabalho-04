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
    
class Enquadramento(BaseModel):
    seq_auto_infracao: int
    num_auto_infracao: str
    sq_enquadramento: int
    administrativo: str
    tp_norma: str
    nu_norma: int
    ultima_atualizacao: datetime

class EnquadramentoCreate(Enquadramento):
    pass

class EnquadramentoOut(Enquadramento):
    id: Optional[str] = Field(default=None, alias="_id")

    class Config:
        json_encoders = {
            ObjectId: str
        }
        populate_by_name = True