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
    
class Especime(BaseModel):
    seq_auto_infracao: int
    num_auto_infracao: int
    seq_especime: int
    quantidade: int
    unidade_medida: str
    caracteristica: str
    tipo: str
    nome_cientifico: str
    nome_popular: str

class EspecimeCreate(Especime):
    pass

class EspecimeOut(Especime):
    id: Optional[str] = Field(default=None, alias="_id")

    class Config:
        json_encoders = {
            ObjectId: str
        }
        populate_by_name = True