from typing import List
from bson import ObjectId
from pydantic import BaseModel, Field
from models.PyObjectId import PyObjectId
    
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
    id: PyObjectId = Field(..., alias="_id")

    class Config:
        json_encoders = {
            ObjectId: str
        }
        populate_by_name = True

class PaginatedEspecimeResponse(BaseModel):
    total: int
    page: int
    size: int
    items: List[EspecimeOut]

    class Config:
        json_encoders = {
            ObjectId: str
        }
        populate_by_name = True