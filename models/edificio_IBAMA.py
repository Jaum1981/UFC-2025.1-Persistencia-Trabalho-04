from typing import List
from bson import ObjectId
from pydantic import BaseModel, Field
from models.PyObjectId import PyObjectId
    
class Edf_Pub_Civil_IBAMA(BaseModel):
    nome: str
    nomeabrev: str
    municipio: str
    estado: str
    situacao_fisica: str
    lat: str
    long: str

class Edf_Pub_Civil_IBAMACreate(Edf_Pub_Civil_IBAMA):
    pass

class Edf_Pub_Civil_IBAMAOut(Edf_Pub_Civil_IBAMA):
    id: PyObjectId = Field(..., alias="_id")

    class Config:
        json_encoders = {
            ObjectId: str
        }
        populate_by_name = True
    
class PaginatedEdf_Pub_Civil_IBAMAResponse(BaseModel):
    total: int
    page: int
    size: int
    items: List[Edf_Pub_Civil_IBAMAOut]

    class Config:
        json_encoders = {
            ObjectId: str
        }
        populate_by_name = True