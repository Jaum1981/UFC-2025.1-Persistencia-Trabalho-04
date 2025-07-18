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
    id: Optional[str] = Field(default=None, alias="_id")

    class Config:
        json_encoders = {
            ObjectId: str
        }
        populate_by_name = True