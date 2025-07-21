from datetime import datetime
from typing import List, Optional
from bson import ObjectId
from pydantic import BaseModel, Field, validator

class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v, *args, **kwargs):
        if not ObjectId.is_valid(v):
            raise ValueError("ID inválido")
        return str(v)

class Bioma(BaseModel):
    seq_auto_infracao: Optional[int] = Field(alias="SEQ_AUTO_INFRACAO")
    num_auto_infracao: Optional[int] = Field(alias="NUM_AUTO_INFRACAO")
    cd_serie_auto_infracao: Optional[str] = Field(alias="CD_SERIE_AUTO_INFRACAO")
    bioma: str = Field(alias="BIOMA")
    ultima_atualizacao: datetime = Field(alias="ULTIMA_ATUALIZACAO_RELATORIO")

    @validator('num_auto_infracao', pre=True)
    def parse_num_auto_infracao(cls, value):
        if isinstance(value, str) and value.isdigit():
            return int(value)
        return value

    class Config:
        populate_by_name = True

class BiomaCreate(Bioma):
    pass

class BiomaOut(Bioma):
    id: Optional[str] = Field(default=None, alias="_id")

    class Config:
        json_encoders = {
            ObjectId: str
        }
        populate_by_name = True

class PaginationMeta(BaseModel):
    total_items: int
    total_pages: int
    current_page: int
    limit: int

class PaginatedBiomasResponse(BaseModel):
    meta: PaginationMeta
    data: List[BiomaOut]