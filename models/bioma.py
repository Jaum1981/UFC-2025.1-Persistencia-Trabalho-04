from datetime import datetime
from typing import List, Optional
from bson import ObjectId
from pydantic import BaseModel, Field, validator
from models.PyObjectId import PyObjectId

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
    id: PyObjectId = Field(..., alias="_id")

    class Config:
        json_encoders = {
            ObjectId: str
        }
        populate_by_name = True

class PaginatedBiomaResponse(BaseModel):
    total: int
    page: int
    size: int
    items: List[BiomaOut]

    class Config:
        json_encoders = {
            ObjectId: str
        }
        populate_by_name = True