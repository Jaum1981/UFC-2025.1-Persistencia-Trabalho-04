from datetime import datetime
from typing import List
from bson import ObjectId
from pydantic import BaseModel, Field
from models.PyObjectId import PyObjectId
    
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
    id: PyObjectId = Field(..., alias="_id")

    class Config:
        json_encoders = {
            ObjectId: str
        }
        populate_by_name = True

class PaginatedEnquadramentoResponse(BaseModel):
    total: int
    page: int
    size: int
    items: List[EnquadramentoOut]

    class Config:
        json_encoders = {
            ObjectId: str
        }
        populate_by_name = True