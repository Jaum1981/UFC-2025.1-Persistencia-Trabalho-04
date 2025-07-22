from datetime import datetime
from typing import List
from pydantic import BaseModel
    
class Infrator(BaseModel):
    nome_infrator: str
    infracao_area: str
    municipio: str
    estado: str
    des_local_infracao: str
    historico_infracoes: List[str] = []
    dt_inicio_ato_inequivoco: datetime
    dt_fim_ato_inequivoco: datetime