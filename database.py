import os
import motor.motor_asyncio
from dotenv import load_dotenv

load_dotenv()

client = motor.motor_asyncio.AsyncIOMotorClient(
    os.getenv("MONGO_URL")
)

database = client["IBAMAdb"]

auto_infracao_collection = database["auto_infracao"]
enquadramento_collection = database["enquadramento"]
bioma_collection = database["bioma"]
especime_collection = database["especime"]
edificio_IBAMA_collection = database["edificio_IBAMA"]