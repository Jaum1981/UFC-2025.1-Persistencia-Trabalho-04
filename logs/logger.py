import logging
import yaml
import os

# Obtém o diretório atual (onde está o logger.py)
base_dir = os.path.dirname(os.path.abspath(__file__))

# Constrói o caminho absoluto para o config.yaml em app/core/
config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "config.yml"))

# Carrega as configurações do arquivo YAML
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

# Mapeia a string para o nível correspondente do logging
log_level = getattr(logging, config["logging"]["level"].upper(), logging.INFO)

# Configuração básica de logging
logging.basicConfig(
    level=log_level,
    format=config["logging"]["format"],
    handlers=[
        logging.FileHandler(config["logging"]["file"]),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
