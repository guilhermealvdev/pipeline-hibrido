from pathlib import Path

# Este arquivo está em: <raiz>/src/utils/paths.py
# .parent sobe de paths.py -> utils
# .parent.parent sobe de utils -> src
# .parent.parent.parent sobe de src -> raiz do projeto
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"