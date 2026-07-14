"""
Camada SILVER: limpeza, padronização, normalização de chaves e
integração entre as bases vindas da Bronze.
"""
import glob
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.utils.paths import BRONZE_DIR, SILVER_DIR


def _latest_partition(entity_dir: Path) -> Path:
    partitions = sorted(entity_dir.glob("ingestion_date=*"))
    if not partitions:
        raise FileNotFoundError(f"Nenhuma partiçao em {entity_dir}")
    return partitions[-1]


def load_bronze(entity_name: str) -> pd.DataFrame:
    entity_dir = BRONZE_DIR / entity_name
    latest = _latest_partition(entity_dir)
    arquivo = next(latest.glob("*.parquet"))
    return pd.read_parquet(arquivo)


def load_streaming_events() -> pd.DataFrame:
    pattern = str(BRONZE_DIR / "streaming_eventos" / "ingestion_date=*" / "evento_*.json")
    arquivos = glob.glob(pattern)
    registros = []
    for f in arquivos:
        with open(f, encoding="utf-8") as fh:
            registros.append(json.load(fh))
    return pd.DataFrame(registros)


def limpar(df: pd.DataFrame, nome: str) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    # Normaliza chave de UF (maiuscula, sem espaço)
    for col in ("sigla_uf", "uf_sigla"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    # Normalizaa chave de municipio (7 digitos, texto)
    if "id_municipio" in df.columns:
        df["id_municipio"] = df["id_municipio"].astype(str).str.strip().str.zfill(7)

    antes = len(df)
    df = df.drop_duplicates()
    removidas = antes - len(df)
    if removidas:
        print(f"[{nome}] removidas {removidas} linhas duplicadas")

    nulos = (df.isna().mean() * 100).round(2)
    relevantes = nulos[nulos > 0]
    if not relevantes.empty:
        print(f"[{nome}] % nulos por coluna:\n{relevantes}")

    return df


def run():
    entidades = [
        "uf", "municipio", "meta_alfabetizacao_brasil",
        "meta_alfabetizacao_uf", "meta_alfabetizacao_municipio", "dados_alunos",
    ]

    tabelas = {}
    for nome in entidades:
        bruto = load_bronze(nome)
        tabelas[nome] = limpar(bruto, nome)
        print(f"[{nome}] {len(tabelas[nome])} linhas carregadas")

    # Base principal... Dados de alunos, enriquecida com nome do municipio
    base = tabelas["dados_alunos"]
    if "id_municipio" in tabelas["municipio"].columns:
        base = base.merge(
            tabelas["municipio"][["id_municipio"]].drop_duplicates(),
            on="id_municipio", how="left", indicator=True
        )
        sem_correspondencia = (base["_merge"] == "left_only").sum()
        print(f"Registros de alunos sem município correspondente: {sem_correspondencia}")
        base = base.drop(columns="_merge")

    # Eventos de streaming: salvos separados (granularidade diferente)
    streaming_df = load_streaming_events()
    if not streaming_df.empty:
        streaming_limpo = limpar(streaming_df, "streaming_eventos")
        streaming_limpo.to_parquet(SILVER_DIR / "streaming_eventos_silver.parquet", index=False)
        print(f"streaming_eventos_silver salvo ({len(streaming_limpo)} linhas)")

    out_path = SILVER_DIR / "alfabetizacao_integrado.parquet"
    base.to_parquet(out_path, index=False)
    print(f"\nDataset integrado salvo em {out_path} ({len(base)} linhas, {len(base.columns)} colunas)")


if __name__ == "__main__":
    run()