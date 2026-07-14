"""
Camada GOLD: datasets analíticos prontos para consumo (dashboards,
análises estatísticas, treinamento de modelos de ML).
"""
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.utils.paths import BRONZE_DIR, GOLD_DIR, SILVER_DIR


def carregar_silver() -> pd.DataFrame:
    return pd.read_parquet(SILVER_DIR / "alfabetizacao_integrado.parquet")


def carregar_meta_municipio() -> pd.DataFrame:
    entity_dir = BRONZE_DIR / "meta_alfabetizacao_municipio"
    latest = sorted(entity_dir.glob("ingestion_date=*"))[-1]
    arquivo = next(latest.glob("*.parquet"))
    df = pd.read_parquet(arquivo)
    df.columns = [c.strip().lower() for c in df.columns]
    df["id_municipio"] = df["id_municipio"].astype(str).str.strip().str.zfill(7)
    return df


def indicador_por_municipio(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["alfabetizado_flag"] = (df["alfabetizado"] == "Sim").astype(int)
    resultado = (
        df.groupby(["id_municipio", "ano"], dropna=False)
        .agg(
            taxa_alfabetizacao_calculada=("alfabetizado_flag", "mean"),
            total_alunos=("id_aluno", "count"),
        )
        .reset_index()
    )
    resultado["taxa_alfabetizacao_calculada"] = (resultado["taxa_alfabetizacao_calculada"] * 100).round(2)
    return resultado


def metas_vs_resultados(indicador_df: pd.DataFrame, meta_df: pd.DataFrame) -> pd.DataFrame:
    colunas_meta = [c for c in meta_df.columns if c.startswith("meta_alfabetizacao_")]
    colunas = ["id_municipio", "ano"] + colunas_meta
    meta_reduzido = meta_df[colunas].drop_duplicates()
    return indicador_df.merge(meta_reduzido, on=["id_municipio", "ano"], how="left")


def evolucao_temporal(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["alfabetizado_flag"] = (df["alfabetizado"] == "Sim").astype(int)
    resultado = (
        df.groupby("ano", dropna=False)
        .agg(taxa_alfabetizacao_media=("alfabetizado_flag", "mean"))
        .reset_index()
        .sort_values("ano")
    )
    resultado["taxa_alfabetizacao_media"] = (resultado["taxa_alfabetizacao_media"] * 100).round(2)
    return resultado


def run():
    df = carregar_silver()
    meta_df = carregar_meta_municipio()

    indicador_df = indicador_por_municipio(df)
    metas_df = metas_vs_resultados(indicador_df, meta_df)
    evolucao_df = evolucao_temporal(df)

    datasets = {
        "indicador_por_municipio": indicador_df,
        "metas_vs_resultados": metas_df,
        "evolucao_temporal": evolucao_df,
    }

    for nome, dataset in datasets.items():
        caminho = GOLD_DIR / f"{nome}.parquet"
        dataset.to_parquet(caminho, index=False)
        print(f"[{nome}] salvo em {caminho} ({len(dataset)} linhas)")


if __name__ == "__main__":
    run()