"""
Regras de qualidade de dados aplicadas sobre a camada Silver.

Cobre: duplicidade, valores ausentes, integridade referencial.
"""
import sys

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.utils.paths import SILVER_DIR


def checar_duplicidade(df: pd.DataFrame, nome: str, chave: list) -> bool:
    exatas = df.duplicated().sum()
    print(f"[{nome}] duplicidade exata: {exatas} linhas")

    chave_valida = [c for c in chave if c in df.columns]
    ok = exatas == 0
    if chave_valida:
        por_chave = df.duplicated(subset=chave_valida).sum()
        print(f"[{nome}] duplicidade por chave {chave_valida}: {por_chave} linhas")
        ok = ok and (por_chave == 0)
    return ok


def checar_nulos_criticos(df: pd.DataFrame, nome: str, colunas_criticas: list) -> bool:
    ok = True
    for col in colunas_criticas:
        if col not in df.columns:
            continue
        n_nulos = df[col].isna().sum()
        print(f"[{nome}] nulos em '{col}': {n_nulos}")
        if n_nulos > 0:
            ok = False
    return ok


def checar_integridade_referencial(df: pd.DataFrame, ref_df: pd.DataFrame, chave: str, nome: str) -> bool:
    if chave not in df.columns or chave not in ref_df.columns:
        print(f"[{nome}] AVISO: coluna '{chave}' não encontrada para checagem")
        return False
    orfaos = (~df[chave].isin(ref_df[chave])).sum()
    print(f"[{nome}] registros órfãos ({chave} sem correspondência): {orfaos}")
    return orfaos == 0


def run():
    df = pd.read_parquet(SILVER_DIR / "alfabetizacao_integrado.parquet")

    print("=== Verificação de qualidade: alfabetizacao_integrado ===\n")

    r1 = checar_duplicidade(df, "alfabetizacao_integrado", chave=["id_aluno", "ano"])
    r2 = checar_nulos_criticos(df, "alfabetizacao_integrado", colunas_criticas=["id_municipio", "ano", "id_aluno"])

    print("\n=== Resumo ===")
    print(f"Duplicidade OK: {r1}")
    print(f"Nulos em colunas críticas OK: {r2}")

if __name__ == "__main__":
    run()