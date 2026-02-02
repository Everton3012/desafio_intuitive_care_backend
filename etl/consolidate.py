import logging
import zipfile
import pandas as pd

from pathlib import Path
from etl.logging_config import setup_logging


RAW_DIR = Path("data/raw")
FINAL_DIR = Path("data/final")
FINAL_DIR.mkdir(parents=True, exist_ok=True)

logger = setup_logging("consolidate", "pipeline.log", logging.INFO)

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {
        "CNPJ": "CNPJ",
        "Cnpj": "CNPJ",
        "CNPJ_OPERADORA": "CNPJ",
        "CNPJ Operadora": "CNPJ",
        "RAZAO_SOCIAL": "RAZAO_SOCIAL",
        "Razao_Social": "RAZAO_SOCIAL",
        "RazaoSocial": "RAZAO_SOCIAL",
        "Razão Social": "RAZAO_SOCIAL",
        "REGISTRO_OPERADORA": "RegistroANS",
        "Registro_ANS": "RegistroANS",
        "REG_ANS": "RegistroANS",
        "MODALIDADE": "Modalidade",
        "Modalidade": "Modalidade",
        "UF": "UF",
    }
    cols = {c: mapping[c] for c in df.columns if c in mapping}
    if cols:
        df = df.rename(columns=cols)
    return df

def run(despesas_path: Path | None = None) -> Path:
    logger.info("Iniciando consolidação final com dados cadastrais.")

    if despesas_path is None:
        despesas_path = FINAL_DIR / "despesas_por_operadora_trimestre.csv"

    if not despesas_path.exists():
        logger.error("Arquivo de despesas não encontrado. Execute process_files.py primeiro.")
        raise FileNotFoundError(str(despesas_path))

    despesas = pd.read_csv(despesas_path, sep=";")
    logger.info(f"Registros de despesas: {len(despesas)}")

    cadastro_files = list(RAW_DIR.glob("Relatorio_cadop*.csv"))
    if not cadastro_files:
        logger.error("Arquivos de operadoras não encontrados em data/raw.")
        raise FileNotFoundError("Relatorio_cadop*.csv não encontrado em data/raw.")

    logger.info("Carregando cadastros de operadoras (ativas + canceladas).")

    cadastros = []
    for f in cadastro_files:
        logger.info(f"Lendo cadastro: {f.name}")
        df_cad = pd.read_csv(f, sep=";", encoding="latin1")
        df_cad = _normalize_columns(df_cad)
        if "CNPJ" in df_cad.columns:
            df_cad["CNPJ"] = df_cad["CNPJ"].astype(str)
        cadastros.append(df_cad)

    cadastro = pd.concat(cadastros, ignore_index=True)
    logger.info(f"Total de operadoras carregadas: {len(cadastro)}")

    despesas = _normalize_columns(despesas)
    if "CNPJ" not in despesas.columns:
        if "CNPJ" in cadastro.columns and "RegistroANS" in cadastro.columns and "REG_ANS" in despesas.columns:
            pass

    if "REG_ANS" in despesas.columns:
        despesas = despesas.rename(columns={"REG_ANS": "RegistroANS"})
    if "RegistroANS" in cadastro.columns:
        cadastro["RegistroANS"] = cadastro["RegistroANS"].astype(str)
    if "RegistroANS" in despesas.columns:
        despesas["RegistroANS"] = despesas["RegistroANS"].astype(str)

    cols_keep = []
    for c in ["RegistroANS", "CNPJ", "RAZAO_SOCIAL"]:
        if c in cadastro.columns:
            cols_keep.append(c)

    cadastro_min = cadastro[cols_keep].copy() if cols_keep else cadastro.copy()
    if "RegistroANS" in cadastro_min.columns:
        cadastro_min = cadastro_min.drop_duplicates(subset=["RegistroANS"])
    elif "CNPJ" in cadastro_min.columns:
        cadastro_min = cadastro_min.drop_duplicates(subset=["CNPJ"])

    logger.info("Realizando merge despesas x operadoras.")
    if "RegistroANS" in despesas.columns and "RegistroANS" in cadastro_min.columns:
        merged = despesas.merge(cadastro_min, on="RegistroANS", how="left", suffixes=("", "_cad"))
    else:
        raise RuntimeError("Não foi possível identificar chave de merge (RegistroANS).")

    if "RAZAO_SOCIAL" not in merged.columns and "RAZAO_SOCIAL_cad" in merged.columns:
        merged["RAZAO_SOCIAL"] = merged["RAZAO_SOCIAL_cad"]
    if "CNPJ" not in merged.columns and "CNPJ_cad" in merged.columns:
        merged["CNPJ"] = merged["CNPJ_cad"]

    out_csv = FINAL_DIR / "despesas_consolidadas_final.csv"
    merged.to_csv(out_csv, index=False, sep=";")
    logger.info(f"Arquivo final gerado: {out_csv}")

    out_zip = FINAL_DIR / "consolidado_despesas.zip"
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(out_csv, out_csv.name)

    logger.info(f"ZIP final gerado: {out_zip}")
    logger.info("Consolidação final concluída com sucesso.")
    return out_csv

if __name__ == "__main__":
    run()
