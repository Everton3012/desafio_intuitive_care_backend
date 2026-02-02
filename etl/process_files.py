# etl/process_files.py
import logging
import re
import zipfile
import pandas as pd

from pathlib import Path
from etl.logging_config import setup_logging


RAW_DIR = Path("data/raw")
EXTRACTED_DIR = Path("data/extracted")
FINAL_DIR = Path("data/final")

EXTRACTED_DIR.mkdir(parents=True, exist_ok=True)
FINAL_DIR.mkdir(parents=True, exist_ok=True)

logger = setup_logging("process_files", "pipeline.log", logging.INFO)

def _extract_zip_files() -> None:
    zips = list(RAW_DIR.glob("*.zip"))
    if not zips:
        logger.info("Nenhum ZIP encontrado em data/raw.")
        return

    for zip_path in zips:
        extract_path = EXTRACTED_DIR / zip_path.stem
        if extract_path.exists():
            logger.info(f"ZIP já extraído: {zip_path.name}")
            continue

        logger.info(f"Extraindo: {zip_path.name}")
        extract_path.mkdir(parents=True, exist_ok=True)

        try:
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(extract_path)
        except Exception as e:
            logger.error(f"Falha ao extrair {zip_path.name}: {e}")

def _read_file(file_path: Path) -> pd.DataFrame | None:
    try:
        suf = file_path.suffix.lower()
        if suf in [".csv", ".txt"]:
            return pd.read_csv(file_path, sep=";", encoding="latin1")
        if suf in [".xls", ".xlsx"]:
            return pd.read_excel(file_path)
    except Exception as e:
        logger.error(f"Erro ao ler {file_path}: {e}")
    return None

def run() -> Path:
    logger.info("Iniciando processamento de despesas assistenciais.")
    _extract_zip_files()

    results: list[pd.DataFrame] = []
    candidates = list(EXTRACTED_DIR.rglob("*"))
    if not candidates:
        logger.error("Nenhum arquivo encontrado em data/extracted. Verifique os ZIPs em data/raw.")
        raise FileNotFoundError("Nenhum arquivo para processar em data/extracted.")

    for file_path in candidates:
        if file_path.suffix.lower() not in [".csv", ".txt", ".xls", ".xlsx"]:
            continue

        df = _read_file(file_path)
        if df is None:
            continue

        required = {"DESCRICAO", "REG_ANS", "VL_SALDO_FINAL"}
        if not required.issubset(df.columns):
            logger.info(f"Ignorado (colunas ausentes): {file_path.name}")
            continue

        before = len(df)
        df = df[df["DESCRICAO"].astype(str).str.contains("EVENTOS|SINISTROS|ASSISTENC", case=False, na=False)]
        after = len(df)
        logger.info(f"{file_path.name} | Registros: {before} -> {after}")

        try:
            df["VL_SALDO_FINAL"] = (
                df["VL_SALDO_FINAL"]
                .astype(str)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
                .astype(float)
            )
        except Exception as e:
            logger.error(f"Erro ao converter valores em {file_path.name}: {e}")
            continue

        m = re.search(r"(\d)T(\d{4})", file_path.name)
        if not m:
            logger.info(f"Ignorado (sem trimestre/ano no nome): {file_path.name}")
            continue

        trimestre = int(m.group(1))
        ano = int(m.group(2))

        grouped = df.groupby("REG_ANS")["VL_SALDO_FINAL"].sum().reset_index()
        grouped["ano"] = ano
        grouped["trimestre"] = trimestre

        results.append(grouped)

    if not results:
        logger.error("Nenhum dado válido foi processado.")
        raise RuntimeError("Nenhum dado válido foi processado.")

    final_df = pd.concat(results, ignore_index=True)
    final_df["VL_SALDO_FINAL"] = final_df["VL_SALDO_FINAL"].round(2)

    out = FINAL_DIR / "despesas_por_operadora_trimestre.csv"
    final_df.to_csv(out, index=False, sep=";")
    logger.info(f"Arquivo gerado: {out} | Linhas: {len(final_df)}")
    return out

if __name__ == "__main__":
    run()
