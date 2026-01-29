import pandas as pd
import zipfile
from pathlib import Path
import re
import logging

RAW_DIR = Path("data/raw")
EXTRACTED_DIR = Path("data/extracted")
FINAL_DIR = Path("data/final")
LOG_DIR = Path("logs")

EXTRACTED_DIR.mkdir(parents=True, exist_ok=True)
FINAL_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "etl.log", encoding="utf-8"),
        logging.StreamHandler()
    ],
)

logger = logging.getLogger("etl")


def extract_zip_files():
    for zip_path in RAW_DIR.glob("*.zip"):
        extract_path = EXTRACTED_DIR / zip_path.stem

        if extract_path.exists():
            logger.info(f"ZIP já extraído: {zip_path.name}")
            continue

        logger.info(f"Extraindo: {zip_path.name}")
        extract_path.mkdir(parents=True, exist_ok=True)

        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(extract_path)
        except Exception as e:
            logger.error(f"Falha ao extrair {zip_path.name}: {e}")

    logger.info("Extração concluída.")


def read_file(file_path: Path):
    try:
        if file_path.suffix.lower() in [".csv", ".txt"]:
            return pd.read_csv(file_path, sep=";", encoding="latin1")

        if file_path.suffix.lower() in [".xls", ".xlsx"]:
            return pd.read_excel(file_path)

    except Exception as e:
        logger.error(f"Erro ao ler {file_path.name}: {e}")

    return None


def process_files():
    logger.info("Iniciando consolidação de despesas assistenciais.")

    results = []

    for file_path in EXTRACTED_DIR.rglob("*"):
        if file_path.suffix.lower() not in [".csv", ".txt", ".xls", ".xlsx"]:
            continue

        logger.info(f"Lendo: {file_path}")

        df = read_file(file_path)
        if df is None:
            continue

        required = {"DESCRICAO", "REG_ANS", "VL_SALDO_FINAL"}
        if not required.issubset(df.columns):
            logger.warning(f"Arquivo ignorado (colunas ausentes): {file_path.name}")
            continue

        total_before = len(df)

        df = df[df["DESCRICAO"].astype(str).str.contains(
            "EVENTOS|SINISTROS|ASSISTENC", case=False, na=False
        )]

        total_after = len(df)
        logger.info(f"Registros: {total_before} -> {total_after}")

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

        match = re.search(r"(\d)T(\d{4})", file_path.name)
        if not match:
            logger.warning(f"Data não identificada no nome: {file_path.name}")
            continue

        quarter = int(match.group(1))
        year = int(match.group(2))

        grouped = df.groupby("REG_ANS")["VL_SALDO_FINAL"].sum().reset_index()
        grouped["ano"] = year
        grouped["trimestre"] = quarter

        results.append(grouped)

    if not results:
        logger.error("Nenhum dado válido processado.")
        return

    final_df = pd.concat(results, ignore_index=True)

    final_df = (
        final_df
        .groupby(["REG_ANS", "ano", "trimestre"], as_index=False)["VL_SALDO_FINAL"]
        .sum()
    )

    final_df["VL_SALDO_FINAL"] = final_df["VL_SALDO_FINAL"].round(2)

    output_file = FINAL_DIR / "despesas_por_operadora_trimestre.csv"
    final_df.to_csv(output_file, index=False, sep=";")

    logger.info(f"Arquivo final: {output_file}")
    logger.info(f"Total de linhas: {len(final_df)}")


if __name__ == "__main__":
    extract_zip_files()
    process_files()
