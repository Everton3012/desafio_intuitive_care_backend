import logging
import re
import zipfile
import pandas as pd

from pathlib import Path
from etl.logging_config import setup_logging


DATA_FINAL = Path("data/final")
RAW_DIR = Path("data/raw")
OUTPUT_ZIP = Path("Teste_Everton_Brandao.zip")

logger = setup_logging("validate_and_aggregate", "pipeline.log", logging.INFO)


def is_valid_cnpj(cnpj: str) -> bool:
    cnpj = re.sub(r"\D", "", str(cnpj))
    if len(cnpj) != 14 or cnpj == cnpj[0] * 14:
        return False

    def calc_digit(base: str, weights: list[int]) -> str:
        s = sum(int(base[i]) * weights[i] for i in range(len(weights)))
        r = s % 11
        return "0" if r < 2 else str(11 - r)

    w1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    w2 = [6] + w1

    return calc_digit(cnpj, w1) == cnpj[12] and calc_digit(cnpj, w2) == cnpj[13]


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df


def _load_cadastro_operadoras() -> pd.DataFrame:
    files = sorted(RAW_DIR.glob("Relatorio_cadop*.csv"))
    if not files:
        raise FileNotFoundError("Nenhum cadastro encontrado em data/raw (Relatorio_cadop*.csv).")

    cadastros = []
    for f in files:
        logger.info(f"Lendo cadastro: {f.name}")
        df = pd.read_csv(f, sep=";", encoding="latin1", dtype=str)
        df = _normalize_columns(df)

        col_map = {
            "REGISTRO_OPERADORA": "REGISTROANS",
            "REGISTRO_ANS": "REGISTROANS",
            "REG_ANS": "REGISTROANS",
            "MODALIDADE": "MODALIDADE",
            "UF": "UF",
            "CNPJ": "CNPJ",
        }

        renamed = {}
        for old, new in col_map.items():
            if old in df.columns:
                renamed[old] = new

        df = df.rename(columns=renamed)

        if "CNPJ" not in df.columns:
            logger.warning(f"Cadastro ignorado (sem CNPJ): {f.name}")
            continue

        keep = [c for c in ["CNPJ", "REGISTROANS", "MODALIDADE", "UF"] if c in df.columns]
        df = df[keep].copy()

        df["CNPJ"] = df["CNPJ"].astype(str).str.replace(r"\D", "", regex=True)

        cadastros.append(df)

    if not cadastros:
        raise RuntimeError("Cadastros encontrados, mas nenhum possuía colunas mínimas para join.")

    cadastro = pd.concat(cadastros, ignore_index=True)

    cadastro["HAS_UF"] = cadastro["UF"].notna() if "UF" in cadastro.columns else False
    cadastro = cadastro.sort_values(by=["CNPJ", "HAS_UF"], ascending=[True, False]).drop(columns=["HAS_UF"])
    cadastro = cadastro.drop_duplicates(subset=["CNPJ"], keep="first")

    return cadastro


def run(input_csv_path: Path | None = None) -> Path:
    logger.info("Iniciando validação, enriquecimento e agregação.")

    if input_csv_path is None:
        input_csv_path = DATA_FINAL / "despesas_consolidadas_final.csv"

    if not input_csv_path.exists():
        raise FileNotFoundError(f"Arquivo consolidado não encontrado: {input_csv_path}")

    despesas = pd.read_csv(input_csv_path, sep=";", dtype=str)
    despesas = _normalize_columns(despesas)

    logger.info(f"Registros iniciais: {len(despesas)}")

    required = ["CNPJ", "RAZAO_SOCIAL", "VL_SALDO_FINAL"]
    for col in required:
        if col not in despesas.columns:
            raise KeyError(f"Coluna ausente no consolidado: {col}")

    despesas["CNPJ"] = despesas["CNPJ"].astype(str).str.replace(r"\D", "", regex=True)
    despesas["RAZAO_SOCIAL"] = despesas["RAZAO_SOCIAL"].astype(str).str.strip()

    despesas["VL_SALDO_FINAL"] = (
        despesas["VL_SALDO_FINAL"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    despesas["VL_SALDO_FINAL"] = pd.to_numeric(despesas["VL_SALDO_FINAL"], errors="coerce")

    before = len(despesas)
    despesas = despesas[despesas["RAZAO_SOCIAL"].notna() & (despesas["RAZAO_SOCIAL"] != "")]
    razao_drop = before - len(despesas)

    before = len(despesas)
    despesas = despesas[despesas["VL_SALDO_FINAL"].notna() & (despesas["VL_SALDO_FINAL"] >= 0)]
    valor_drop = before - len(despesas)

    despesas["CNPJ_VALIDO"] = despesas["CNPJ"].apply(is_valid_cnpj)
    invalidos = int((~despesas["CNPJ_VALIDO"]).sum())

    logger.info(f"Razão Social vazia descartada: {razao_drop}")
    logger.info(f"Valores inválidos/negativos descartados: {valor_drop}")
    logger.warning(f"CNPJs inválidos descartados: {invalidos}")

    despesas = despesas[despesas["CNPJ_VALIDO"]].drop(columns=["CNPJ_VALIDO"])

    cadastro = _load_cadastro_operadoras()
    cadastro = _normalize_columns(cadastro)

    if "UF" not in cadastro.columns:
        cadastro["UF"] = None
    if "MODALIDADE" not in cadastro.columns:
        cadastro["MODALIDADE"] = None
    if "REGISTROANS" not in cadastro.columns:
        cadastro["REGISTROANS"] = None

    df = despesas.merge(cadastro, on="CNPJ", how="left")

    df["UF"] = df["UF"].fillna("SEM_MATCH")

    agg = (
        df.groupby(["RAZAO_SOCIAL", "UF"], dropna=False)
        .agg(
            total_despesas=("VL_SALDO_FINAL", "sum"),
            media_trimestral=("VL_SALDO_FINAL", "mean"),
            desvio_padrao=("VL_SALDO_FINAL", "std"),
        )
        .reset_index()
        .sort_values("total_despesas", ascending=False)
    )

    output_csv = DATA_FINAL / "despesas_agregadas.csv"
    agg.to_csv(output_csv, index=False, sep=";")

    with zipfile.ZipFile(OUTPUT_ZIP, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(output_csv, output_csv.name)

    logger.info("Processo concluído com sucesso.")
    logger.info(f"Arquivo final: {output_csv}")
    logger.info(f"ZIP gerado: {OUTPUT_ZIP}")

    return output_csv


if __name__ == "__main__":
    run()
