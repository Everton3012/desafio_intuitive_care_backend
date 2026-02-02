import logging
import requests

from pathlib import Path
from bs4 import BeautifulSoup
from etl.logging_config import setup_logging


RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

ATIVAS_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"
CANCELADAS_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_canceladas/"

logger = setup_logging("download_operadoras", "pipeline.log", logging.INFO)

def _download_text_file(url: str, out_path: Path) -> Path:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    out_path.write_bytes(r.content)
    logger.info(f"Salvo em: {out_path}")
    return out_path

def _find_latest_link(base_url: str, allowed_ext: tuple[str, ...]) -> str:
    r = requests.get(base_url, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    links = []
    for a in soup.find_all("a"):
        href = a.get("href", "")
        href_low = href.lower()
        if any(href_low.endswith(ext) for ext in allowed_ext):
            links.append(href)

    if not links:
        raise RuntimeError(f"Nenhum arquivo encontrado em {base_url}")

    links.sort(reverse=True)
    return base_url + links[0]

def run() -> dict[str, Path]:
    logger.info("Baixando cadastros de operadoras (ativas e canceladas).")

    ativas_link = _find_latest_link(ATIVAS_URL, (".csv", ".txt", ".zip"))
    canceladas_link = _find_latest_link(CANCELADAS_URL, (".csv", ".txt", ".zip"))

    ativas_name = Path(ativas_link).name
    canceladas_name = Path(canceladas_link).name

    ativas_out = RAW_DIR / ("Relatorio_cadop.csv" if ativas_name.lower().endswith((".csv", ".txt")) else ativas_name)
    canceladas_out = RAW_DIR / ("Relatorio_cadop_canceladas.csv" if canceladas_name.lower().endswith((".csv", ".txt")) else canceladas_name)

    if ativas_out.exists():
        logger.info(f"Arquivo já existe, pulando: {ativas_out.name}")
    else:
        logger.info(f"Baixando: {Path(ativas_link).name}")
        _download_text_file(ativas_link, ativas_out)

    if canceladas_out.exists():
        logger.info(f"Arquivo já existe, pulando: {canceladas_out.name}")
    else:
        logger.info(f"Baixando: {Path(canceladas_link).name}")
        _download_text_file(canceladas_link, canceladas_out)

    return {"ativas": ativas_out, "canceladas": canceladas_out}

if __name__ == "__main__":
    run()
