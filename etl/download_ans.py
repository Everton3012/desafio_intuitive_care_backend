import logging
import re
import requests

from bs4 import BeautifulSoup
from etl.logging_config import setup_logging
from pathlib import Path
from urllib.parse import urljoin


RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"

logger = setup_logging("download_ans", "pipeline.log", logging.INFO)


def _list_links(url: str) -> list[str]:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    links = []
    for a in soup.find_all("a"):
        href = a.get("href", "")
        if href and href not in ["../", "./"]:
            links.append(href)
    return links


def _discover_year_dirs() -> list[str]:
    links = _list_links(BASE_URL)
    years = []
    for href in links:
        m = re.fullmatch(r"(\d{4})\/", href)
        if m:
            years.append(m.group(1))
    years.sort(reverse=True)
    return years


def _discover_zip_links_for_year(year: str) -> list[tuple[str, str]]:
    year_url = urljoin(BASE_URL, f"{year}/")
    links = _list_links(year_url)
    zips = []
    for href in links:
        if href.lower().endswith(".zip"):
            zips.append((urljoin(year_url, href), href))
    zips.sort(key=lambda x: x[1], reverse=True)
    return zips


def _pick_last_n_zips(n: int) -> list[tuple[str, str]]:
    years = _discover_year_dirs()
    picked: list[tuple[str, str]] = []

    for y in years:
        zips = _discover_zip_links_for_year(y)
        for item in zips:
            picked.append(item)
            if len(picked) >= n:
                return picked

    return picked


def _download_file(url: str, out_path: Path) -> None:
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)


def run(last_n_quarters: int = 3) -> list[Path]:
    logger.info(f"Baixando os últimos {last_n_quarters} arquivos trimestrais (ZIP) da ANS.")

    targets = _pick_last_n_zips(last_n_quarters)
    if not targets:
        raise RuntimeError("Nenhum ZIP encontrado em demonstracoes_contabeis.")

    downloaded: list[Path] = []

    for url, filename in targets:
        out_path = RAW_DIR / filename
        if out_path.exists():
            logger.info(f"ZIP já existe, pulando: {filename}")
            downloaded.append(out_path)
            continue

        logger.info(f"Baixando: {filename}")
        _download_file(url, out_path)
        logger.info(f"Salvo em: {out_path}")
        downloaded.append(out_path)

    return downloaded


if __name__ == "__main__":
    run()
