import os
import sys
import shutil
import subprocess


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def run_pipeline_and_import() -> str:
    """Executa o pipeline ETL e importa os dados no banco."""
    
    shared_dir = os.getenv("SHARED_DIR", os.path.join(BASE_DIR, "shared"))
    os.makedirs(shared_dir, exist_ok=True)

    # 1) Roda pipeline (gera CSVs em data/final e data/raw)
    p1 = subprocess.run(
        [sys.executable, "run_pipeline.py"],
        cwd=BASE_DIR,
        capture_output=True,
        text=True
    )
    if p1.returncode != 0:
        raise RuntimeError(p1.stderr or p1.stdout or "Falha ao executar pipeline")

    # 2) Copia os arquivos necessários para o /shared (volume)
    files = [
        ("data/final/despesas_consolidadas_final.csv", "despesas_consolidadas_final.csv"),
        ("data/final/despesas_agregadas.csv", "despesas_agregadas.csv"),
        ("data/raw/Relatorio_cadop.csv", "Relatorio_cadop.csv"),
        ("data/raw/Relatorio_cadop_canceladas.csv", "Relatorio_cadop_canceladas.csv"),
    ]

    for src_rel, dst_name in files:
        src = os.path.join(BASE_DIR, src_rel)
        dst = os.path.join(shared_dir, dst_name)
        if not os.path.exists(src):
            raise RuntimeError(f"Arquivo não encontrado: {src}")
        shutil.copyfile(src, dst)

    # 3) Executa import no banco via psql
    sql_file = os.path.join(BASE_DIR, "sql", "02_import.sql")

    env = os.environ.copy()
    env["PGPASSWORD"] = os.getenv("DB_PASSWORD", "intuitive123")

    p2 = subprocess.run(
        [
            "psql",
            "-h", os.getenv("DB_HOST", "db"),
            "-p", str(os.getenv("DB_PORT", "5432")),
            "-U", os.getenv("DB_USER", "intuitive"),
            "-d", os.getenv("DB_NAME", "intuitivecare"),
            "-v", "ON_ERROR_STOP=1",
            "-f", sql_file,
        ],
        cwd=BASE_DIR,
        env=env,
        capture_output=True,
        text=True,
    )

    if p2.returncode != 0:
        raise RuntimeError(p2.stderr or p2.stdout or "Falha ao importar no banco")

    return (p1.stdout + "\n" + p1.stderr + "\n" + p2.stdout + "\n" + p2.stderr)[-6000:]