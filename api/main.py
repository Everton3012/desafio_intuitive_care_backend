# api/main.py
import os
import logging
import threading
import subprocess
import sys
from datetime import datetime
from fastapi import Header
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from api.db import get_conn, get_cursor
from api.schemas import OperadoraListResponse, EstatisticasResponse
from api import queries

PIPELINE_LOCK = threading.Lock()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

os.environ['PGCLIENTENCODING'] = 'UTF8'
app = FastAPI(
    title="IntuitiveCare - Teste Técnico", 
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def run_pipeline_and_import() -> str:
    # 1) pipeline (gera CSVs)
    p1 = subprocess.run(
        [sys.executable, "run_pipeline.py"],
        cwd=BASE_DIR,
        capture_output=True,
        text=True
    )
    if p1.returncode != 0:
        raise RuntimeError(p1.stderr or p1.stdout or "Falha ao executar pipeline")

    container = os.getenv("POSTGRES_CONTAINER", "intuitivecare_postgres")

    # 2) copia DDL e IMPORT SQL para dentro do container (não depende de mount)
    ddl_local = os.path.join(BASE_DIR, "sql", "01_ddl.sql")
    imp_local = os.path.join(BASE_DIR, "sql", "02_import.sql")

    for local_path, dst in [(ddl_local, "/tmp/01_ddl.sql"), (imp_local, "/tmp/02_import.sql")]:
        p_cp = subprocess.run(
            ["docker", "cp", local_path, f"{container}:{dst}"],
            capture_output=True,
            text=True
        )
        if p_cp.returncode != 0:
            raise RuntimeError(p_cp.stderr or p_cp.stdout or f"Falha ao copiar {os.path.basename(local_path)}")

    # 3) copia os CSVs pro /tmp do container (necessário pro COPY FROM '/tmp/...csv')
    files = [
        (os.path.join(BASE_DIR, "data", "final", "despesas_consolidadas_final.csv"), "/tmp/despesas_consolidadas_final.csv"),
        (os.path.join(BASE_DIR, "data", "final", "despesas_agregadas.csv"), "/tmp/despesas_agregadas.csv"),
        (os.path.join(BASE_DIR, "data", "raw", "Relatorio_cadop.csv"), "/tmp/Relatorio_cadop.csv"),
        (os.path.join(BASE_DIR, "data", "raw", "Relatorio_cadop_canceladas.csv"), "/tmp/Relatorio_cadop_canceladas.csv"),
    ]

    for src, dst in files:
        p_csv = subprocess.run(
            ["docker", "cp", src, f"{container}:{dst}"],
            capture_output=True,
            text=True
        )
        if p_csv.returncode != 0:
            raise RuntimeError(p_csv.stderr or p_csv.stdout or f"Falha ao copiar CSV {os.path.basename(src)}")

    # 4) executa DDL e depois import no banco
    def run_psql(file_in_container: str):
        p = subprocess.run(
            ["docker", "exec", "-i", container,
             "psql", "-U", "intuitive", "-d", "intuitivecare", "-v", "ON_ERROR_STOP=1",
             "-f", file_in_container],
            capture_output=True,
            text=True
        )
        if p.returncode != 0:
            raise RuntimeError(p.stderr or p.stdout or f"Falha ao executar {file_in_container}")
        return p.stdout + "\n" + (p.stderr or "")

    out_ddl = run_psql("/tmp/01_ddl.sql")
    out_imp = run_psql("/tmp/02_import.sql")

    return (p1.stdout + "\n" + p1.stderr + "\n" + out_ddl + "\n" + out_imp)[-6000:]

@app.get("/")
def root():
    return {"message": "API IntuitiveCare", "version": "1.0.0"}


@app.get("/health")
def health_check():
    try:
        with get_conn() as conn:
            with get_cursor(conn) as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "unhealthy", "error": str(e)}


@app.get("/api/operadoras", response_model=OperadoraListResponse)
def list_operadoras(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    q: str | None = Query(None),
    situacao: str | None = Query(None, regex="^(ATIVA|CANCELADA)$"),
):
    try:
        offset = (page - 1) * limit
        q_clean = (q or "").strip()

        with get_conn() as conn:
            with get_cursor(conn) as cur:
                if situacao:
                    if q_clean:
                        q_like = f"%{q_clean}%"
                        cur.execute(
                            queries.Q_OPERADORAS_COUNT_FILTER_SITUACAO,
                            {"q_like": q_like, "situacao": situacao},
                        )
                        total = cur.fetchone()["total"]

                        cur.execute(
                            queries.Q_OPERADORAS_LIST_FILTER_SITUACAO,
                            {"q_like": q_like, "situacao": situacao, "limit": limit, "offset": offset},
                        )
                        rows = cur.fetchall()
                    else:
                        cur.execute(
                            queries.Q_OPERADORAS_COUNT_ALL_SITUACAO,
                            {"situacao": situacao},
                        )
                        total = cur.fetchone()["total"]

                        cur.execute(
                            queries.Q_OPERADORAS_LIST_ALL_SITUACAO,
                            {"situacao": situacao, "limit": limit, "offset": offset},
                        )
                        rows = cur.fetchall()
                else:
                    if q_clean:
                        q_like = f"%{q_clean}%"
                        cur.execute(queries.Q_OPERADORAS_COUNT_FILTER, {"q_like": q_like})
                        total = cur.fetchone()["total"]

                        cur.execute(
                            queries.Q_OPERADORAS_LIST_FILTER,
                            {"q_like": q_like, "limit": limit, "offset": offset},
                        )
                        rows = cur.fetchall()
                    else:
                        cur.execute(queries.Q_OPERADORAS_COUNT_ALL)
                        total = cur.fetchone()["total"]

                        cur.execute(
                            queries.Q_OPERADORAS_LIST_ALL,
                            {"limit": limit, "offset": offset},
                        )
                        rows = cur.fetchall()

        return {"data": rows, "total": total, "page": page, "limit": limit}

    except Exception as e:
        logger.error(f"Erro: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/operadoras/{cnpj}")
def get_operadora(cnpj: str):
    try:
        cnpj = "".join([c for c in cnpj if c.isdigit()])
        
        with get_conn() as conn:
            with get_cursor(conn) as cur:
                cur.execute(queries.Q_OPERADORA_DETAIL, {"cnpj": cnpj})
                row = cur.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail="Operadora não encontrada")
        
        return row
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/operadoras/{cnpj}/despesas")
def get_despesas_operadora(cnpj: str):
    try:
        cnpj = "".join([c for c in cnpj if c.isdigit()])
        
        with get_conn() as conn:
            with get_cursor(conn) as cur:
                cur.execute(queries.Q_OPERADORA_DESPESAS, {"cnpj": cnpj})
                rows = cur.fetchall()
        
        return {"cnpj": cnpj, "despesas": rows}
        
    except Exception as e:
        logger.error(f"Erro: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/estatisticas", response_model=EstatisticasResponse)
def get_estatisticas():
    try:
        with get_conn() as conn:
            with get_cursor(conn) as cur:
                cur.execute(queries.Q_ESTATS)
                stats = cur.fetchone()
                
                cur.execute(queries.Q_TOP5)
                top5 = cur.fetchall()
                
                cur.execute(queries.Q_UF_TOP5)
                topuf = cur.fetchall()

        return {
            "total_despesas": float(stats["total"]) if stats else 0,
            "media_despesas": float(stats["media"]) if stats else 0,
            "top_5_operadoras": top5 or [],
            "despesas_por_uf_top5": topuf or [],
        }
        
    except Exception as e:
        logger.error(f"Erro: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/api/admin/atualizar")
def atualizar_dados(x_pipeline_token: str | None = Header(default=None)):
    token = os.getenv("PIPELINE_TOKEN")
    if token and x_pipeline_token != token:
        raise HTTPException(status_code=401, detail="Token inválido")

    # impede duas execuções ao mesmo tempo
    if not PIPELINE_LOCK.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="Atualização já está em andamento.")

    started_at = datetime.utcnow().isoformat()
    try:
        last_output = run_pipeline_and_import()
        return {
            "status": "success",
            "message": "Pipeline executado e banco atualizado com sucesso.",
            "started_at": started_at,
            "finished_at": datetime.utcnow().isoformat(),
            "last_output": last_output,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        PIPELINE_LOCK.release()