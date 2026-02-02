import os
import logging
import threading

from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, Query, Header
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from api.db import get_conn, get_cursor
from api.schemas import OperadoraListResponse, EstatisticasResponse
from api import queries
from api.pipeline import run_pipeline_and_import


PIPELINE_LOCK = threading.Lock()

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
    situacao: str | None = Query(None, pattern="^(ATIVA|CANCELADA)$"),
):
    try:
        offset = (page - 1) * limit
        q_clean = (q or "").strip()

        with get_conn() as conn:
            with get_cursor(conn) as cur:
                if situacao:
                    if q_clean:
                        q_like = f"%{q_clean}%"
                        cur.execute(queries.Q_OPERADORAS_COUNT_FILTER_SITUACAO, {"q_like": q_like, "situacao": situacao})
                        total = cur.fetchone()["total"]
                        cur.execute(queries.Q_OPERADORAS_LIST_FILTER_SITUACAO, {"q_like": q_like, "situacao": situacao, "limit": limit, "offset": offset})
                        rows = cur.fetchall()
                    else:
                        cur.execute(queries.Q_OPERADORAS_COUNT_ALL_SITUACAO, {"situacao": situacao})
                        total = cur.fetchone()["total"]
                        cur.execute(queries.Q_OPERADORAS_LIST_ALL_SITUACAO, {"situacao": situacao, "limit": limit, "offset": offset})
                        rows = cur.fetchall()
                else:
                    if q_clean:
                        q_like = f"%{q_clean}%"
                        cur.execute(queries.Q_OPERADORAS_COUNT_FILTER, {"q_like": q_like})
                        total = cur.fetchone()["total"]
                        cur.execute(queries.Q_OPERADORAS_LIST_FILTER, {"q_like": q_like, "limit": limit, "offset": offset})
                        rows = cur.fetchall()
                    else:
                        cur.execute(queries.Q_OPERADORAS_COUNT_ALL)
                        total = cur.fetchone()["total"]
                        cur.execute(queries.Q_OPERADORAS_LIST_ALL, {"limit": limit, "offset": offset})
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

    if not PIPELINE_LOCK.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="Atualização já está em andamento.")

    started_at = datetime.now(timezone.utc)
    try:
        last_output = run_pipeline_and_import()
        return {
            "status": "success",
            "message": "Pipeline executado e banco atualizado com sucesso.",
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc),
            "last_output": last_output,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        PIPELINE_LOCK.release()