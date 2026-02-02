FROM python:3.11-slim

WORKDIR /app

# deps do sistema: psql client + certificados
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# instala dependências python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copia o projeto
COPY . .

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
