FROM python:3.11-slim

WORKDIR /app

# Instala dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia os arquivos do bot
COPY bot.py .
COPY database.py .
COPY config.json .

# Cria o diretório de dados persistentes
RUN mkdir -p /data

# O banco SQLite ficará em /data (volume persistente no Fly.io)
ENV DB_PATH=/data/database.db

CMD ["python", "bot.py"]
