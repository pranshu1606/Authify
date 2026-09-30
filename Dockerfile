FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY schema.py ./
COPY generator ./generator
COPY compactor ./compactor
COPY query ./query
COPY web ./web
COPY docker/start.sh ./start.sh
RUN chmod +x start.sh

ENV DATA_DIR=/data SEED=42 HOST=0.0.0.0 PORT=8000 PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["./start.sh"]
