FROM python:3.12-slim

RUN apt-get update && apt-get install -y curl gnupg2 unixodbc-dev && \
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
      -o /usr/share/keyrings/microsoft-prod.asc && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.asc] \
      https://packages.microsoft.com/debian/12/prod bookworm main" \
      > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    apt-get clean

WORKDIR /app

COPY src/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config/ ./config/
COPY src/     ./src/

RUN mkdir -p data/bronze data/silver data/gold data/knime_silver_process reports

CMD ["python", "src/pipeline.py"]