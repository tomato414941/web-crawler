FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc ca-certificates && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
COPY src/ src/

RUN pip install --no-cache-dir ".[api]"

ENV SSL_CERT_FILE=/usr/local/lib/python3.12/site-packages/certifi/cacert.pem

EXPOSE 8080

CMD ["crawler", "serve", "--port", "8080"]
