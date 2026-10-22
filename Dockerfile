FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc ca-certificates && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN sed -i -E '/^mozilla\/(Comodo_AAA_Services_root|emSign_(ECC_Root_CA|Root_TLS_CA|Root_CS_CA)_-_G3)\.crt$/s/^/!/' /etc/ca-certificates.conf \
    && update-ca-certificates \
    && python -m pip install --no-cache-dir --cert /etc/ssl/certs/ca-certificates.crt --upgrade pip==26.2.2

COPY pyproject.toml .
COPY src/ src/
COPY scripts/ scripts/

RUN pip install --no-cache-dir ".[api]" "certifi>=2026.10.19"

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_DIR=/etc/ssl/certs

EXPOSE 8080

CMD ["crawler", "serve", "--port", "8080"]
