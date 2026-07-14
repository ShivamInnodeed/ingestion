FROM python:3.11-slim-bookworm

RUN set -eux; \
    printf 'Acquire::ForceIPv4 "true";\nAcquire::Retries "10";\nAcquire::http::Timeout "30";\nAcquire::https::Timeout "30";\n' > /etc/apt/apt.conf.d/80-network-tweaks; \
    if [ -f /etc/apt/sources.list.d/debian.sources ]; then \
      sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list.d/debian.sources; \
      sed -i 's|http://security.debian.org|https://security.debian.org|g' /etc/apt/sources.list.d/debian.sources; \
    fi; \
    if [ -f /etc/apt/sources.list ]; then \
      sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list; \
      sed -i 's|http://security.debian.org|https://security.debian.org|g' /etc/apt/sources.list; \
    fi; \
    apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --default-timeout=1200 --retries 20 -r /tmp/requirements.txt
RUN python -m playwright install --with-deps chromium

# Optional offline wheels (for air-gapped servers). Safe to keep empty.
COPY wheels/ /tmp/wheels/
RUN set -e; \
    if [ -d /tmp/wheels ] && [ "$(ls -A /tmp/wheels 2>/dev/null | wc -l)" -gt 0 ]; then \
      if ls /tmp/wheels/*.whl >/dev/null 2>&1; then pip install --no-cache-dir /tmp/wheels/*.whl; fi; \
      if ls /tmp/wheels/*.zip >/dev/null 2>&1; then pip install --no-cache-dir /tmp/wheels/*.zip; fi; \
    fi
COPY . /app
ENV PYTHONPATH=/app

# Pre-download embedding model into the image so the server does not need HuggingFace access.
RUN python -c "from sentence_transformers import SentenceTransformer; m=SentenceTransformer('all-MiniLM-L6-v2'); m.save('/app/models/all-MiniLM-L6-v2')"

EXPOSE 8000
CMD ["uvicorn", "api.scheduler_service:app", "--host", "0.0.0.0", "--port", "8000"]
