FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

# Install dependencies first for layer caching, then the project in editable mode
# (configs and specs are resolved relative to the repository root).
COPY pyproject.toml README.md ./
COPY frameworks ./frameworks
COPY synthetic-data ./synthetic-data
RUN pip install -e ".[aws]"

COPY configs ./configs
COPY contracts ./contracts

RUN useradd --create-home --uid 10001 cardpulse && mkdir -p /data/landing && chown -R cardpulse /data /app
USER cardpulse

CMD ["cardpulse-ingest", "--help"]
