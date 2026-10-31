# CardPulse runtime image: the frameworks, CLIs, configs and synthetic data generator.
#
#   docker build -f docker/Dockerfile -t cardpulse .
#   docker run --rm cardpulse cardpulse-silver --help
#
# CI builds it on every pull request and publishes ghcr.io/nabatripathy/cardpulse on merge to main.
FROM python:3.12-slim

LABEL org.opencontainers.image.title="cardpulse" \
      org.opencontainers.image.description="CardPulse card intelligence and next-best-offer data platform (synthetic data only)" \
      org.opencontainers.image.source="https://github.com/nabatripathy/cardpulse" \
      org.opencontainers.image.licenses="NONE"

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

# Dependencies and packages first for layer caching. Editable install, so configs and
# specs resolve relative to /app (the repository root inside the image).
COPY pyproject.toml README.md ./
COPY frameworks ./frameworks
COPY synthetic-data ./synthetic-data
RUN pip install -e ".[aws]"

COPY configs ./configs
COPY contracts ./contracts
COPY tools ./tools

RUN useradd --create-home --uid 10001 cardpulse \
    && mkdir -p /data/landing \
    && chown -R cardpulse /data /app
USER cardpulse

CMD ["sh", "-c", "cardpulse-synth --help && cardpulse-ingest --help && cardpulse-silver --help"]
