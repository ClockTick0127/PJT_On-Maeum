# syntax=docker/dockerfile:1
FROM python:3.11-slim-bookworm AS base
WORKDIR /app
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_ROOT_USER_ACTION=ignore PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

COPY --from=analysis pyproject.toml /packages/analysis/pyproject.toml
COPY --from=analysis README.md /packages/analysis/README.md
COPY --from=analysis src /packages/analysis/src
RUN --mount=type=cache,target=/root/.cache/pip pip install /packages/analysis

COPY --from=silver pyproject.toml /packages/silver/pyproject.toml
COPY --from=silver README.md /packages/silver/README.md
COPY --from=silver src /packages/silver/src
RUN --mount=type=cache,target=/root/.cache/pip pip install /packages/silver

COPY pyproject.toml /packages/gold-profile/pyproject.toml
COPY src /packages/gold-profile/src
RUN --mount=type=cache,target=/root/.cache/pip pip install /packages/gold-profile

FROM base AS test
RUN --mount=type=cache,target=/root/.cache/pip pip install "/packages/gold-profile[dev]"
COPY tests /app/tests
COPY pyproject.toml /app/pyproject.toml
RUN python -m pytest /app/tests

FROM base AS runtime
CMD ["gold-profile", "run"]
