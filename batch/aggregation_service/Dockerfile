# syntax=docker/dockerfile:1

FROM python:3.11-slim AS base

WORKDIR /app

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# 동일 저장소의 분석 패키지를 라이브러리로 설치한다.
COPY ai/realtime-analysis-service/pyproject.toml /packages/analysis/pyproject.toml
COPY ai/realtime-analysis-service/README.md /packages/analysis/README.md
COPY ai/realtime-analysis-service/src /packages/analysis/src

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install /packages/analysis

# 집계 서비스 설치
COPY batch/aggregation_service/pyproject.toml /packages/aggregation/pyproject.toml
COPY batch/aggregation_service/src /packages/aggregation/src

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install /packages/aggregation

# 기존과 동일한 가구 설정 파일을 사용한다.
COPY ai/realtime-analysis-service/config/baselines.json /app/config/baselines.json

FROM base AS test

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install "/packages/aggregation[dev]"

COPY batch/aggregation_service/tests /app/tests

RUN python -m pytest /app/tests

FROM base AS runtime

EXPOSE 8000

CMD ["python", "-m", "aggregation_service"]
