# syntax=docker/dockerfile:1
# 빌드 컨텍스트는 이 폴더이고, 분석 패키지는 named context `analysis`로 받는다:
#   docker build --build-context analysis=ai/realtime-analysis-service \
#     -f collection/session-lake-loader/Dockerfile --target runtime collection/session-lake-loader
# (저장소 루트를 컨텍스트로 쓰면 aggregation-service와 같은 컨텍스트를 공유하게 되어
#  비ASCII 경로에서 Compose 동시 빌드가 실패한다.)

FROM python:3.11-slim AS base

WORKDIR /app

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# 같은 저장소의 분석 패키지를 라이브러리로 설치한다(ORM 모델·DB URL·상태 서버 공유).
COPY --from=analysis pyproject.toml /packages/analysis/pyproject.toml
COPY --from=analysis README.md /packages/analysis/README.md
COPY --from=analysis src /packages/analysis/src

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install /packages/analysis

COPY pyproject.toml /packages/loader/pyproject.toml
COPY src /packages/loader/src

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install /packages/loader

FROM base AS test

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install "/packages/loader[dev]"

COPY tests /app/tests

# PostgreSQL 표시 테스트는 TEST_DATABASE_URL이 없으면 건너뛴다.
RUN python -m pytest /app/tests

FROM base AS runtime

EXPOSE 8000

CMD ["session-lake-loader", "run"]
