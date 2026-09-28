# syntax=docker/dockerfile:1
# 빌드 컨텍스트는 이 폴더이고, 분석 패키지는 named context `analysis`로 받는다:
#   docker build --build-context analysis=ai/realtime-analysis-service \
#     -f batch/power_silver_service/Dockerfile --target runtime batch/power_silver_service
# (저장소 루트를 컨텍스트로 쓰면 aggregation-service와 같은 컨텍스트를 공유하게 되어
#  비ASCII 경로에서 Compose 동시 빌드가 실패한다. session-lake-loader와 같은 방식이다.)

# bookworm으로 고정한다. trixie에는 openjdk-17이 없고, Spark 3.5의 지원 Java는 17까지다.
FROM python:3.11-slim-bookworm AS base

WORKDIR /app

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# PySpark는 JVM 위에서 돈다. Spark 3.5는 Java 17까지 지원한다.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

# 같은 저장소의 분석 패키지를 라이브러리로 설치한다(ORM 모델·DB URL 공유).
COPY --from=analysis pyproject.toml /packages/analysis/pyproject.toml
COPY --from=analysis README.md /packages/analysis/README.md
COPY --from=analysis src /packages/analysis/src

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install /packages/analysis

COPY pyproject.toml /packages/power-silver/pyproject.toml
COPY src /packages/power-silver/src

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install /packages/power-silver

FROM base AS test

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install "/packages/power-silver[dev]"

COPY tests /app/tests
COPY pyproject.toml /app/pyproject.toml

# Spark 로컬 모드로 실제 Parquet를 쓰고 읽는 테스트까지 여기서 돈다.
RUN python -m pytest /app/tests

FROM base AS runtime

# 기본은 로컬 모드다. 클러스터에 붙일 때는 SPARK_MASTER=spark://host:7077 를 준다.
CMD ["power-silver", "run"]
