# syntax=docker/dockerfile:1
FROM python:3.11-slim AS base

WORKDIR /app

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore

# 의존성 레이어
COPY pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install $(python -c "import tomllib; print(*tomllib.load(open('pyproject.toml', 'rb'))['project']['dependencies'])")

# 소스 및 Alembic 마이그레이션 파일
COPY README.md alembic.ini ./
COPY src ./src
COPY config ./config
COPY alembic ./alembic

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-deps .

FROM base AS test

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install $(python -c "import tomllib; print(*tomllib.load(open('pyproject.toml', 'rb'))['project']['optional-dependencies']['dev'])")
COPY tests ./tests
# 부하 테스트 도구는 운영 이미지에 필요 없다. 테스트가 임포트하므로 여기서만 싣는다.
COPY tools ./tools
RUN pytest

FROM base AS inference
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install torch==2.11.0 --index-url https://download.pytorch.org/whl/cpu && \
    pip install '.[inference,demo]'
EXPOSE 8000
CMD ["sh", "-c", "alembic upgrade head && exec python -m realtime_analysis"]

FROM base AS runtime

EXPOSE 8000

CMD ["sh", "-c", "alembic upgrade head && exec python -m realtime_analysis"]
