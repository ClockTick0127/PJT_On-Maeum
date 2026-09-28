# syntax=docker/dockerfile:1
# Runs on top of the gold-profile image so power_silver.storage, the hdfs client and
# the gold-profile / household-report CLIs are all available in one container.
ARG BASE_IMAGE=nilm-gold-profile:local
FROM ${BASE_IMAGE} AS base
WORKDIR /app
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_ROOT_USER_ACTION=ignore PYTHONUNBUFFERED=1

COPY pyproject.toml /packages/history-generator/pyproject.toml
COPY src /packages/history-generator/src
RUN --mount=type=cache,target=/root/.cache/pip pip install "/packages/history-generator[profiles]"

# The simulator physics engine is imported from a read-only mount, never copied:
#   -v <repo>/infrastructure/mqtt/simulator:/simulator:ro
COPY scenarios /app/scenarios

FROM base AS test
RUN --mount=type=cache,target=/root/.cache/pip pip install "/packages/history-generator[dev]"
COPY tests /app/tests
COPY pyproject.toml /app/pyproject.toml
RUN python -m pytest /app/tests -q

FROM base AS runtime
ENTRYPOINT ["history-generator"]
CMD ["--help"]
