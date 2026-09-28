# syntax=docker/dockerfile:1
# Build context is the repository ai/ directory, not the service subdirectory.
FROM python:3.11-slim AS service
WORKDIR /app
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_ROOT_USER_ACTION=ignore PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY realtime-analysis-service/pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install $(python -c "import tomllib; print(*tomllib.load(open('pyproject.toml', 'rb'))['project']['dependencies'])")
COPY realtime-analysis-service/README.md realtime-analysis-service/alembic.ini ./
COPY realtime-analysis-service/src ./src
COPY realtime-analysis-service/config ./config
COPY realtime-analysis-service/alembic ./alembic
RUN --mount=type=cache,target=/root/.cache/pip pip install --no-deps .

FROM service AS fixture-publisher
RUN --mount=type=cache,target=/root/.cache/pip pip install 'aiomqtt>=2,<3'
COPY assets/nilm_r3/fixtures /assets/fixtures
RUN python -m realtime_analysis.asset_check --asset-root /assets --panels
USER 65532:65532
ENTRYPOINT ["python", "-m", "realtime_analysis.fixture_publisher"]

FROM service AS model-runtime
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install torch==2.11.0 --index-url https://download.pytorch.org/whl/cpu && pip install 'numpy>=1.26,<3'
COPY assets/nilm_r3/weights /assets/weights
COPY assets/nilm_r3/source_snapshot/m1/normalization.json /assets/source_snapshot/m1/normalization.json
RUN python -m realtime_analysis.asset_check --asset-root /assets --forward
USER 65532:65532
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=5s --start-period=60s --retries=6 \
    CMD python -c "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:'+os.getenv('HTTP_PORT','8000')+'/ready',timeout=4).close()"

FROM model-runtime AS inference
ENV MODEL_BACKEND=selected_scene MODEL_ASSET_ROOT=/assets MODEL_DEVICE=cpu MODEL_DTYPE=float32 MODEL_THREADS=1
ENTRYPOINT ["python", "-m", "realtime_analysis.scene_entrypoint"]

# Configurable target shared by Jenkins/EC2 and local Compose. Unlike the
# fail-closed targets below, it permits MODEL_BACKEND=fake|real|selected_scene
# so environments can switch modes without rebuilding a different target.
FROM model-runtime AS configurable-inference
ENV MODEL_BACKEND=real MODEL_ASSET_ROOT=/assets MODEL_WINDOW_SIZE=255 MODEL_DEVICE=cpu MODEL_DTYPE=float32 MODEL_THREADS=1
ENTRYPOINT ["python", "-m", "realtime_analysis.configurable_entrypoint"]

# Backward-compatible alias for the local Compose build.
FROM configurable-inference AS local-inference

FROM model-runtime AS realtime-inference
ENV MODEL_BACKEND=real MODEL_ASSET_ROOT=/assets MODEL_WINDOW_SIZE=255 MODEL_DEVICE=cpu MODEL_DTYPE=float32 MODEL_THREADS=1
ENTRYPOINT ["python", "-m", "realtime_analysis.real_entrypoint"]
