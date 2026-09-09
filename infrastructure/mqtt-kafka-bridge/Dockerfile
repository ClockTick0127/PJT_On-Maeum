FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bridge.py test_bridge.py smoke.py ./
RUN python -m unittest -v test_bridge
RUN useradd --uid 10001 --create-home bridge
USER bridge
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=6 CMD python -c "import pathlib,time; p=pathlib.Path('/tmp/bridge-ready'); raise SystemExit(0 if p.exists() and time.time()-p.stat().st_mtime<20 else 1)"
CMD ["python", "bridge.py"]
