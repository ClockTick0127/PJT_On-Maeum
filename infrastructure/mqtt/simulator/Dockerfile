FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 BROWSER=none
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN useradd --uid 10001 --create-home simulator
USER simulator
EXPOSE 8085
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=6 CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8085/api/status', timeout=2)"
# 컨테이너 안에서는 0.0.0.0에 바인딩하고, 외부 노출 범위는 compose의 포트 매핑(127.0.0.1)으로 제한한다.
CMD ["python", "web_server.py", "--bind-host", "0.0.0.0"]
