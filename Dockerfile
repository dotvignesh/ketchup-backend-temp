FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_SYSTEM_PYTHON=1

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY requirements.txt .
RUN uv pip install --system -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uv", "run", "--no-project", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
