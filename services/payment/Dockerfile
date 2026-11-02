FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.lock pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.lock
COPY libs/contracts ./libs/contracts
RUN pip install --no-cache-dir --no-deps ./libs/contracts
COPY services/payment/src ./services/payment/src
RUN pip install --no-cache-dir --no-deps .
COPY alembic.ini ./
COPY services/payment/migrations ./services/payment/migrations

RUN useradd --create-home --uid 10001 payment
USER payment
EXPOSE 8000
CMD ["uvicorn", "payment.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
