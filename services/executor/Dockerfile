FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock
COPY libs/contracts ./libs/contracts
RUN pip install --no-cache-dir --no-deps ./libs/contracts
COPY services/executor ./services/executor
RUN pip install --no-cache-dir --no-deps ./services/executor
COPY executor-alembic.ini ./
RUN useradd --create-home --uid 10002 executor && mkdir /state && chown executor:executor /state
USER executor
EXPOSE 8001
CMD ["uvicorn", "executor.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8001"]
