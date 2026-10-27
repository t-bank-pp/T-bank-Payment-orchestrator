# T-bank Payment orchestrator

Учебный оркестратор тестовых платежей. Деньги, получатели и каналы вымышлены.

В рамках **TBPO-5** добавлена основа `payment-service`: FastAPI, настройки из окружения, асинхронное подключение к PostgreSQL 16, модели и первая миграция Alembic. **TBPO-10** добавляет получение платежа и его попыток. Создание платежей, маршрутизация и Kafka-процессы добавляются отдельными задачами.

## Быстрый запуск в Docker

Нужен запущенный Docker с Compose v2. Команды выполняются из корня репозитория.

```sh
docker compose up -d --wait postgres
docker compose build payment
docker compose run --rm payment alembic upgrade head
docker compose up -d --wait payment
```

- Swagger: <http://localhost:8000/docs>
- OpenAPI: <http://localhost:8000/openapi.json>
- Liveness: <http://localhost:8000/health/live> — процесс работает, без обращения к БД.
- Readiness: <http://localhost:8000/health/ready> — выполняет `SELECT 1`, возвращает 503 при недоступной БД.

## Получение платежа

`GET /payments/{payment_id}` принимает UUID и возвращает текущие данные платежа:

```json
{
  "payment_id": "b14024b9-9c83-4f44-b4ca-3084bcb8395a",
  "amount_minor": 125000,
  "currency": "RUB",
  "recipient": "merchant-demo-001",
  "status": "in_progress",
  "attempts": [
    {
      "attempt_id": "8595990f-a31e-4874-9db5-0e20880a454c",
      "number": 1,
      "channel_id": "A",
      "outcome": null
    }
  ]
}
```

Попытки отсортированы по `number`. Если результата ещё нет, `outcome` присутствует со значением `null`; если попыток нет, возвращается `[]`. Поля идемпотентности, хеш запроса, версия и внутреннее объяснение выбора в этот ответ не включаются. Чтение платежа и попыток выполняется одним SQL-запросом.

Несуществующий UUID возвращает `404` с телом `{"error":{"code":"PAYMENT_NOT_FOUND","message":"Платёж не найден"}}`. Некорректный формат UUID возвращает стандартную ошибку валидации FastAPI `422`. Схемы обоих ответов, `200` и `404`, доступны в `/docs`. Для проверки успешного чтения нужен платёж в БД; создание через POST реализуется в TBPO-11.

## Управление локальным окружением

Миграции запускаются явно, а не при старте каждого экземпляра API. Readiness проверяет соединение с БД; сама по себе она не подтверждает применение миграций.

PostgreSQL доступен локально на `127.0.0.1:55432`, API — на `127.0.0.1:8000`. Внутри Compose приложение использует `postgres:5432`. Учётные данные `payment/payment` предназначены только для локального окружения.

```sh
docker compose stop             # остановить, сохранив данные
docker compose down             # удалить контейнеры, сохранив том БД
```

## Локальная разработка

Нужен Python 3.12. База остаётся в Docker, приложение работает в виртуальном окружении.

```sh
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.lock
python -m pip install --no-deps -e .
python -m pip install --no-deps -e libs/contracts
cp .env.example .env
docker compose up -d --wait postgres
alembic upgrade head
uvicorn payment.main:create_app --factory --reload
```

Значения переменных окружения имеют приоритет над `.env`. Реальный `.env` не попадает в Git. Обязательная настройка `PAYMENT_DATABASE_URL` принимает URL вида `postgresql+asyncpg://user:password@host:port/database`. Оставшиеся настройки и локальный пример находятся в `.env.example`.

## Структура

```text
services/payment/
  src/payment/
    api/               # GET платежа и публичные схемы ответа
    config.py          # настройки Pydantic Settings
    main.py            # фабрика FastAPI и health endpoints
    db/
      base.py          # DeclarativeBase и имена ограничений
      models.py        # Payment, PaymentAttempt, OutboxMessage
      queries.py       # чтение платежа и попыток одним запросом
      session.py       # async engine, sessions и get_session
  migrations/          # Alembic и начальная ревизия
  tests/               # API/config и интеграционные проверки PostgreSQL
  Dockerfile
alembic.ini
compose.yaml
pyproject.toml
requirements.lock      # закреплённые runtime-зависимости
requirements-dev.lock  # runtime и инструменты разработки
```

Пакет `payment` установлен из `services/payment/src`, поэтому команды запускаются из корня. Будущий `executor` сможет использовать отдельный пакет и образ.

## Схема и транзакции

- `payments`: UUID, уникальный `idempotency_key`, `request_hash`, `amount_minor` типа bigint с проверкой `> 0`, валюта, получатель, статус, версия и даты.
- `payment_attempts`: UUID, внешний ключ на `payments`, уникальная пара `(payment_id, number)`, канал, исход, код причины, JSONB с объяснением и даты.
- `outbox`: UUID сообщения, topic, строковый message_key, JSONB payload и даты создания/публикации. Есть частичный индекс для неопубликованных записей.
- Все даты хранятся как `timestamptz`. UUID создаются и при ORM-вставке, и при вставке SQL с использованием значения по умолчанию PostgreSQL.
- `outcome` допускает `NULL`, `succeeded`, `failed`, `unknown`; `published_at` остаётся `NULL` до подтверждения отправки.

`get_session` выдаёт сессию без автоматического commit. Будущий обработчик POST должен сохранить платёж, попытку и outbox внутри одного `async with session.begin()`. Версия платежа подготовлена для последующей реализации оптимистичной блокировки. ORM обновляет `updated_at` при UPDATE через SQLAlchemy; прямые SQL-обновления должны задавать её явно.

После изменения схемы:

```sh
alembic revision --autogenerate -m "describe change"
# проверить созданный файл миграции
alembic upgrade head
alembic check
```

Начальная ревизия поддерживает `downgrade base`; откат удаляет три таблицы и их данные. Выполняйте его только в тестовом окружении.

## Проверки

Общие контракты находятся в `libs/contracts`. Пакет содержит команду `ExecuteAttempt`, JSON-кодек и пример сообщения в `libs/contracts/examples/execute_attempt.json`. Он устанавливается отдельно от `payment`; для локальных проверок выполните обе команды установки из раздела разработки.

```sh
ruff check .
ruff format --check .
mypy
mypy --config-file libs/contracts/pyproject.toml libs/contracts/contracts
pytest -q -m "not integration"
```

Интеграционные тесты выполняют реальные миграции на PostgreSQL, проверяют уникальность, внешний ключ, положительную сумму, JSONB, даты, общий commit/rollback и соответствие моделей схеме.

```sh
TEST_DATABASE_URL=postgresql+asyncpg://payment:payment@127.0.0.1:55432/postgres pytest -q
```

`TEST_DATABASE_URL` задаётся явно и указывает на локальный/тестовый PostgreSQL. Пользователю нужно право `CREATE DATABASE`: тесты создают базы `tbpo5_test_<uuid>` и удаляют только эти базы. Без этой переменной интеграционные тесты пропускаются. `pytest` из корня запускает тесты payment и контрактов; GitHub Actions устанавливает оба пакета, задаёт переменную и выполняет все проверки на PostgreSQL 16.

Зависимости закреплены в двух lock-файлах. Для обновления после изменения `pyproject.toml` используйте `uv pip compile pyproject.toml -o requirements.lock` и `uv pip compile pyproject.toml --extra dev -o requirements-dev.lock` под Python 3.12.
