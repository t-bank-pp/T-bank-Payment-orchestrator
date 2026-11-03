# Executor — основа сервиса (TBPO-15)

Отдельный пакет `tbank-executor`, FastAPI и собственная PostgreSQL 16.
Сервис не импортирует payment и не подключается к payment_db. Общий контракт
`ExecuteAttempt` берётся из `libs/contracts`. Версии внешних зависимостей
закреплены общим `requirements.lock`; набор executor является его подмножеством.

## Запуск

Из корня репозитория:

```sh
docker compose up -d --wait executor-postgres
docker compose build executor
docker compose run --rm executor alembic -c executor-alembic.ini upgrade head
docker compose up -d --wait executor
```

Swagger: http://127.0.0.1:8001/docs. `/health/live` проверяет процесс,
`/health/ready` выполняет SELECT 1 и возвращает 503 при недоступной БД.
Миграции не выполняются автоматически при старте API.

База с хоста: `127.0.0.1:55433`, из контейнера: `executor-postgres:5432`;
пользователь/пароль `executor`, база `executor_db`. Это локальные параметры.
Порты опубликованы только на loopback. Контейнер приложения работает без root.

Для локального запуска после установки корневых зависимостей и contracts:

```sh
python -m pip install --no-deps -e services/executor
export EXECUTOR_DATABASE_URL=postgresql+asyncpg://executor:executor@127.0.0.1:55433/executor_db
alembic -c executor-alembic.ini upgrade head
uvicorn executor.main:create_app --factory --port 8001
```

Префикс настроек — `EXECUTOR_`; `PAYMENT_DATABASE_URL` не используется.
`EXECUTOR_DATABASE_CONNECT_TIMEOUT` по умолчанию 3 секунды, допустимо (0, 30].

## Долговременная регистрация команды

`register_attempt(session, command) -> bool` принимает валидированный
`ExecuteAttempt` и работает в транзакции вызывающего кода без собственного commit.
True означает новую попытку; False — ранее принятую попытку. Вызывающий код
обязан откатить всю транзакцию при `CommandConflict` или любой другой ошибке.

```python
async with database.sessions() as session, session.begin():
    created = await register_attempt(session, command)
# Будущий Kafka consumer подтверждает offset только после успешного commit.
```

- `executor_attempts`: `attempt_id` PK, `payment_id` без внешнего ключа в payment,
  `command_hash`, исходный `payload`, `state=queued`, `created_at` с часовым поясом.
- `inbox`: `event_id` PK, локальный FK `attempt_id`, `payload_hash`, полный
  `payload`, `received_at` с часовым поясом. Запись означает приём, не исполнение.

Уникальные ограничения и INSERT ON CONFLICT защищают от параллельных доставок
при стандартной изоляции PostgreSQL READ COMMITTED. Повтор того же event_id
ничего не меняет. Новое событие с прежним attempt_id и тем же содержимым добавит
только inbox. Хеш команды исключает event_id/occurred_at; остальные поля конверта
и data должны совпадать. Хеш события включает весь нормализованный конверт:
повтор event_id с другой датой или данными отклоняется. При конфликте откатывается
и новая попытка, если она уже была вставлена в этой транзакции.

Исходный payload попытки не перезаписывается повторами, состояние не сбрасывается.
Даты сохраняются как timestamptz. Числа сериализуются без преобразования во float.

Этап не подключает Kafka consumer, обработку очереди queued, адаптеры каналов,
вебхуки, списание и события результата. Защита от повторов регистрации сама по
себе не гарантирует однократный внешний вызов — его идемпотентность будет
реализована вместе с исполнением. Публичного HTTP endpoint для запуска попытки нет.

## Проверки

```sh
TEST_EXECUTOR_DATABASE_URL=postgresql+asyncpg://executor:executor@127.0.0.1:55433/postgres \
  pytest -q services/executor/tests
```

Тесты создают и удаляют только собственные базы `tbpo15_test_<uuid>`. Нужна
роль с правом CREATE DATABASE. Без переменной интеграционные тесты пропускаются.
Проверяются дубли, конфликтующие ID, параллельные доставки, откат, миграции,
ограничения и readiness. Для полного pytest задаются также TEST_DATABASE_URL
и TEST_KAFKA_BOOTSTRAP_SERVERS из корневого README. CI использует отдельный
PostgreSQL executor и выполняет миграции, alembic check и все тесты.
