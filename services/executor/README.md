# Executor — основа сервиса и Kafka consumer (TBPO-15/16)

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

**TBPO-17** добавляет [обработчик queued и демонстрационный канал A](EXECUTION.md).
Вебхуки, сверка unknown, другие каналы и события результата добавляются отдельно. Защита от повторов регистрации сама по
себе не гарантирует однократный внешний вызов — его идемпотентность будет
реализована вместе с исполнением. Публичного HTTP endpoint для запуска попытки нет.

## Приём команд из Kafka (TBPO-16)

После миграции executor и запуска Kafka с явным созданием топика:

```sh
docker compose up -d --wait executor-postgres kafka
docker compose build executor executor-consumer
docker compose run --rm executor alembic -c executor-alembic.ini upgrade head
docker compose up -d executor-consumer
```

Локально: `python -m executor.consumer`. Это отдельный процесс от HTTP API.
Он подписан на `attempt.commands`; group `executor-attempts-v1`, auto commit
выключен, isolation_level=read_committed. Новая группа начинает с earliest;
существующая продолжает со своего подтверждённого offset. Для нескольких процессов:
`docker compose up -d --scale executor-consumer=2 executor-consumer`.

Записи обрабатываются последовательно в каждом процессе. После проверки
контракта и точного UTF-8 ключа payment_id выполняется транзакция регистрации.
Только после её commit consumer подтверждает `{topic-partition: offset + 1}`
для конкретной записи. Позиции других прочитанных/предзагруженных записей не
подтверждаются. Ошибка/таймаут БД не подтверждает offset; при сбое Kafka commit
регистрация уже может сохраниться. Повтор использует inbox и attempt_id.

При rebalance commit может отказать. Процесс закрывает consumer и подключается
заново со стабильной group, читая подтверждённые позиции; подтверждённые в БД
повторы не создают вторую попытку. Тот же путь используется при временных ошибках.
Это at-least-once приём в queued; исполнение канала пока не запускается.
История, удалённая retention Kafka, этим механизмом не восстанавливается.

Невалидный JSON/контракт (включая tombstone), неверный/отсутствующий ключ или
конфликт ID сохраняются в `executor_rejections`. Постоянные ошибки представления JSONB (например NUL в строке или переполнение числа) также сохраняются как INVALID_COMMAND; ошибки подключения и транзакции повторяются. Поля: topic, partition,
kafka_offset (составной PK), reason, raw_key/raw_value (bytea, nullable), received_at
(timestamptz). Причины: INVALID_COMMAND, KEY_MISMATCH, COMMAND_CONFLICT. Неудачная
регистрация сначала полностью откатывается, затем отдельная транзакция сохраняет
отклонённую запись. Её commit также предшествует подтверждению offset.

Повтор позиции с теми же байтами не меняет сохранённое отклонение. Другие байты
в существующей позиции вызывают SourceRecordConflict без подтверждения — например,
после удаления и создания топика заново. Для новой истории нужен отдельный топик/
контур БД; старые записи нельзя автоматически считать принятыми. Rejections —
локальный журнал для разбора, без автоматического повторного исполнения и без
Kafka DLQ. Тело сообщения и данные подключения не выводятся в логи consumer.

| Настройка | По умолчанию | Назначение |
|---|---|---|
| EXECUTOR_KAFKA_BOOTSTRAP_SERVERS | 127.0.0.1:19092 | В Compose kafka:9092 |
| EXECUTOR_CONSUMER_GROUP | executor-attempts-v1 | Стабильная группа |
| EXECUTOR_CONSUMER_REQUEST_TIMEOUT | 5 | Предел start/commit, секунды |
| EXECUTOR_CONSUMER_PROCESSING_TIMEOUT | 10 | Предел обработки и commit БД |
| EXECUTOR_CONSUMER_POLL_INTERVAL | 1 | Максимальное ожидание getone перед проверкой сигнала |
| EXECUTOR_CONSUMER_RETRY_INTERVAL | 1 | Пауза перед переподключением |
| EXECUTOR_CONSUMER_SHUTDOWN_TIMEOUT | 5 | Предел закрытия клиента |

SIGINT/SIGTERM прекращает получение новых записей; текущая обработка и закрытие
ограничены таймаутами. Зависшее закрытие завершает процесс с ошибкой, Compose
перезапускает его. При увеличении таймаутов увеличьте stop_grace_period относительно
суммы processing + request + shutdown (по умолчанию 30 секунд).

Семантика offset и ручного подтверждения: [документация aiokafka](https://aiokafka.readthedocs.io/en/stable/consumer.html).

## Проверки

```sh
TEST_EXECUTOR_DATABASE_URL=postgresql+asyncpg://executor:executor@127.0.0.1:55433/postgres \
  pytest -q services/executor/tests
```

Тесты создают и удаляют только собственные базы `tbpo15_test_<uuid>`. Нужна
роль с правом CREATE DATABASE. Без переменной интеграционные тесты пропускаются.
Проверяются дубли, конфликтующие ID, параллельные доставки, откат, миграции,
ограничения и readiness. Kafka-проверки при TEST_KAFKA_BOOTSTRAP_SERVERS проверяют
POST → outbox → Kafka → executor, перезапуск после потери commit offset и
некорректную запись перед корректной в одной партиции. Используются отдельные
группы; топик не удаляется. Обычные worker/consumer на время тестов остановлены.
Для полного pytest задаются также TEST_DATABASE_URL
и TEST_KAFKA_BOOTSTRAP_SERVERS из корневого README. CI использует отдельный
PostgreSQL executor и выполняет миграции, alembic check и все тесты.
