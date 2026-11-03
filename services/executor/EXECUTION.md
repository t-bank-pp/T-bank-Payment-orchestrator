# Исполнение через тестовый канал A (TBPO-17)

`python -m executor.execution` — отдельный worker для queued из executor_db.
Kafka читает executor-consumer. Поддерживается синхронный демонстрационный
канал A. Это собственный тестовый HTTP-контракт, а не контракт Т-Банка или
готового внешнего провайдера.

## Полное демо

Из корня репозитория:

```sh
docker compose up -d --wait postgres executor-postgres kafka
docker compose build payment payment-worker executor executor-consumer executor-worker demo-channel
docker compose run --rm payment alembic upgrade head
docker compose run --rm executor alembic -c executor-alembic.ini upgrade head
docker compose --profile demo up -d --wait payment executor payment-worker executor-consumer demo-channel executor-worker
```

Профиль demo явно включает worker исполнения и имитатор. Без него обычный запуск
проекта сохраняет команды в queued. Payment: 127.0.0.1:8000; имитатор: 127.0.0.1:18080,
Swagger `/docs`. Имитатор не подключается к банкам.

Отправить POST /payments из корневого README, меняя только recipient:

| recipient | Результат executor | Виртуальное списание |
|---|---|---|
| demo-success или обычный получатель | succeeded | Сумма одной операции |
| demo-decline | failed, DEMO_DECLINED | 0 |
| demo-timeout | unknown, CHANNEL_TIMEOUT | Сумма записана, ответ задержан |

Имитатор хранит SQLite в постоянном томе demo_channel, файл /state/channel.sqlite3.
Запись результата и виртуального списания атомарна. Повтор attempt_id с тем же
нормализованным запросом возвращает прежний результат; другое тело даёт 409.
Перезапуск контейнера сохраняет журнал. Тесты используют отдельные SQLite-файлы.
Это локальный имитатор, а не платёжная инфраструктура.

**Payment пока остаётся in_progress:** результат хранится в executor_attempts.
Передача результата в payment, итоговый статус API, callbacks и сверка unknown
идут отдельными задачами. Статус executor не означает завершения всего потока.

## Захват и сохранение

1. Просроченные in_progress переводятся в unknown/EXECUTION_LEASE_EXPIRED.
2. Одна queued блокируется FOR UPDATE SKIP LOCKED. Состояние in_progress,
   случайный lease_token и lease_expires_at фиксируются до HTTP-вызова.
3. HTTP выполняется вне транзакции с общим дедлайном и таймаутами клиента.
4. Результат сохраняется только для того же token, in_progress и действующего
   lease. Терминальный или просроченный результат поздний ответ не перезапишет.

Часы lease берутся из PostgreSQL. Другие worker не выбирают in_progress.
Масштабирование: `docker compose --profile demo up -d --scale executor-worker=2 executor-worker`.
Токен защищает запись результата; провайдер также обязан поддерживать
идемпотентность внешнего вызова по attempt_id.

Подтверждённый ответ с совпадающим attempt_id завершает попытку succeeded/failed.
Таймаут, ошибка связи, HTTP не 200, неверный ответ или другой ID дают unknown.
pending/unknown из корректного ответа также дают unknown. Другие каналы не
вызываются: unknown/UNSUPPORTED_CHANNEL. В логах worker только ID и тип ошибки.

При падении после захвата неизвестно, успел ли уйти запрос. При падении после
списания, но до commit результата, исход также неизвестен. По истечении lease
такие попытки становятся unknown при следующем проходе worker. Они не возвращаются
в queued, не вызываются повторно и не переключаются на другой канал автоматически.
После timeout имитатор может показывать succeeded через GET — это ожидаемый повод
для будущей сверки, а не доказательство отказа.

Повтор Kafka-команды не сбрасывает state. SIGINT/SIGTERM прекращает новые захваты;
текущий вызов и операции БД ограничены таймаутами. Если все worker остановлены,
обработка истёкших lease возобновится при запуске. Миграция вниз отказывается
удалять новую схему при наличии не-queued попыток, сохраняя результаты.

## HTTP-контракт имитатора

POST /attempts, обязательный `Idempotency-Key: <attempt_id>`. Тело:

```json
{"attempt_id":"UUID","payment_id":"UUID","channel_id":"A","amount_minor":125000,"currency":"RUB","recipient":"demo-success"}
```

Сумма — строго integer 1..9223372036854775807. Новый запрос и идентичный повтор:
200 с одним и тем же ответом:

```json
{"attempt_id":"UUID","outcome":"succeeded","reason_code":"DEMO_APPROVED","provider_operation_id":"UUID"}
```

Другое тело прежнего attempt_id: 409; отсутствующий/неверный ключ и невалидные
данные: 422. Ошибки используют стандартный detail FastAPI.
GET /attempts/{attempt_id}: сохранённый ответ или 404, без списания.
Адаптер принимает outcome succeeded/failed/pending/unknown, обязательные reason_code
(1–100 символов) и provider_operation_id (1–255); лишние поля отклоняются.

## Настройки

| Переменная | По умолчанию |
|---|---|
| EXECUTOR_CHANNEL_A_BASE_URL | http://127.0.0.1:18080 |
| EXECUTOR_EXECUTION_REQUEST_TIMEOUT | 5 секунд |
| EXECUTOR_EXECUTION_DATABASE_TIMEOUT | 5 секунд |
| EXECUTOR_EXECUTION_LEASE_SECONDS | 60 секунд |
| EXECUTOR_EXECUTION_POLL_INTERVAL | 0.5 секунды |
| DEMO_CHANNEL_DATABASE | /tmp/tbank-channel-a.sqlite3 (Docker: /state/channel.sqlite3) |
| DEMO_CHANNEL_DELAY_SECONDS | 8 секунд для demo-timeout |

Lease должен превышать request timeout + два DB timeout; настройки проверяются
при старте. При увеличении таймаутов увеличьте stop_grace_period. HTTP redirects
и неявные прокси окружения отключены у CLI worker. Другому провайдеру нужен
адаптер к его согласованному контракту, одной замены URL недостаточно.
