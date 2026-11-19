# Каналы A, B и C

Executor использует channel_id из команды ExecuteAttempt. Адаптеры приводят
ответ каждого тестового API к succeeded, failed или unknown. Общий HTTP-клиент
ограничивает время запроса, проверяет идентификатор попытки и формат ответа.
Результат сохраняется с исходными идентификаторами и ключом Kafka payment_id.

| Канал | Создание | Ответ | Запрос статуса |
|---|---|---|---|
| A | POST /attempts, Idempotency-Key | 200, outcome | GET /attempts/{attempt_id} |
| B | POST /transfers, X-Transfer-Key | 200, transfer_state | GET /transfers/{attempt_id} |
| C | POST /jobs, Idempotency-Key | 202, phase=accepted | GET /jobs/by-attempt/{attempt_id} |

B принимает transfer_ref, payment_ref, amount (minor/currency) и beneficiary.
Его ответ содержит operation_ref, explanation и DONE/REJECTED/PENDING.
C принимает reference (attempt_id/payment_id), value (minor/currency) и recipient.
Он возвращает job_id, attempt_id и phase. accepted/pending дают unknown;
settled/rejected подтверждают исход. Имя операции сохраняется как provider_operation_id.

Повторы используют attempt_id как ключ идемпотентности. Имитаторы хранят журнал
операций в отдельных постоянных SQLite-томах. Повтор возвращает исходную
операцию и прежнее виртуальное списание, другое тело того же attempt_id даёт 409.
При unknown попытка ожидает сверку или подтверждение канала.

## Канал C и callbacks

Запись операции C и callback_outbox атомарна в SQLite. В отдельном процессе
python -m executor.demo_callback_delivery берётся сохранённое событие.
Оно подписывается по контракту из [RESULTS.md](RESULTS.md) и отправляется в
POST /callbacks/C. После 200 отмечается доставленным. Сбой перед отметкой
приводит к повтору с тем же event_id, который обрабатывается через дедупликацию
Executor. Время и подпись отправки обновляются на каждом повторе.

Доставка использует DEMO_CALLBACK_DATABASE, DEMO_CALLBACK_URL,
DEMO_CALLBACK_SECRET, DEMO_CALLBACK_REQUEST_TIMEOUT (5 секунд),
DEMO_CALLBACK_RETRY_INTERVAL (1 секунда) и DEMO_CALLBACK_POLL_INTERVAL (0.5 секунды).
Статус операции также доступен через GET, что позволяет завершить сверку
при задержке вебхука.

## Запуск полного демо через канал C

Команды выполняются из корня проекта. Значение секрета ниже предназначено
для локального демо. Executor и отправитель callback используют одинаковое значение.

```sh
export COMPOSE_PROFILES=demo,channels,callbacks
export PAYMENT_FIXED_CHANNEL_ID=C
export DEMO_CALLBACK_SECRET=local-demo-channel-c-secret-at-least-32-characters
export EXECUTOR_CALLBACK_SECRETS='{"C":"local-demo-channel-c-secret-at-least-32-characters"}'

docker compose build
docker compose up -d --wait postgres executor-postgres kafka
docker compose run --rm payment alembic upgrade head
docker compose run --rm executor alembic -c executor-alembic.ini upgrade head
docker compose up -d --wait

curl -s http://127.0.0.1:8000/payments \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: channel-c-demo-001' \
  -d '{"amount_minor":125000,"currency":"RUB","recipient":"demo-success"}'
```

Используйте возвращённый payment_id в GET /payments/{payment_id}. По приходу
вебхука needs_review меняется на succeeded. demo-decline даёт failed после
подтверждения. Канал B выбирается значением PAYMENT_FIXED_CHANNEL_ID=B.
Примеры его API доступны в Swagger http://127.0.0.1:18081/docs,
C - http://127.0.0.1:18082/docs.

Для сверки запустите docker compose --profile reconciliation up -d executor-reconciliation.
Worker поддерживает все три адаптера и выполняет GET на канале исходной команды.
Используйте docker compose stop для остановки с сохранением данных.

PAYMENT_FIXED_CHANNEL_ID задаёт фиксированный канал для новых платежей.
Значение по умолчанию A. Повтор прежнего idempotency key сохраняет исходный
платёж и его канал. EXECUTOR_CHANNEL_A_BASE_URL, EXECUTOR_CHANNEL_B_BASE_URL,
EXECUTOR_CHANNEL_C_BASE_URL задают отдельные адреса адаптеров.
