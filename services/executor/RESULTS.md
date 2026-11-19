# Уточнение результата попытки

Первый результат сохраняет существующий AttemptResult v1 без новых полей.
Состояние попытки и соответствующая строка outbox получают state_version=1.
Сверка или подписанный callback могут подтвердить unknown как succeeded/failed.
Они вызывают общий apply_confirmation в транзакции вызывающего кода.

Обработчик блокирует попытку, проверяет payment_id, channel_id и известный
provider_operation_id. Подтверждение unknown увеличивает state_version до 2,
создаёт отдельный event_id и новую строку outbox в той же транзакции.
Первое событие сохраняется для повторной доставки. Подтверждение уже
подтверждённого исхода не создаёт новую версию. Противоречащий исход отклоняется.
Подтверждение in_progress создаёт первый результат и очищает lease, поэтому
поздний ответ worker не перезаписывает его.

Уточнение использует AttemptResultUpdate с type=AttemptResult, version=2 и
целым state_version >= 2. version описывает схему сообщения, state_version
описывает изменение конкретной попытки. Идентификаторы и correlation_id
сохраняются из исходной команды. Общий decode_attempt_result_event читает обе
схемы. Старый decode_attempt_result продолжает читать только v1.

Consumer Payment для этого протокола добавляется в TBPO-32. Его нужно обновить
до включения сверки и callbacks: consumer v1 отклоняет события v2. Разные
publisher могут доставить версии в обратном порядке, поэтому получатель
должен учитывать state_version, включая ситуацию, когда v2 пришла первой.
Существующие v1 трактуются как state_version=1.

Миграция сохраняет версии существующих результатов и заменяет уникальность
attempt_id в outbox на пару attempt_id/state_version. Downgrade отклоняется
при наличии уточнённых событий, чтобы сохранить историю.

## Запрос статуса (TBPO-29)

`python -m executor.reconciliation` выбирает unknown и сохраняет время следующей
проверки до HTTP. После падения worker проверка станет доступна по расписанию.
Адаптер A выполняет GET /attempts/{attempt_id}; POST при сверке не используется.
404, таймаут и неоднозначный ответ сохраняют unknown для следующей проверки.
Подтверждённый ответ идёт через apply_confirmation, включая проверку известного
provider_operation_id. HTTP выполняется вне транзакции БД.

EXECUTOR_RECONCILIATION_INTERVAL задаёт интервал проверки попытки (30 секунд),
EXECUTOR_RECONCILIATION_POLL_INTERVAL - ожидание пустой очереди (0.5 секунды),
EXECUTOR_RECONCILIATION_REQUEST_TIMEOUT - общий предел HTTP (5 секунд).
БД ограничена EXECUTOR_EXECUTION_DATABASE_TIMEOUT. Compose-процесс включается
явно после обновления consumer Payment до TBPO-32:
`docker compose --profile demo --profile reconciliation up -d executor-reconciliation`.

## Подписанный callback (TBPO-30)

POST /callbacks/{channel_id} принимает JSON с event_id, occurred_at (UTC),
payment_id, attempt_id, channel_id, outcome (succeeded/failed), reason_code
и provider_operation_id. Это контракт учебного канала. Произвольному провайдеру
нужен адаптер к его формату и подписи.

Подпись X-Callback-Signature - hex HMAC-SHA256 от
`<X-Callback-Timestamp>.<исходные байты тела>`. X-Callback-Timestamp содержит
Unix-время отправки в секундах. EXECUTOR_CALLBACK_MAX_AGE задаёт допустимое
отклонение (300 секунд). Для повторной доставки отправитель может поставить
новую подпись и время отправки, сохранив event_id и тело.

EXECUTOR_CALLBACK_SECRETS задаёт JSON-словарь секретов по каналу, например
`{"A":"<случайный секрет длиной не менее 32 символов>"}`. По умолчанию словарь
пустой, endpoint возвращает 503. Секреты следует включать после TBPO-32.
Для реального окружения подпись и транспорт согласуются с провайдером.

Ответ 200 означает commit состояния, outbox и записи executor_callbacks.
Повтор того же event_id с теми же данными безопасен. Изменённое тело того же
event_id, другой канал/платёж/операция и противоречащий подтверждённый исход
дают 409. Отсутствующая попытка даёт 404 с возможностью доставки позже,
неверная подпись или время - 401, неверное тело - 422. Размер тела ограничен
16 КиБ. Ошибка БД откатывает все изменения, отправитель повторяет доставку.
