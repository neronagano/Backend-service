# Log Service

Асинхронный сервис сбора и поиска логов: FastAPI принимает события, Kafka хранит очередь,
worker записывает пакеты в Elasticsearch. Сервис можно запускать на одном сервере через
Docker Compose. Автоматическое развёртывание и CI/CD в проект не входят.

```text
POST /logs → Kafka app-logs → worker → Elasticsearch → GET /logs
                                  ↘ app-logs-dlq
```

- Публикация с подтверждением Kafka, пакетная запись и повторяемые ID документов.
- DLQ для некорректных событий и документов, отклонённых Elasticsearch.
- Ограниченные по времени запросы, повторы временных ошибок и безопасное завершение.
- Версионирование схемы Elasticsearch и отдельная команда миграции.
- Необязательный API-ключ, лимит тела запроса и проверки состояния.

## Быстрый запуск

Нужны Docker с Compose v2; выделите Docker не менее 4 ГиБ памяти.

```bash
git clone https://github.com/neronagano/Backend-service.git
cd Backend-service
docker compose up --build -d --wait --wait-timeout 300
```

По умолчанию доступны:

| Сервис | Адрес |
| --- | --- |
| API | http://localhost:8080 |
| Swagger | http://localhost:8080/docs |
| Kafka для внешнего источника | localhost:9092 |
| Elasticsearch | http://localhost:9200 |

Порты привязаны к `127.0.0.1`. Для изменения настроек скопируйте `.env.example` в `.env`.
Без `API_KEY` локальный API работает без авторизации. Для запуска на сервере задайте ключ,
отключите Swagger и настройте HTTPS на reverse proxy — [инструкция](docs/operations.md).

Compose создаёт топики `app-logs` и `app-logs-dlq` перед запуском приложений. Kafka и
Elasticsearch сохраняют данные в именованных volumes. API и worker работают без root,
с файловой системой только для чтения и ограничениями ресурсов. Зависимости, включая
транзитивные, закреплены в `uv.lock`; Docker собирает окружение через `uv sync --locked`.

## API

```bash
curl -X POST http://localhost:8080/logs -H 'Content-Type: application/json' \
  -d '{"service":"payments","level":"ERROR","message":"Database connection timeout"}'
curl 'http://localhost:8080/logs?service=payments&level=ERROR&q=timeout&limit=20'
```

Если ключ настроен, добавьте заголовок `X-API-Key`. В Swagger для этого есть **Authorize**.
В Windows PowerShell можно отправить событие через `Invoke-RestMethod`:

```powershell
$body = @{ service = 'payments'; message = 'Application started' } | ConvertTo-Json
Invoke-RestMethod http://localhost:8080/logs -Method Post -ContentType application/json -Body $body
```

| Метод | Путь | Результат |
| --- | --- | --- |
| POST | `/logs` | `202`, ID и `status: queued` после подтверждения Kafka |
| GET | `/logs` | Поиск и пагинация |
| GET | `/logs/{id}` | Событие по UUID или `404` |
| GET | `/live` | Процесс отвечает; не обращается к зависимостям |
| GET | `/health` | Доступность индекса Elasticsearch; `503` при сбое |
| GET | `/ready` | Доступность Kafka и Elasticsearch; используется Docker healthcheck |

`queued` не означает, что событие уже появилось в поиске. При обычных настройках запись
и обновление индекса занимают несколько секунд. Ответ `503` или сетевой таймаут не исключают
позднюю доставку: при повторе отправляйте **тот же ID события**.

При отсутствии `id` и `timestamp` API создаёт их. Для прямой отправки в Kafka оба поля
обязательны. Время — ISO 8601 с часовым поясом. Неизвестные поля запрещены.

```json
{
  "id": "f2cf8277-14d4-47e7-8c60-8476ac0045fb",
  "timestamp": "2026-10-12T20:38:47+03:00",
  "service": "payments",
  "level": "ERROR",
  "message": "Database connection timeout",
  "trace_id": "request-42"
}
```

Уровни: `DEBUG`, `INFO`, `WARN`, `ERROR`, `CRITICAL`. Фильтры: `service`, `level`, `q`,
`from_time`, `to_time`, `offset`, `limit`. Сортировка: время по убыванию, затем ID по возрастанию.
Максимум 1000 событий на странице и 10000 в окне пагинации. Поиск не фиксирует снимок:
новые события могут сдвигать страницы. Неверные параметры дают `422`, неверный ключ — `401`,
слишком большое тело — `413`, слишком медленная передача тела — `408`, сбой зависимости — `503`.

## Доставка и DLQ

Worker собирает до 200 событий либо ждёт секунду после первого события. Bulk-запросы
ограничены 2 МиБ. Проверяется результат каждого документа, включая ответы HTTP 200 с ошибками.
Временные ошибки сети, `429` и `5xx` повторяются с увеличением задержки до 10 секунд.
Весь пакет ограничен 120 секундами; при превышении worker завершается без подтверждения offsets.

Некорректные события и постоянные ошибки отдельных документов уходят в DLQ. Исходные байты,
ключ и заголовки сохраняются; служебные заголовки `dlq.*` содержат причину, топик, partition,
offset и время ошибки. Ключ DLQ — `source_topic:partition:offset`.

Offsets подтверждаются только после записи всех корректных событий и подтверждения всех
отправок в DLQ. При недоступности DLQ offsets остаются неподтверждёнными. Ошибка доступа к
индексу или несовместимая схема останавливают worker, не перемещая весь пакет в DLQ.

Доставка — **at least once**: при сбое после записи, но до commit возможны повторы.
В Elasticsearch одинаковый ID перезаписывает тот же документ; в DLQ возможны несколько
записей с одним ключом. При остановке незавершённый пакет остаётся в Kafka для повторного
чтения. [Просмотр DLQ, исправление событий и диагностика](docs/operations.md#dlq).

## Проверки

Нужен uv; Python 3.12 задан в `.python-version`. По умолчанию устанавливается группа `dev`:

```bash
uv sync --locked
uv run --locked pytest -q
uv run --locked ruff check .
uv run --locked ruff format --check .
```

Проверки в Linux-контейнере не требуют запущенных Kafka и Elasticsearch:

```bash
docker compose --profile test run --build --rm --no-deps tests
```

Проверка работающего сервиса и отдельная проверка DLQ:

```bash
python scripts/smoke_test.py
uv run --locked python scripts/check_dlq.py
```

Smoke-проверка отправляет события, ждёт индексацию, проверяет повторную отправку с тем же ID,
фильтры, пагинацию и валидацию. Если API защищён, передайте ключ через переменную окружения
`API_KEY` в терминале. Скрипт DLQ отправляет одно некорректное событие и проверяет сохранение
его байтов и координат, не изменяя offsets группы worker. Обе проверки создают тестовые данные.

Тестовый источник прямой отправки в Kafka:

```bash
docker compose run --rm producer python -m log_service.producer --count 20 --service demo-service
```

## Структура

```text
log_service/
  main.py          # фабрика приложения, lifespan и регистрация роутеров
  api/             # маршруты, зависимости, авторизация и лимит запросов
  config.py        # проверяемые настройки окружения
  models.py        # контракты событий и ответов
  kafka.py         # публикация и проверка доступности брокера
  dlq.py           # сохранение исходного сообщения и метаданных ошибки
  worker.py        # пакеты, повторы и подтверждение offsets
  storage.py       # Elasticsearch: запись, поиск и проверка ответов
  schema.py        # текущая версия и mapping индекса
  migrations.py    # команда создания, обновления и проверки схемы
  producer.py      # тестовый источник
scripts/           # проверки работающего контура
tests/            # тесты без внешних сервисов
```

`create_app()` позволяет изолировать приложения в тестах. Подключения создаются и закрываются
в `lifespan`; маршруты получают их через `Depends`. [Настройки, миграции и эксплуатация](docs/operations.md).
