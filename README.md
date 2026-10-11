# Log Service

Небольшой бэкенд для сбора и поиска логов приложений.
Сервис-источник отправляет сообщения в Kafka. Обработчик читает сообщения
и записывает их пакетами в Elasticsearch. API позволяет отправить лог
и найти сохранённые записи.

```text
Источник -> Kafka -> worker -> Elasticsearch
              ^                    ^
              |                    |
          POST /logs          GET /logs
```

## Запуск

Нужен Docker с Docker Compose.

```bash
git clone https://github.com/neronagano/Backend-service.git
cd Backend-service
docker compose up --build -d --wait
```

- Swagger: http://localhost:8080/docs
- API: http://localhost:8080
- Elasticsearch: http://localhost:9200
- Kafka для внешнего источника: `localhost:9092`, топик `app-logs`

Compose запускает Kafka, Elasticsearch, API и обработчик. Для Kafka используется
KRaft. Данные Kafka и Elasticsearch сохраняются в именованных Docker volumes.
Порты открыты только на `127.0.0.1`. Конфигурация рассчитана на локальный
учебный запуск, аутентификация пока не добавлена.

## Отправка и поиск

```bash
curl -X POST http://localhost:8080/logs \
  -H 'Content-Type: application/json' \
  -d '{"service":"payments","level":"ERROR","message":"Database connection timeout"}'
```

API создаёт ID и время события, если они не указаны. Ответ `202` со статусом
`queued` означает, что Kafka приняла сообщение. Поиск станет доступен после
пакетной записи и обновления индекса, обычно через несколько секунд.

```bash
curl 'http://localhost:8080/logs?service=payments&level=ERROR&q=timeout&limit=20'
```

| Метод | Путь | Назначение |
| --- | --- | --- |
| POST | `/logs` | Отправить лог в Kafka |
| GET | `/logs` | Найти логи |
| GET | `/logs/{id}` | Получить лог по UUID |
| GET | `/health` | Проверить API и доступность Elasticsearch |
| GET | `/docs` | Открыть Swagger |

Для поиска доступны `service`, `level`, `q`, `from_time`, `to_time`, `offset`
и `limit`. Время передаётся в ISO 8601 с часовым поясом, например
`2026-10-11T10:00:00+03:00`. Допустимые уровни: `DEBUG`, `INFO`, `WARN`,
`ERROR`, `CRITICAL`. Результаты отсортированы по времени от новых к старым.
Одна страница содержит до 1000 записей, окно пагинации ограничено 10000 записями.

Ошибочный запрос возвращает `422`, отсутствующий лог `404`, недоступный
Elasticsearch или ошибка отправки в Kafka `503`.

## Отдельный источник логов

Тестовый источник запускается отдельным контейнером и отправляет сообщения
напрямую в Kafka:

```bash
docker compose run --rm producer
```

По умолчанию отправляются 20 сообщений от `demo-service`.

```bash
docker compose run --rm producer python -m log_service.producer --count 100 --service payments
```

Другой сервис может отправлять в топик такой JSON. Для сообщений в Kafka
поля `id` и `timestamp` обязательны. Источник должен сохранять ID при повторной
отправке одного события.

```json
{
  "id": "f2cf8277-14d4-47e7-8c60-8476ac0045fb",
  "timestamp": "2026-10-11T10:00:00+03:00",
  "service": "payments",
  "level": "ERROR",
  "message": "Database connection timeout",
  "trace_id": "request-42"
}
```

## Обработка пакетов

Пакет отправляется при накоплении 200 сообщений или через одну секунду
после первого сообщения. Объём отдельного Bulk-запроса ограничен 2 МиБ.
Kafka offsets подтверждаются после успешной записи всех документов пакета.
Проверяется результат каждого документа, включая ошибки внутри ответа HTTP 200.

При недоступности Elasticsearch пакет повторяется с увеличением задержки
до 10 секунд. ID события используется как ID документа, поэтому повторная
обработка перезаписывает тот же документ. Доставка работает по принципу
at least once.

В этой версии некорректное сообщение из Kafka останавливает обработчик без
подтверждения offsets. Отдельный топик для таких сообщений пока не реализован.
Кэш, авторизация, метрики и веб-интерфейс оставлены для следующих версий.

## Настройки

Значения по умолчанию позволяют запускать Compose без файла `.env`.
Для изменения настроек можно скопировать `.env.example` в `.env`.
Внутри Compose адреса Kafka и Elasticsearch задаются именами сервисов.

| Переменная | Значение по умолчанию |
| --- | --- |
| API_PORT | 8080 |
| KAFKA_TOPIC | app-logs |
| KAFKA_GROUP_ID | log-indexer |
| ELASTICSEARCH_INDEX | app-logs |
| BATCH_SIZE | 200 |
| FLUSH_INTERVAL | 1 секунда |
| BULK_MAX_BYTES | 2097152 |

Просмотр состояния и журналов:

```bash
docker compose ps
docker compose logs -f worker
```

Остановка с сохранением данных:

```bash
docker compose down
```

## Проверки и запуск без контейнера приложения

Нужен Python 3.12 или новее.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
pytest -q
```

Проверка работающего Compose через API:

```bash
python scripts/smoke_test.py
```

Скрипт отправляет события, проверяет запись, поиск, фильтры, пагинацию
и повторную отправку с тем же ID.

Для разработки можно запустить только инфраструктуру, затем API и обработчик
в двух терминалах с активированным окружением:

```bash
docker compose up -d kafka elasticsearch
uvicorn log_service.main:app --reload --port 8080
```

```bash
python -m log_service.worker
```
