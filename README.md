# Log Service

Небольшой сервис для хранения и поиска логов приложений.
API написан на Python и FastAPI. Логи поступают через Kafka,
обработчик записывает их пакетами в Elasticsearch.

Для локального запуска нужны Python 3.12, Kafka на порту 9092
и Elasticsearch на порту 9200.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
uvicorn log_service.main:app --reload
```

В отдельном терминале запускается обработчик:

```bash
python -m log_service.worker
```

Документация API доступна по адресу `/docs`. Проверка состояния: `/health`.
Отправка лога: `POST /logs`, поиск: `GET /logs`, получение одного лога: `GET /logs/{id}`.
Ответ `202` означает, что лог принят в Kafka. Запись в Elasticsearch выполняется асинхронно.

```bash
pytest -q
```
