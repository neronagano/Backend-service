# Log Service

Небольшой сервис для хранения и поиска логов приложений.
API написан на Python и FastAPI, логи хранятся в Elasticsearch.

Для локального запуска нужны Python 3.12 и Elasticsearch на порту 9200.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
uvicorn log_service.main:app --reload
```

Документация API доступна по адресу `/docs`. Проверка состояния: `/health`.
Поиск логов: `/logs`, получение одного лога: `/logs/{id}`.

```bash
pytest -q
```
