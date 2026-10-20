# Эксплуатация

## Запуск на сервере

Compose рассчитан на один сервер и доверенную внутреннюю сеть. Kafka и Elasticsearch
работают по одному экземпляру, без аутентификации между контейнерами. Потеря диска или
сервера требует восстановления из резервной копии; это не отказоустойчивый кластер.

Для Linux проверьте `vm.max_map_count`: для Elasticsearch 8.19 Elastic указывает значение
`1048576`. [Требования Elasticsearch в Docker](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/docker.html#docker-prod-prerequisites).

```bash
sysctl vm.max_map_count
sudo sysctl -w vm.max_map_count=1048576
```

Для сохранения после перезагрузки задайте значение в `/etc/sysctl.conf`.
На Docker Desktop параметр относится к Linux VM/WSL; способ настройки зависит от платформы
и приведён в той же документации Elastic.

1. Скопируйте `.env.example` в `.env`. Установите `API_KEY` длиной от 16 символов,
   `DOCS_ENABLED=false`, `AUTO_MIGRATE=false`. `.env` исключён из Git.
2. Соберите образ и подготовьте индекс:

   ```bash
   docker compose build api
   docker compose up -d --wait kafka elasticsearch
   docker compose --profile tools run --rm --no-deps migrate
   docker compose --profile tools run --rm --no-deps migrate python -m log_service.migrations --check
   docker compose up -d --wait --wait-timeout 300 api worker
   ```

3. Настройте HTTPS на reverse proxy, направив запросы на `127.0.0.1:8080`.
   Ограничьте частоту запросов на прокси. Не публикуйте порты Kafka и Elasticsearch в Интернет.
4. Проверьте `/ready`, отправку и поиск через `scripts/smoke_test.py`. Для защищённого
   API задайте `API_KEY` также в окружении терминала: smoke-скрипт не читает `.env`.

Случайный ключ можно создать командой `python -c "import secrets; print(secrets.token_urlsafe(32))"`.
Ключ общий для API; отдельных пользователей и разграничения прав здесь нет.
Не передавайте его в URL. Проверки `/live`, `/health`, `/ready` доступны без ключа.

## Настройки

Все настройки приложения читаются из окружения или `.env`. Неверные значения останавливают
запуск. В Compose адреса зависимостей задаются именами сервисов; переменные с `localhost`
в `.env.example` предназначены для приложения, запущенного вне Docker.

| Переменная | По умолчанию | Назначение |
| --- | --- | --- |
| `API_PORT` | `8080` | Порт API на localhost; только Compose |
| `API_KEY` | пусто | Общий ключ `X-API-Key`; пустое значение отключает проверку |
| `DOCS_ENABLED` | `true` | Swagger и OpenAPI; ReDoc отключён |
| `AUTO_MIGRATE` | `true` | Создать или обновить схему при запуске; на сервере предпочтительно `false` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Брокеры для запуска вне Docker |
| `KAFKA_TOPIC` | `app-logs` | Топик событий |
| `KAFKA_DLQ_TOPIC` | `app-logs-dlq` | Отдельный топик отклонённых событий |
| `KAFKA_GROUP_ID` | `log-indexer` | Группа worker |
| `ELASTICSEARCH_URL` | `http://localhost:9200` | Адрес Elasticsearch вне Docker |
| `ELASTICSEARCH_INDEX` | `app-logs` | Конкретный индекс; alias этим мигратором не обслуживается |
| `BATCH_SIZE` | `200` | От 1 до 1000 событий в пакете |
| `FLUSH_INTERVAL` | `1` | Ожидание пакета в секундах; максимум 30 |
| `BULK_MAX_BYTES` | `2097152` | Размер отдельного Bulk-запроса в байтах |
| `REQUEST_TIMEOUT` | `10` | Таймаут HTTP-запроса к Elasticsearch, секунды |
| `STARTUP_TIMEOUT` | `30` | Ограничение запуска подключений, секунды |
| `PUBLISH_TIMEOUT` | `10` | Ожидание подтверждения публикации Kafka, секунды |
| `SHUTDOWN_TIMEOUT` | `10` | Ожидание остановки каждого клиента Kafka, секунды |
| `WORKER_BATCH_TIMEOUT` | `120` | Общий срок обработки пакета; максимум 240 секунд |
| `MAX_REQUEST_BYTES` | `131072` | Размер тела HTTP-запроса, включая chunked-запросы |
| `BODY_READ_TIMEOUT` | `10` | Время получения всего тела, секунды |
| `KAFKA_RETENTION_HOURS` | `168` | Хранение Kafka; только Compose |
| `KAFKA_RETENTION_BYTES` | `1073741824` | Лимит Kafka на partition; только Compose |

Новые топики создаются с тремя partitions и replication factor 1. Удаление старых сегментов
Kafka определяется временем **или** размером; 7 дней не гарантированы при заполнении лимита.
Повторный `kafka-init` не меняет существующие partitions и настройки топиков. Для существующих
топиков с индивидуальными настройками используйте `kafka-configs.sh` и `kafka-topics.sh`.
При смене имён топиков пересоздайте `kafka-init`, чтобы он выполнился заново.

Elasticsearch автоматически не удаляет старые события. Настройте срок хранения и snapshots
под свой объём данных. Сервис создаёт один shard без replicas для односерверного запуска.
При переходе на кластер настройте replicas и отдельную политику хранения.

## Миграции

Схема хранится в `log_service/schema.py`, версия — в `_meta.log_service_schema_version`
индекса. Версия 1 создаёт исходный mapping или добавляет метаданные версии к совместимому
индексу старой версии приложения. Повторный запуск безопасен; документы не удаляются.

Перед обновлением сделайте Elasticsearch snapshot в настроенный snapshot repository.
Копирование файлов работающего volume не заменяет snapshot.

```bash
docker compose stop api worker
docker compose build api
docker compose --profile tools run --rm --no-deps migrate
docker compose --profile tools run --rm --no-deps migrate python -m log_service.migrations --check
docker compose up -d --wait api worker
```

При `AUTO_MIGRATE=false` приложение только проверяет схему и не запускается, если требуется
миграция. Более новая версия схемы и несовместимые типы полей также останавливают запуск.
Текущий мигратор поддерживает создание и совместимое обновление mapping; изменение типов
полей требует нового индекса и отдельного reindex. Автоматического downgrade нет.
Не меняйте номер версии вручную, чтобы обойти проверку. Для отката несовместимой схемы
нужен совместимый образ и восстановление индекса из snapshot.

## DLQ

Причины отправки: `invalid_event` или `index_rejected_http_<status>`. Исходное сообщение
сохраняется без JSON-перекодирования. Исходный ключ находится в `dlq.source_key`, если
он был задан; остальные координаты — в `dlq.source_topic`, `dlq.source_partition`,
`dlq.source_offset`, `dlq.source_timestamp`. Исходные заголовки `dlq.*` заменяются служебными,
чтобы источник не мог подменить причину и координаты ошибки.

Для топиков с именами по умолчанию:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic app-logs-dlq --from-beginning \
  --max-messages 10 --property print.key=true --property print.headers=true
```

1. Найдите причину ошибки и исправьте конфигурацию/схему либо содержимое события.
2. Сохраните исправленное событие в JSON. Сохраните прежний ID, если он существовал;
   для сообщения без ID назначьте его один раз и используйте при всех повторах.
3. Отправьте JSON через `POST /logs` с ключом API. Дождитесь появления события в поиске.

Не отправляйте весь DLQ обратно автоматически: некорректное сообщение снова попадёт в DLQ.
Запись в DLQ не удаляется после ручного повтора и хранится по политике Kafka. Возможны
дубликаты с одинаковыми координатами — учитывайте ключ записи при разборе.
Слишком большое исходное сообщение с дополнительными заголовками может превысить лимит
сообщения DLQ: worker завершится без подтверждения offset. В таком случае сначала увеличьте
лимит DLQ/брокера, сохранив исходное сообщение, затем перезапустите worker.

## Диагностика и остановка

```bash
docker compose ps --all
docker compose logs --tail 100 api worker
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --group log-indexer --describe
docker compose restart worker
docker compose stop
```

| Симптом | Что проверить |
| --- | --- |
| `/live` отвечает, `/ready` возвращает `503` | Поля `kafka` и `elasticsearch` в ответе, состояние зависимостей |
| `202`, но событие не находится | Lag группы, журнал worker, DLQ; `/ready` не проверяет работу consumer |
| Повторы Bulk-запросов | Доступность Elasticsearch, `429`/`5xx`, свободное место |
| Worker регулярно перезапускается | Ошибки схемы/доступа, DLQ, превышение срока обработки пакета |
| Ошибка версии схемы | Выполнена ли миграция для текущего образа и правильного индекса |

API ограничен 100 одновременно обслуживаемыми соединениями/запросами Uvicorn;
при перегрузке он может отвечать `503`. Контейнеры приложения имеют ограничения памяти,
а Docker-журналы ограничены тремя файлами по 10 МиБ. Статус `unhealthy` сам по себе не
перезапускает контейнер: `restart: unless-stopped` срабатывает после выхода процесса.
Наблюдайте за lag worker, ошибками, DLQ и диском средствами сервера.

`docker compose stop` и `docker compose down` сохраняют именованные volumes.
Флаг `down -v` удаляет данные — для обычной остановки он не нужен.
До удаления Kafka-сегментов незавершённые пакеты можно прочитать повторно; при долгом
простое сверх retention непроиндексированные сообщения могут быть утрачены.

Снимки Elasticsearch и внешние резервные копии Kafka в проекте не настраиваются автоматически.
Проверьте восстановление на отдельном окружении перед хранением важных данных.
