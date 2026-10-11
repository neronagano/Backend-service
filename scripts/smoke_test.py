"""Check the running API and the Kafka-to-Elasticsearch pipeline."""

import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta
from uuid import uuid4


def run(base_url: str) -> None:
    def request(method: str, path: str, body=None, expected=200):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        query = urllib.request.Request(
            base_url.rstrip("/") + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            response = urllib.request.urlopen(query, timeout=15)
        except urllib.error.HTTPError as exc:
            response = exc
        with response:
            result = json.load(response)
            assert response.status == expected, (method, path, response.status, result)
            return result

    assert request("GET", "/health")["status"] == "ok"
    assert "/logs" in request("GET", "/openapi.json")["paths"]
    service = "smoke-" + uuid4().hex[:8]
    start = datetime.now(UTC) - timedelta(minutes=1)
    messages = [("INFO", "Application started"), ("ERROR", "Database connection timeout"), ("WARN", "Queue is nearly full")]
    events = []
    for number, (level, message) in enumerate(messages):
        event = {
            "id": str(uuid4()),
            "timestamp": (start + timedelta(seconds=number)).isoformat(),
            "service": service,
            "level": level,
            "message": message,
        }
        result = request("POST", "/logs", event, expected=202)
        assert result == {"id": event["id"], "status": "queued"}
        events.append(event)
    request("POST", "/logs", events[0], expected=202)

    def search(**filters):
        return request("GET", "/logs?" + urllib.parse.urlencode({"service": service, **filters}))

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        page = search()
        if page["total"] >= 3:
            break
        time.sleep(0.25)
    else:
        raise AssertionError("Logs did not become searchable within 30 seconds")
    assert page["total"] == 3, page
    assert {event["id"] for event in page["items"]} == {event["id"] for event in events}
    assert request("GET", "/logs/" + events[0]["id"])["message"] == events[0]["message"]
    assert search(level="ERROR")["total"] == 1
    assert search(q="timeout")["items"][0]["id"] == events[1]["id"]
    assert search(offset=1, limit=1)["items"][0]["id"] == events[1]["id"]
    assert search(
        from_time=(start + timedelta(seconds=1.5)).isoformat(),
        to_time=(start + timedelta(seconds=2.5)).isoformat(),
    )["total"] == 1
    request("GET", "/logs?limit=0", expected=422)
    request("GET", "/logs/" + str(uuid4()), expected=404)
    request("POST", "/logs", {"service": service, "message": ""}, expected=422)
    print("Passed: publishing, indexing, stable IDs, search filters, pagination, validation")
    print(f"Test service: {service}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8080")
    run(parser.parse_args().url)
