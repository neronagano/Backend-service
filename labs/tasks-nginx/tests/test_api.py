import asyncio

import httpx
import pytest

from api.app import create_app


@pytest.fixture
async def client():
    transport = httpx.ASGITransport(app=create_app())
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as session:
        yield session


async def test_health_and_empty_list(client):
    assert (await client.get("/apihealth")).json() == {"status": "ok"}
    assert (await client.get("/api/tasks")).json() == []


async def test_add_task_and_read_it(client):
    response = await client.post("/api/tasks", json={"title": "  Подготовить отчёт  "})
    assert response.status_code == 201
    assert response.json() == {"id": 1, "title": "Подготовить отчёт"}
    assert (await client.get("/api/tasks")).json() == [response.json()]


@pytest.mark.parametrize("payload", [{}, {"title": ""}, {"title": "   "},
                                     {"title": "x" * 201}, {"title": 123},
                                     {"title": "Test", "unknown": 1}])
async def test_invalid_task_is_not_added(client, payload):
    assert (await client.post("/api/tasks", json=payload)).status_code == 422
    assert (await client.get("/api/tasks")).json() == []


async def test_concurrent_requests_get_unique_ids(client):
    responses = await asyncio.gather(*[
        client.post("/api/tasks", json={"title": f"Задача {number}"})
        for number in range(25)
    ])
    assert all(response.status_code == 201 for response in responses)
    assert {response.json()["id"] for response in responses} == set(range(1, 26))
    assert len((await client.get("/api/tasks")).json()) == 25


async def test_app_does_not_serve_static_page(client):
    assert (await client.get("/")).status_code == 404
