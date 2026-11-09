import asyncio
from typing import Annotated

from fastapi import FastAPI, status
from pydantic import BaseModel, ConfigDict, StringConstraints


class TaskInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    title: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]


class Task(BaseModel):
    id: int
    title: str


class TaskStore:
    def __init__(self):
        self.tasks: list[Task] = []
        self.lock = asyncio.Lock()

    async def list_tasks(self) -> list[Task]:
        async with self.lock:
            return self.tasks.copy()

    async def add_task(self, data: TaskInput) -> Task:
        async with self.lock:
            task = Task(id=len(self.tasks) + 1, title=data.title)
            self.tasks.append(task)
            return task


def create_app() -> FastAPI:
    app = FastAPI(openapi_url=None)
    store = TaskStore()

    @app.get("/apihealth")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/tasks", response_model=list[Task])
    async def list_tasks() -> list[Task]:
        return await store.list_tasks()

    @app.post("/api/tasks", response_model=Task, status_code=status.HTTP_201_CREATED)
    async def add_task(data: TaskInput) -> Task:
        return await store.add_task(data)

    return app


app = create_app()
