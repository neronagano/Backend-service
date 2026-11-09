import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import time

import httpx


ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
uv = shutil.which("uv") or str(Path.home() / ".local/bin/uv")
nginx = shutil.which("nginx") or "/usr/sbin/nginx"
runtime = ROOT / ".run"
runtime.mkdir(exist_ok=True)
for port in (8005, 8085):
    with socket.socket() as probe:
        if probe.connect_ex(("127.0.0.1", port)) == 0:
            raise SystemExit(f"Port {port} is already in use; stop the demo before checking")


def wait_ready(client, url, process):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Server exited with code {process.returncode}")
        try:
            if client.get(url).status_code == 200:
                return
        except httpx.RequestError:
            pass
        time.sleep(.1)
    raise TimeoutError(url)


def stop(process):
    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


backend = proxy = None
with (runtime / "check-api.log").open("w") as api_log, (runtime / "check-nginx.log").open("w") as nginx_log:
    try:
        command = [nginx, "-p", str(ROOT) + "/", "-c", "nginx.conf"]
        subprocess.run(command + ["-t"], check=True)
        backend = subprocess.Popen([uv, "run", "--locked", "uvicorn", "api.app:app",
                                    "--host", "127.0.0.1", "--port", "8005"], stdout=api_log, stderr=api_log)
        with httpx.Client(timeout=3, trust_env=False) as client:
            wait_ready(client, "http://127.0.0.1:8005/apihealth", backend)
            proxy = subprocess.Popen(command + ["-g", "daemon off;"], stdout=nginx_log, stderr=nginx_log)
            base = "http://127.0.0.1:8085"
            wait_ready(client, base + "/apihealth", proxy)
            page = client.get(base + "/")
            assert page.status_code == 200 and "Список задач" in page.text
            assert page.headers["server"].startswith("nginx")
            assert client.get("http://127.0.0.1:8005/").status_code == 404
            for path in ("/style.css", "/app.js"):
                assert client.get(base + path).status_code == 200
            assert client.get(base + "/apihealth").json() == {"status": "ok"}
            assert client.get(base + "/api/tasks").json() == []
            created = client.post(base + "/api/tasks", json={"title": "Проверить nginx"})
            assert created.status_code == 201
            task = {"id": 1, "title": "Проверить nginx"}
            assert created.json() == task
            assert client.get(base + "/api/tasks").json() == [task]
            assert client.post(base + "/api/tasks", json={"title": " "}).status_code == 422
            assert client.get(base + "/api/tasks").json() == [task]
            listeners = subprocess.check_output(["ss", "-ltnH", "sport = :8005"], text=True)
            assert "127.0.0.1:8005" in listeners and "0.0.0.0:8005" not in listeners
            stop(backend)
            assert client.get(base + "/").status_code == 200
            assert client.get(base + "/api/tasks").status_code == 502
            print(json.dumps({"static_page": 200, "health": "ok", "created": task,
                              "post_status": 201, "invalid_status": 422,
                              "backend_bind": "127.0.0.1:8005", "static_without_backend": 200,
                              "api_without_backend": 502, "result": "passed"}, ensure_ascii=False, indent=2))
    finally:
        stop(proxy)
        stop(backend)
