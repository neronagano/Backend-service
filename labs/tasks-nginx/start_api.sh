#!/usr/bin/env bash
set -eu
cd "$(dirname "$0")"
export PATH="$HOME/.local/bin:$PATH"
exec uv run --locked uvicorn api.app:app --host 127.0.0.1 --port 8005
