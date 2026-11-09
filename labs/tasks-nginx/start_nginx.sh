#!/usr/bin/env bash
set -eu
cd "$(dirname "$0")"
mkdir -p .run
nginx -p "$PWD/" -c nginx.conf -t
exec nginx -p "$PWD/" -c nginx.conf -g 'daemon off;'
