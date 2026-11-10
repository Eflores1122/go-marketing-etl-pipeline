#!/usr/bin/env bash
# Build the browser ETL and stage the files GitHub Pages serves.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

mkdir -p site/data
GOOS=js GOARCH=wasm go build -o site/etl.wasm .
cp "$(go env GOROOT)/misc/wasm/wasm_exec.js" site/wasm_exec.js
cp google_clicks.csv facebook_clicks.csv site_events.csv bonus_query.sql site/data/

go_version="$(go version | awk '{print $3}')"
printf '{"go_version":"%s","goos":"js","goarch":"wasm"}\n' "$go_version" > site/build.json
ls -lh site/etl.wasm site/wasm_exec.js site/data
