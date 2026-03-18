# mini-redis

A minimal Redis-like server built with Python and FastAPI.

## Structure

```text
main.py
redis.py
core_commands/
common/
performance/
```

- `main.py` contains the REST-style API server.
- `redis.py` contains the RESP-style in-memory command core.
- `core_commands/` contains command handlers split by data type.
- `common/` contains shared helpers used by the REST server.
- `performance/` contains the mini-redis vs MongoDB benchmark tooling.

## Performance Benchmark

The MongoDB comparison benchmark is implemented under `performance/`.
It compares mini-redis RESP commands and MongoDB CRUD equivalents under the same workload mix.

Main entry points:

- `performance/run_benchmarks.py`: runs RESP vs MongoDB comparison and writes JSON/CSV/PNG reports
- `performance/check_connections.py`: verifies both targets are reachable before a full run
- `performance/README.md`: detailed setup, environment variables, and output format

Quick start:

```bash
pip install -r performance/requirements.txt
python -m performance.check_connections
python -m performance.run_benchmarks
```

The benchmark report includes latency and load metrics such as `avg`, `p50`, `p95`, `p99`, and throughput (`RPS`).

## Features

- REST-style endpoints for strings, lists, sets, hashes, and sorted sets
- TTL support for keys
- Integer and score increment operations

## Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000
```

You can also start the API with:

```bash
python main.py
```

The API listens on `0.0.0.0:8000` by default, so local requests still work through `http://127.0.0.1:8000`.

If you want to expose the RESP/TCP server on AWS, run:

```bash
MINIREDIS_HOST=0.0.0.0 MINIREDIS_PORT=6379 python server.py
```

On EC2, make sure the instance security group allows inbound traffic to the port you use (`8000` for the API, `6379` for the RESP server).

## API

### Store a string value

```bash
curl -X PUT http://127.0.0.1:8000/keys/name \
  -H "Content-Type: application/json" \
  -d '{"value":"redis"}'
```

Example response:

```json
{
  "result": "OK",
  "key": "name",
  "value": "redis"
}
```

### Get a value

```bash
curl http://127.0.0.1:8000/keys/name
```

Example response:

```json
{
  "key": "name",
  "value": "redis"
}
```

If the key does not exist, `value` is returned as `null`.

### Increment a value

```bash
curl -X POST http://127.0.0.1:8000/keys/count/increment
```

Example response:

```json
{
  "result": "OK",
  "key": "count",
  "value": 1
}
```

If the key does not exist, `increment` creates it with value `1`.

If the stored value is not an integer string, the API returns HTTP 400:

```json
{
  "detail": "value is not an integer"
}
```

### Add a list item

```bash
curl -X POST http://127.0.0.1:8000/lists/numbers/items/right \
  -H "Content-Type: application/json" \
  -d '{"value":"1"}'
```

### Add a set member

```bash
curl -X PUT http://127.0.0.1:8000/sets/tags/members/python
```

## Automation

The repository now includes a GitHub Actions workflow at `.github/workflows/test-and-update-notion.yml`.

What it does:

- runs when `main` receives a push
- executes all tests under `tests/`
- saves the raw output as `testresult.txt`
- uploads the raw output as a workflow artifact
- appends the latest result summary to the Notion page `327bd214-dd7e-80aa-a930-c2ff985f64a3`

Required GitHub secrets:

- `NOTION_TOKEN`: internal integration token for the target workspace
- `NOTION_PAGE_ID`: optional override for the target page id

Notion setup:

1. Create an internal Notion integration.
2. Share the target page with that integration using `... > Add connections`.
3. Store the integration token in the `NOTION_TOKEN` repository secret.

The Notion update step uses `scripts/update_notion_test_results.py` and prepends the latest run summary to the top of the page.
