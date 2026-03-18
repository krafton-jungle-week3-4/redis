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

## Features

- REST-style endpoints for strings, lists, sets, hashes, and sorted sets
- TTL support for keys
- Integer and score increment operations

## Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload
```

The server starts at `http://127.0.0.1:8000`.

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
