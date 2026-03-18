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
- writes the raw unittest output to `testresult.txt`
- builds a structured QA report in `qa_report.json`
- uploads both files as workflow artifacts
- appends a QA status table to the Notion page `327bd214-dd7e-80aa-a930-c2ff985f64a3`

Required GitHub secrets:

- `NOTION_TOKEN`: internal integration token for the target workspace
- `NOTION_PAGE_ID`: optional override for the target page id

Notion setup:

1. Create an internal Notion integration.
2. Share the target page with that integration using `... > Add connections`.
3. Store the integration token in the `NOTION_TOKEN` repository secret.

The Notion update step uses `scripts/update_notion_test_results.py` to append the latest QA summary block and table to the page.
The test execution and QA case mapping are handled by `scripts/run_qa_suite.py`.
