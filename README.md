# mini-redis

A minimal Redis-like server built with Python and FastAPI.

## Features

- `set`: store a value in a global in-memory dictionary
- `get`: read a value from the global in-memory dictionary
- `increment`: increase an integer value stored in the global in-memory dictionary

## Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload
```

The server starts at `http://127.0.0.1:8000`.

## API

### Set a value

```bash
curl -X POST http://127.0.0.1:8000/set \
  -H "Content-Type: application/json" \
  -d '{"key":"name","value":"redis"}'
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
curl http://127.0.0.1:8000/get/name
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
curl -X POST http://127.0.0.1:8000/increment/count
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
