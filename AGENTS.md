# AGENTS.md

## Purpose

This document defines the collaboration contract for the minimal Redis implementation project using Python and `restTCP`.

The main goal is to prevent conflicts between:
- the server/protocol owner
- the `redis.py` core owner

한국어 메모:
이 문서는 기능 설명서라기보다 "서로 어디까지 책임지고, 무엇을 먼저 합의해야 하는지"를 고정하는 협업 기준 문서다.

---

## Current Scope

At the current stage, the project must stay within this minimum scope:

- supported data type: `string` only
- storage type: `dict[str, str]`
- supported commands:
  - `PING`
  - `ECHO`
  - `SET`
  - `GET`
  - `DEL`
  - `EXISTS`
  - `TYPE`
- input command format: `list[str]`
- `redis.py` interface:

```python
execute(command: list[str]) -> dict
```

Expected return examples:

```python
{"type": "simple_string", "value": "OK"}
{"type": "bulk_string", "value": "hello"}
{"type": "null", "value": None}
{"type": "integer", "value": 1}
{"type": "error", "value": "unknown command"}
```

한국어 메모:
지금 단계에서는 "문자열만 저장하는 아주 작은 Redis"만 맞추는 것이 목표다.

---

## Out of Scope

The following are explicitly excluded at this stage:

- `test.py`
- DB integration
- persistence
- concurrency
- scale-out
- non-string data types
- TTL / expire
- transaction
- pub/sub

한국어 메모:
범위 밖 기능을 끌어오면 담당자 간 합의 포인트가 너무 많아져서 오히려 개발이 꼬일 수 있다.

---

## Ownership

### 1. Server / Protocol Owner

Responsibilities:
- receive client input
- parse raw protocol input into `list[str]`
- call `redis.execute(command)`
- convert the returned dict into the actual `restTCP` wire response

Non-responsibilities:
- command semantics
- in-memory storage logic
- Redis core behavior definitions

한국어 메모:
1번 담당은 "입력/출력 변환" 담당이지, Redis 명령의 실제 의미를 구현하는 담당은 아니다.

### 2. `redis.py` Core Owner

Responsibilities:
- own the in-memory store
- validate command names
- validate argument counts
- execute command behavior
- return only the agreed response dict format

Non-responsibilities:
- socket handling
- protocol wire formatting
- network-level response generation

한국어 메모:
2번 담당은 "명령 실행 엔진" 담당이고, 프로토콜 문자열 포맷은 몰라도 된다.

---

## Shared Boundary

The only shared boundary between the two owners is:

```python
list[str] -> dict
```

This means:
- the server layer produces `list[str]`
- the core layer returns `dict`
- protocol-specific details must not leak into `redis.py`

한국어 메모:
이 경계가 흔들리면 서버 코드와 코어 코드가 서로 침범하게 되고, 협업 충돌이 거의 반드시 난다.

---

## Mandatory Agreements Before Coding

These items must be agreed before implementation begins.

### 1. Command Input Contract

Must be fixed:
- `execute()` always receives a fully parsed `list[str]`
- command name is always `command[0]`
- command matching uses a single normalization rule
  - recommended: normalize with `upper()`
- empty command handling must be defined

Why this matters:
- if one side expects parsed tokens and the other expects raw text, integration breaks immediately

한국어 메모:
이건 제일 먼저 맞춰야 한다. 입력 형식 이해가 다르면 나머지는 다 맞아도 연동이 안 된다.

### 2. Return Contract

Must be fixed:
- `execute()` always returns a dict
- the dict always includes `type` and `value`
- allowed `type` values are fixed
- command errors are returned in the agreed error format

Recommended rule:
- normal command failures should return `{"type": "error", "value": ...}`
- avoid leaking raw Python exceptions for expected command-level errors

Why this matters:
- otherwise the server layer cannot serialize responses consistently

### 3. Per-Command Behavior

Must be fixed:
- exact argument count for each command
- exact success response type for each command
- missing-key behavior
- exact integer semantics for `DEL` and `EXISTS`
- exact `TYPE` result for existing and missing keys

Why this matters:
- two developers may implement different behavior for the same command and both think they are correct

### 4. Error Rules

Must be fixed:
- the exact message for unknown command
- the exact message for wrong number of arguments
- the exact behavior for empty command input
- whether all command errors are wrapped as `type="error"`

Why this matters:
- integration may appear to work, but debugging and future tests become unstable

한국어 메모:
에러 메시지는 사소해 보여도 협업 때 제일 많이 충돌나는 부분 중 하나다.

### 5. Key / Value Rules

Must be fixed:
- all stored values are plain strings
- no implicit numeric conversion
- key and value are used exactly as parsed by the server
- empty string handling must be defined

Recommended discussion:
- empty string value: usually safe to allow
- empty string key: must be explicitly allowed or rejected by agreement

Why this matters:
- parser assumptions and core assumptions may diverge on edge cases

### 6. Storage Rules

Must be fixed:
- storage is exactly one `dict[str, str]`
- all existing keys are treated as `"string"`
- `GET`, `DEL`, `EXISTS`, and `TYPE` all use the same storage source

Why this matters:
- otherwise key existence and returned value may become inconsistent

### 7. Responsibility Split

Must be fixed:
- server handles parsing and wire encoding only
- `redis.py` handles command semantics only
- protocol-specific response formatting does not enter `redis.py`

Why this matters:
- once logic is duplicated across layers, every small change becomes risky

### 8. Protocol Mapping

Must be fixed:
- how each internal response dict maps to the `restTCP` wire response
- how `null` is represented on the wire
- how `error` is represented on the wire

Why this matters:
- the core may be correct internally while the client still receives the wrong response

---

## Fixed Interface and Rules

These rules should be treated as frozen unless both owners re-agree explicitly.

### Function Signature

```python
execute(command: list[str]) -> dict
```

### Input Rules

- `command` is already tokenized
- `command[0]` is the command name
- command matching uses uppercase normalization

Example:

```python
["SET", "name", "redis"]
["GET", "name"]
["PING"]
```

### Storage Rules

- storage type: `dict[str, str]`
- one shared in-memory dictionary only
- all stored values are strings
- all existing keys are of type `"string"`

### Allowed Response Types

- `simple_string`
- `bulk_string`
- `null`
- `integer`
- `error`

---

## Command Contracts

### `PING`

Input:

```python
["PING"]
```

Response:

```python
{"type": "simple_string", "value": "PONG"}
```

Rule:
- no extra argument is supported at this stage

### `ECHO`

Input:

```python
["ECHO", message]
```

Response:

```python
{"type": "bulk_string", "value": message}
```

Rule:
- exactly one argument is required

### `SET`

Input:

```python
["SET", key, value]
```

Action:
- store `value` at `key`

Response:

```python
{"type": "simple_string", "value": "OK"}
```

### `GET`

Input:

```python
["GET", key]
```

If found:

```python
{"type": "bulk_string", "value": value}
```

If missing:

```python
{"type": "null", "value": None}
```

### `DEL`

Input:

```python
["DEL", key]
```

If deleted:

```python
{"type": "integer", "value": 1}
```

If missing:

```python
{"type": "integer", "value": 0}
```

### `EXISTS`

Input:

```python
["EXISTS", key]
```

If found:

```python
{"type": "integer", "value": 1}
```

If missing:

```python
{"type": "integer", "value": 0}
```

### `TYPE`

Input:

```python
["TYPE", key]
```

If found:

```python
{"type": "bulk_string", "value": "string"}
```

If missing:

```python
{"type": "bulk_string", "value": "none"}
```

---

## Error Contract

Unknown command:

```python
{"type": "error", "value": "unknown command"}
```

Wrong number of arguments:
- must also use `type="error"`
- the message must be fixed by agreement

Recommended example:

```python
{"type": "error", "value": "wrong number of arguments"}
```

Empty command:
- should also be handled as an error response

한국어 메모:
최소 구현에서는 "에러도 응답의 한 종류"라고 생각하는 편이 구현과 연동이 단순하다.

---

## Practical Edge Cases to Freeze

These points are easy to overlook, but should be decided early.

### Empty String Handling

Must decide:
- whether `SET "" "value"` is allowed
- whether `SET "key" ""` is allowed

Recommended:
- allow empty string values
- explicitly decide whether empty string keys are allowed

### Parsing Responsibility

Must decide:
- how raw input such as `"SET name hello world"` becomes `list[str]`

Fixed rule:
- raw splitting/parsing belongs to the server/protocol owner
- `redis.py` only sees the already parsed token list

### Exact Arity Table

The following token counts must be fixed:

- `PING`: 1 token
- `ECHO`: 2 tokens
- `SET`: 3 tokens
- `GET`: 2 tokens
- `DEL`: 2 tokens
- `EXISTS`: 2 tokens
- `TYPE`: 2 tokens

### No Multi-Key Support

At this stage:
- `DEL key1 key2` is not supported
- `EXISTS key1 key2` is not supported

한국어 메모:
원래 Redis와 다를 수 있어도 괜찮다. 지금은 "최소 구현 기준으로 서로 정확히 맞추는 것"이 더 중요하다.

---

## Collaboration Checklist

Before implementation starts, both owners should explicitly confirm:

- supported command list
- exact argument count per command
- response type names
- missing-key behavior
- `TYPE` behavior for missing keys
- unknown command error message
- wrong-arity error message
- whether command errors are returned as error dicts
- empty string key/value policy
- exact server/core responsibility boundary
- exact `restTCP` mapping rule for each response type

---

## Scope Discipline

Do not pull in:

- extra commands
- extra data types
- TTL / expire
- persistence
- database logic
- concurrency handling
- clustering
- transaction
- pub/sub

한국어 메모:
지금 목표는 "작아도 정확하게 맞는 구현"이다. 기능을 늘리는 것보다 기준을 고정하는 것이 더 중요하다.

---

## One-Line Summary

The server owner is responsible for parsing input and encoding output, the `redis.py` owner is responsible for command execution and storage, and both sides must freeze the `list[str] -> dict` contract before writing code.
