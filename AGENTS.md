# AGENTS.md

## Purpose

This document defines the collaboration contract for the minimal Redis implementation project using Python and `restTCP`.

The main goal is to prevent conflicts between:
- the server/protocol owner
- the `redis.py` core owner

Korean note:
이 문서는 기능 명세를 길게 설명하는 문서가 아니라, 같이 개발할 때 서로 기준이 흔들리지 않도록 잡아두는 협업 문서입니다.
누가 어떤 역할을 맡는지, 무엇을 먼저 합의해야 하는지, 그리고 어떤 규칙은 끝까지 고정해야 하는지를 미리 분명하게 적어둡니다.

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

Korean note:
지금 단계의 목표는 작더라도 정확하게 맞는 최소 구현을 만드는 것입니다.
즉, 문자열만 저장할 수 있는 가장 단순한 Redis 사이클을 확실하게 맞추는 것이 우선입니다.

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

Korean note:
지금은 기능을 넓히는 것보다, 하기로 한 범위를 정확히 맞추는 것이 더 중요합니다.
범위를 넓히면 논의할 내용이 급격히 많아지고, 그만큼 팀원끼리 해석이 엇갈릴 가능성도 커집니다.

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

Korean note:
1번 담당은 입력을 받아서 파싱하고, 코어가 준 결과를 다시 프로토콜 응답으로 바꿔주는 역할입니다.
쉽게 말해 "서버와 코어를 이어주는 사람"이지, Redis 명령의 실제 동작을 정의하는 사람은 아닙니다.

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

Korean note:
2번 담당은 저장소와 명령 실행 로직을 맡습니다.
소켓 통신이나 응답 문자열 포맷처럼 네트워크 세부사항은 몰라도 되게 만드는 것이 가장 좋습니다.

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

Korean note:
이 경계가 이 프로젝트에서 가장 중요합니다.
서버는 파싱된 `list[str]`를 넘기고, 코어는 약속된 `dict`를 돌려주는 것까지만 책임지면 됩니다.
이 선이 흐려지기 시작하면 서버와 코어가 서로의 역할을 침범하게 되고, 유지보수가 빠르게 어려워집니다.

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

Korean note:
이건 가장 먼저 맞춰야 하는 부분입니다.
입력 형식에 대한 이해가 다르면, 나머지 구현이 다 맞아도 처음부터 연동이 되지 않습니다.
그래서 "무엇이 들어오고, 어떤 모양으로 들어오는지"를 제일 먼저 고정해야 합니다.

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

Korean note:
서버 입장에서는 항상 같은 모양의 응답을 받는 것이 가장 안전합니다.
응답 형식이 상황마다 달라지면 직렬화 로직이 복잡해지고, 디버깅 포인트도 훨씬 많아집니다.

### 3. Per-Command Behavior

Must be fixed:
- exact argument count for each command
- exact success response type for each command
- missing-key behavior
- exact integer semantics for `DEL` and `EXISTS`
- exact `TYPE` result for existing and missing keys

Why this matters:
- two developers may implement different behavior for the same command and both think they are correct

Korean note:
이 부분은 실제 협업에서 가장 자주 어긋나는 영역입니다.
예를 들어 없는 key를 `GET`했을 때 무엇을 돌려줄지, `TYPE`은 `none`인지 다른 값인지 같은 규칙이 다르면,
각자 자기 기준으로는 맞는 코드를 짜더라도 최종 결과는 서로 맞지 않게 됩니다.

### 4. Error Rules

Must be fixed:
- the exact message for unknown command
- the exact message for wrong number of arguments
- the exact behavior for empty command input
- whether all command errors are wrapped as `type="error"`

Why this matters:
- integration may appear to work, but debugging and future tests become unstable

Korean note:
에러 메시지는 겉보기엔 사소해 보여도 협업할 때 꽤 중요합니다.
나중에 테스트를 붙이거나 로그를 비교할 때 문자열 한두 글자 차이 때문에 계속 충돌이 날 수 있기 때문입니다.
그래서 에러 형식과 문구는 초반에 대충 넘기지 않는 편이 좋습니다.

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

Korean note:
특히 빈 문자열과 숫자처럼 보이는 문자열은 초반에 기준을 정해두는 것이 좋습니다.
예를 들어 `"123"`을 그냥 문자열로 볼지, 숫자처럼 특별 취급할지가 흔들리기 시작하면 구현 전체가 애매해집니다.

### 6. Storage Rules

Must be fixed:
- storage is exactly one `dict[str, str]`
- all existing keys are treated as `"string"`
- `GET`, `DEL`, `EXISTS`, and `TYPE` all use the same storage source

Why this matters:
- otherwise key existence and returned value may become inconsistent

Korean note:
모든 명령이 같은 저장소를 바라본다는 점을 명확히 해두어야 결과가 서로 어긋나지 않습니다.
한쪽은 존재한다고 보고 다른 쪽은 없다고 보는 상황이 생기면, 바로 디버깅이 어려워집니다.

### 7. Responsibility Split

Must be fixed:
- server handles parsing and wire encoding only
- `redis.py` handles command semantics only
- protocol-specific response formatting does not enter `redis.py`

Why this matters:
- once logic is duplicated across layers, every small change becomes risky

Korean note:
서버와 코어의 역할이 섞이기 시작하면, 작은 수정도 두 군데를 동시에 바꿔야 하는 문제가 생깁니다.
처음에는 빨라 보일 수 있지만, 나중에는 오히려 가장 큰 혼란의 원인이 됩니다.

### 8. Protocol Mapping

Must be fixed:
- how each internal response dict maps to the `restTCP` wire response
- how `null` is represented on the wire
- how `error` is represented on the wire

Why this matters:
- the core may be correct internally while the client still receives the wrong response

Korean note:
내부 로직이 맞더라도, 네트워크 응답 형식으로 바꾸는 과정이 틀리면 사용자 입장에서는 그냥 "안 되는 것처럼" 보입니다.
그래서 이 부분도 서버와 코어가 각자 생각하지 말고, 함께 맞춰두는 것이 중요합니다.

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

Korean note:
최소 구현에서는 에러도 하나의 정상적인 응답 종류라고 생각하는 편이 훨씬 단순합니다.
예외를 여기저기 던지는 것보다, 약속된 형식으로 돌려주는 편이 서버와의 연결도 훨씬 깔끔해집니다.

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

Korean note:
원래 Redis와 완전히 같지 않아도 괜찮습니다.
지금은 최소 구현 기준으로 서로 정확히 맞추는 것이 더 중요하고, 멀티키까지 열어두면 논의해야 할 내용만 불필요하게 늘어납니다.

---

## External-Facing Rules

These rules must be shared not only with the server owner, but also with the core owner and the person writing tests.

### Raw Request to Token List

Fixed proposal:
- one request is one line
- the server reads one line and converts it into one `list[str]`
- leading and trailing spaces are ignored
- multiple spaces between tokens are treated as a single separator
- command names are normalized to uppercase before calling `execute()`

Example:

```text
SET name redis
```

becomes:

```python
["SET", "name", "redis"]
```

Korean note:
즉, 1번 담당이 정해야 하는 가장 중요한 외부 규칙은 "raw 입력 한 줄이 어떤 `list[str]`가 되는가"입니다.
이 규칙은 2번 담당과 테스트 작성자도 반드시 같이 알아야 합니다.

### Empty or Blank Input

Fixed proposal:
- an empty line or a line with only spaces is treated as an invalid command
- the server returns an error response

Recommended response:

```python
{"type": "error", "value": "empty command"}
```

Korean note:
빈 입력을 무시할지, 에러로 볼지는 초반에 꼭 정해야 합니다.
테스트를 쓸 때도 이 기준이 없으면 사람마다 다르게 해석하게 됩니다.

### Space Handling in Values

Fixed proposal for minimum scope:
- tokens are separated by spaces
- quoted strings are not supported at this stage
- therefore `ECHO hello world` is treated as too many arguments
- `SET name hello world` is also treated as too many arguments

Korean note:
지금 최소 구현에서는 공백이 들어간 문자열 value까지 지원하지 않는 편이 가장 단순합니다.
이렇게 정하면 파서와 코어 모두 훨씬 깔끔해지고, 테스트도 단순해집니다.

### Arity Validation Boundary

Fixed proposal:
- the server is responsible only for line parsing
- `redis.py` is responsible for command arity validation

Korean note:
즉, 서버는 "파싱 가능한가"까지만 보고, 인자 개수가 맞는지는 코어가 판단하는 구조가 좋습니다.
이렇게 해야 명령 규칙이 한 군데에만 모입니다.

### Response Mapping Boundary

Fixed proposal:
- `redis.py` returns the agreed dict
- the server maps that dict into the wire response
- tests for `redis.py` and tests for the server may be separated, but both must share the same response contract

Korean note:
코어는 내부 응답 규격을 책임지고, 서버는 그 응답을 실제 프로토콜 형식으로 바꾸는 책임만 집니다.
이 경계가 분명해야 테스트도 층별로 나눠서 쓰기 쉬워집니다.

### Shared Examples

The following examples should be shared across all owners:

```text
PING              -> ["PING"]
ECHO hello        -> ["ECHO", "hello"]
SET name redis    -> ["SET", "name", "redis"]
GET name          -> ["GET", "name"]
DEL name          -> ["DEL", "name"]
EXISTS name       -> ["EXISTS", "name"]
TYPE name         -> ["TYPE", "name"]
```

Korean note:
이 예시는 단순해 보여도 굉장히 중요합니다.
서버 담당, 코어 담당, 테스트 담당이 모두 같은 예시를 보고 같은 그림을 떠올릴 수 있어야 협업 충돌이 줄어듭니다.

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
- raw line to `list[str]` parsing rule
- blank input handling rule
- whether spaces inside values are supported

Korean note:
이 체크리스트는 "나중에 물어보자"가 아니라, 개발 전에 짧게라도 먼저 맞춰두기 위한 용도입니다.
초반에 5분 투자하면 뒤에서 몇 시간을 아낄 수 있습니다.

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

Korean note:
지금 목표는 "작아도 정확하게 맞는 구현"입니다.
기능을 더 넣는 것보다, 지금 하기로 한 것들을 서로 같은 기준으로 완성하는 것이 훨씬 더 중요합니다.

---

## One-Line Summary

The server owner is responsible for parsing input and encoding output, the `redis.py` owner is responsible for command execution and storage, and both sides must freeze the `list[str] -> dict` contract before writing code.
