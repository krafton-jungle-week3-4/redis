# Mini Redis

하루 안에 Redis의 핵심 개념을 직접 구현하고, AI로 만든 코드를 팀이 이해하고 설명할 수 있는 수준까지 소화하는 것을 목표로 한 프로젝트입니다.

## 1. 프로젝트 소개

Mini Redis는 Python으로 구현한 in-memory key-value 저장소입니다.  
단순 저장/조회만이 아니라, **자료형 명령 처리, 동시성 제어, TTL, 복구, 무효화, 성능 비교**까지 포함한 작은 Redis 시스템을 만드는 데 집중했습니다.

## 2. 우리가 해결하려 한 문제

이번 과제에서 저희가 중요하게 본 질문은 아래와 같습니다.

- 여러 요청이 동시에 들어와도 데이터가 꼬이지 않게 할 수 있는가
- 만료된 데이터를 어떻게 처리할 것인가
- 삭제/버전 전환 이후 오래된 조회 결과를 어떻게 무효화할 것인가
- 서버가 내려가도 데이터를 어떻게 복구할 것인가
- 메모리 저장소가 왜 빠른지 어떻게 보여줄 것인가

## 3. 기술 스택

- Python
- RESP 스타일 TCP 서버
- In-memory hash-table 기반 저장 구조
- MongoDB 비교 벤치마크
- unittest

## 4. 전체 구조

```text
server.py            # RESP/TCP 서버 엔트리포인트
redis.py             # 명령 실행 진입점 + 동시성 제어
core/                # 라우팅, 저장소, 에러 계약, 자료형별 명령
managers/            # snapshot, restore, TTL, AOF, version, invalidation
resp_protocol/       # 파서/응답 인코더/어댑터
performance/         # 성능 비교
tests/               # 테스트
```

## 5. 구현한 기능

### String
- `SET`, `GET`, `DEL`
- `INCR`, `DECR`
- `MSET`, `MGET`
- `EXISTS`, `TYPE`
- `EXPIRE`, `TTL`, `PERSIST`

### List
- `LPUSH`, `RPUSH`
- `LPOP`, `RPOP`
- `LRANGE`

### Set
- `SADD`, `SREM`
- `SISMEMBER`, `SMEMBERS`
- `SINTER`, `SUNION`, `SCARD`

### Hash
- `HSET`, `HGET`, `HDEL`
- `HGETALL`, `HEXISTS`
- `HINCRBY`, `HLEN`

### Sorted Set
- `ZADD`, `ZSCORE`
- `ZRANK`, `ZREVRANK`
- `ZRANGE`, `ZREVRANGE`
- `ZINCRBY`, `ZREM`, `ZCARD`

## 6. 핵심 설계 포인트

### 6-1. 동시성 문제 해결

여러 요청이 동시에 같은 key를 수정할 때 값을 안전하게 지키기 위해 **Single Writer + Queue** 구조를 적용했습니다.

- 모든 쓰기 명령은 queue에 넣고
- writer thread 하나가 순서대로 처리합니다.

즉, 요청은 동시에 들어와도 실제 메모리 반영은 순차적으로 일어나도록 설계했습니다.

### 6-2. TTL 처리

만료된 값은 두 방식으로 처리합니다.

- **Lazy Expiration**: 조회 시 만료 여부 확인 후 삭제
- **Background Cleanup**: 주기적으로 만료 key 정리

### 6-3. 데이터 무효화

삭제, 타입 변경, 버전 전환 이후 오래된 결과가 남지 않도록 `managers/invalidation_manager.py`를 통해 캐시 무효화를 처리했습니다.

### 6-4. 복구와 내구성

메모리 기반 구조의 한계를 보완하기 위해 두 가지를 구현했습니다.

- **Snapshot / Restore**
- **AOF(Append Only File) 기반 복구**

복구 중에는 새 요청을 잠시 대기시켜 데이터가 섞이지 않도록 했습니다.

## 7. 외부 사용 방식

이 프로젝트는 RESP 스타일 TCP 서버를 통해 Redis와 비슷한 명령 흐름을 확인할 수 있습니다.

## 8. 테스트와 검증

다음 관점으로 테스트를 구성했습니다.

- 기본 CRUD 테스트
- 자료형별 명령 테스트
- 동시성 테스트
- snapshot / restore 테스트
- AOF 복구 테스트
- invalidation / version 테스트
- 프로토콜 테스트

즉, 기능뿐 아니라 엣지 케이스와 운영 시나리오도 확인하려고 했습니다.

## 9. 성능 비교

`performance/` 폴더에서 mini-redis와 MongoDB를 비교하는 벤치마크를 구성했습니다.

비교 항목은 다음과 같습니다.

- 응답 속도
- 처리량(RPS)
- p50 / p95 / p99 지연 시간

## 9-1. 성능 그래프 시각자료

## 10. 데모 흐름

발표에서는 아래 순서로 보여줄 예정입니다.

1. String 저장 / 조회 / 삭제
2. Sorted Set 점수 증가와 랭킹 확인
3. TTL 설정과 만료 확인
4. Snapshot 또는 AOF 복구
5. 테스트 코드와 성능 비교 구조 소개

## 11. 프로젝트를 통해 배운 점

이번 프로젝트를 통해 단순 자료구조 구현보다 더 중요한 것이 **동시성, 만료, 무효화, 복구 같은 운영 관점의 설계**라는 점을 배웠습니다.

또한 AI를 활용해 구현 속도를 높일 수 있어도, 핵심 로직은 반드시 사람이 이해하고 검증해야 한다는 점을 다시 확인했습니다.

## 12. 실행 방법

### RESP 서버

```bash
python server.py
```

### 테스트 실행

```bash
python -m unittest discover -s tests
```

### 성능 비교 실행

```bash
python -m performance.check_connections
python -m performance.run_benchmarks
```
