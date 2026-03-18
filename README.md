# Mini Redis
<img width="2381" height="1419" alt="image" src="https://github.com/user-attachments/assets/ec49ddc9-56a6-4692-95a8-bca6cefd1402" />

하루 안에 Redis의 핵심 개념을 직접 구현하고, AI로 만든 코드를 팀이 이해하고 설명할 수 있는 수준까지 소화하는 것을 목표로 한 프로젝트입니다.

## 1. 프로젝트 소개

Mini Redis는 Python으로 구현한 in-memory key-value 저장소입니다.  
단순 저장/조회만이 아니라, **자료형 명령 처리, 동시성 제어, TTL, 복구, 무효화, 성능 비교**까지 포함한 작은 Redis 시스템을 만드는 데 집중했습니다.

## 2. 우리가 해결하려 한 문제

이번 과제에서 저희가 중요하게 본 질문은 아래와 같습니다.

- 여러 요청이 동시에 들어와도 데이터가 꼬이지 않게 할 수 있는가
- 만료된 데이터를 어떻게 처리할 것인가
- 외부에서 쉽게 사용할 수 있는 구조를 만들 수 있는가
- 삭제/버전 전환 이후 오래된 조회 결과를 어떻게 무효화할 것인가
- 서버가 내려가도 데이터를 어떻게 복구할 것인가

## 3. 데모 흐름

발표에서는 아래 6개 흐름을 한 파일에서 시연할 예정입니다.

- 데모 파일: `[demo_test_cases.py](D:\03Dev\05Jungle\Week3\mini-redis\demo_test_cases.py)`
- 실행: `python demo_test_cases.py`

시연 순서는 다음과 같습니다.

1. **동시성 제어**
   - `test_01_single_writer_queue_keeps_zincrby_consistent`
2. **TTL 만료 처리**
   - `test_02_expired_value_behaves_like_missing_key`
3. **외부 사용 구조**
   - `test_03_malformed_resp_returns_error_and_keeps_processing`
4. **데이터 무효화**
   - `test_04_delete_invalidates_cached_get_result`
5. **장애 후 복구**
   - `test_05_aof_replay_recovers_data_after_cleared_state`
6. **추가 데모: Snapshot 복구 중 요청 처리**
   - `test_06_write_requests_wait_until_restore_completes`

## 4. 성능 비교

`performance/` 폴더에서 mini-redis와 MongoDB를 비교하는 벤치마크를 구성했습니다.

비교 항목은 다음과 같습니다.

- 응답 속도
- 처리량(RPS)
- p50 / p95 / p99 지연 시간

## 4-1. 평균/P95 지연 시간 비교

![네트워크 E2E Latency Summary](./docs/perf-latency-summary.jpg)

- 이 그래프는 `PING`, `SET`, `GET`, `EXISTS`, `TYPE`, `DEL` 명령 기준으로 mini-redis와 MongoDB의 평균 지연 시간과 P95 지연 시간을 비교한 결과입니다.
- 전 구간에서 mini-redis가 더 낮은 지연 시간을 보였고, 특히 `SET`, `GET` 같은 기본 명령에서 메모리 기반 저장소의 응답성이 더 안정적이라는 점을 확인할 수 있었습니다.

## 4-2. 부하 증가 시 처리량/지연 시간 비교

![Network E2E Load Summary](./docs/perf-load-summary.jpg)

- 이 그래프는 동시성(concurrency)이 증가할 때 처리량과 P95 지연 시간이 어떻게 변하는지 비교한 결과입니다.
- mini-redis는 높은 처리량을 유지하면서도 지연 시간 증가 폭이 상대적으로 작았고, MongoDB는 부하가 커질수록 P95 지연 시간이 더 크게 증가하는 경향을 보였습니다.

## 5. 기술 스택

- Python
- RESP 스타일 TCP 서버
- In-memory hash-table 기반 저장 구조
- MongoDB 비교 벤치마크
- unittest

## 6. 전체 구조

```text
server.py            # RESP/TCP 서버 엔트리포인트
redis.py             # 명령 실행 진입점 + 동시성 제어
core/                # 라우팅, 저장소, 에러 계약, 자료형별 명령
managers/            # snapshot, restore, TTL, AOF, version, invalidation
resp_protocol/       # 파서/응답 인코더/어댑터
performance/         # 성능 비교
tests/               # 테스트
```

## 7. 구현한 기능

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

## 8. 핵심 설계 포인트

### 8-1. 동시성 문제 해결

여러 요청이 동시에 같은 key를 수정할 때 값을 안전하게 지키기 위해 **Single Writer + Queue** 구조를 적용했습니다.

- 모든 쓰기 명령은 queue에 넣고
- writer thread 하나가 순서대로 처리합니다.

### 8-2. TTL 처리

만료된 값은 두 방식으로 처리합니다.

- **Lazy Expiration**: 조회 시 만료 여부 확인 후 삭제
- **Background Cleanup**: 주기적으로 만료 key 정리

### 8-3. 데이터 무효화

삭제, 타입 변경, 버전 전환 이후 오래된 결과가 남지 않도록 `managers/invalidation_manager.py`를 통해 캐시 무효화를 처리했습니다.

### 8-4. 복구와 내구성

메모리 기반 구조의 한계를 보완하기 위해 아래를 구현했습니다.

- **Snapshot / Restore**
- **AOF(Append Only File) 기반 복구**

복구 중에는 새 요청을 잠시 대기시켜 데이터가 섞이지 않도록 했습니다.

## 9. 테스트와 검증

다음 관점으로 테스트를 구성했습니다.

- 기본 CRUD 테스트
- 자료형별 명령 테스트
- 동시성 테스트
- snapshot / restore 테스트
- AOF 복구 테스트
- invalidation / version 테스트
- 프로토콜 테스트

## 10. 프로젝트를 통해 배운 점

이번 프로젝트를 통해 단순 자료구조 구현보다 더 중요한 것이 **동시성, 만료, 무효화, 복구 같은 운영 관점의 설계**라는 점을 배웠습니다.

또한 AI를 활용해 구현 속도를 높일 수 있어도, 핵심 로직은 반드시 사람이 이해하고 검증해야 한다는 점을 다시 확인했습니다.

## 11. 실행 방법

### RESP 서버

```bash
python server.py
```

### 테스트 실행

```bash
python -m unittest discover -s tests
```

### 데모 테스트 실행

```bash
python demo_test_cases.py
```

### 성능 비교 실행

```bash
python -m performance.check_connections
python -m performance.run_benchmarks
```

