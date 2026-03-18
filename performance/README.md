# Performance Benchmarks

`performance/` 디렉토리는 `mini-redis`를 두 층으로 측정하기 위한 코드입니다.

- `core_execute`: `redis.execute()`를 프로세스 내부에서 직접 호출하는 microbenchmark
- `network_e2e`: RESP 기반 mini-redis 서버와 MongoDB를 실제 네트워크 왕복 포함 조건에서 비교하는 benchmark

현재 비교 대상 명령은 아래처럼 맞춰져 있습니다.

- `PING` <-> MongoDB `ping` command
- `SET` <-> MongoDB `replace_one(..., upsert=True)`
- `GET` <-> MongoDB `find_one`
- `EXISTS` <-> MongoDB `_id` 존재 확인
- `TYPE` <-> MongoDB 존재 시 `"string"`, 없으면 `"none"`으로 매핑
- `DEL` <-> MongoDB `delete_one`

`ECHO`는 MongoDB에 의미 있는 대응 명령이 없어서 교차 백엔드 plot 비교에서는 제외합니다.

측정 편향을 줄이기 위해 아래 규칙을 적용합니다.

- latency/load 모두 연산 순서를 고정하지 않고 seed 기반으로 섞습니다
- backend 간에는 같은 random seed로 같은 연산 mix를 사용합니다
- benchmark가 끝나면 생성한 benchmark key를 정리합니다
- `network_e2e`는 `PING` baseline을 별도로 기록하고, `avg over ping`만 추가로 계산합니다

## 해석 규칙

- `core_execute`와 서버 호스트에서 실행한 `network_e2e`만 backend 성능 비교 근거로 사용합니다.
- 개발 머신에서 공인 IP로 실행한 `network_e2e`는 사용자 체감 RTT 측정으로 해석합니다.
- 원격 run에서 RESP와 MongoDB가 비슷하게 보이면, storage 성능이 아니라 공용 네트워크 RTT가 지배적일 가능성이 큽니다.

## 파일 구성

- `test_performance.py`: 각 서버가 실제 요청을 받을 수 있는지 확인하는 smoke test
- `config.py`: 환경 변수 기반 설정 로더
- `clients.py`: RESP, MongoDB용 클라이언트 구현
- `benchmark.py`: latency/load 측정 로직
- `plot_results.py`: `matplotlib` 그래프 생성
- `check_connections.py`: 원격 대상 round-trip 연결 확인
- `run_benchmarks.py`: 전체 벤치마크 실행 진입점
- `requirements.txt`: 성능 비교용 추가 의존성

## 설치

```bash
pip install -r performance/requirements.txt
```

## 환경 변수

기본값은 로컬 개발 환경이지만, AWS 원격 벤치마크를 위해 아래 환경 변수를 지원합니다.

```bash
export MINIREDIS_RESP_HOST=<aws-mini-redis-hostname>
export MINIREDIS_RESP_PORT=6379
export MINIREDIS_RESP_LABEL=aws-mini-redis
export MINIREDIS_RESP_CONNECT_TIMEOUT_SEC=5
export MINIREDIS_RESP_SOCKET_TIMEOUT_SEC=30
export MINIREDIS_RESP_TCP_NODELAY=true
export MINIREDIS_RESP_KEEPALIVE=true

export MONGO_URI='mongodb://<user>:<password>@<aws-mongo-host>:27017/?retryWrites=false'
export MONGO_DB_NAME=mini_redis_benchmark
export MONGO_COLLECTION_NAME=kv_store
export MONGO_LABEL=aws-mongodb
export MONGO_APP_NAME=mini-redis-benchmark
export MONGO_SERVER_SELECTION_TIMEOUT_MS=10000
export MONGO_CONNECT_TIMEOUT_MS=10000
export MONGO_SOCKET_TIMEOUT_MS=30000

export PERF_LATENCY_ITERATIONS=200
export PERF_LOAD_TOTAL_REQUESTS=2000
export PERF_CONCURRENCY_LEVELS=1,4,8,16
export PERF_PROFILES=core,network
export PERF_RANDOM_SEED=1729
export PERF_OUTPUT_DIR=performance/results/aws-run
```

TLS가 필요한 환경이면 추가로 설정합니다.

```bash
export MINIREDIS_RESP_TLS=true
export MINIREDIS_RESP_TLS_SERVER_HOSTNAME=<tls-server-name>
export MINIREDIS_RESP_TLS_VERIFY=true
export MINIREDIS_RESP_TLS_CA_FILE=/absolute/path/to/ca.pem

export MONGO_TLS=true
export MONGO_TLS_CA_FILE=/absolute/path/to/ca.pem
export MONGO_TLS_ALLOW_INVALID_CERTIFICATES=false
export MONGO_DIRECT_CONNECTION=true
```

`MONGO_URI`에 사용자 이름과 비밀번호가 포함돼도, 결과 JSON에는 redact된 형태만 저장됩니다.

DocumentDB 같이 URI 쿼리 옵션이 필요한 경우에는 `MONGO_URI`에 그대로 포함시키면 됩니다.

## Smoke Test 실행

```bash
python -m unittest performance.test_performance
```

원격 대상만 빠르게 확인하려면 아래 스크립트를 먼저 실행할 수 있습니다.

```bash
python -m performance.check_connections
```

하지만 `run_benchmarks.py`도 실행 시작 전에 자동으로 연결 확인을 먼저 수행합니다.

## 벤치마크 실행

같은 코드로 두 가지 표준 실행 방식을 권장합니다.

### 1. Same-Host Run

서버가 떠 있는 같은 호스트에서 실행합니다. `network_e2e` 결과를 backend 비교 기준으로 볼 때는 이 실행을 우선합니다.

주의:
서버의 bind 기본값은 `0.0.0.0`이지만, benchmark client는 wildcard 주소로 접속하지 않습니다.
같은 호스트에서 접속할 때는 loopback 주소 `127.0.0.1`을 사용합니다.

```bash
MINIREDIS_RESP_HOST=127.0.0.1 \
MINIREDIS_RESP_PORT=6379 \
MONGO_URI='mongodb://127.0.0.1:27017' \
PERF_PROFILES=core,network \
python -m performance.run_benchmarks
```

### 2. Remote Run

개발 머신에서 공인 endpoint로 실행합니다. 이 결과는 네트워크 포함 end-to-end RTT 측정으로 해석합니다.

```bash
python -m performance.run_benchmarks
```

환경 변수를 실행 시점에 같이 넘기려면 이렇게 실행하면 됩니다.

```bash
MINIREDIS_RESP_HOST=52.79.191.34 \
MINIREDIS_RESP_PORT=6379 \
MONGO_URI='mongodb://<user>:<password>@52.79.191.34:27017/?retryWrites=false' \
MONGO_DB_NAME=mini_redis_benchmark \
MONGO_COLLECTION_NAME=kv_store \
PERF_LATENCY_ITERATIONS=200 \
PERF_LOAD_TOTAL_REQUESTS=2000 \
PERF_CONCURRENCY_LEVELS=1,4,8,16 \
PERF_PROFILES=core,network \
PERF_RANDOM_SEED=1729 \
python -m performance.run_benchmarks
```

실행 결과는 기본적으로 `performance/results/<timestamp>/` 아래에 저장됩니다.

## 생성 결과물

- `benchmark_report.json`
- `core_execute_summary.csv`
- `connection_summary.json`
- `network_e2e_latency_summary.csv`
- `network_e2e_avg_over_ping.csv`
- `network_e2e_load_summary.csv`
- `core_execute_summary.png` 또는 `core_execute_summary.svg`
- `network_e2e_latency_summary.png` 또는 `network_e2e_latency_summary.svg`
- `network_e2e_avg_over_ping.png` 또는 `network_e2e_avg_over_ping.svg`
- `network_e2e_load_summary.png` 또는 `network_e2e_load_summary.svg`
