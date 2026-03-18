# Performance Benchmarks

`performance/` 디렉토리는 미래의 `RESP 기반 mini-redis 서버`와 `MongoDB`를 같은 조건에서 비교하기 위한 코드입니다.

## 파일 구성

- `test_performance.py`: 각 서버가 실제 요청을 받을 수 있는지 확인하는 smoke test
- `config.py`: 환경 변수 기반 설정 로더
- `clients.py`: RESP, MongoDB용 클라이언트 구현
- `benchmark.py`: latency/load 측정 로직
- `plot_results.py`: `matplotlib` 그래프 생성
- `run_benchmarks.py`: 전체 벤치마크 실행 진입점
- `requirements.txt`: 성능 비교용 추가 의존성

## 설치

```bash
pip install -r performance/requirements.txt
```

## 환경 변수

```bash
export MINIREDIS_RESP_HOST=127.0.0.1
export MINIREDIS_RESP_PORT=6379
export MONGO_URI=mongodb://127.0.0.1:27017
export MONGO_DB_NAME=mini_redis_benchmark
export MONGO_COLLECTION_NAME=kv_store
export PERF_LATENCY_ITERATIONS=200
export PERF_LOAD_TOTAL_REQUESTS=2000
export PERF_CONCURRENCY_LEVELS=1,4,8,16
```

## Smoke Test 실행

```bash
python -m unittest performance.test_performance
```

## 벤치마크 실행

```bash
python -m performance.run_benchmarks
```

실행 결과는 기본적으로 `performance/results/<timestamp>/` 아래에 저장됩니다.

## 생성 결과물

- `benchmark_report.json`
- `latency_summary.csv`
- `load_summary.csv`
- `latency_summary.png`
- `load_summary.png`
