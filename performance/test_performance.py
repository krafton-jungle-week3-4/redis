from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from io import BufferedReader
import json
from pathlib import Path
import os
import socket
import statistics
import time
import unittest
import uuid


HOST = os.getenv("MINIREDIS_RESP_HOST", "127.0.0.1")
PORT = int(os.getenv("MINIREDIS_RESP_PORT", "6379"))
LATENCY_ITERATIONS = int(os.getenv("PERF_LATENCY_ITERATIONS", "200"))
LOAD_TOTAL_REQUESTS = int(os.getenv("PERF_LOAD_TOTAL_REQUESTS", "2000"))
BUFFER_SIZE = int(os.getenv("PERF_BUFFER_SIZE", "4096"))
RESULTS_DIR = Path(__file__).resolve().parent / "results"


def parse_concurrency_levels(raw_value: str) -> tuple[int, ...]:
    # 부하 테스트에 사용할 동시성 레벨 목록을 환경 변수에서 읽어온다.
    levels = []
    for chunk in raw_value.split(","):
        value = chunk.strip()
        if not value:
            continue
        level = int(value)
        if level <= 0:
            raise ValueError("PERF_CONCURRENCY_LEVELS must contain only positive integers")
        levels.append(level)
    if not levels:
        raise ValueError("PERF_CONCURRENCY_LEVELS must not be empty")
    return tuple(levels)


CONCURRENCY_LEVELS = parse_concurrency_levels(os.getenv("PERF_CONCURRENCY_LEVELS", "1,4,8,16"))


def encode_resp_command(*parts: str) -> bytes:
    # 문자열 명령을 RESP 배열 형식 바이트로 인코딩한다.
    message = [f"*{len(parts)}\r\n".encode()]
    for part in parts:
        encoded_part = part.encode()
        message.append(f"${len(encoded_part)}\r\n".encode())
        message.append(encoded_part + b"\r\n")
    return b"".join(message)


def read_resp_line(reader: BufferedReader) -> bytes:
    # RESP 한 줄을 읽고 CRLF를 제거한 뒤 반환한다.
    line = reader.readline()
    if not line:
        raise RuntimeError("connection closed while reading RESP line")
    if not line.endswith(b"\r\n"):
        raise RuntimeError(f"invalid RESP line ending: {line!r}")
    return line[:-2]


def read_resp_frame(reader: BufferedReader) -> str | None:
    # 응답 프레임을 소비해 다음 요청을 보낼 수 있게 만든다.
    prefix = reader.read(1)
    if not prefix:
        raise RuntimeError("connection closed while reading RESP frame")
    if prefix == b"+":
        return read_resp_line(reader).decode()
    if prefix == b"$":
        length = int(read_resp_line(reader).decode())
        if length == -1:
            return None
        payload = reader.read(length)
        if len(payload) != length:
            raise RuntimeError("connection closed while reading RESP bulk string")
        trailer = reader.read(2)
        if trailer != b"\r\n":
            raise RuntimeError(f"invalid RESP bulk string trailer: {trailer!r}")
        return payload.decode()
    if prefix == b":":
        return read_resp_line(reader).decode()
    if prefix == b"-":
        error_text = read_resp_line(reader).decode()
        raise RuntimeError(f"RESP server returned an error: {error_text}")
    if prefix == b"H":
        http_prefix = prefix + reader.read(BUFFER_SIZE)
        raise RuntimeError(f"server is speaking HTTP, not RESP: {http_prefix[:32]!r}")
    raise RuntimeError(f"unsupported RESP frame prefix: {prefix!r}")


def percentile(samples: list[float], percent: float) -> float:
    # 백분위 지연시간을 계산한다.
    if not samples:
        return 0.0
    ordered = sorted(samples)
    index = max(0, int((percent / 100) * len(ordered) + 0.999999) - 1)
    return ordered[index]


def summarize_samples(samples: list[float]) -> dict[str, float | int]:
    # 수집한 지연시간을 사람이 읽기 쉬운 요약값으로 정리한다.
    if not samples:
        return {
            "count": 0,
            "avg_ms": 0.0,
            "p50_ms": 0.0,
            "p95_ms": 0.0,
            "p99_ms": 0.0,
            "min_ms": 0.0,
            "max_ms": 0.0,
        }
    return {
        "count": len(samples),
        "avg_ms": round(statistics.fmean(samples), 6),
        "p50_ms": round(percentile(samples, 50), 6),
        "p95_ms": round(percentile(samples, 95), 6),
        "p99_ms": round(percentile(samples, 99), 6),
        "min_ms": round(min(samples), 6),
        "max_ms": round(max(samples), 6),
    }


def distribute_requests(total_requests: int, worker_count: int) -> list[int]:
    # 전체 요청 수를 worker 수만큼 고르게 나눈다.
    base, remainder = divmod(total_requests, worker_count)
    assignments = [base] * worker_count
    for index in range(remainder):
        assignments[index] += 1
    return assignments


class RespPerformanceTests(unittest.TestCase):
    def open_connection(self) -> tuple[socket.socket, BufferedReader]:
        # RESP 서버에 연결하고 읽기 스트림을 함께 준비한다.
        try:
            connection = socket.create_connection((HOST, PORT), timeout=3)
        except OSError as exc:
            self.skipTest(f"RESP server is not reachable at {HOST}:{PORT}: {exc}")
        return connection, connection.makefile("rb")

    def send_command(
        self,
        connection: socket.socket,
        reader: BufferedReader,
        *parts: str,
    ) -> tuple[float, str | None]:
        # 한 개 명령을 전송하고 왕복 시간과 응답을 반환한다.
        payload = encode_resp_command(*parts)
        start = time.perf_counter()
        connection.sendall(payload)
        response = read_resp_frame(reader)
        elapsed_ms = (time.perf_counter() - start) * 1000
        return elapsed_ms, response

    def print_metric(self, label: str, summary: dict[str, float | int]) -> None:
        # 측정 결과를 한 줄로 출력한다.
        print(
            f"{label}: "
            f"count={summary['count']} "
            f"avg={summary['avg_ms']:.3f}ms "
            f"p50={summary['p50_ms']:.3f}ms "
            f"p95={summary['p95_ms']:.3f}ms "
            f"p99={summary['p99_ms']:.3f}ms "
            f"min={summary['min_ms']:.3f}ms "
            f"max={summary['max_ms']:.3f}ms"
        )

    def write_report(self, report: dict[str, object], suffix: str) -> Path:
        # 측정 결과를 JSON 파일로 저장한다.
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        file_path = RESULTS_DIR / f"{timestamp}-{suffix}.json"
        with file_path.open("w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)
        print(f"saved report to {file_path}")
        return file_path

    def test_latency_profile_for_supported_commands(self) -> None:
        # AGENTS 범위의 명령만 대상으로 단건 지연시간을 측정한다.
        connection, reader = self.open_connection()
        with connection, reader:
            for _ in range(10):
                self.send_command(connection, reader, "PING")

            set_keys = [f"perf:latency:set:{uuid.uuid4().hex}:{index}" for index in range(LATENCY_ITERATIONS)]
            del_keys = [f"perf:latency:del:{uuid.uuid4().hex}:{index}" for index in range(LATENCY_ITERATIONS)]

            ping_samples = [self.send_command(connection, reader, "PING")[0] for _ in range(LATENCY_ITERATIONS)]
            echo_samples = [
                self.send_command(connection, reader, "ECHO", "benchmark-message")[0]
                for _ in range(LATENCY_ITERATIONS)
            ]
            set_samples = [
                self.send_command(connection, reader, "SET", key, "value")[0]
                for key in set_keys
            ]
            get_samples = [
                self.send_command(connection, reader, "GET", key)[0]
                for key in set_keys
            ]
            exists_samples = [
                self.send_command(connection, reader, "EXISTS", key)[0]
                for key in set_keys
            ]
            type_samples = [
                self.send_command(connection, reader, "TYPE", key)[0]
                for key in set_keys
            ]

            for key in del_keys:
                self.send_command(connection, reader, "SET", key, "value")
            del_samples = [
                self.send_command(connection, reader, "DEL", key)[0]
                for key in del_keys
            ]

        report = {
            "host": HOST,
            "port": PORT,
            "iterations": LATENCY_ITERATIONS,
            "latency_ms": {
                "PING": summarize_samples(ping_samples),
                "ECHO": summarize_samples(echo_samples),
                "SET": summarize_samples(set_samples),
                "GET": summarize_samples(get_samples),
                "EXISTS": summarize_samples(exists_samples),
                "TYPE": summarize_samples(type_samples),
                "DEL": summarize_samples(del_samples),
            },
        }

        for command_name, summary in report["latency_ms"].items():
            self.print_metric(command_name, summary)
        self.write_report(report, "latency")

        self.assertTrue(all(summary["count"] == LATENCY_ITERATIONS for summary in report["latency_ms"].values()))

    def test_mixed_load_profile_for_supported_commands(self) -> None:
        # 지원 명령을 섞은 상태에서 동시 부하와 처리량을 측정한다.
        seed_prefix = f"perf:load:seed:{uuid.uuid4().hex}"
        seed_keys = [f"{seed_prefix}:{index}" for index in range(max(64, LOAD_TOTAL_REQUESTS // 4))]

        connection, reader = self.open_connection()
        with connection, reader:
            for index, key in enumerate(seed_keys):
                self.send_command(connection, reader, "SET", key, f"value-{index}")

        def run_worker(worker_id: int, request_count: int, start_index: int) -> dict[str, object]:
            # worker 하나가 자기 요청 몫을 처리하며 지연시간을 모은다.
            latencies: list[float] = []
            error_count = 0

            try:
                connection = socket.create_connection((HOST, PORT), timeout=3)
                reader = connection.makefile("rb")
            except OSError as exc:
                return {"latencies_ms": latencies, "error_count": request_count, "error": str(exc)}

            with connection, reader:
                last_set_key = f"perf:load:worker:{worker_id}:bootstrap"
                for offset in range(request_count):
                    request_index = start_index + offset
                    seed_key = seed_keys[request_index % len(seed_keys)]
                    slot = request_index % 7

                    try:
                        if slot == 0:
                            elapsed_ms, _ = self.send_command(connection, reader, "PING")
                        elif slot == 1:
                            elapsed_ms, _ = self.send_command(connection, reader, "ECHO", "load-message")
                        elif slot == 2:
                            elapsed_ms, _ = self.send_command(connection, reader, "GET", seed_key)
                        elif slot == 3:
                            last_set_key = f"perf:load:worker:{worker_id}:{request_index}"
                            elapsed_ms, _ = self.send_command(connection, reader, "SET", last_set_key, "value")
                        elif slot == 4:
                            elapsed_ms, _ = self.send_command(connection, reader, "EXISTS", seed_key)
                        elif slot == 5:
                            elapsed_ms, _ = self.send_command(connection, reader, "TYPE", seed_key)
                        else:
                            elapsed_ms, _ = self.send_command(connection, reader, "DEL", last_set_key)
                        latencies.append(elapsed_ms)
                    except Exception:
                        error_count += 1

            return {"latencies_ms": latencies, "error_count": error_count}

        load_report = []
        for concurrency in CONCURRENCY_LEVELS:
            assignments = distribute_requests(LOAD_TOTAL_REQUESTS, concurrency)
            futures = []
            start_time = time.perf_counter()

            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                next_start_index = 0
                for worker_id, request_count in enumerate(assignments):
                    futures.append(
                        executor.submit(run_worker, worker_id, request_count, next_start_index)
                    )
                    next_start_index += request_count

            elapsed_seconds = time.perf_counter() - start_time
            worker_results = [future.result() for future in futures]
            latencies = [
                latency
                for worker_result in worker_results
                for latency in worker_result["latencies_ms"]
            ]
            error_count = sum(int(worker_result["error_count"]) for worker_result in worker_results)
            success_count = len(latencies)
            summary = summarize_samples(latencies)

            result = {
                "concurrency": concurrency,
                "total_requests": LOAD_TOTAL_REQUESTS,
                "success_count": success_count,
                "error_count": error_count,
                "elapsed_seconds": round(elapsed_seconds, 6),
                "throughput_rps": round(
                    success_count / elapsed_seconds if elapsed_seconds else 0.0,
                    6,
                ),
                "latency_ms": summary,
            }
            load_report.append(result)

            print(
                f"LOAD concurrency={concurrency} "
                f"throughput={result['throughput_rps']:.3f}rps "
                f"errors={error_count}"
            )
            self.print_metric(f"LOAD-{concurrency}", summary)

        report = {
            "host": HOST,
            "port": PORT,
            "total_requests": LOAD_TOTAL_REQUESTS,
            "concurrency_levels": list(CONCURRENCY_LEVELS),
            "load": load_report,
        }
        self.write_report(report, "load")

        self.assertTrue(all(row["success_count"] > 0 for row in load_report))


if __name__ == "__main__":
    unittest.main()
