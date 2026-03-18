from __future__ import annotations

import argparse
from html import escape
import json
from pathlib import Path


COLORS = ["#1b9e77", "#d95f02", "#7570b3", "#e7298a"]


def create_plots(
    report: dict[str, object],
    output_dir: Path,
    filename_prefix: str = "",
) -> list[Path]:
    available = _available_backends(report)
    if not available:
        return []

    try:
        return _create_matplotlib_plots(available, output_dir, filename_prefix)
    except ImportError:
        return _create_svg_plots(available, output_dir, filename_prefix)


def create_plots_from_result_files(
    latency_json_path: Path,
    load_json_path: Path,
    output_dir: Path | None = None,
    label: str | None = None,
) -> list[Path]:
    latency_report = json.loads(latency_json_path.read_text(encoding="utf-8"))
    load_report = json.loads(load_json_path.read_text(encoding="utf-8"))

    backend_name = label or _infer_backend_label(latency_report, load_report)
    report = _build_report_from_result_files(backend_name, latency_report, load_report)
    target_dir = output_dir or latency_json_path.parent
    filename_prefix = _infer_filename_prefix(latency_json_path, load_json_path)
    return create_plots(report, target_dir, filename_prefix=filename_prefix)


def _available_backends(report: dict[str, object]) -> dict[str, dict[str, object]]:
    backends = report["backends"]
    return {
        backend_name: backend_result
        for backend_name, backend_result in backends.items()
        if "error" not in backend_result
    }


def _backend_label(backend_name: str, backend_result: dict[str, object]) -> str:
    return str(backend_result.get("label", backend_name.upper()))


def _build_report_from_result_files(
    backend_name: str,
    latency_report: dict[str, object],
    load_report: dict[str, object],
) -> dict[str, object]:
    normalized_load = []
    for row in load_report["load"]:
        latency_summary = row["latency_ms"]
        normalized_load.append(
            {
                "concurrency": row["concurrency"],
                "total_requests": row["total_requests"],
                "success_count": row["success_count"],
                "error_count": row["error_count"],
                "elapsed_seconds": row["elapsed_seconds"],
                "throughput_rps": row["throughput_rps"],
                "avg_latency_ms": latency_summary["avg_ms"],
                "p95_latency_ms": latency_summary["p95_ms"],
                "p99_latency_ms": latency_summary["p99_ms"],
            }
        )

    return {
        "backends": {
            backend_name: {
                "latency": {
                    operation.lower(): summary
                    for operation, summary in latency_report["latency_ms"].items()
                },
                "load": normalized_load,
            }
        }
    }


def _infer_backend_label(
    latency_report: dict[str, object],
    load_report: dict[str, object],
) -> str:
    host = latency_report.get("host") or load_report.get("host")
    port = latency_report.get("port") or load_report.get("port")
    if host is not None and port is not None:
        return f"{host}:{port}"
    return "result"


def _infer_filename_prefix(latency_json_path: Path, load_json_path: Path) -> str:
    latency_stem = latency_json_path.stem
    load_stem = load_json_path.stem
    if latency_stem.endswith("-latency") and load_stem.endswith("-load"):
        latency_prefix = latency_stem[: -len("-latency")]
        load_prefix = load_stem[: -len("-load")]
        if latency_prefix == load_prefix:
            return f"{latency_prefix}-"
    return ""


def _latency_over_ping(backend_result: dict[str, object]) -> dict[str, dict[str, float | int]]:
    if "latency_over_ping" in backend_result:
        return backend_result["latency_over_ping"]

    latency_result = backend_result["latency"]
    ping_summary = latency_result["ping"]
    adjusted: dict[str, dict[str, float | int]] = {}
    for operation, summary in latency_result.items():
        adjusted[operation] = {
            "count": summary["count"],
            "avg_ms": round(max(float(summary["avg_ms"]) - float(ping_summary["avg_ms"]), 0.0), 6),
            "p50_ms": round(max(float(summary["p50_ms"]) - float(ping_summary["p50_ms"]), 0.0), 6),
            "p95_ms": round(max(float(summary["p95_ms"]) - float(ping_summary["p95_ms"]), 0.0), 6),
            "p99_ms": round(max(float(summary["p99_ms"]) - float(ping_summary["p99_ms"]), 0.0), 6),
            "min_ms": round(max(float(summary["min_ms"]) - float(ping_summary["min_ms"]), 0.0), 6),
            "max_ms": round(max(float(summary["max_ms"]) - float(ping_summary["max_ms"]), 0.0), 6),
        }
    return adjusted


def _build_adjusted_available(
    available: dict[str, dict[str, object]],
) -> dict[str, dict[str, object]]:
    return {
        backend_name: {
            **backend_result,
            "latency": _latency_over_ping(backend_result),
        }
        for backend_name, backend_result in available.items()
    }


def _create_matplotlib_plots(
    available: dict[str, dict[str, object]],
    output_dir: Path,
    filename_prefix: str,
) -> list[Path]:
    import matplotlib.pyplot as plt

    latency_path = output_dir / f"{filename_prefix}latency_summary.png"
    adjusted_latency_path = output_dir / f"{filename_prefix}latency_over_ping.png"
    load_path = output_dir / f"{filename_prefix}load_summary.png"
    backend_names = list(available.keys())
    operations = _ordered_operations(available)
    x_positions = list(range(len(operations)))
    width = 0.8 / max(len(backend_names), 1)

    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    for index, backend_name in enumerate(backend_names):
        latency_result = available[backend_name]["latency"]
        bar_positions = [
            position - 0.4 + (width / 2) + (index * width) for position in x_positions
        ]
        average_values = [latency_result[operation]["avg_ms"] for operation in operations]
        p95_values = [latency_result[operation]["p95_ms"] for operation in operations]
        axes[0].bar(
            bar_positions,
            average_values,
            width=width,
            color=COLORS[index % len(COLORS)],
            label=_backend_label(backend_name, available[backend_name]),
        )
        axes[1].bar(
            bar_positions,
            p95_values,
            width=width,
            color=COLORS[index % len(COLORS)],
            label=_backend_label(backend_name, available[backend_name]),
        )

    for axis, title in zip(axes, ["Average Latency (ms)", "P95 Latency (ms)"]):
        axis.set_xticks(x_positions)
        axis.set_xticklabels([operation.upper() for operation in operations])
        axis.set_title(title)
        axis.set_ylabel("milliseconds")
        axis.legend()

    fig.tight_layout()
    fig.savefig(latency_path, dpi=150)
    plt.close(fig)

    adjusted_available = _build_adjusted_available(available)
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    for index, backend_name in enumerate(backend_names):
        latency_result = adjusted_available[backend_name]["latency"]
        bar_positions = [
            position - 0.4 + (width / 2) + (index * width) for position in x_positions
        ]
        average_values = [latency_result[operation]["avg_ms"] for operation in operations]
        p95_values = [latency_result[operation]["p95_ms"] for operation in operations]
        axes[0].bar(
            bar_positions,
            average_values,
            width=width,
            color=COLORS[index % len(COLORS)],
            label=_backend_label(backend_name, adjusted_available[backend_name]),
        )
        axes[1].bar(
            bar_positions,
            p95_values,
            width=width,
            color=COLORS[index % len(COLORS)],
            label=_backend_label(backend_name, adjusted_available[backend_name]),
        )

    for axis, title in zip(
        axes,
        ["Average Latency Over PING (ms)", "P95 Latency Over PING (ms)"],
    ):
        axis.set_xticks(x_positions)
        axis.set_xticklabels([operation.upper() for operation in operations])
        axis.set_title(title)
        axis.set_ylabel("milliseconds")
        axis.legend()

    fig.tight_layout()
    fig.savefig(adjusted_latency_path, dpi=150)
    plt.close(fig)

    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    for index, backend_name in enumerate(backend_names):
        load_result = available[backend_name]["load"]
        concurrency_levels = [row["concurrency"] for row in load_result]
        throughput_values = [row["throughput_rps"] for row in load_result]
        p95_values = [row["p95_latency_ms"] for row in load_result]

        axes[0].plot(
            concurrency_levels,
            throughput_values,
            marker="o",
            color=COLORS[index % len(COLORS)],
            label=_backend_label(backend_name, available[backend_name]),
        )
        axes[1].plot(
            concurrency_levels,
            p95_values,
            marker="o",
            color=COLORS[index % len(COLORS)],
            label=_backend_label(backend_name, available[backend_name]),
        )

    axes[0].set_title("Throughput Under Load")
    axes[0].set_xlabel("concurrency")
    axes[0].set_ylabel("requests/sec")
    axes[0].legend()

    axes[1].set_title("P95 Latency Under Load")
    axes[1].set_xlabel("concurrency")
    axes[1].set_ylabel("milliseconds")
    axes[1].legend()

    fig.tight_layout()
    fig.savefig(load_path, dpi=150)
    plt.close(fig)

    return [latency_path, adjusted_latency_path, load_path]


def _ordered_operations(available: dict[str, dict[str, object]]) -> list[str]:
    operations: list[str] = []
    for backend_result in available.values():
        for operation in backend_result["latency"].keys():
            if operation not in operations:
                operations.append(operation)
    return operations


def _create_svg_plots(
    available: dict[str, dict[str, object]],
    output_dir: Path,
    filename_prefix: str,
) -> list[Path]:
    latency_path = output_dir / f"{filename_prefix}latency_summary.svg"
    adjusted_latency_path = output_dir / f"{filename_prefix}latency_over_ping.svg"
    load_path = output_dir / f"{filename_prefix}load_summary.svg"
    latency_path.write_text(_render_latency_svg(available), encoding="utf-8")
    adjusted_latency_path.write_text(
        _render_latency_svg(_build_adjusted_available(available), title="Latency Over PING"),
        encoding="utf-8",
    )
    load_path.write_text(_render_load_svg(available), encoding="utf-8")
    return [latency_path, adjusted_latency_path, load_path]


def _render_latency_svg(
    available: dict[str, dict[str, object]],
    title: str = "Latency Summary",
) -> str:
    operations = _ordered_operations(available)
    avg_series = [
        (
            backend_name,
            [available[backend_name]["latency"][operation]["avg_ms"] for operation in operations],
        )
        for backend_name in available
    ]
    p95_series = [
        (
            backend_name,
            [available[backend_name]["latency"][operation]["p95_ms"] for operation in operations],
        )
        for backend_name in available
    ]

    max_value = max(
        [value for _, values in avg_series + p95_series for value in values],
        default=1.0,
    )
    scale_max = max_value * 1.15 if max_value else 1.0
    return _render_two_panel_svg(
        title=title,
        legend_labels=[_backend_label(name, result) for name, result in available.items()],
        left_panel=_render_bar_panel(
            title="Average Latency (ms)" if title == "Latency Summary" else "Average Over PING (ms)",
            y_label="milliseconds",
            categories=[operation.upper() for operation in operations],
            series=avg_series,
            scale_max=scale_max,
        ),
        right_panel=_render_bar_panel(
            title="P95 Latency (ms)" if title == "Latency Summary" else "P95 Over PING (ms)",
            y_label="milliseconds",
            categories=[operation.upper() for operation in operations],
            series=p95_series,
            scale_max=scale_max,
        ),
    )


def _render_load_svg(available: dict[str, dict[str, object]]) -> str:
    throughput_series = []
    latency_series = []
    all_throughput_values: list[float] = []
    all_latency_values: list[float] = []

    for backend_name, backend_result in available.items():
        load_result = backend_result["load"]
        throughput_values = [
            (row["concurrency"], row["throughput_rps"]) for row in load_result
        ]
        latency_values = [
            (row["concurrency"], row["p95_latency_ms"]) for row in load_result
        ]
        throughput_series.append((backend_name, throughput_values))
        latency_series.append((backend_name, latency_values))
        all_throughput_values.extend(value for _, value in throughput_values)
        all_latency_values.extend(value for _, value in latency_values)

    throughput_max = max(all_throughput_values, default=1.0) * 1.15
    latency_max = max(all_latency_values, default=1.0) * 1.15
    return _render_two_panel_svg(
        title="Load Summary",
        legend_labels=[_backend_label(name, result) for name, result in available.items()],
        left_panel=_render_line_panel(
            title="Throughput Under Load",
            y_label="requests/sec",
            series=throughput_series,
            scale_max=throughput_max,
        ),
        right_panel=_render_line_panel(
            title="P95 Latency Under Load",
            y_label="milliseconds",
            series=latency_series,
            scale_max=latency_max,
        ),
    )


def _render_two_panel_svg(
    title: str,
    legend_labels: list[str],
    left_panel: str,
    right_panel: str,
) -> str:
    width = 1200
    height = 420
    legend_items = []
    legend_x = 810
    for index, label in enumerate(legend_labels):
        x = legend_x + (index * 120)
        color = COLORS[index % len(COLORS)]
        legend_items.append(
            f'<rect x="{x}" y="22" width="14" height="14" fill="{color}" rx="2" />'
        )
        legend_items.append(
            f'<text x="{x + 20}" y="34" font-size="12" fill="#34495e">{escape(label)}</text>'
        )

    return "\n".join(
        [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
            '<rect width="100%" height="100%" fill="#f7f9fc" />',
            f'<text x="40" y="36" font-size="24" font-weight="700" fill="#1f2937">{escape(title)}</text>',
            *legend_items,
            f'<g transform="translate(30, 58)">{left_panel}</g>',
            f'<g transform="translate(610, 58)">{right_panel}</g>',
            "</svg>",
        ]
    )


def _render_bar_panel(
    title: str,
    y_label: str,
    categories: list[str],
    series: list[tuple[str, list[float]]],
    scale_max: float,
) -> str:
    panel_width = 560
    panel_height = 330
    plot_left = 72
    plot_top = 24
    plot_width = 450
    plot_height = 220
    group_width = plot_width / max(len(categories), 1)
    bar_width = min(36.0, (group_width * 0.72) / max(len(series), 1))
    shapes = [
        f'<rect width="{panel_width}" height="{panel_height}" rx="14" fill="#ffffff" stroke="#d7dee8" />',
        f'<text x="{panel_width / 2}" y="28" text-anchor="middle" font-size="18" font-weight="600" fill="#1f2937">{escape(title)}</text>',
        f'<text x="18" y="{plot_top + (plot_height / 2)}" text-anchor="middle" transform="rotate(-90 18 {plot_top + (plot_height / 2)})" font-size="12" fill="#6b7280">{escape(y_label)}</text>',
    ]

    shapes.extend(_render_y_axis(plot_left, plot_top, plot_height, plot_width, scale_max))
    category_count = len(categories)
    rotate_labels = category_count >= 6
    total_bar_width = bar_width * len(series)

    for category_index, category in enumerate(categories):
        center_x = plot_left + (group_width * category_index) + (group_width / 2)
        label_y = plot_top + plot_height + 26
        if rotate_labels:
            shapes.append(
                f'<text x="{center_x}" y="{label_y}" text-anchor="end" transform="rotate(-28 {center_x} {label_y})" font-size="11" fill="#4b5563">{escape(category)}</text>'
            )
        else:
            shapes.append(
                f'<text x="{center_x}" y="{label_y}" text-anchor="middle" font-size="12" fill="#4b5563">{escape(category)}</text>'
            )

        start_x = center_x - (total_bar_width / 2)
        for series_index, (_, values) in enumerate(series):
            value = values[category_index]
            bar_height = _scaled_height(value, plot_height, scale_max)
            x = start_x + (series_index * bar_width)
            y = plot_top + plot_height - bar_height
            color = COLORS[series_index % len(COLORS)]
            shapes.append(
                f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_width - 4:.2f}" height="{bar_height:.2f}" rx="4" fill="{color}" />'
            )

    return "\n".join(shapes)


def _render_line_panel(
    title: str,
    y_label: str,
    series: list[tuple[str, list[tuple[int, float]]]],
    scale_max: float,
) -> str:
    panel_width = 560
    panel_height = 330
    plot_left = 72
    plot_top = 24
    plot_width = 450
    plot_height = 220
    shapes = [
        f'<rect width="{panel_width}" height="{panel_height}" rx="14" fill="#ffffff" stroke="#d7dee8" />',
        f'<text x="{panel_width / 2}" y="28" text-anchor="middle" font-size="18" font-weight="600" fill="#1f2937">{escape(title)}</text>',
        f'<text x="18" y="{plot_top + (plot_height / 2)}" text-anchor="middle" transform="rotate(-90 18 {plot_top + (plot_height / 2)})" font-size="12" fill="#6b7280">{escape(y_label)}</text>',
        f'<text x="{plot_left + (plot_width / 2)}" y="{plot_top + plot_height + 44}" text-anchor="middle" font-size="12" fill="#6b7280">concurrency</text>',
    ]

    shapes.extend(_render_y_axis(plot_left, plot_top, plot_height, plot_width, scale_max))

    x_values = sorted(
        {point[0] for _, points in series for point in points}
    )
    if len(x_values) == 1:
        x_positions = {x_values[0]: plot_left + (plot_width / 2)}
    else:
        min_x = min(x_values)
        max_x = max(x_values)
        x_positions = {
            value: plot_left + ((value - min_x) / (max_x - min_x)) * plot_width
            for value in x_values
        }

    axis_y = plot_top + plot_height
    for value in x_values:
        x = x_positions[value]
        shapes.append(f'<line x1="{x:.2f}" y1="{axis_y}" x2="{x:.2f}" y2="{axis_y + 6}" stroke="#9ca3af" />')
        shapes.append(
            f'<text x="{x:.2f}" y="{axis_y + 22}" text-anchor="middle" font-size="12" fill="#4b5563">{value}</text>'
        )

    for index, (_, points) in enumerate(series):
        color = COLORS[index % len(COLORS)]
        path_commands = []
        for point_index, (x_value, y_value) in enumerate(points):
            x = x_positions[x_value]
            y = plot_top + plot_height - _scaled_height(y_value, plot_height, scale_max)
            command = "M" if point_index == 0 else "L"
            path_commands.append(f"{command} {x:.2f} {y:.2f}")
            shapes.append(f'<circle cx="{x:.2f}" cy="{y:.2f}" r="4.5" fill="{color}" />')
        if path_commands:
            shapes.append(
                f'<path d="{" ".join(path_commands)}" fill="none" stroke="{color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" />'
            )

    return "\n".join(shapes)


def _render_y_axis(
    plot_left: float,
    plot_top: float,
    plot_height: float,
    plot_width: float,
    scale_max: float,
) -> list[str]:
    axis_y = plot_top + plot_height
    elements = [
        f'<line x1="{plot_left}" y1="{plot_top}" x2="{plot_left}" y2="{axis_y}" stroke="#374151" stroke-width="1.2" />',
        f'<line x1="{plot_left}" y1="{axis_y}" x2="{plot_left + plot_width}" y2="{axis_y}" stroke="#374151" stroke-width="1.2" />',
    ]
    tick_count = 5
    for tick_index in range(tick_count):
        ratio = tick_index / (tick_count - 1)
        value = scale_max * ratio
        y = axis_y - (plot_height * ratio)
        elements.append(
            f'<line x1="{plot_left}" y1="{y:.2f}" x2="{plot_left + plot_width}" y2="{y:.2f}" stroke="#e5e7eb" />'
        )
        elements.append(
            f'<text x="{plot_left - 10}" y="{y + 4:.2f}" text-anchor="end" font-size="11" fill="#6b7280">{_format_tick(value)}</text>'
        )
    return elements


def _scaled_height(value: float, plot_height: float, scale_max: float) -> float:
    if scale_max <= 0:
        return 0.0
    return (value / scale_max) * plot_height


def _format_tick(value: float) -> str:
    if value >= 1000:
        return f"{value:,.0f}"
    if value >= 10:
        return f"{value:.1f}"
    return f"{value:.3f}".rstrip("0").rstrip(".")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create benchmark plots from standalone latency/load JSON files."
    )
    parser.add_argument("latency_json", type=Path)
    parser.add_argument("load_json", type=Path)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--label", default=None)
    args = parser.parse_args()

    plot_paths = create_plots_from_result_files(
        args.latency_json,
        args.load_json,
        output_dir=args.output_dir,
        label=args.label,
    )
    for plot_path in plot_paths:
        print(plot_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
