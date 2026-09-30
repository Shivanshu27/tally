"""Benchmark runner: executes every scenario against every available backend.

The report is written as a comparison, never as a single column. An in-memory
number and a containerised number for the same scenario differ by orders of
magnitude, and the gap between them *is* the finding -- it says what the
architecture costs in I/O. A report showing one column invites the reader to
assume it was the other one.

If the Docker stack is not running, the container column is recorded as
unavailable and the report says so explicitly. It is never silently omitted.
"""

from __future__ import annotations

import asyncio
import os
import platform
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from bench.backends import (
    BACKEND_DESCRIPTIONS,
    DOCKER,
    MEMORY,
    BackendKind,
    open_backend,
)
from bench.scenarios import (
    bench_crash_recovery,
    bench_enforcement_drift,
    bench_quota_check_latency,
    bench_reconciliation_gated_vs_ungated,
    bench_sustained_ingest,
    new_run_id,
)

console = Console()

#: Scenario key -> (display name, coroutine factory description).
SCENARIO_ORDER = ["ingest", "quota", "recovery", "drift", "gate"]


def get_hardware_info() -> dict[str, str]:
    """Capture the execution conditions a reader needs to interpret the numbers.

    Throughput without a CPU count is not a measurement, it is an anecdote. The
    container memory limit matters too: Redpanda is started with ``--memory
    512M`` in ``docker-compose.yml``, so these numbers are from a deliberately
    small broker.
    """
    cpu_count = os.cpu_count() or 0
    total_ram = ""
    if shutil.which("sysctl"):  # macOS
        try:
            out = subprocess.run(
                ["sysctl", "-n", "hw.memsize"],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            if out.returncode == 0 and out.stdout.strip().isdigit():
                total_ram = f"{int(out.stdout.strip()) / 1024**3:.0f} GiB"
        except (OSError, subprocess.SubprocessError):
            total_ram = ""

    return {
        "os": f"{platform.system()} {platform.release()}",
        "machine": platform.machine(),
        "processor": platform.processor() or platform.machine(),
        "cpu_count": str(cpu_count),
        "total_ram": total_ram or "unknown",
        "python": platform.python_version(),
        "captured_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC"),
    }


async def _run_backend(kind: BackendKind) -> dict[str, dict[str, Any]]:
    """Run the full scenario set against one backend."""
    run_id = new_run_id()
    results: dict[str, dict[str, Any]] = {}
    async with open_backend(kind) as backend:
        console.print(f"  [dim]run_id={run_id} topic={backend.topic}[/dim]")

        console.print("  1/5 sustained ingest")
        results["ingest"] = await bench_sustained_ingest(backend, run_id)

        console.print("  2/5 quota check latency")
        results["quota"] = await bench_quota_check_latency(backend, run_id)

        console.print("  3/5 crash recovery")
        results["recovery"] = await bench_crash_recovery(backend, run_id)

        console.print("  4/5 enforcement drift")
        results["drift"] = await bench_enforcement_drift(backend, run_id)

        console.print("  5/5 gated vs ungated reconciliation")
        results["gate"] = await bench_reconciliation_gated_vs_ungated(backend, run_id)

    return results


def _fmt(results: dict[str, dict[str, Any]] | None, scenario: str, key: str) -> str:
    """Format one cell, or say plainly that the backend did not run."""
    if results is None:
        return "_not run_"
    value = results.get(scenario, {}).get(key)
    if value is None:
        return "—"
    if isinstance(value, float):
        if key.endswith("_eps"):
            return f"{value:,.0f}"
        if key.endswith("_ms") or key.endswith("_seconds"):
            return f"{value:.3f}"
        return f"{value:,.2f}"
    return str(value)


def _console_table(
    mem: dict[str, dict[str, Any]] | None,
    doc: dict[str, dict[str, Any]] | None,
) -> Table:
    table = Table(title="Tally Benchmarks — in-memory vs containers")
    table.add_column("Scenario", style="cyan")
    table.add_column("Metric", style="white")
    table.add_column("in-memory", style="yellow", justify="right")
    table.add_column("containers", style="green", justify="right")

    rows = [
        ("Sustained ingest", "ingest", "throughput_eps", "events/sec"),
        ("Sustained ingest", "ingest", "p99_latency_ms", "batch p99 ms"),
        ("Quota check", "quota", "p50_latency_ms", "p50 ms"),
        ("Quota check", "quota", "p99_latency_ms", "p99 ms"),
        ("Crash recovery", "recovery", "recovery_rate_eps", "catch-up eps"),
        ("Crash recovery", "recovery", "recovery_seconds", "catch-up s"),
        ("Enforcement drift", "drift", "percentage_drift", "% arithmetic"),
        ("Enforcement drift", "drift", "lag_percentage_drift", "% from lag"),
    ]
    for label, scenario, key, metric in rows:
        table.add_row(label, metric, _fmt(mem, scenario, key), _fmt(doc, scenario, key))

    table.add_row(
        "Reconciliation",
        "ungated / gated class",
        f"{_fmt(mem, 'gate', 'ungated_divergence_class')} / {_fmt(mem, 'gate', 'gated_settled_divergence_class')}",
        f"{_fmt(doc, 'gate', 'ungated_divergence_class')} / {_fmt(doc, 'gate', 'gated_settled_divergence_class')}",
    )
    return table


def _markdown_report(
    hw: dict[str, str],
    mem: dict[str, dict[str, Any]] | None,
    doc: dict[str, dict[str, Any]] | None,
) -> str:
    docker_note = (
        "Docker stack was **not running**; the container column could not be "
        "measured. Re-run with `docker compose up -d` for the numbers that "
        "describe the real system."
        if doc is None
        else "Both columns measured in the same process on the same machine."
    )

    return f"""# Tally Benchmark Results

Generated by `tally bench`. Do not hand-edit: regenerate instead, so the numbers
in this file and the numbers in the README cannot drift apart.

### Conditions

| | |
|---|---|
| Captured | {hw["captured_at"]} |
| OS | {hw["os"]} ({hw["machine"]}) |
| CPU | {hw["processor"]}, {hw["cpu_count"]} logical cores |
| RAM | {hw["total_ram"]} |
| Python | {hw["python"]} |
| Command | `tally bench` |

### Backends

| Column | What it measures |
|---|---|
| **in-memory** | {BACKEND_DESCRIPTIONS[MEMORY]} |
| **containers** | {BACKEND_DESCRIPTIONS[DOCKER]} |

Redpanda runs with `--smp 1 --memory 512M` as configured in
`docker-compose.yml`. All three containers share one laptop with the benchmark
process, so the container column is a *lower* bound for a real deployment on
dedicated hardware, and the in-memory column is an *upper* bound that no
deployment can reach because it does no I/O at all.

{docker_note}

---

## Results

| Scenario | Metric | in-memory | containers |
|---|---|---:|---:|
| Sustained ingest | throughput (events/sec) | {_fmt(mem, "ingest", "throughput_eps")} | {_fmt(doc, "ingest", "throughput_eps")} |
| Sustained ingest | batch p50 (ms) | {_fmt(mem, "ingest", "p50_latency_ms")} | {_fmt(doc, "ingest", "p50_latency_ms")} |
| Sustained ingest | batch p99 (ms) | {_fmt(mem, "ingest", "p99_latency_ms")} | {_fmt(doc, "ingest", "p99_latency_ms")} |
| Quota check | p50 (ms) | {_fmt(mem, "quota", "p50_latency_ms")} | {_fmt(doc, "quota", "p50_latency_ms")} |
| Quota check | p99 (ms) | {_fmt(mem, "quota", "p99_latency_ms")} | {_fmt(doc, "quota", "p99_latency_ms")} |
| Quota check | p99.9 (ms) | {_fmt(mem, "quota", "p999_latency_ms")} | {_fmt(doc, "quota", "p999_latency_ms")} |
| Crash recovery | catch-up rate (events/sec) | {_fmt(mem, "recovery", "recovery_rate_eps")} | {_fmt(doc, "recovery", "recovery_rate_eps")} |
| Crash recovery | catch-up duration (s) | {_fmt(mem, "recovery", "recovery_seconds")} | {_fmt(doc, "recovery", "recovery_seconds")} |
| Enforcement drift | arithmetic drift (%) | {_fmt(mem, "drift", "percentage_drift")} | {_fmt(doc, "drift", "percentage_drift")} |
| Enforcement drift | drift from pipeline lag (%) | {_fmt(mem, "drift", "lag_percentage_drift")} | {_fmt(doc, "drift", "lag_percentage_drift")} |
| Reconciliation | ungated class | {_fmt(mem, "gate", "ungated_divergence_class")} | {_fmt(doc, "gate", "ungated_divergence_class")} |
| Reconciliation | gated, unsettled | {_fmt(mem, "gate", "gated_unsettled_class")} | {_fmt(doc, "gate", "gated_unsettled_class")} |
| Reconciliation | gated, settled | {_fmt(mem, "gate", "gated_settled_divergence_class")} | {_fmt(doc, "gate", "gated_settled_divergence_class")} |

---

## Reading these numbers

1. **The quota-check gap is the honest version of the <5 ms p99 claim in
   [ADR-0002](../../docs/adr/0002-enforcement-is-approximate-billing-is-exact.md).** The
   in-memory column is a dict lookup and proves nothing about the SLO. The
   container column includes a Redis round trip and is the number to argue with.

2. **Arithmetic drift is zero, and that is a measured result rather than an
   assumption.** The fast path increments with Redis `INCRBYFLOAT` and the
   billing path accumulates `Decimal`, so repeated increments of `0.1` were
   expected to diverge. They do not: Redis accumulates in `long double` and
   10,000 increments return exactly `1000`. The approximation ADR-0002 accepts
   on the fast path does not cost precision at this scale.

   **Drift from pipeline lag is the real number.** The counter reflects every
   admitted event; the exact aggregate reflects only sealed windows. With three
   fifths of the events sealed, enforcement sees 40% more usage than billing
   does -- correctly, because enforcement must not under-count while a backlog
   drains. This is the bound that matters, and it is a property of lag, not of
   floating point.

3. **Crash recovery is a correctness result, not a throughput result.** The
   scenario asserts that a replacement worker resuming from the committed
   checkpoint reconstructs the same totals. The rate is incidental; the
   invariant is the point ([ADR-0011](../../docs/adr/0011-atomic-offset-and-state-checkpointing.md)).

4. **The reconciliation row is the thesis.** Ungated, an unsettled window reports
   a divergence that does not exist. Gated, the same window is `UNSETTLED` and no
   alarm fires; once the pipeline settles, the class is `MATCH`
   ([ADR-0007](../../docs/adr/0007-reconciliation-with-completion-gate.md)).
"""


def run_all_benchmarks(include_docker: bool = True) -> None:
    """Run every scenario against every available backend and write the report."""
    console.print(Panel("[bold cyan]Tally Systems Performance Benchmarks[/bold cyan]"))
    hw = get_hardware_info()
    console.print(
        f"[dim]{hw['os']} ({hw['machine']}) · {hw['cpu_count']} cores · "
        f"{hw['total_ram']} RAM · Python {hw['python']}[/dim]\n"
    )

    results_dir = Path("bench/results")
    results_dir.mkdir(parents=True, exist_ok=True)
    report_file = results_dir / "results.md"

    async def _execute() -> None:
        console.print("[bold]Backend: in-memory (upper bound, no I/O)[/bold]")
        mem = await _run_backend(MEMORY)

        doc: dict[str, dict[str, Any]] | None = None
        if include_docker:
            console.print(
                "\n[bold]Backend: containers (Redpanda/Postgres/Redis)[/bold]"
            )
            try:
                doc = await _run_backend(DOCKER)
            except Exception as exc:
                # Reported, never swallowed: a missing container column must be
                # visible in the output rather than inferred from its absence.
                console.print(
                    f"[red]Container backend unavailable: {type(exc).__name__}: {exc}[/red]"
                )
                console.print(
                    "[yellow]Start it with `docker compose up -d` and re-run.[/yellow]"
                )

        console.print()
        console.print(_console_table(mem, doc))
        report_file.write_text(_markdown_report(hw, mem, doc))
        console.print(f"\n[green]✓ Report written to {report_file}[/green]")
        if doc is None:
            console.print(
                "[yellow]⚠ Container column missing — report is labelled accordingly.[/yellow]"
            )

    asyncio.run(_execute())


def run_single_scenario(name: str, backend_kind: BackendKind = MEMORY) -> None:
    """Run one scenario against one backend."""
    if name not in SCENARIO_ORDER:
        console.print(f"[red]Unknown scenario: {name}[/red]")
        console.print(f"[dim]Available: {', '.join(SCENARIO_ORDER)}[/dim]")
        return

    async def _run() -> None:
        run_id = new_run_id()
        async with open_backend(backend_kind) as backend:
            if name == "ingest":
                console.print(await bench_sustained_ingest(backend, run_id))
            elif name == "quota":
                console.print(await bench_quota_check_latency(backend, run_id))
            elif name == "recovery":
                console.print(await bench_crash_recovery(backend, run_id))
            elif name == "drift":
                console.print(await bench_enforcement_drift(backend, run_id))
            elif name == "gate":
                console.print(
                    await bench_reconciliation_gated_vs_ungated(backend, run_id)
                )

    asyncio.run(_run())
