"""Typer command-line interface for Tally.

Commands:
- tally serve: Run the FastAPI ingest & query API
- tally worker: Run the stream aggregation consumer worker
- tally relay: Run the transactional outbox relay worker
- tally seed: Seed demo tenants, plans, and quotas
- tally reconcile: Execute completion-gated reconciliation with --explain
- tally bench: Run benchmark scenarios (ingest, quota, recovery, drift, gate)
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Annotated

import typer
import uvicorn
from rich.console import Console
from rich.table import Table

from tally.adapters.log.memory import MemoryEventLog
from tally.adapters.store.memory import MemoryStore
from tally.app.reconcile import ReconciliationEngine
from tally.domain.models import Tenant

app = typer.Typer(
    name="tally",
    help="Multi-tenant usage metering and quota platform.",
    add_completion=False,
)
console = Console()


@app.command()
def serve(
    host: Annotated[str, typer.Option(help="Host to bind")] = "0.0.0.0",
    port: Annotated[int, typer.Option(help="Port to bind")] = 8080,
    reload: Annotated[bool, typer.Option(help="Enable hot reload")] = False,
) -> None:
    """Start the Tally API server."""
    console.print(
        f"[bold green]Starting Tally Ingest API on {host}:{port}[/bold green]"
    )
    uvicorn.run("tally.api.app:app", host=host, port=port, reload=reload)


@app.command()
def seed(
    tenants: Annotated[int, typer.Option(help="Number of demo tenants to seed")] = 5,
) -> None:
    """Seed demo tenants and quotas into the system."""

    async def _run_seed() -> None:
        store = MemoryStore()
        plans = ["free", "pro", "enterprise"]
        limits = [Decimal("1000"), Decimal("50000"), Decimal("1000000")]

        table = Table(title="Seeded Tenants & Quotas")
        table.add_column("Tenant ID", style="cyan")
        table.add_column("Plan", style="magenta")
        table.add_column("API Calls Quota", style="green")

        for i in range(1, tenants + 1):
            t_id = f"tenant_{i:03d}"
            plan = plans[(i - 1) % len(plans)]
            limit = limits[(i - 1) % len(limits)]
            tenant = Tenant(
                tenant_id=t_id,
                plan=plan,
                quotas={"api_calls": limit, "bytes_processed": limit * 1024},
            )
            await store.save_tenant(tenant)
            table.add_row(t_id, plan, f"{limit:,}")

        console.print(table)
        console.print("[green]✓ Seed completed successfully[/green]")

    asyncio.run(_run_seed())


@app.command()
def reconcile(
    tenant: Annotated[str, typer.Option(help="Tenant ID to audit")] = "tenant_001",
    metric: Annotated[str, typer.Option(help="Metric name")] = "api_calls",
    explain: Annotated[
        bool, typer.Option(help="Print evidence for divergences")
    ] = True,
    gate: Annotated[bool, typer.Option(help="Enforce completion gate")] = True,
) -> None:
    """Run reconciliation on an event window."""

    async def _run() -> None:
        store = MemoryStore()
        event_log = MemoryEventLog()
        reconciler = ReconciliationEngine(
            aggregate_store=store,
            late_store=store,
            outbox_store=store,
            consumer=event_log,
        )

        now = datetime.now(UTC)
        w_start = now - timedelta(minutes=5)
        w_end = now - timedelta(minutes=4)

        result = await reconciler.reconcile_window(
            tenant_id=tenant,
            metric=metric,
            window_start=w_start,
            window_end=w_end,
            current_watermark=now,
            enforce_gate=gate,
            raw_events=[Decimal("10"), Decimal("25"), Decimal("15")],
        )

        table = Table(title=f"Reconciliation Audit: {tenant} ({metric})")
        table.add_column("Property", style="cyan")
        table.add_column("Value", style="yellow")

        table.add_row("Window Range", f"{w_start.isoformat()} -> {w_end.isoformat()}")
        table.add_row("Recomputed Total", f"{result.recomputed_quantity}")
        table.add_row("Served Total", f"{result.served_quantity}")
        table.add_row(
            "Divergence Class", f"[bold]{result.divergence_class.value}[/bold]"
        )
        table.add_row("Difference", f"{result.difference}")

        console.print(table)

        if explain and result.evidence:
            console.print("\n[bold]Evidence Trail:[/bold]")
            for ev in result.evidence:
                console.print(f"  • {ev}")

    asyncio.run(_run())


@app.command()
def bench(
    scenario: Annotated[
        str,
        typer.Option(
            help="Scenario to run: 'ingest', 'quota', 'drift', 'gate', 'recovery', or 'all'"
        ),
    ] = "all",
    backend: Annotated[
        str,
        typer.Option(
            help=(
                "'memory' (in-process fakes, no I/O) or 'docker' (Redpanda, "
                "Postgres and Redis from docker-compose). 'all' runs both and "
                "reports them side by side."
            )
        ),
    ] = "all",
) -> None:
    import sys
    from pathlib import Path

    if str(Path.cwd()) not in sys.path:
        sys.path.insert(0, str(Path.cwd()))

    from bench.runner import run_all_benchmarks, run_single_scenario

    if backend not in ("all", "memory", "docker"):
        typer.echo(f"Unknown backend: {backend}", err=True)
        raise typer.Exit(code=2)

    if scenario == "all":
        run_all_benchmarks(include_docker=backend in ("all", "docker"))
    else:
        run_single_scenario(scenario, "docker" if backend == "docker" else "memory")


if __name__ == "__main__":
    app()
