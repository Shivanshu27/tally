"""In-memory billing sink for hermetic tests and benchmark runs."""

from __future__ import annotations

from typing import Any

from tally.domain.ports import BillingSink


class MemoryBillingSink(BillingSink):
    """In-memory billing sink enforcing idempotency on aggregate_key."""

    def __init__(self) -> None:
        # aggregate_key -> list of payload versions delivered
        self.deliveries: dict[str, list[dict[str, Any]]] = {}
        self.fail_mode: bool = False

    async def deliver(self, aggregate_key: str, payload: dict[str, Any]) -> bool:
        if self.fail_mode:
            return False

        if aggregate_key not in self.deliveries:
            self.deliveries[aggregate_key] = []
        self.deliveries[aggregate_key].append(payload)
        return True

    def get_delivery_count(self, aggregate_key: str) -> int:
        return len(self.deliveries.get(aggregate_key, []))

    def reset(self) -> None:
        self.deliveries.clear()
        self.fail_mode = False
