"""Store adapters."""

from tally.adapters.store.memory import MemoryStore
from tally.adapters.store.postgres import PostgresStore

__all__ = ["MemoryStore", "PostgresStore"]
