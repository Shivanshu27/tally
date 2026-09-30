# Build Session Log 001 — Foundation and Architecture Setup

**Date:** 2026-09-28  
**Engineer:** Shivanshu Singla  

### Goals
- Establish repository skeleton and configuration files.
- Document architectural design, PRD, and all 12 ADRs.
- Identify local environment constraints (active ports, host services).
- Prepare pure domain models, ports, and layer validation.

### Environmental Findings & Trap Avoidance
- **Port Collisions**: Investigated active Docker containers on the host machine. Found Postgres already running on port 5432 and Kafka on ports 9092-9093. 
  - *Resolution*: Mapped Tally defaults to 5439 (Postgres), 19092 (Redpanda), 6389 (Redis), 8080 (API).
- **FastAPI / Python 3.12 Annotations Trap**: Highlighted in `CLAUDE.md` to keep all `Annotated[..., Depends(...)]` at module scope so FastAPI runtime inspection never treats dependencies as query parameters.
- **Strict Decimal Rule**: Checked that `UsageEvent` and database schema specify `Decimal` / `NUMERIC` to avoid 64-bit IEEE float accumulation bugs.
