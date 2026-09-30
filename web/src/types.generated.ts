/**
 * AUTO-GENERATED FILE - DO NOT EDIT MANUALLY.
 * Generated from FastAPI OpenAPI 3.1 schema by scripts/generate_types.py.
 * Implements requirement: 'Types generated from the OpenAPI schema, not hand-written.'
 */

/** Batch ingestion request payload. */
export interface BatchEventsRequest {
  /** Batch of 1 to 1000 usage events */
  events: UsageEvent[];
}

/** Batch ingestion response payload. */
export interface BatchIngestResponse {
  accepted_count: number;
  rejected_count: number;
  results: Record<string, EventIngestOutcome>;
}

/** Overall pipeline health, buffer depth, and stream metrics. */
export interface DashboardOverview {
  status: string;
  env: string;
  timestamp: string;
  buffer_depth: number;
  high_watermark: number;
  shed_watermark: number;
  shed_count: number;
  bloom_count: number;
  outbox_pending: number;
  partitions: DashboardPartitionLag[];
}

/** Consumer lag and watermark latency per partition. */
export interface DashboardPartitionLag {
  partition: number;
  lag: number;
  watermark_lag_ms?: number;
}

/** Classification of divergence between independent replay and served aggregates.

``MATCH`` exists as a distinct value rather than being folded into ``REAL``
with a zero difference: agreement is the expected outcome and a reader
scanning reconciliation output must be able to tell success from failure by
the class alone, without also inspecting the magnitude of the difference.

There is deliberately no ``IN_FLIGHT`` value. Events inside the watermark
grace buffer are one of the three conditions the completion gate checks, and
a window failing any of them is ``UNSETTLED`` -- which condition failed is
carried in the record's evidence. A separate class would have split one
concept ("we refuse to compare yet") across two values with no caller able
to act differently on them. */
export type DivergenceClass = 'MATCH' | 'REAL' | 'UNSETTLED' | 'LATE_ARRIVAL';

/** Per-event outcome in a batch ingestion response (keyed by event_id). */
export interface EventIngestOutcome {
  event_id: string;
  accepted: boolean;
  reason_code: ReasonCode;
  message?: string;
}

export interface HTTPValidationError {
  detail?: ValidationError[];
}

/** Transactional outbox message to publish aggregates to external sinks. */
export interface OutboxMessage {
  id?: number | null;
  aggregate_key: string;
  payload: Record<string, any>;
  created_at?: string;
  published_at?: string | null;
  attempts?: number;
}

/** Result of an approximate hot-path quota check. */
export interface QuotaCheckResult {
  tenant_id: string;
  metric: string;
  allowed: boolean;
  current_usage: string;
  limit: string;
  as_of: string;
  approximate?: boolean;
  reason_code?: ReasonCode;
}

/** Machine-readable reason codes for all pipeline decisions. */
export type ReasonCode = 'OK' | 'QUOTA_EXCEEDED' | 'BUFFER_FULL' | 'LOW_PRIORITY_SHED' | 'INVALID_SCHEMA' | 'DUPLICATE_EVENT' | 'UNSETTLED_PIPELINE';

/** Reconciliation audit request. */
export interface ReconcileRequest {
  tenant_id: string;
  metric: string;
  window_start: string;
  window_end: string;
  enforce_gate?: boolean;
}

/** Comparison record between independent replay and served aggregates. */
export interface ReconciliationRecord {
  window_start: string;
  window_end: string;
  tenant_id: string;
  metric: string;
  recomputed_quantity: string;
  served_quantity: string;
  divergence_class: DivergenceClass;
  difference: string;
  evidence?: string[];
}

/** Tenant summary showing both approximate (Redis) and exact (Postgres) usage. */
export interface TenantOverview {
  tenant_id: string;
  plan: string;
  quotas: Record<string, string>;
  approximate_usage: Record<string, string>;
  exact_usage: Record<string, string>;
  drift_percent: Record<string, number>;
  created_at: string;
}

/** The wire and domain contract for a single usage measurement.

Quantity is strictly Decimal to avoid float representation errors.
Both occurred_at (event time) and received_at (processing time) are mandatory. */
export interface UsageEvent {
  /** Client-supplied UUID - the idempotency key */
  event_id: string;
  /** Tenant identifier for tenancy and partitioning */
  tenant_id: string;
  /** Metric name e.g. api_calls, bytes_processed */
  metric: string;
  /** Usage quantity - strictly Decimal, never float */
  quantity: number | string;
  /** Event time in UTC, when usage physically happened */
  occurred_at: string;
  /** Processing time in UTC, stamped at the ingest API */
  received_at: string;
  /** Optional secondary idempotency key */
  idempotency_key?: string | null;
  /** Priority for load shedding: 'high' (billable) or 'low' (telemetry) */
  priority?: string;
  /** Custom event metadata */
  metadata?: Record<string, string>;
}

export interface ValidationError {
  loc: (string | number)[];
  msg: string;
  type: string;
  input?: any;
  ctx?: Record<string, any>;
}

/** Immutable aggregate bucket representing usage in a tumbling window.

Append-only: corrections for late arrivals are recorded as a new revision row. */
export interface WindowAggregate {
  tenant_id: string;
  metric: string;
  window_start: string;
  window_end: string;
  quantity: string;
  event_count: number;
  revision?: number;
  sealed_at?: string | null;
  computed_at?: string;
}

/** Tumbling window representation and current lifecycle status. */
export interface WindowOverview {
  tenant_id: string;
  metric: string;
  window_start: string;
  window_end: string;
  quantity: string;
  event_count: number;
  revision: number;
  sealed_at: string | null;
  computed_at: string;
  status: string;
}
