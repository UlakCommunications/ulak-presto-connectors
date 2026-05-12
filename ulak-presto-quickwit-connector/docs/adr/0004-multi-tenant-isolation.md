# ADR 0004 — Multi-tenant query isolation

**Date:** 2026-05-12  
**Status:** Accepted  
**Deciders:** Fatih Yuce

---

## Context (J52)

Trino runs as a single instance serving multiple tenants. Each tenant has their
own Quickwit cluster. The question: can tenant A's Grafana reach tenant B's
Quickwit data via the shared Trino?

## Threat model

| Vector | Description | Status |
|--------|-------------|--------|
| **URL escape** | `//qwurl=http://tenant-b-host/` embedded in TVF arg | **Mitigated** — R02: `validateQwUrl()` enforces catalog allowlist; defaults to catalog-configured URL only |
| **Catalog cross-access** | Tenant A user queries `SELECT … FROM tenant_b_catalog.…` | **Not mitigated at connector level** — requires Trino system access control |
| **Redis cache key collision** | SHA-256 hash of normalized query used as key (R28) | **Mitigated** — 256-bit key; collision negligible |
| **Cache poisoning** | Tenant A crafts a query whose SHA-256 matches tenant B's cached result | **Negligible** — SHA-256 preimage resistance; keys also include the full qwUrl |
| **Index traversal via qwindex** | `//qwindex=tenant-b-index` in TVF arg | **Partially mitigated** — `qwurl` is locked; `qwindex` override still allowed. Add `qw-allowed-indexes` property if needed (see below) |
| **Trino query log** | Admin can read all queries from all tenants in Trino UI | **Out of scope** — Trino-level concern; restrict Trino UI access |

## Decision

### Already done (connector-level)
- **R02** `qw-allowed-urls`: URL override locked to catalog config by default. Tenant A cannot reach tenant B's Quickwit URL.
- **R28** SHA-256 cache key: collision-resistant Redis keys.

### Recommended (infra-level, not connector code)

1. **Trino system access control** (`etc/access-control.properties`):
   ```properties
   access-control.name=file
   security.config-file=etc/rules.json
   ```
   Add per-user or per-group catalog restrictions in `rules.json` so
   tenant A users can only query their own catalog.

2. **Per-tenant Trino instances** (strongest isolation): run a separate
   Trino pod per tenant namespace. Eliminates all cross-catalog risk.

### Optional future connector enhancement

Add `qw-allowed-indexes` catalog property (comma-separated or `*`) to
restrict which Quickwit indexes can be queried via `//qwindex=` override,
mirroring the `qw-allowed-urls` pattern from R02. Track as J57b.

## Current deployment model

"Tenant = cluster" boundary is enforced at the Quickwit layer: each
tenant has a separate Quickwit instance with its own network address.
The Trino catalog `qw-connection-url` points to that instance. With R02
in place, `//qwurl=` overrides are blocked, so tenant A's Trino queries
physically cannot reach tenant B's network endpoint unless:
- They share the same `qw-connection-url` (single-cluster multi-index
  deployment), in which case `//qwindex=` isolation is the concern.
- A Trino catalog misconfiguration points to the wrong Quickwit instance.

## Consequences

- R02 is the primary connector-level isolation control — keep it.
- Trino access control (rules.json) is required for catalog-level isolation.
- Until rules.json is configured, any Trino user can query any catalog.
- Per-tenant Trino instances eliminate the shared-catalog risk entirely
  at the cost of higher resource usage.
