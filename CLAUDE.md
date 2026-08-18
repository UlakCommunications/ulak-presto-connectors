# CLAUDE.md — Presto / Trino Quickwit Connector

## Overview
High-performance Trino connector for Quickwit and OpenSearch/Elasticsearch with automated rollup routing, query rewriting, and multi-catalog support.

## Key Commands
- **Unit Tests:** `./mvnw clean test` (or `./mvnw test -pl ulak-presto-quickwit-connector`)
- **Docker Build & Push:** `./push.sh <version> <platform>` (e.g. `./push.sh 0.0.1-develop-latest linux/amd64`)
- **Restart Coordinator:** `kubectl --kubeconfig ~/.kube/yucemonitoring.config -n yucemonitoring rollout restart deployment/maya-trino-multi-coordinator`
- **Trino CLI:** `kubectl --kubeconfig ~/.kube/yucemonitoring.config -n yucemonitoring exec -it deployment/maya-trino-multi-coordinator -- trino`
- **Postgres (Grafana/Tenant):** `kubectl --kubeconfig ~/.kube/yucemonitoring.config -n yucemonitoring exec -it deployment/maya-postgres-1 -- env PGPASSWORD=5iE!16hEB1 psql -U postgres -d grafana`

## Architecture & Configuration
- **History Time Threshold:** Configurable via `history-time-threshold-seconds` in `quickwit.properties` (defaults to `10800` / 3 hours). Queries with time range > threshold route to the configured `historyindex` (e.g. `metrics3_15`).
- **History Query Rewriting:** `QwQueryRewriter.java` standardizes aggregation field rewriting (`field_<agg>`). Handled for both standard rollup indexes and `_sum` suffix flow schemas.
- **Rhino Javascript Engine:** Query evaluation executes with native `Math` support via scope injection (`var math = Math;`).
- **Buildx Driver:** `push.sh` targets the shared BuildKit container builder `mybuilder` for insecure registry support.

## READY FOR HANDOVER (Tue Aug 18 18:50:00 +03 2026)
Successfully integrated and merged catalog-configured `history-time-threshold-seconds` (MR !33), enabling dynamic 3-hour history threshold routing without hardcoded constants. Updated Helm charts across all Trino deployments. Diagnosed and resolved the Overlay Dashboard destination peer matching issue by rewriting `data_gen` top-level topology generators to emit matched Hub-Spoke WireGuard endpoints and WAN IPs. Fixed `push.sh` Buildx container builder setup and verified all unit tests and builds. All changes are committed and pushed to remote branches.
