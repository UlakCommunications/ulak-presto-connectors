# CLAUDE.md — Presto Quickwit Connector

## Build & Deploy Commands
- **Quickwit Unit Tests:** `./mvnw test -pl ulak-presto-quickwit-connector`
- **Build & Push Image:** `./push.sh 0.0.1-develop-latest linux/amd64` (targets Nexus registry `192.168.57.205:35000`)
- **Restart Trino Coordinator:**
  `kubectl --kubeconfig /tmp/vc-yucemonitoring.config -n yucemonitoring rollout restart deployment/maya-trino-multi-coordinator`
- **Tail Coordinator Logs:**
  `kubectl --kubeconfig /tmp/vc-yucemonitoring.config -n yucemonitoring logs -l app=trino,component=coordinator --tail=100 -f`

## Database Access
- **Connect to Grafana PostgreSQL:**
  `kubectl exec -it -n yucemonitoring maya-postgres-1-0 -- env PGPASSWORD=5iE!16hEB1 psql -U postgres -d grafana`

## Verification Scripts
- Scratch scripts stored in `~/.gemini/antigravity/brain/2f1cfc13-5ae0-4d8b-9ccf-79492e00664a/scratch/`:
  - `print_qos_history_rows.py`: Query wrapper executing live QoS queries against Trino with history enabled.
  - `fix_qos_dashboard.py`: Fixes typos (`columns1` -> `columns`) on QoS panels while preserving folder settings.

## READY FOR HANDOVER (Thu Jul 30 18:40:00 +03 2026)
Successfully resolved historical QoS data querying and empty dashboard crashes. Updated rollup task configurations in `tasks.json` to include `m_site_id`, `m_name`, and `m_site_name` dimensions for `netlink_${ROLLUP_INTERVAL}m` metric tasks, restarted `qw-rollup-engine`, and backfilled rollup documents from today 00:00. Rewrote the Java query routing logic in the Presto Quickwit connector to parse a dynamic SQL comment header (`//nohistorysuffix=...`) to exclude specific fields from suffix renaming, avoiding hardcoded metrics inside the Java source code. Tested, built, pushed, and restarted the Trino coordinator with the updated connector.
