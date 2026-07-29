# CLAUDE.md — Presto Quickwit Connector

## Build & Deploy Commands
- **Compile & Package:** `./mvnw clean install -DskipTests` (run from root)
- **Quickwit Unit Tests:** `./mvnw test -pl ulak-presto-quickwit-connector`
- **Build & Push Image:** `./push.sh 0.0.1-develop-latest linux/amd64` (pushes to `192.168.57.202:35000/maya/trino:0.0.1-develop-latest`)
- **Rollout Restart Trino Coordinator:**
  `kubectl --kubeconfig /home/fatihyuce/.kube/yucemonitoring.config --insecure-skip-tls-verify -n yucemonitoring rollout restart deployment/maya-trino-multi-coordinator`
- **Tail Coordinator Logs:**
  `kubectl --kubeconfig /home/fatihyuce/.kube/yucemonitoring.config --insecure-skip-tls-verify -n yucemonitoring logs -l app=trino,component=coordinator --tail=100 -f`

## Database Access
- **Connect to Grafana PostgreSQL:**
  `kubectl exec -it -n yucemonitoring maya-postgres-1-0 -- env PGPASSWORD=5iE!16hEB1 psql -U postgres -d grafana`

## Verification Scripts
- Scratch scripts stored in `~/.gemini/antigravity/brain/80e69544-7831-431d-8205-da65ca0f774c/scratch/`:
  - `compare_buckets.py`: Compares date histogram bucket aggregation values between `value` and `value_avg` indexes.
  - `test_all_panels.py`: Executes verification queries against all panels in raw and history modes.

## READY FOR HANDOVER (Wed Jul 29 15:02:00 +03 2026)
We successfully fixed history query failures on flow and resource utilization dashboards. We modified `QwQueryRewriter.java` to spare flow metric fields (`u`, `ac`, `ab`, `t`, `u_ac`, `t_ab`) and the default value field (`value`) from history suffix renaming. This ensures backward compatibility with older rolled-up documents where metric averages were stored directly in `value`. The connector was rebuilt, packaged, pushed to the Nexus registry, deployed, and verified to return complete history dataset ranges across all panels.
