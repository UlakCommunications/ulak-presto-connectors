# TODO.md — Next Steps and Open Actions

## Current State
- The Quickwit connector has been updated to spare flow metric fields (`u`, `ac`, `ab`, `t`, `u_ac`, `t_ab`) and default value fields (`value`) from history suffix renaming.
- Pre presto connectors have been successfully built, pushed to local Nexus registry, and deployed via rollout-restart on the Trino coordinator.
- End-to-end local test validation queries for all resource utilization panels (CPU, RAM, DISK, etc.) succeeded on the new pod.

## Next Steps for the Next Session
1. **User/Grafana Dashboard Verification**:
   - Ask the user to verify that BOTH the **Traffic Statistics** and **Hub Resource Utilization** dashboards load successfully in Grafana.
   - Verify that toggling the time range (e.g., last 15 mins vs last 6 hours) routes queries dynamically between live and history indexes, and renders graphs correctly without null/zero values or casting errors.

2. **Monitor Logs**:
   - Monitor Trino coordinator logs for any unexpected query rewriter failures:
     `kubectl --kubeconfig /home/fatihyuce/.kube/yucemonitoring.config --insecure-skip-tls-verify -n yucemonitoring logs -l app=trino,component=coordinator --tail=100 -f`
