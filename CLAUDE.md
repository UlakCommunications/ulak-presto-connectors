# CLAUDE.md — Presto InfluxDB / Quickwit Connector

## Build and Test Commands
- **Compile and Package Connector:** `./mvnw clean install` (run from root)
- **Run Unit Tests (Quickwit only):** `./mvnw clean test -pl ulak-presto-quickwit-connector`
- **Build and Push Docker Image (Nexus/Maya):** `./push.sh 0.0.1-develop-latest linux/amd64` (pushes to `192.168.57.202:35000/maya/trino:0.0.1-develop-latest`)

## K8s Deployment and Tailing Commands (yucemonitoring)
- **Rollout Restart Trino Coordinator:**
  `KUBECONFIG=/home/fatihyuce/.kube/yucemonitoring-direct.config kubectl rollout restart deployment/maya-trino-multi-coordinator -n yucemonitoring`
- **Check Trino Pod Status:**
  `KUBECONFIG=/home/fatihyuce/.kube/yucemonitoring-direct.config kubectl get pods -n yucemonitoring | grep trino`
- **Tail Trino Coordinator Logs:**
  `KUBECONFIG=/home/fatihyuce/.kube/yucemonitoring-direct.config kubectl logs -n yucemonitoring -l app=maya-trino,component=coordinator --tail=100 -f`

## K8s Deployment and Tailing Commands (OGM Demo)
- **Rollout Restart Trino Coordinator:**
  `KUBECONFIG=/home/fatihyuce/.kube/config-ogm-demo kubectl rollout restart deployment/maya-trino-coordinator -n maya3`
- **Check Trino Pod Status:**
  `KUBECONFIG=/home/fatihyuce/.kube/config-ogm-demo kubectl get pods -n maya3 | grep trino`
- **Tail Trino Coordinator Logs:**
  `KUBECONFIG=/home/fatihyuce/.kube/config-ogm-demo kubectl logs -n maya3 -l app=trino,component=coordinator --tail=100 -f`

## Database Access Commands
- **Connect to Grafana PostgreSQL Settings DB:**
  `KUBECONFIG=/home/fatihyuce/.kube/yucemonitoring-direct.config kubectl exec -it -n yucemonitoring maya-postgres-1-0 -- env PGPASSWORD=5iE!16hEB1 psql -U postgres -d grafana`

## Verification Scripts
- Scratch scripts are stored in `/home/fatihyuce/.gemini/antigravity/brain/4837bf22-a4cb-4684-8f23-011b35ab0895/scratch/`
  - `wait_for_trino_pod.py`: Monitors K8s Trino coordinator readiness.
  - `test_exec_next_uri.py`: Runs end-to-end Trino queries on history indices using curl via Trino statement HTTP endpoints.
  - `query_trino_b32_clean_math.py`: Tests Base32 encoded queries with standard `Math` library.
