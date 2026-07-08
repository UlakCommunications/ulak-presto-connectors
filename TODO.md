# TODO.md — Next Steps and Open Actions

## Current State
- The Quickwit connector is fully fixed. Suffix rewriting for history indices (`_min`, `_max`, `_avg`, `_sum`, `_count`) runs successfully on clean JSON without number serialization issues.
- Standard `Math` is now used in database view configurations in place of the `eval(unescape(...))` workaround.
- The new connector image (`0.0.1-develop-latest`) has been compiled, packaged, pushed, and Trino rollout-restarted.

## Next Steps for the Next Session
1. **User/Grafana Verification:**
   - Ask the user to verify the dashboards in Grafana to confirm they load successfully without empty panels or errors.
2. **Monitor Logs:**
   - Monitor Trino coordinator logs during user verification:
     `KUBECONFIG=/home/fatihyuce/.kube/yucemonitoring-direct.config kubectl logs -n yucemonitoring -l app=maya-trino,component=coordinator --tail=100 -f`
3. **Database Settings Check:**
   - In case any other views show syntax or runtime evaluation errors, check `public.maya_global_settings` table in PostgreSQL. A complete backup of the views prior to the `Math` conversion is saved at:
     `/home/fatihyuce/.gemini/antigravity/brain/4837bf22-a4cb-4684-8f23-011b35ab0895/scratch/original_views_backup.json`
     To restore original views, run:
     `python3 /home/fatihyuce/.gemini/antigravity/brain/4837bf22-a4cb-4684-8f23-011b35ab0895/scratch/restore_views_from_backup.py`

## Open Decisions / To Be Decided
- **PostgreSQL view cleanup:** Confirm if any views that do not use rollup (e.g. raw-only indices) need any performance tuning or index-matching checks.
