# Superset — BI serving on the Gold layer

Superset runs in the Docker Compose dev stack under the opt-in `bi` profile
(~1.5 Gi on the Docker side — deliberately outside the k3s RAM budget, see
roadmap pillar 9):

```bash
# 1. Expose the in-cluster Trino to the Docker host (from WSL)
kubectl port-forward svc/trino 8080:8080 -n lakehouse --address 0.0.0.0 &

# 2. Start Superset (first boot takes ~1 min: db upgrade + init)
docker compose -f docker-compose.dev.yml --profile bi up -d superset

# 3. Open http://localhost:8088  (admin / $SUPERSET_ADMIN_PASSWORD, default: admin)
```

The `Trino-Iceberg` database connection
(`trino://admin@host.docker.internal:8080/iceberg`) is pre-registered by the
container entrypoint — datasets can be added straight from
`iceberg.gold.daily_revenue` and `iceberg.gold.customer_metrics`.

## Dashboard

The "Lakehouse Gold" dashboard mirrors the Streamlit demo
(`demo/dashboard.py`): daily revenue trend, order counts, top customers by
lifetime value and churn split.

Exports are versioned in this directory (`lakehouse_gold_dashboard.zip`,
Superset native format). Round-trip:

```bash
# Export (after editing in the UI) — from the running container:
docker compose -f docker-compose.dev.yml --profile bi exec superset \
  superset export-dashboards -f /app/superset_home/lakehouse_gold_dashboard.zip
docker compose -f docker-compose.dev.yml --profile bi cp \
  superset:/app/superset_home/lakehouse_gold_dashboard.zip observability/superset/

# Import (fresh instance):
docker compose -f docker-compose.dev.yml --profile bi cp \
  observability/superset/lakehouse_gold_dashboard.zip superset:/tmp/
docker compose -f docker-compose.dev.yml --profile bi exec superset \
  superset import-dashboards -p /tmp/lakehouse_gold_dashboard.zip -u admin
```

The initial export is produced manually once traffic has populated the Gold
tables (same manual step as the Grafana capture in roadmap 2.5).

In-cluster deployment is out of scope until the RAM freed by the legacy
cleanup is re-budgeted — quota math first (roadmap pillar 9).
