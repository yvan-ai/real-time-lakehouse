# Compte rendu — Roadmap v2 : vérification live ✅ FAITE (2026-07-12)

**Statut** : la boucle de vérification est **terminée**. Il ne reste que les
captures d'écran (étape manuelle, voir « Reste à faire » en bas).
*(Note de travail interne, en français volontairement.)*

## Preuves obtenues le 2026-07-12

| Critère « Done when » | Preuve |
|---|---|
| DAG `lakehouse_batch` end-to-end | **Run vert complet** : batch_bronze_silver → dbt_build_gold → quality_gate → register_lineage (run `scheduled__2026-07-11`) |
| Gate rouge ⇒ DAG rouge | Déclenché **organiquement** : la rétention 24 h de `raw.events` avait vidé la lane EL → `bronze_kafka_events` 14/16 → gate failed → DAG failed. Après re-run du loader : gate 96/96 |
| Gold produit par dbt | 5 jours, 385 commandes, 218 180,92 de CA dans `gold.daily_revenue` ; 200 clients dans `customer_metrics` — suites GX gold 33/33 sur les tables dbt (parité Spark→dbt validée) |
| GitOps effectif | selfHeal a recréé un Deployment supprimé en ~20 s, et a réverté un ConfigMap modifié sans commit en ~3 min |
| Lineage complet | 22 jobs dans Marquez : Spark (bronze/silver/gold), **dbt par modèle** (build.run/test), Debezium, Flink, exchange-rates-loader |
| Persistance Nessie | Pod tué → tables intactes, fichiers RocksDB dans le PVC |

## Les 9 bugs débusqués (et corrigés) par le live

1. **Loader 403** — Cloudflare rejette le User-Agent urllib par défaut → UA custom.
2. **Projet ArgoCD sans destination `spark`** → toutes les syncs invalides.
3. **Contrôleur ArgoCD affamé** (LimitRange 200m + quota requests.cpu 250m)
   → sync infinie ; ressources explicites + quota 500m/1500m.
4. **Catalogue Nessie perdu à chaque restart** — 0.62 écrit RocksDB en dur dans
   `/tmp/nessie-rocksdb-store`, le PVC était monté ailleurs → subPath sur le
   chemin réel (vérifié par `docker diff`).
5. **KPO startup_timeout 300 s** < premier pull d'image → 1800 s.
6. **curl sans reprise sur transfert partiel** (l'egress WSL tronque le bundle
   AWS de 280 Mo) → boucle de reprise `-C -` dans le Dockerfile.
7. **Le nœud ne peut pas puller la couche de 750 Mo depuis GHCR** → build local
   + import containerd ; sudo sans mot de passe indisponible → import via pod
   `kubectl debug node/` privilégié.
8. **Walker Airflow en boucle récursive** sur le montage ConfigMap (`..data`)
   → zéro DAG parsé → `.airflowignore` embarqué dans le ConfigMap.
9. **openlineage-dbt 1.24 sans adapter trino** (`NotImplementedError`) → 1.51.

## Leçons d'exploitation (runbook)

- **Reboot WSL** ⇒ pods `Unknown`/CrashLoop : force-delete des pods périmés,
  reset des backoffs ; le StrimziPodSet peut rester bloqué sur un état stale
  (supprimer le SPS, l'opérateur le recrée). Les ClusterRoles Strimzi avaient
  été supprimés par le nettoyage legacy → `install-strimzi.sh` les restaure.
- **Kafka emptyDir** : survit à un restart de conteneur in-place, pas à une
  recréation de pod. `raw.events` a une rétention de 24 h : relancer le loader
  avant une démo si les événements ont expiré (le gate le rappellera).
- **ConfigMaps montés** : ~60–90 s de propagation kubelet ; et surtout, avec
  selfHeal actif, **un apply non commité est réverté par ArgoCD** — commiter
  puis appliquer, jamais l'inverse.
- **CLI Airflow via kubectl exec** : exporter
  `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` (construite dans la commande du
  conteneur, absente du spec) sinon la CLI retombe sur sqlite.

## Reste à faire (manuel, avec le stack encore chaud)

Captures pour le README (roadmap 2.5 / 3.5 / pilier 9) :
- Run DAG vert : `kubectl port-forward svc/airflow 8081:8080 -n orchestration`
  → http://localhost:8081 (admin / voir `secrets/airflow.env`)
- Graphe Marquez : `kubectl port-forward svc/marquez-web 3000:3000 -n lineage`
  → http://localhost:3000 (22 jobs, namespace lakehouse)
- Grafana « Lakehouse Pipeline » : `kubectl port-forward svc/grafana 3001:3000 -n monitoring`
- Superset : `docker compose -f docker-compose.dev.yml --profile bi up -d superset`
  + port-forward Trino `--address 0.0.0.0` → dashboard + export JSON vers
  `observability/superset/`.

Cette note peut être supprimée une fois les captures intégrées au README.
