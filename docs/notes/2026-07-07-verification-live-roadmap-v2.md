# Compte rendu — Roadmap v2 : vérification live en attente

**Date** : 2026-07-07 · **Statut** : à faire — reprendre ici à la prochaine session.
*(Note de travail interne, en français volontairement. À supprimer une fois la
vérification faite et les captures intégrées au README.)*

## Contexte

La roadmap v2 (piliers 4–9 + image Spark prébakée) est **mergée sur `main`**
(PR #1, 13 commits), la CI/CD est verte de bout en bout, les Data Docs GX + dbt
sont publiées sur Pages, le README est à jour (sections descriptives + métriques).

## Le constat (vérifié sur le cluster le 2026-07-07)

**La v2 est code-complete mais n'a jamais tourné sur le k3s.** Le repo décrit un
état que la machine ne réalise pas encore :

| Vérification | Résultat |
|---|---|
| `kubectl get pods -n orchestration` | vide — **aucun pod Airflow**, le DAG n'a jamais tourné |
| `kubectl get pods -n argocd` | vide — **ArgoCD pas installé** (pas même les CRDs) |
| Dernier Job `quality-gate` | antérieur au merge (v1) |
| Graphe Marquez | sans les arêtes dbt ni l'arête exchange-rates |
| Captures README | une seule (Streamlit v1) — rien sur les nouveautés |

Conséquence : les critères « Done when » de la roadmap sont **non vérifiés** —
personne n'a vu un `lakehouse_batch` passer de bout en bout, un gate rouge rendre
le DAG rouge, ni un merge sur `main` rouler les pods sans `kubectl apply`.

## La boucle de clôture (dans l'ordre)

1. **Déployer** : `./scripts/deploy.sh` (Airflow + ConfigMaps dags/dbt), puis
   `./scripts/install-argocd.sh` et
   `kubectl apply -f infra/argocd/project.yaml -f infra/argocd/apps/lakehouse-local.yaml`.
2. **Trafic** : `make demo` + le loader
   `python3 pipelines/ingestion/exchange_rates_loader.py` (port-forward du broker).
3. **DAG end-to-end** : déclencher `lakehouse_batch` (UI via
   `kubectl port-forward svc/airflow 8081:8080 -n orchestration`, ou
   `kubectl exec -n orchestration deploy/airflow -- airflow dags trigger lakehouse_batch`).
   Puis **casser volontairement une expectation** et re-déclencher : le gate doit
   rendre le DAG rouge — c'est le test le plus démonstratif du projet.
4. **Preuve GitOps** : merger un bump d'image sur `main` et constater que les pods
   roulent sans intervention (auto-sync ArgoCD, polling ~3 min).
5. **Captures → README** : run DAG vert/rouge, graphe Marquez complet
   (roadmap 3.5), dashboard Grafana « Lakehouse Pipeline » (roadmap 2.5),
   dashboard Superset + export JSON vers `observability/superset/` (pilier 9).

## Pièges connus

- **Opérateurs déjà installés par scripts** : `terraform import` requis avant que
  `infra/terraform/local` les gère (sinon doublon) — voir `infra/terraform/README.md`.
- **Fichiers env déplacés** : les credentials réels sont désormais dans
  `infra/kubernetes/overlays/local/secrets/` (bootstrap/deploy pointent déjà dessus).
- **Repo privé = casse** : passer en privé supprime le site Pages (recréer via
  `gh api -X POST .../pages -f build_type=workflow`) et exigera des credentials
  ArgoCD + éventuel imagePullSecret GHCR. Basculer `ENABLE_PAGES=false` d'abord.
- **RAM** : Airflow (1,25 Gi) + ArgoCD core (~0,5 Gi) arrivent en plus — surveiller
  les quotas au premier deploy (`kubectl describe quota -A`).

## Critère de fin

Les cases restantes de `docs/roadmap.md` (2.5, 3.5, dashboard Superset) cochées,
et le README montrant les preuves visuelles. Cette note peut alors être supprimée.
