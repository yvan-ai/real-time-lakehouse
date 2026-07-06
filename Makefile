# ── Real-time lakehouse — developer entrypoints ──────────────────────────────
# `make help` lists all targets.

.PHONY: help setup lint format typecheck test validate quality quality-gate \
        bootstrap deploy batch iceberg-init dev-up dev-down demo dashboard clean

PYTHON ?= python3

help:            ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

setup:           ## Install dev + test dependencies
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements-test.txt ruff mypy pre-commit yamllint
	pre-commit install

lint:            ## Ruff lint + format check + yamllint
	ruff check .
	ruff format --check .
	yamllint -c .yamllint .

format:          ## Auto-format Python code
	ruff check --fix .
	ruff format .

typecheck:       ## Run mypy static type checks
	mypy

test:            ## Run unit tests with coverage
	$(PYTHON) -m pytest quality/tests -v --cov --cov-report=term-missing

validate:        ## Build + validate Kubernetes manifests (kustomize + kubeconform)
	kustomize build infra/kubernetes/overlays/local \
	  | kubeconform -strict -ignore-missing-schemas -kubernetes-version 1.29.0 -summary

quality:         ## Run Great Expectations checkpoints (requires Trino)
	$(PYTHON) quality/great-expectations/runner.py --layer all

quality-gate:    ## Run the GX quality gate as a Kubernetes Job (needs cluster)
	./scripts/run-quality-gate.sh

bootstrap:       ## Provision the full local stack (k3s + operators + services)
	./scripts/bootstrap.sh

deploy:          ## Apply all Kubernetes manifests to the current context
	./scripts/deploy.sh

batch:           ## Trigger a Bronze→Silver→Gold batch run on the cluster
	./scripts/run-batch.sh

iceberg-init:    ## Create all Iceberg tables (Nessie catalog on MinIO)
	./scripts/run-iceberg-init.sh

demo:            ## Live demo: traffic generator + dashboard (needs running cluster)
	./scripts/demo.sh

dashboard:       ## Streamlit dashboard only (assumes tunnels/env already set)
	streamlit run demo/dashboard.py

dev-up:          ## Start the lightweight Docker Compose dev stack
	docker compose -f docker-compose.dev.yml up -d

dev-down:        ## Stop the Docker Compose dev stack
	docker compose -f docker-compose.dev.yml down

clean:           ## Remove caches and build artefacts
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage coverage.xml
