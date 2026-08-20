COMPOSE = docker compose
DAYS ?= 1

.PHONY: up seed tick reset down psql dbt run

up:           ## Start Postgres (app-db) + billing API
	$(COMPOSE) up -d --wait app-db
	$(COMPOSE) up -d billing-api

seed: up      ## Generate ~18 months of history across all three sources
	$(COMPOSE) run --rm simulator python -m generator.cli seed

tick:         ## Advance simulated time (make tick DAYS=14) to exercise incremental loads
	$(COMPOSE) run --rm simulator python -m generator.cli tick --days $(DAYS)

reset:        ## Drop the app schema (then `make seed` to start over)
	$(COMPOSE) run --rm simulator python -m generator.cli reset

psql:         ## Open a psql shell against the app DB
	$(COMPOSE) exec app-db psql -U endeavoriq

run:          ## Full pipeline: EL (all three sources) then dbt build
	python3 el/extract_postgres.py
	python3 el/extract_billing_api.py
	python3 el/extract_usage_files.py
	cd transform && dbt build --profiles-dir .

dbt:          ## Build your dbt project (requires dbt-duckdb installed locally)
	cd transform && dbt build --profiles-dir .

down:         ## Stop everything and remove volumes
	$(COMPOSE) down -v
