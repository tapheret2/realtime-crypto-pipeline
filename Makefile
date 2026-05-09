# realtime-crypto-pipeline — common dev commands
# Use `make help` to list targets.

SHELL := /bin/bash
COMPOSE := docker compose

.DEFAULT_GOAL := help

.PHONY: help up down restart logs ps test lint fmt psql smoke clean

help: ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

up: ## Start the full stack in the background.
	$(COMPOSE) up -d

down: ## Stop the stack and remove volumes.
	$(COMPOSE) down -v

restart: down up ## Restart the stack (destructive — drops volumes).

logs: ## Tail logs for every service.
	$(COMPOSE) logs -f --tail=200

ps: ## Show running containers.
	$(COMPOSE) ps

test: ## Run the pytest suite locally.
	pytest -v

lint: ## Run ruff over the codebase.
	ruff check .

fmt: ## Auto-format with ruff.
	ruff format .

psql: ## Open a psql shell against the running Postgres.
	$(COMPOSE) exec postgres psql -U crypto -d crypto

smoke: ## End-to-end smoke test: produce 5 messages, query Postgres.
	bash scripts/smoke.sh

clean: ## Remove generated artifacts.
	rm -rf .pytest_cache .ruff_cache **/__pycache__ checkpoints spark-warehouse
