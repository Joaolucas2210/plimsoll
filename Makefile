# Plimsoll — entry points. Cargo stays the build tool; these are the commands a newcomer needs.
.DEFAULT_GOAL := help
SHELL := /usr/bin/env bash
.PHONY: help demo gates test build up down

help: ## This list
	@printf 'Plimsoll — see the load line in one command: make demo\n\n'
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "}{printf "  \033[36m%-8s\033[0m %s\n", $$1, $$2}'

demo: ## One local node, no Docker: a query over the memory ceiling fails alone (422), 429, 504, node keeps serving
	@scripts/demo.sh

gates: ## What CI runs before the live tests: fmt, clippy -D warnings, tests, plane invariants
	@scripts/gates.sh

test: ## cargo test --workspace
	@cargo test --workspace

build: ## Release binary at target/release/plimsoll-server
	@cargo build --release -p plimsoll-server

up: ## Compose quickstart (catalog + coordinator + 2 workers); needs the exports in README → Quickstart
	@docker compose up -d --wait

down: ## Stop the quickstart and drop its warehouse volume
	@docker compose down -v
