SHELL := /bin/bash

.PHONY: up down logs fmt lint

up:
	docker compose up -d --build
	@echo "Airflow UI: http://localhost:8080 (airflow/airflow)"

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=200

fmt:
	python -m ruff format .

lint:
	python -m ruff check .
