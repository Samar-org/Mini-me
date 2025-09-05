SHELL := /bin/bash

up:
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=200

migrate:
	docker compose exec backend alembic upgrade head

makemigration:
	docker compose exec backend alembic revision -m "$(msg)" --autogenerate

shell:
	docker compose exec backend bash
