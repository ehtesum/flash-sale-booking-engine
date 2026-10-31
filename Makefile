.PHONY: up down seed test smoke bench logs ps

up:
	docker compose up -d --build

down:
	docker compose down

ps:
	docker compose ps

seed:
	python scripts/seed_catalog.py

test:
	pytest tests -v

smoke:
	python tests/smoke_test.py

bench:
	python benchmark/stress_test.py

logs:
	docker compose logs -f

