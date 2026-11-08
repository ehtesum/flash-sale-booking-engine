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
	pytest tests/test_engine_features.py -v

smoke:
	python tests/e2e_verification.py

bench:
	python benchmark/stress_test.py

logs:
	docker compose logs -f

