.PHONY: install test lint typecheck format generate run demo report up down

install:
	pip install -e ".[dev]"

test:
	pytest

lint:
	ruff check .
	ruff format --check .

typecheck:
	mypy src

format:
	ruff format .
	ruff check --fix .

generate:
	oulad generate

run:
	oulad run-all

# Full story on synthetic data: first load, a no-op rerun, a late file for one
# presentation, then the report (docs/results.md and docs/img/*.png).
demo:
	rm -rf data
	oulad generate
	oulad run-all --label "initial load"
	oulad run-all --label "rerun, nothing new"
	oulad generate-late --presentation 2014B
	oulad run-all --label "late studentVle file for 2014B"
	oulad report

report:
	oulad report

# Optional: MinIO as an S3-compatible lake (see README, "Storage")
up:
	docker compose up -d

down:
	docker compose down -v
