UV ?= uv
TOFU ?= tofu
# ADR-020 workers bundle: the release tag it is built for and its output directory.
WORKERS_TAG ?= main-0000000000000000000000000000000000000000
WORKERS_DIR ?= dist/workers

.PHONY: verify verify-python verify-go verify-tofu verify-compose verify-compose-production verify-influx verify-ingestion workers-units workers-binaries workers-bundle

verify: verify-python verify-go verify-tofu verify-compose verify-compose-production

verify-python:
	$(UV) lock --check
	$(UV) sync --locked --extra dev
	$(UV) run --locked --no-sync python -m pytest -q

verify-go:
	go test -race ./...

verify-tofu:
	$(TOFU) -chdir=infra/opentofu init -backend=false -input=false
	$(TOFU) -chdir=infra/opentofu fmt -check -recursive
	$(TOFU) -chdir=infra/opentofu validate -no-color

verify-compose:
	docker compose config --quiet

verify-compose-production:
	IMAGE_TAG=main-0000000000000000000000000000000000000000 docker compose --env-file .env.production.example -f compose.production.yaml config --quiet

verify-influx:
	$(UV) sync --locked --extra dev
	RUN_INFLUX_INTEGRATION=1 $(UV) run --locked --no-sync python -m pytest -q tests/influx
	RUN_INFLUX_INTEGRATION=1 go test -race -count=1 -run 'TestInfluxIntegration' ./...

# Needs ElasticMQ, DynamoDB Local and InfluxDB running first; locally, start them with
# scripts/dev/run_local_elasticmq.sh, scripts/dev/run_local_dynamodb.sh and
# scripts/dev/run_local_influxd.sh. Both flags are set so the Influx-backed tests never skip.
verify-ingestion:
	$(UV) sync --locked --extra dev
	RUN_INGESTION_INTEGRATION=1 RUN_INFLUX_INTEGRATION=1 $(UV) run --locked --no-sync python -m pytest -q tests/ingestion

# ADR-020 workers bundle (ops/workers/README.md). workers-units fails when ops/workers/systemd
# drifted from roles.yaml; regenerate it with `python -m scripts.workers.units --write`.
# workers-binaries runs `go build` for every Go role; CI runs it, then workers-bundle packs
# WORKERS_DIR/limnopulse-workers-$(WORKERS_TAG).tar.gz and verifies it.
workers-units:
	$(UV) run --locked --no-sync python -m scripts.workers.units

workers-binaries:
	$(UV) run --locked --no-sync python -m scripts.workers.build_bundle binaries --out $(WORKERS_DIR)/bin

workers-bundle:
	$(UV) run --locked --no-sync python -m scripts.workers.build_bundle bundle --tag $(WORKERS_TAG) --binaries $(WORKERS_DIR)/bin --out $(WORKERS_DIR)
