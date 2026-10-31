UV ?= uv
TOFU ?= tofu

.PHONY: verify verify-python verify-go verify-tofu verify-compose verify-compose-production verify-influx verify-ingestion

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
