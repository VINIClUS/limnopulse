# Limnopulse

Phase 1 FastAPI foundation for Limnopulse.

## Local Setup

```bash
cp .env.example .env
python -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
docker compose up -d redis dynamodb-local
python scripts/dev/init_dynamodb.py
python scripts/dev/seed_local.py
python -m uvicorn limnopulse_api.main:app --reload --host 0.0.0.0 --port 8000
```

## Local Auth

With `APP_ENV=local` and `AUTH_MODE=dev`, use:

```text
X-Dev-User-Sub: local-user-001
X-Dev-User-Email: local@example.test
```

Dev headers authenticate identity only. Tenant access still requires an active membership in `LimnopulseDomain`.

## Tests

```bash
python -m pytest -q
```
