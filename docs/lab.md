# Debian VPS lab

This product contract is consumed by the `debian-vps-lab` harness. It is a
single-guest, disposable validation path, not a deployment configuration for a
real environment.

Run the product hooks only from a guest carrying `/etc/vinisantana-lab`:

```bash
ops/lab/deploy
ops/lab/verify
ops/lab/smoke.py
ops/lab/reset
```

`deploy` reuses the local Compose dependencies, `init_dynamodb.py`,
`seed_local.py`, and the FastAPI application. `reset` removes only the
LimnoPulse Compose project, its volumes, and the lab API systemd unit.

The lab uses synthetic data only. DynamoDB Local, ElasticMQ, and the fake email
sender are local emulations. Cognito, SES, EventBridge, and production MQTT are
explicitly omitted. Never use production credentials, real tenant data, or
production endpoints in this path.

The smoke test publishes six synthetic `do_mg_l` readings, opens a low-oxygen
alert, then replays the same evaluator slot to prove the alert event is not
duplicated. The evaluator remains a one-shot process; this lab does not install
a production scheduler.
