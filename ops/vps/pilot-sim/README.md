# Simulated HTTPS pilot connector

A stand-in for a real `https_direct` connector, used for the first production pilot of the
[P3 ingestion rollout](../../../docs/p3-ingestion-rollout-runbook.md) (step 4). Every 5 minutes it
posts one batch of temperature, pH and dissolved-oxygen readings for one DeviceIntegration to
`POST /ingest/v2/observations`. It runs on the VPS host with the system `python3` (stdlib only), so
the ingest token never leaves the host.

| File | Installed as |
|---|---|
| `pilot_sim.py` | `/opt/limnopulse/pilot/pilot_sim.py` |
| `pilot-sim.env.example` | `/opt/limnopulse/pilot/pilot-sim.env` (0600) |
| `limnopulse-pilot-sim.service`, `limnopulse-pilot-sim.timer` | `/etc/systemd/system/` |

The token file `/opt/limnopulse/pilot/ingest.token` (root, 0600) holds only the `lpi_...` token.
Write it straight from the issuance, with `umask 077`, so the token is never printed: the token is
returned once and only its digest and fingerprint are stored.

## Install

The VPS has no clone of this repository, so copy the files over first, then:

```bash
install -d -m 0700 /opt/limnopulse/pilot
install -m 0644 /tmp/pilot_sim.py /opt/limnopulse/pilot/pilot_sim.py
install -m 0600 /tmp/pilot-sim.env.example /opt/limnopulse/pilot/pilot-sim.env
install -m 0644 /tmp/limnopulse-pilot-sim.service /tmp/limnopulse-pilot-sim.timer /etc/systemd/system/
systemctl daemon-reload
systemctl start limnopulse-pilot-sim.service      # one batch now
journalctl -u limnopulse-pilot-sim.service -n 5    # expect "status=202"
systemctl enable --now limnopulse-pilot-sim.timer
```

The device's observations must not be older than its Deployment: a v1 device backfilled by M1 is
deployed from its `created_at`, and the connector always sends the current time.

## Stop

```bash
systemctl disable --now limnopulse-pilot-sim.timer
```

To end the pilot for good, also revoke its credential
(`POST /v2/tenants/{tenant_id}/integration-accounts/{integration_account_id}/ingest-credentials/{ingest_credential_id}/revoke`)
and delete the token file.
