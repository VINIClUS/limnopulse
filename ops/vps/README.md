# VPS bootstrap runbook — Fase 1

One-time, manual, root-on-`103.199.184.166` steps. None of this is run by
CI; CI only builds/pushes images (`build.yml`) and triggers the deploy user's
forced-command (`deploy.yml`). See
`/home/vinicius/.claude/plans/fa-a-o-ci-cd-do-ticklish-dragon.md` for the full
plan and its rationale.

Do these in order. Steps 1-4 can happen any time; step 5 (Caddy) is the
highest-risk step because the Caddyfile is shared with the unrelated
`cnesdata` stack — do it last, deliberately, and alone.

**The VPS has no clone of this repo** — per design, it only ever pulls
built images from GHCR, never source. Every path below that names a file
"from this repo" must be copied over first. From your workstation, with a
checkout of this repo:

```bash
scp compose.production.yaml \
    ops/vps/limnopulse-deploy.sh \
    ops/vps/Caddyfile.limnopulse \
    root@103.199.184.166:/tmp/
```

The steps below assume those three files are sitting in `/tmp` on the VPS.

## 1. Stack directory

```bash
mkdir -p /opt/limnopulse
mv /tmp/compose.production.yaml /opt/limnopulse/
```

Create `/opt/limnopulse/.env` from `.env.production.example` in this repo
(copy its *content*, e.g. by pasting — it's a template, not meant to be
scp'd verbatim), filled in with real values (Fase 0 `tofu apply` outputs, a
generated InfluxDB admin token/password, the `limnopulse-api` IAM user's
access key). **Never generate this file from CI; create it by hand and
never commit it.**

## 2. Deploy user

```bash
useradd --system --create-home --shell /usr/sbin/nologin limnopulse-deploy
mkdir -p /home/limnopulse-deploy/.ssh
chmod 700 /home/limnopulse-deploy/.ssh
```

Install the scp'd deploy script as `/usr/local/bin/limnopulse-deploy`
(root:root, 0755):

```bash
install -o root -g root -m 0755 /tmp/limnopulse-deploy.sh /usr/local/bin/limnopulse-deploy
```

Generate a dedicated deploy key locally (not one already in use elsewhere)
and add the **public** half to
`/home/limnopulse-deploy/.ssh/authorized_keys`, locked to the forced-command,
mirroring `cnesdata-prod-deploy`'s entry:

```
command="/usr/local/bin/limnopulse-deploy",no-port-forwarding,no-agent-forwarding,no-X11-forwarding,no-pty ssh-ed25519 AAAA... limnopulse-deploy-ci
```

```bash
chown -R limnopulse-deploy:limnopulse-deploy /home/limnopulse-deploy/.ssh
chmod 600 /home/limnopulse-deploy/.ssh/authorized_keys
```

Register the **private** half as the `VPS_SSH_KEY` GitHub Actions secret, and
`ssh-keyscan 103.199.184.166` output as `VPS_KNOWN_HOSTS`. Both live only in
`deploy.yml`'s `production` environment, never in `build.yml`'s scope.

## 3. GitHub Environment protection

In repo Settings > Environments, create `production` with at least one
required reviewer. `deploy.yml` references `environment: production` but the
protection rule itself is not expressible in the workflow file — this is the
control that keeps promotion human-gated (design §17.3: "Merge to `main`
never automatically deploys production").

## 4. GHCR package visibility

After the first `build.yml` push to `main`, each of the four packages it
creates (`api`, `frontend`, `alert-evaluator`, `notifications`) is
**private** by default regardless of the repo being public. In
`https://github.com/users/VINIClUS/packages/container/<name>/settings`,
switch visibility to public for all four — otherwise `docker compose pull`
on the VPS fails `unauthorized`.

## 5. Caddy — do this alone, deliberately

The Caddyfile is root-owned, shared with `cnesdata`, and mounted read-only
into `cnesdata-caddy-1` from `/opt/cnesdata/caddy/Caddyfile`. No
forced-command touches it. A bad reload takes `cnesdata` down with it.

```bash
cp /opt/cnesdata/caddy/Caddyfile /opt/cnesdata/caddy/Caddyfile.bak.$(date -u +%Y%m%dT%H%M%SZ)
cat /tmp/Caddyfile.limnopulse >> /opt/cnesdata/caddy/Caddyfile   # append, never rewrite
docker exec cnesdata-caddy-1 caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile
docker exec cnesdata-caddy-1 caddy reload  --config /etc/caddy/Caddyfile --adapter caddyfile
```

Then confirm `cnesdata` itself is unaffected before moving on:

```bash
curl -sSI https://cnesdata.vinisantana.com/ | head -3
curl -sSI https://api.vinisantana.com/api/  | head -3
```

DNS: add an A record for `influx.limnopulse.com` -> `103.199.184.166`
(DNS-only, matching the existing `limnopulse.com`/`api.limnopulse.com`
records) before this step, or the reload will bring up a site block whose
certificate can't be issued yet.

## 6. First deploy

Trigger `deploy.yml` (`workflow_dispatch`) with the `main-<sha>` tag that
`build.yml` produced, approve the environment gate, and watch
`/var/log/limnopulse-deploy.log` on the VPS. Then run the verification
checklist in the plan document.

Finally, clean up the staging copies: `rm -f /tmp/limnopulse-deploy.sh /tmp/Caddyfile.limnopulse`.
