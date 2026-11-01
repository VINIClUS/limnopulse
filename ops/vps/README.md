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

## 2. GitHub Environment protection

Do this **before** step 3 registers any secret — secrets should be scoped to
this environment, not to the repo, and creating the environment first is
what makes that possible.

In repo Settings > Environments, create `production` with at least one
required reviewer. `deploy.yml` references `environment: production` but the
protection rule itself is not expressible in the workflow file — this is the
control that keeps promotion human-gated (design §17.3: "Merge to `main`
never automatically deploys production").

## 3. Deploy user

```bash
useradd --system --create-home --shell /bin/bash limnopulse-deploy
usermod -aG docker limnopulse-deploy
mkdir -p /home/limnopulse-deploy/.ssh
chmod 700 /home/limnopulse-deploy/.ssh
```

`/bin/bash`, not `/usr/sbin/nologin` — `nologin` intercepts the SSH
forced-command before it ever runs and always prints "This account is
currently not available.", regardless of `authorized_keys`. The `command=`
restriction below is what actually locks the account down, not the shell.
Verified empirically; matches the working `cnesdeploy`/`cnesdeployprod` shell
choice on this same VPS.

Install the scp'd deploy script as `/usr/local/bin/limnopulse-deploy`
(root:root, 0755), and pre-create its log file (the deploy script runs as
`limnopulse-deploy`, which can't create a file directly under `/var/log`):

```bash
install -o root -g root -m 0755 /tmp/limnopulse-deploy.sh /usr/local/bin/limnopulse-deploy
touch /var/log/limnopulse-deploy.log
chown limnopulse-deploy:limnopulse-deploy /var/log/limnopulse-deploy.log
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

`/opt/limnopulse` (step 1) also needs to be owned by `limnopulse-deploy`, not
root, since the deploy script writes `.env.image` there and runs `docker
compose` as that user:

```bash
chown -R limnopulse-deploy:limnopulse-deploy /opt/limnopulse
```

Register the **private** half as the `VPS_SSH_KEY` secret, and
`ssh-keyscan 103.199.184.166` output as `VPS_KNOWN_HOSTS`, both scoped to the
`production` environment created in step 2 — never to the repo, never to
`build.yml`'s scope.

## 4. GHCR package visibility

After the first `build.yml` push to `main`, each of the four packages it
creates (`api`, `frontend`, `alert-evaluator`, `notifications`) is
**private** by default regardless of the repo being public. In
`https://github.com/users/VINIClUS/packages/container/<name>/settings`,
switch visibility to public for all four — otherwise `docker compose pull`
on the VPS fails `unauthorized`.

ADR-020 adds the `workers` package, the workers bundle that `build.yml` pushes
as an OCI artifact. Make it public too: the activation helper (step 7) pulls it
as root with no registry credentials.

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

Then confirm `cnesdata` itself is unaffected before moving on. Run this from outside the VPS (from
the VPS itself the hosts may not answer), and compare with the same request made before the
reload:

```bash
for u in https://cnesdata.com.br/ https://dev.cnesdata.com.br/ https://api.cnesdata.com.br/ https://api.dev.cnesdata.com.br/; do
  printf '%s %s\n' "$(curl -s -o /dev/null -w '%{http_code}' "$u")" "$u"
done
```

To change a block later, for example adding a route, edit the live file **in place**. It is a
single-file bind mount, so `sed -i` or an editor that writes a new file leaves the container on the
old inode. Back it up first, rewrite it through the same file handle, and check that `stat -c %i`
is unchanged. Then run `caddy validate` and `caddy reload` as above.

DNS: add an A record for `influx.limnopulse.com` -> `103.199.184.166`
(DNS-only, matching the existing `limnopulse.com`/`api.limnopulse.com`
records) before this step, or the reload will bring up a site block whose
certificate can't be issued yet.

## 6. First deploy

**Do not promote the first `main-<sha>` tag that `build.yml` ever produces.**
`build.yml`'s frontend build-args (`VITE_COGNITO_USER_POOL_ID`,
`VITE_COGNITO_CLIENT_ID`) come from repository Variables that don't exist
until Fase 0's `tofu apply` has run and someone sets them — before that, the
frontend image bakes in blank auth config, and `deploy.yml` promotes the
*exact* built artifact, never rebuilding. Promoting that tag ships a live
site with a login that can never succeed. Order:

1. Finish Fase 0 (`tofu apply`) and get real Cognito IDs.
2. Set `vars.VITE_COGNITO_USER_POOL_ID` / `vars.VITE_COGNITO_CLIENT_ID` in
   repo Settings > Secrets and variables > Actions > Variables.
3. Push anything to `main` (even empty) so `build.yml` produces the first
   tag with real auth config baked in.
4. Only then trigger `deploy.yml` (`workflow_dispatch`) with that tag,
   approve the environment gate, and watch
   `/var/log/limnopulse-deploy.log` on the VPS. Then run the verification
   checklist in the plan document.

Finally, clean up the staging copies: `rm -f /tmp/limnopulse-deploy.sh /tmp/Caddyfile.limnopulse`.

## 7. Workers runtime (ADR-020)

Do this once, as root, before promoting the first release that has a workers
bundle. The deploy refuses a release without one. This step installs what
ADR-020 adds to the VPS:

- a pinned oras;
- the root-owned activation helper and its `sudoers` rule;
- the release directories;
- the per-role configuration.

Every path and name below comes from `ops/workers/roles.yaml`. `<vps>` is the
deploy host. From your workstation, with a checkout of this repo:

```bash
scp ops/vps/limnopulse-deploy.sh ops/vps/limnopulse-workers-activate root@<vps>:/tmp/
scp ops/vps/sudoers.d/limnopulse-deploy root@<vps>:/tmp/limnopulse-deploy.sudoers
```

### 7.1 oras, pinned and checksum-verified

The helper pulls each bundle with oras. Install the release that `deploy.yml`
pins in `ORAS_VERSION` and `ORAS_SHA256`, and stop if the checksum does not
match. Moving to another release means changing both places.

```bash
cd "$(mktemp -d)"
curl -fsSLO https://github.com/oras-project/oras/releases/download/v1.3.6/oras_1.3.6_linux_amd64.tar.gz
echo '66fb0ad7206f3f6fa77ed5b552049d364ccf4b50b1684554f731f76302b52ca0  oras_1.3.6_linux_amd64.tar.gz' | sha256sum --check --strict -
tar -xzf oras_1.3.6_linux_amd64.tar.gz oras
install -o root -g root -m 0755 oras /usr/local/bin/oras
oras version
```

### 7.2 Activation helper, sudoers rule and deploy script

```bash
install -o root -g root -m 0755 /tmp/limnopulse-workers-activate /usr/local/sbin/limnopulse-workers-activate
visudo -c -f /tmp/limnopulse-deploy.sudoers
install -o root -g root -m 0440 /tmp/limnopulse-deploy.sudoers /etc/sudoers.d/limnopulse-deploy
visudo -c
install -o root -g root -m 0755 /tmp/limnopulse-deploy.sh /usr/local/bin/limnopulse-deploy
```

- **sudoers:** install the file only after `visudo -c -f` accepts it, then
  check the whole configuration again. A broken file under `/etc/sudoers.d`
  breaks sudo for everyone. The rule is exactly `activation.sudoers`: it lets
  `limnopulse-deploy` run only the helper, and the helper validates its own
  arguments.
- **Deploy script:** reinstall it in the same step. `deploy.yml` now sends
  `main-<40 hex> <64 hex>`; the old script refuses that request, and the new
  one needs the helper.

### 7.3 Release directories

```bash
install -d -o root -g root -m 0755 /opt/limnopulse/workers /opt/limnopulse/workers/releases /opt/limnopulse/workers/state
```

These directories stay root-owned inside the deploy user's `/opt/limnopulse`.
The helper refuses to run if any of them is not owned by root or is writable by
anyone else. Create them after step 3's `chown -R`, and if that `chown -R` is
ever run again, run this line again too.

The helper keeps two releases in `releases/<tag>`. `current` is a symlink to
the active one, and `state/` holds each staged release's verified SHA-256, the
recorded rollback floors and the installed units.

### 7.4 Per-role configuration

Each unit reads non-secret settings from `/etc/limnopulse/workers/<role>.env`.
It receives its keys through `LoadCredential=`, from
`/etc/limnopulse/workers/credentials/<role>/<credential>`. Every role has its
own IAM principal, so the names repeat but the keys differ.

| Role | Credentials |
|---|---|
| `alert-evaluator` | `aws-access-key-id`, `aws-secret-access-key`, `influxdb-token` |
| `notification-relay` | `aws-access-key-id`, `aws-secret-access-key` |
| `email-worker` | `aws-access-key-id`, `aws-secret-access-key` |
| `ses-feedback-worker` | `aws-access-key-id`, `aws-secret-access-key` |
| `telegram-worker` | `aws-access-key-id`, `aws-secret-access-key`, `redis-password` |
| `worker-monitor` | `aws-access-key-id`, `aws-secret-access-key` |

```bash
install -d -o root -g root -m 0755 /etc/limnopulse /etc/limnopulse/workers
install -d -o root -g root -m 0700 /etc/limnopulse/workers/credentials
for role in alert-evaluator notification-relay email-worker ses-feedback-worker telegram-worker worker-monitor; do
  env_file="/etc/limnopulse/workers/$role.env"
  [ -e "$env_file" ] || install -o root -g root -m 0600 /dev/null "$env_file"
  install -d -o root -g root -m 0700 "/etc/limnopulse/workers/credentials/$role"
done
```

Put each role's settings in its env file. A secret never goes there: the
Telegram worker's Redis password, for example, is its `redis-password`
credential. Write each credential as its own root-only file, without echoing it
to the terminal or the shell history:

```bash
umask 077
read -rs secret && printf '%s' "$secret" > /etc/limnopulse/workers/credentials/<role>/<credential>
unset secret
```

Roles ship disabled. The helper installs every unit but enables none, so a
role's credentials can wait until an owner enables that role.

### 7.5 Check and clean up

```bash
sudo -u limnopulse-deploy sudo -n /usr/local/sbin/limnopulse-workers-activate status
rm -f /tmp/limnopulse-deploy.sh /tmp/limnopulse-workers-activate /tmp/limnopulse-deploy.sudoers
```

`status` prints `current none` until the first promotion activates a bundle.

### Rollback floor override

The deploy's automatic rollback never overrides a rollback floor (exit `3`). A
refused rollback is logged as needing a manual decision. Once an owner has
decided, root itself can activate an older release:

```bash
/usr/local/sbin/limnopulse-workers-activate activate <tag> --override-floor
```

Run this from a root login. The helper refuses the override whenever
`SUDO_USER` is set, and that includes shells opened with `sudo -i` or
`sudo su`. `limnopulse-deploy` can therefore never pass it.

### Trust boundary

`limnopulse-deploy` owns `/opt/limnopulse` and is in the `docker` group, so it
is already root-equivalent on this host. The helper's ownership checks catch
bootstrap mistakes and keep the release tree root-owned. They are not a
boundary against a shell running as `limnopulse-deploy`. The CI deploy key
itself reaches only the forced command.

## Deployment verification and recovery

Before promoting a release, run `make verify-compose-production` locally.
This parses the production manifest using the example environment and a dummy
image tag; it does not start services or contact the VPS. The build workflow
uses the same gate.

The deploy stages the workers bundle first. A missing or tampered bundle, a
bad `SHA256SUMS` or a rollback floor stops it before anything changes, with
the helper's exit code. It then starts Compose with every profile in
`deploy.compose_profiles` and waits for `api` and each profiled worker to be
healthy. Next it activates the bundle. Only then does it record `IMAGE_TAG`
and `WORKERS_BUNDLE_SHA256` in `.env.image`. Until that last step, Compose
reads the candidate tag from `.env.image.candidate`.

If an image pull, the Compose update, a health wait or the activation fails,
the deploy rolls back to the release that `.env.image` records:

- Compose pulls and starts the previous tag with the same profiles.
- The helper re-activates the previous bundle. If `.env.image` recorded no
  bundle, as with releases from before ADR-020, it runs `deactivate` instead.
  `deactivate` stops the units it removes. A later `activate` only
  try-restarts, so an enabled role stays stopped until someone starts it again
  or the host reboots.
- A rollback-floor refusal is logged and never overridden.
- A recovery failure is reported, and the failed promotion always exits
  nonzero.

On a first deployment with no previous tag, the deploy stops the candidate
services and deactivates the bundle, preserving all volumes. Inspect
`/var/log/limnopulse-deploy.log` before retrying a failed promotion.

Every promotion waits for the profiled telemetry ingest worker. A worker parked
on a configuration error (exit `78`) stays unhealthy. This happens, for
example, after the P3 rollback sets `TELEMETRY_V2_WRITE_ENABLED=false`. While
it is parked, every promotion fails its health wait and rolls back.

For isolated testing, the script accepts `LIMNOPULSE_STACK_DIR`,
`LIMNOPULSE_DEPLOY_LOG` and `LIMNOPULSE_HEALTH_TIMEOUT_SECONDS`. Defaults remain
`/opt/limnopulse`, `/var/log/limnopulse-deploy.log` and 120 seconds. A zero-second
timeout performs one health check and immediately recovers if unhealthy.
The forced SSH command accepts only `main-<40 hex> <64 hex>`: the release
tag and the workers bundle SHA-256. It never accepts shell code or
configuration arguments. CI receives `image_tag` through its environment. It
pulls the bundle, verifies its build-provenance attestation and only then
computes the SHA-256.

The script is installed manually and remains root-owned. Merging a correction
does not update `/usr/local/bin/limnopulse-deploy` on an existing VPS: copy and
install the reviewed script using step 3 before relying on its recovery fixes.
The same applies to the activation helper and the `sudoers` rule (step 7).
