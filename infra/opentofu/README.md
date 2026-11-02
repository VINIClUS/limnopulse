# Limnopulse Cloud Infrastructure

This directory contains the OpenTofu scaffold for cloud infrastructure. Docker Compose remains the development runtime for local dependencies; OpenTofu owns cloud resources.

## Included

- DynamoDB domain and audit tables with on-demand billing, `PK` / `SK` string
  keys, alert indexes, and the sparse `NotificationRelayByAvailableAt` GSI.
- Cognito User Pool and app client for user authentication.
- Encrypted email jobs, Telegram jobs and SES-events queues with redrive after
  eight receives, plus jobs, feedback, and EventBridge-routing DLQs.
- SESv2 configuration set and EventBridge routing for Send, Delivery,
  DeliveryDelay, Bounce, Complaint, and Reject feedback.
- Separate Telegram bot-token and webhook-secret containers with no secret
  versions in state, plus least-privilege worker and FastAPI IAM policies.
- Telemetry ingress queue and DLQ (`telemetry_ingress`, default `false`): SSE,
  256 KiB messages, redrive after eight receives, SecureTransport-deny queue
  policies, age/depth/DLQ CloudWatch alarms, a dedicated `telemetry_ingest`
  worker user, the API send grant and an unattached operator policy. The
  alarms publish to a KMS-encrypted SNS topic; `telemetry_ingress_alarm_email`
  subscribes the on-call mailbox, which must confirm the email AWS sends.
- ADR-020 worker roles (`workers.tf`, `worker_alarms.tf`): one IAM principal per
  Go worker role, one `<role>_enabled` variable per unit role (default `false`),
  and unit-health and queue alarms on their own KMS-encrypted SNS topic. See
  [Worker roles](#worker-roles-adr-020).
- Placeholder variables for Redis and InfluxDB cloud endpoints.

## Not Included Yet

- Redis cloud provisioning.
- InfluxDB managed provisioning.
- Production MQTT TLS/mTLS, broker ACLs, or device credential rotation.
- SES identity verification, production sending-access requests, DNS records or
  sandbox-recipient verification. `SES_FROM_EMAIL` is a runtime secret/config
  chosen per environment, not a generic stack default.
- Container orchestration, worker autoscaling or dashboards. The relay,
  evaluator and monitor schedules are systemd timers in the workers bundle
  (ADR-020), not cloud resources.
- Telegram container orchestration, webhook registration and secret values.
- WhatsApp, SMS, or mobile push delivery.

## Local Validation

```bash
tofu init -backend=false
tofu fmt -check
tofu validate
```

Use `-backend=false` for local scaffold validation so placeholder backend values are not contacted. Copy `env/cloud.tfvars.example` to an untracked `.tfvars` file only when preparing a real cloud plan. Replace backend placeholders with real remote-state infrastructure outside this repository before any real cloud workflow.

## Notification rollout boundary

Creating the relay GSI is not enough to enable publishing. Existing email
NotificationOutboxes need the explicit tenant-scoped `limnopulse-admin
backfill-relay` migration (run under the `limnopulse_admin` role, below), and
the index must be `ACTIVE` before the relay runs. Do not enable the
notification-relay timer or the email-worker in the same infrastructure apply
that first introduces the GSI. Verify SES identity/production access and map the
`ses_configuration_set_name` output to `SES_CONFIGURATION_SET_NAME` on the
email-worker. Start the email-worker before enabling the notification-relay
timer.

The full order, rollback, DLQ handling and PII constraints are documented in
[Phase 3C-A notification operations](../../docs/notifications-phase-3c-a.md).
Telegram uses a separate queue, worker, secrets and staged feature flags; see
[Phase 3C-B Telegram operations](../../docs/notifications-phase-3c-b.md).

## Worker roles (ADR-020)

`workers.tf` gives every Go worker role in
[`ops/workers/roles.yaml`](../../ops/workers/roles.yaml) its own principal
(`iam_principal`). `worker_alarms.tf` holds the roles' alarms.
`tests/unit/test_iam_runtime_transactions.py` derives each role's AWS actions
from the Go import closure of `cmd/<role>`. It fails if a policy grants an
action the closure does not call, misses one, or scopes it to another role's
resource.

| Role | Principal | Exists with | Grants |
|---|---|---|---|
| `alert-evaluator` | user `alert_evaluator` | always | table GetItem, PutItem, UpdateItem; Query on `AlertEvaluationByDue` only |
| `notification-relay` | user `notification_relay` | always | table GetItem, UpdateItem, ConditionCheckItem; Query on the table and `NotificationRelayByAvailableAt`; SendMessage on the email jobs queue, and on the Telegram jobs queue with `telegram_delivery` |
| `email-worker` | user `email_worker` | `email_delivery` | table items; consume the email jobs queue; `ses:SendEmail` on the From identity (mailbox and domain) and the configuration set, only from `ses_from_address` |
| `ses-feedback-worker` | user `ses_feedback_worker` | `email_delivery` | table items; consume the SES events queue |
| `telegram-worker` | user `telegram_worker` | `telegram_delivery` | table items; consume the Telegram jobs queue; read the bot token secret |
| `worker-monitor` | user `worker_monitor` | always | `cloudwatch:PutMetricData`, only to `Limnopulse/Workers` |
| `limnopulse-admin` | role `limnopulse_admin` | always | table UpdateItem; Query on the table |

"Table items" are GetItem, PutItem, UpdateItem and ConditionCheckItem on the
domain table: a transaction's Put, Update and ConditionCheck items each need
their own action. No consumer gets `sqs:GetQueueAttributes` and none gets Query,
because none of them calls it. The email and SES-feedback principals share only
the table item actions both need, and no policy serves two principals.

The closure is read per package, and `internal/alertevaluator/dynamo` holds
both the evaluator's store and the schedule backfills. So the test leaves
`backfill.go` out of the evaluator's derivation and `store.go` out of the
admin's (`GO_ROLE_UNRUN_FILES`). It fails if either file disappears or stops
naming an AWS operation. It also fails if code reachable from the binary's
`main`, an `init` or a package variable can run one of the file's AWS calls.

### Access keys

OpenTofu creates no access key, as for every user in `iam_runtime.tf`: a key in
the state would be a plaintext secret. Each `<principal>_iam_user_name` output
names a user. When enabling a role, an operator creates its key out of band
with their own credentials. They write it straight into the role's
`aws-access-key-id` and `aws-secret-access-key` credentials under
`/etc/limnopulse/workers/credentials/<role>/`
([`ops/workers/README.md`](../../ops/workers/README.md)), so the secret never
reaches a terminal, a shell history, an env file, a ticket or chat.

The function below works in this order:

1. It checks that the destination is writable before any key exists.
2. It creates the key and writes both files as `.new`.
3. It swaps both files in with one script on the host. That script backs up
   the current pair as `.old` first, and if either move fails it puts both
   back.
4. It checks every step. On any failure it deletes the new key, so no live key
   is left that nobody holds, and a running unit keeps a matching id and
   secret.
5. If the delete itself fails, it prints the new key's id, never the secret,
   and the command to delete it.

```bash
install_worker_key() {  # <role> <IAM user> <worker host>
  local dir="/etc/limnopulse/workers/credentials/$1" id secret
  # Runs on the host as root, with the directory as $1: swaps both files in, or restores
  # both (and removes a file that had no previous version).
  local swap='cd "$1" || exit 1
    for f in aws-access-key-id aws-secret-access-key; do
      [ ! -e "$f" ] || cp -p "$f" "$f.old" || exit 1
    done
    if mv aws-access-key-id.new aws-access-key-id &&
       mv aws-secret-access-key.new aws-secret-access-key; then
      rm -f aws-access-key-id.old aws-secret-access-key.old
    else
      for f in aws-access-key-id aws-secret-access-key; do
        if [ -e "$f.old" ]; then mv "$f.old" "$f"; else rm -f "$f"; fi
      done
      exit 1
    fi'
  ssh "$3" "sudo -n test -d $dir -a -w $dir" \
    || { echo "cannot write $dir on $3; no key created" >&2; return 1; }
  read -r id secret < <(aws iam create-access-key --user-name "$2" \
      --query 'AccessKey.[AccessKeyId,SecretAccessKey]' --output text) && [ -n "$secret" ] \
    || { echo "create-access-key failed" >&2; return 1; }
  if printf '%s' "$id" | ssh "$3" "sudo -n sh -c 'umask 077; cat > $dir/aws-access-key-id.new'" &&
     printf '%s' "$secret" | ssh "$3" "sudo -n sh -c 'umask 077; cat > $dir/aws-secret-access-key.new'" &&
     ssh "$3" "sudo -n sh -s $dir" <<<"$swap"; then
    echo "installed a new key for $1"
    return 0
  fi
  echo "installing the key failed; the previous files stay in place" >&2
  ssh "$3" "sudo -n rm -f $dir/aws-access-key-id.new $dir/aws-secret-access-key.new"
  aws iam delete-access-key --user-name "$2" --access-key-id "$id" && return 1
  echo "deleting the new key failed; delete it with:" >&2
  echo "  aws iam delete-access-key --user-name $2 --access-key-id $id" >&2
  return 1
}
```

If `.old` files remain in the directory after a failure, the restore itself
failed. Move them back by hand before restarting the unit.

For example, for the email-worker, run this from this directory:

```bash
install_worker_key email-worker "$(tofu output -raw email_worker_iam_user_name)" <worker-host>
```

The key travels on stdin, so the host's `sudo` runs with `-n` and fails rather
than prompting. That makes the first check fail before any key exists. On such
a host, run the same steps in a root shell on the host itself, with the
operator's AWS credentials there and without the `ssh "$3" "sudo -n …"`
wrapper. To rotate a key, run the
function again (IAM allows two keys per user), restart the unit, then delete the
old key with `aws iam delete-access-key`. Check its id first with
`aws iam list-access-keys`.

### Enabling and disabling a role

`<role>_enabled` turns on that role's alarm actions and nothing else. A plan
refuses `email_worker_enabled` or `ses_feedback_worker_enabled` without
`email_delivery`, and `telegram_worker_enabled` without `telegram_delivery`.
Enable `worker-monitor` first: every other unit-health alarm reads its
datapoints.

To enable a role:

1. If the role has a delivery flag, turn it on; its principal and alarms then
   exist. Leave `<role>_enabled` false.
2. Create the role's access key and install it, as above.
3. Enable its unit or timer with a persistent `systemctl enable --now`.
4. Wait for the first `UnitHealthy` datapoint, then until every alarm of the
   role is `OK`: its unit-health alarm and, for a consumer, its queue age, depth
   and DLQ alarms. CloudWatch acts only on state changes, so an alarm already in
   `ALARM` when actions turn on never notifies for that breach. Drain a backlog
   or a DLQ first rather than enabling over it. List the role's alarm states
   with:

   ```bash
   aws cloudwatch describe-alarms --alarm-name-prefix "<project_name>-<environment>-<role>-" \
     --query 'MetricAlarms[].[AlarmName,StateValue]' --output text
   ```

5. For the email-worker only, pass the SES check below.
6. Set `<role>_enabled = true` in the untracked tfvars and apply it.

To disable a role, reverse the order:

1. Set `<role>_enabled = false` and apply it.
2. Disable the unit or timer. `worker-monitor` stops publishing for the role.
3. Delete the role's access key (`aws iam delete-access-key`) and its
   credential files.
4. Turn a delivery flag off only after every role it carries is disabled and
   its key deleted. `email_delivery` carries the email-worker and the
   SES-feedback worker, and `telegram_delivery` carries the Telegram worker and
   the relay's Telegram send grant. That apply destroys those users, together
   with any key still on one (`force_destroy`), their alarms and the channel's
   other gated resources. The preconditions only refuse `<role>_enabled = true`
   without the flag, so nothing stops it under a running unit.

So a disabled role holds no key. The one exception is steps 2 to 5 of enabling,
because the unit needs its key to produce the datapoint that step 4 waits for.
OpenTofu cannot see an out-of-band key; check with `aws iam list-access-keys`.

### The email-worker's SES check

`ses:SendEmail` is scoped to the From identity, both the mailbox and its domain,
and to the configuration set. The Service Authorization Reference lists
`identity`, `configuration-set` and `template` as the resources of SES v2
`SendEmail`. The sender never sends a template, and the derivation fails if it
starts to. Before setting `email_worker_enabled`, check that scope against the
account, from this directory:

1. Simulate the email principal for all three resources with the From
   condition. Every line must end in `allowed`:

   ```bash
   from=<ses_from_address>
   account="$(aws sts get-caller-identity --query Account --output text)"
   region="$(tofu output -raw aws_region)"
   ses="arn:aws:ses:$region:$account"
   arn="$(aws iam get-user --user-name "$(tofu output -raw email_worker_iam_user_name)" \
     --query User.Arn --output text)"
   aws iam simulate-principal-policy --policy-source-arn "$arn" --action-names ses:SendEmail \
     --resource-arns "$ses:identity/$from" "$ses:identity/${from#*@}" \
       "$ses:configuration-set/$(tofu output -raw ses_configuration_set_name)" \
     --context-entries "ContextKeyName=ses:FromAddress,ContextKeyValues=$from,ContextKeyType=string" \
     --query 'EvaluationResults[].ResourceSpecificResults[].[EvalResourceName,EvalResourceDecision]' \
     --output text
   ```

2. With the email-worker's own key, as root on the worker host, send one
   message to the SES mailbox simulator, which SES accepts in the sandbox too.
   A `MessageId` means SES authorized the identity and the From condition.
   Leave the configuration set out: its events carry no delivery, so the
   SES-feedback worker would retry them into its DLQ. Step 1 covers the
   configuration set. OpenTofu does not manage the identities, and SES applies
   an identity's default configuration set even to this send. So first check
   the mailbox and the domain. Each must print `None` or report that the
   identity does not exist; otherwise skip the send.

   ```bash
   for identity in <ses_from_address> <ses_from_domain>; do
     aws sesv2 get-email-identity --email-identity "$identity" \
       --query ConfigurationSetName --output text
   done
   ```

   ```bash
   dir=/etc/limnopulse/workers/credentials/email-worker
   AWS_ACCESS_KEY_ID="$(cat "$dir/aws-access-key-id")" \
   AWS_SECRET_ACCESS_KEY="$(cat "$dir/aws-secret-access-key")" \
   aws sesv2 send-email --region <aws_region> --from-email-address <ses_from_address> \
     --destination ToAddresses=success@simulator.amazonses.com \
     --content 'Simple={Subject={Data=email-worker check},Body={Text={Data=ADR-020 principal check}}}'
   ```

### limnopulse-admin

The backfills run by hand and rarely, and no enable variable would ever retire a
standing key. So `limnopulse_admin` is a role that an operator assumes for one
run, not a user. It holds exactly the backfills' grants and has short-lived
credentials only.

Its trust names this account, narrowed on `aws:PrincipalArn` by
`limnopulse_admin_trusted_principal_arns`. Set that list in the untracked
tfvars to the operators' IAM role or user ARNs. `ArnLike` patterns are allowed,
and for an IAM Identity Center permission set you give its role ARN, for example
`arn:aws:iam::<account>:role/aws-reserved/sso.amazonaws.com/*AWSReservedSSO_<permission-set>_*`.
The `*` comes right after `sso.amazonaws.com/` because the role path has a
Region segment for some Identity Center instances and none for others. A `*` in
`ArnLike` also matches `/`, so this one pattern covers both.
`aws:PrincipalArn` holds a role's ARN, never a session's. While the list is
empty, any identity in the account whose own policy allows `sts:AssumeRole` on
the role can assume it.

`limnopulse-admin` uses the AWS default credential chain. Add a profile for the
role to `~/.aws/config`:

```ini
[profile limnopulse-admin]
role_arn = <the limnopulse_admin_role_arn output>
source_profile = <your operator profile>
duration_seconds = 3600
```

Then load a session into the shell, run the backfill and drop the session.
`--format env` prints only `export` lines, so `eval` them rather than printing
them:

```bash
eval "$(aws configure export-credentials --profile limnopulse-admin --format env)"
AWS_REGION=<aws_region> DYNAMODB_DOMAIN_TABLE=<dynamodb_domain_table> \
  limnopulse-admin backfill-relay --tenant=<id>
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_CREDENTIAL_EXPIRATION
```

`backfill-relay` is a dry run unless `--apply` is set.

### Alarms

All worker alarms publish to the `worker-alarms` topic, which has its own
rotated KMS key so that CloudWatch can encrypt to it. `worker_alarm_email`
subscribes the on-call mailbox, which must confirm the email AWS sends.
`worker_alarm_actions` adds more ARNs. A role's alarms exist exactly when its
principal does.

- **Unit health:** one alarm per unit role, on `Limnopulse/Workers` `UnitHealthy`
  with `Role=<role>` and no unit. It fires when the Maximum over two-minute
  periods is below 1 in 2 of 3 periods. Missing data is breaching, so a stopped
  monitor or a silent host fires too, but a short spell of zeros, which the
  monitor publishes by design, does not.
- **Queues:** age, depth and DLQ alarms for the email jobs, SES events and
  Telegram jobs queues, on their consumer role's enable variable. The thresholds
  are `worker_queue_age_alarm_threshold_seconds` (300) and
  `worker_queue_depth_alarm_threshold` (100); a DLQ alarm fires on any message.
- **SES routing DLQ:** its own alarm, with `email_delivery`. It belongs to no
  role, so its actions are on whenever it exists.

### Removing the shared `workers` user

`aws_iam_user.workers` in `iam_runtime.tf` is deprecated. No Go worker runs under
it in production, and the per-role principals replace it. Remove it once every
enabled role has run under its own principal:

1. Confirm that `aws iam list-access-keys` shows no key for the
   `workers_iam_user_name` output, or that no key was used since the cutover.
2. In one change, remove the user, its four attachments (`workers_dynamodb`,
   `workers_notification_jobs`, `workers_telegram`, `workers_email`), the
   policies no other principal uses (`dynamodb_domain_access_workers`,
   `notification_jobs_access`, `email_worker` in `ses.tf`, `telegram_worker` in
   `telegram.tf`), the `workers_iam_user_name` output, and the `workers` checks in
   `tests/unit/test_iam_runtime_transactions.py`.
