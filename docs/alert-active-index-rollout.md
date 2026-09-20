# Active alert index rollout

The active-alert GSI is sparse, so existing `open` and `acknowledged` rows do
not appear until their `GSI3PK` and `GSI3SK` attributes have been written.
Run this rollout after the table/index deployment and before enabling the API
or frontend active-alert view for a tenant:

1. Deploy the table and `alert-evaluator` writer changes.
2. For each explicit tenant, run a dry-run and inspect its count:

   ```sh
   alert-evaluator backfill-active-alert-index --tenant-id tnt_123
   ```

3. Apply the backfill for that tenant, repeating with `--limit` when operating
   in batches:

   ```sh
   alert-evaluator backfill-active-alert-index --tenant-id tnt_123 --apply
   ```

4. Re-run the dry-run until it reports zero pending updates, then enable the
   active-alert API/frontend path for that tenant.

The command is dry-run by default, requires explicit tenant IDs, paginates the
historical query, and uses conditional idempotent updates. It is safe to rerun
after interruptions. Do not switch a tenant's consumers to the active index
before its backfill reaches zero pending updates.
