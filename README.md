# google-ads-to-bq (basic template)

Generic Google Ads to BigQuery pipeline. Pulls campaign spend and conversions for the configured accounts and writes them to date-partitioned BigQuery tables. Runs as a Cloud Run Job triggered by Cloud Scheduler.

This is the base template: no client-specific logic. Copy it into a new client folder and extend as needed. Common extensions for client-specific versions include campaign-name location mapping, account-level derivations, a geo/DMA table with resolved metro names, and USD currency conversion.

## What it does

Each run pulls a rolling lookback window (default 30 days, ending yesterday) for every account in `CLIENT_CUSTOMER_IDS` and writes two tables. The write is an idempotent delete-then-reinsert scoped to the accounts pulled this run, so re-running refreshes the window without duplicating rows.

## Tables

Both live in `${BQ_PROJECT}.${BQ_DATASET}` and are partitioned on `date`.

| Table | Grain | Notes |
|---|---|---|
| `google_ads_campaign_daily` | date x campaign | cost, impressions, clicks. Filtered to `impressions > 0`. |
| `google_ads_conversions_daily` | date x campaign x conversion action | optimized and all-conversion counts and values. Filtered to `all_conversions > 0`. |

Shared columns: `source` (always `google`), `customer_id` (leaf account), `currency` (account currency code, no conversion applied).

The two tables are split deliberately: spend metrics segmented by conversion action would duplicate, so cost lives in the campaign table and conversion metrics live in their own table at their own grain.

## Conversions: optimized vs all

`conversions` / `conversions_value` count only conversion actions flagged "Include in Conversions" (what bidding optimizes toward). `all_conversions` / `all_conversions_value` include every conversion action regardless of that setting, so they read higher. Both are stored.

## Adding / removing accounts

Edit `CLIENT_CUSTOMER_IDS` and redeploy. The delete is scoped to accounts actually pulled in the run, so:

- Add an ID: pulled and refreshed from the next run. Run a backfill if you need its history.
- Remove an ID: no longer pulled; its existing data is left untouched.
- An account failing with `GoogleAdsException` is logged, skipped, and its window preserved; the rest of the run continues.

Customer IDs are 10 digits, no dashes. All accounts are queried through the top MCC via `GOOGLE_ADS_LOGIN_CUSTOMER_ID`; intermediate MCCs do not need to be named.

## Configuration

### Required env vars

| Var | Value |
|---|---|
| `BQ_PROJECT` | GCP project holding the dataset |
| `BQ_DATASET` | client dataset |
| `CLIENT_CUSTOMER_IDS` | comma-separated leaf account IDs |
| `GOOGLE_ADS_LOGIN_CUSTOMER_ID` | top MCC, 10 digits no dashes |

### Optional env vars (defaults shown)

| Var | Default |
|---|---|
| `GOOGLE_ADS_API_VERSION` | `v23` |
| `LOOKBACK_DAYS` | `30` |
| `BACKFILL_START` | unset; `YYYY-MM-DD` start for a one-off backfill |
| `BACKFILL_END` | unset; `YYYY-MM-DD` end for chunked backfills, capped at yesterday |
| `BQ_CAMPAIGN_TABLE` | `google_ads_campaign_daily` |
| `BQ_CONVERSIONS_TABLE` | `google_ads_conversions_daily` |

### Secrets (Secret Manager)

`GOOGLE_ADS_DEVELOPER_TOKEN`, `GOOGLE_ADS_CLIENT_ID`, `GOOGLE_ADS_CLIENT_SECRET`, `GOOGLE_ADS_REFRESH_TOKEN`.

The developer token quota (Basic access: 15,000 operations/day) is shared across every pipeline using the token. This job uses 2 operations per account per run.

## Deploy

Run from inside the client folder. Replace the job name, dataset, and IDs per client.

```bash
cd google-to-bq-<client>

gcloud run jobs deploy google-to-bq-<client> \
  --source . \
  --region <REGION> \
  --project <PROJECT_ID> \
  --service-account <SERVICE_ACCOUNT_EMAIL> \
  --set-env-vars "^##^BQ_PROJECT=<PROJECT_ID>##BQ_DATASET=<dataset>##CLIENT_CUSTOMER_IDS=<comma separated IDs>##GOOGLE_ADS_LOGIN_CUSTOMER_ID=<top MCC>" \
  --set-secrets "GOOGLE_ADS_DEVELOPER_TOKEN=GOOGLE_ADS_DEVELOPER_TOKEN:latest,GOOGLE_ADS_CLIENT_ID=GOOGLE_ADS_CLIENT_ID:latest,GOOGLE_ADS_CLIENT_SECRET=GOOGLE_ADS_CLIENT_SECRET:latest,GOOGLE_ADS_REFRESH_TOKEN=GOOGLE_ADS_REFRESH_TOKEN:latest"
```

The `^##^` delimiter lets `CLIENT_CUSTOMER_IDS` hold a comma-separated list without breaking the flag. Env vars and secrets persist across deploys; only pass them when changing them.

## Run

```bash
gcloud run jobs execute google-to-bq-<client> --region <REGION> --project <PROJECT_ID>
```

## Backfill

Execution-scoped overrides; they do not persist into the job config. For large ranges, chunk by year and run sequentially with `--wait`:

```bash
gcloud run jobs execute google-to-bq-<client> --region <REGION> --project <PROJECT_ID> --wait \
  --update-env-vars BACKFILL_START=2022-01-01,BACKFILL_END=2022-12-31
```

A failed chunk is safe to rerun; the scoped delete makes each chunk idempotent. After backfilling, confirm the vars did not persist:

```bash
gcloud run jobs describe google-to-bq-<client> --region <REGION> --project <PROJECT_ID> \
  --format "value(template.template.containers[0].env)"
```

If `BACKFILL_START`/`BACKFILL_END` appear, remove them with `gcloud run jobs update ... --remove-env-vars BACKFILL_START,BACKFILL_END`.

## Schedule

OAuth against the Cloud Run Admin API v2 (not OIDC):

```bash
gcloud scheduler jobs create http google-to-bq-<client>-daily \
  --location <REGION> \
  --project <PROJECT_ID> \
  --schedule "<CRON_SCHEDULE>" \
  --time-zone "<TIME_ZONE>" \
  --uri "https://run.googleapis.com/v2/projects/<PROJECT_ID>/locations/<REGION>/jobs/google-to-bq-<client>:run" \
  --http-method POST \
  --oauth-service-account-email <SERVICE_ACCOUNT_EMAIL> \
  --oauth-token-scope "https://www.googleapis.com/auth/cloud-platform"
```

Force a test trigger: `gcloud scheduler jobs run google-to-bq-<client>-daily --location <REGION> --project <PROJECT_ID>`.

## IAM

The service account needs:

- `roles/run.invoker` on the job (Scheduler trigger)
- `roles/bigquery.dataEditor` on the dataset and `roles/bigquery.jobUser` on the project
- `roles/secretmanager.secretAccessor` on the four secrets

## Extending for a client

Common additions:

- **Location from campaign name**: add a `derive_location()` (token map or ordered rule chain), a `location` column to each schema and row dict.
- **Geo / DMA table**: add `fetch_geo_rows()` against `geographic_view` plus the metro-name lookup via the `geo_target_constant` resource. Filter `location_type` to one value in aggregations or spend double counts.
- **USD conversion**: add the Frankfurter-based `fetch_fx_to_usd()` / `add_usd_columns()` pair and `_usd` schema columns. Send a custom User-Agent and use the v1 URL format.
- **Account-level derivations** (e.g. store_type): pull `customer.descriptive_name` in the queries and derive in the row dict.

## Notes

- Tables auto-create on first run (date-partitioned when the schema has a `date` field). Schema changes to existing tables are not migrated automatically; add columns with `ALTER TABLE` or recreate.
- Conversion metrics keep attributing for days after the click; the rolling 30-day window exists to capture that, so recent days' conversions creep up across runs by design.
- Cloud Run job defaults (10-minute timeout, modest memory) suit daily runs. For heavy backfills, raise `--task-timeout` and `--memory`, or chunk by year. An OOM'd or timed-out run is safe to rerun.
