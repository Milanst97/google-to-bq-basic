import os
import logging
from datetime import date, timedelta

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("google-ads-to-bq")

API_VERSION = os.environ.get("GOOGLE_ADS_API_VERSION", "v23")
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "30"))
BACKFILL_START = os.environ.get("BACKFILL_START")
BACKFILL_END = os.environ.get("BACKFILL_END")

BQ_PROJECT = os.environ["BQ_PROJECT"]
BQ_DATASET = os.environ["BQ_DATASET"]
CAMPAIGN_TABLE = os.environ.get("BQ_CAMPAIGN_TABLE", "google_ads_campaign_daily")
CONVERSIONS_TABLE = os.environ.get("BQ_CONVERSIONS_TABLE", "google_ads_conversions_daily")

CLIENT_CUSTOMER_IDS = [
    c.strip() for c in os.environ["CLIENT_CUSTOMER_IDS"].split(",") if c.strip()
]

SOURCE_VALUE = "google"

CAMPAIGN_SCHEMA = [
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("customer_id", "STRING"),
    bigquery.SchemaField("campaign_id", "STRING"),
    bigquery.SchemaField("campaign_name", "STRING"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("cost", "FLOAT"),
    bigquery.SchemaField("impressions", "INTEGER"),
    bigquery.SchemaField("clicks", "INTEGER"),
    bigquery.SchemaField("source", "STRING"),
]

CONVERSIONS_SCHEMA = [
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("customer_id", "STRING"),
    bigquery.SchemaField("campaign_id", "STRING"),
    bigquery.SchemaField("campaign_name", "STRING"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("conversion_event", "STRING"),
    bigquery.SchemaField("conversions", "FLOAT"),
    bigquery.SchemaField("conversions_value", "FLOAT"),
    bigquery.SchemaField("all_conversions", "FLOAT"),
    bigquery.SchemaField("all_conversions_value", "FLOAT"),
    bigquery.SchemaField("source", "STRING"),
]


def build_ads_client():
    config = {
        "developer_token": os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"],
        "client_id": os.environ["GOOGLE_ADS_CLIENT_ID"],
        "client_secret": os.environ["GOOGLE_ADS_CLIENT_SECRET"],
        "refresh_token": os.environ["GOOGLE_ADS_REFRESH_TOKEN"],
        "login_customer_id": os.environ["GOOGLE_ADS_LOGIN_CUSTOMER_ID"],
        "use_proto_plus": True,
    }
    return GoogleAdsClient.load_from_dict(config, version=API_VERSION)


def fetch_campaign_rows(ads_client, customer_id, start_date, end_date):
    ga_service = ads_client.get_service("GoogleAdsService")
    query = f"""
        SELECT
          segments.date,
          campaign.id,
          campaign.name,
          customer.currency_code,
          metrics.cost_micros,
          metrics.impressions,
          metrics.clicks
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
          AND metrics.impressions > 0
    """
    rows = []
    for batch in ga_service.search_stream(customer_id=customer_id, query=query):
        for r in batch.results:
            rows.append({
                "date": r.segments.date,
                "customer_id": customer_id,
                "campaign_id": str(r.campaign.id),
                "campaign_name": r.campaign.name,
                "currency": r.customer.currency_code,
                "cost": r.metrics.cost_micros / 1_000_000,
                "impressions": int(r.metrics.impressions),
                "clicks": int(r.metrics.clicks),
                "source": SOURCE_VALUE,
            })
    return rows


def fetch_conversion_rows(ads_client, customer_id, start_date, end_date):
    ga_service = ads_client.get_service("GoogleAdsService")
    query = f"""
        SELECT
          segments.date,
          campaign.id,
          campaign.name,
          customer.currency_code,
          segments.conversion_action_name,
          metrics.conversions,
          metrics.conversions_value,
          metrics.all_conversions,
          metrics.all_conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
          AND metrics.all_conversions > 0
    """
    rows = []
    for batch in ga_service.search_stream(customer_id=customer_id, query=query):
        for r in batch.results:
            rows.append({
                "date": r.segments.date,
                "customer_id": customer_id,
                "campaign_id": str(r.campaign.id),
                "campaign_name": r.campaign.name,
                "currency": r.customer.currency_code,
                "conversion_event": r.segments.conversion_action_name,
                "conversions": float(r.metrics.conversions),
                "conversions_value": float(r.metrics.conversions_value),
                "all_conversions": float(r.metrics.all_conversions),
                "all_conversions_value": float(r.metrics.all_conversions_value),
                "source": SOURCE_VALUE,
            })
    return rows


def ensure_table(bq_client, table_id, schema):
    try:
        bq_client.get_table(table_id)
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        if any(f.name == "date" for f in schema):
            table.time_partitioning = bigquery.TimePartitioning(field="date")
        bq_client.create_table(table)
        logger.info("Created table %s", table_id)


def write_window(bq_client, table_name, schema, rows, start_date, end_date, customer_ids):
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"
    ensure_table(bq_client, table_id, schema)

    delete_sql = f"""
        DELETE FROM `{table_id}`
        WHERE source = @source
          AND date BETWEEN @start_date AND @end_date
          AND customer_id IN UNNEST(@customer_ids)
    """
    bq_client.query(
        delete_sql,
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("source", "STRING", SOURCE_VALUE),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            bigquery.ArrayQueryParameter("customer_ids", "STRING", list(customer_ids)),
        ]),
    ).result()

    if not rows:
        logger.info("%s: no rows to insert", table_name)
        return 0

    bq_client.load_table_from_json(
        rows, table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=schema,
        ),
    ).result()
    logger.info("%s: inserted %d rows", table_name, len(rows))
    return len(rows)


def run_job():
    end_date = date.today() - timedelta(days=1)
    if BACKFILL_END:
        end_date = min(date.fromisoformat(BACKFILL_END), end_date)
    if BACKFILL_START:
        start_date = date.fromisoformat(BACKFILL_START)
    else:
        start_date = date.today() - timedelta(days=LOOKBACK_DAYS)
    start_str, end_str = start_date.isoformat(), end_date.isoformat()

    ads_client = build_ads_client()
    bq_client = bigquery.Client(project=BQ_PROJECT)

    campaign_rows, conversion_rows, errors = [], [], {}
    pulled_ids = []
    for customer_id in CLIENT_CUSTOMER_IDS:
        try:
            campaign_rows.extend(
                fetch_campaign_rows(ads_client, customer_id, start_str, end_str))
            conversion_rows.extend(
                fetch_conversion_rows(ads_client, customer_id, start_str, end_str))
            pulled_ids.append(customer_id)
            logger.info("Customer %s pulled", customer_id)
        except GoogleAdsException as e:
            errors[customer_id] = e.error.code().name
            logger.exception("Failed for customer %s", customer_id)

    inserted_campaign = write_window(
        bq_client, CAMPAIGN_TABLE, CAMPAIGN_SCHEMA, campaign_rows, start_date, end_date, pulled_ids)
    inserted_conv = write_window(
        bq_client, CONVERSIONS_TABLE, CONVERSIONS_SCHEMA, conversion_rows, start_date, end_date, pulled_ids)

    logger.info(
        "Done. window=%s..%s campaign_rows=%d conversion_rows=%d clients=%d errors=%s",
        start_str, end_str, inserted_campaign, inserted_conv,
        len(CLIENT_CUSTOMER_IDS), errors or "none",
    )


if __name__ == "__main__":
    run_job()
