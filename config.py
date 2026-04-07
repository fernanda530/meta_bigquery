import os
from dotenv import load_dotenv

load_dotenv()

# META
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
META_AD_ACCOUNT_ID = os.getenv("META_AD_ACCOUNT_ID")
META_API_VERSION = os.getenv("META_API_VERSION", "v25.0")
META_FIELDS = os.getenv(
    "META_FIELDS",
    "date_start,date_stop,campaign_id,campaign_name,status,effective_status,objective,ad_id,ad_name,spend,actions,cost_per_action_type"
)

META_BREAKDOWNS = os.getenv("META_BREAKDOWNS", "age,gender")
META_LEVEL = os.getenv("META_LEVEL", "ad")
META_DATE_PRESET = os.getenv("META_DATE_PRESET", "yesterday")
META_RESULT_ACTION_TYPE = os.getenv("META_RESULT_ACTION_TYPE")

# BIGQUERY
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")


# CONTROL
MODO_PRUEBA = os.getenv("MODO_PRUEBA", "True").lower() == "true"
EXPORTAR_EXCEL_PREVIEW = os.getenv("EXPORTAR_EXCEL_PREVIEW", "True").lower() == "true"

GOOGLE_SHEETS_SPREADSHEET_ID = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
ACTUALIZAR_GOOGLE_SHEETS = os.getenv("ACTUALIZAR_GOOGLE_SHEETS", "False").lower() == "true"
