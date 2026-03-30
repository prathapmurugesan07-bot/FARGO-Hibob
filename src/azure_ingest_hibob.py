from __future__ import annotations

from datetime import datetime
from io import BytesIO
import os
import re

from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv

from hibob_client import HiBobClient

load_dotenv()

DEFAULT_RAW_CONTAINER_NAME = "raw"
DEFAULT_HIBOB_CONTAINER_NAME = "hibob"
EMPLOYEE_BLOB_PREFIX = "hibob/employees_data"


def get_required_env_var(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def normalize_column_name(column_name: object) -> str:
    normalized = str(column_name).replace(".", "_")
    normalized = re.sub(r"[^a-zA-Z0-9_]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.lower()


def transform_for_hibob(dataframe):
    transformed = dataframe.copy()
    transformed.columns = [normalize_column_name(name) for name in transformed.columns]
    return transformed


def create_run_context(started_at: datetime | None = None) -> dict[str, str]:
    started = started_at or datetime.utcnow()
    return {
        "year": started.strftime("%Y"),
        "month": started.strftime("%m"),
        "day": started.strftime("%d"),
        "run_time": started.strftime("%Y%m%d_%H%M%S"),
    }


def build_blob_name(run_context: dict[str, str], suffix: str) -> str:
    return (
        f"{EMPLOYEE_BLOB_PREFIX}/{run_context['year']}/{run_context['month']}/{run_context['day']}/"
        f"{suffix}"
    )


def upload_dataframe(
    dataframe,
    blob_service_client: BlobServiceClient,
    container_name: str,
    blob_name: str,
    label: str,
) -> None:
    if dataframe is None or dataframe.empty:
        print(f"⚠️ {label} is empty; skipping upload for {blob_name}")
        return

    csv_bytes = dataframe.to_csv(index=False).encode("utf-8")
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(
        BytesIO(csv_bytes),
        length=len(csv_bytes),
        overwrite=True,
        content_settings=ContentSettings(content_type="text/csv; charset=utf-8"),
    )
    print(f"✅ {label} uploaded to {container_name}/{blob_name} ({len(dataframe)} rows)")


def main() -> None:
    hibob_service_user = get_required_env_var("HIBOB_SERVICE_USER")
    hibob_token = get_required_env_var("HIBOB_TOKEN")
    azure_account_name = get_required_env_var("AZURE_STORAGE_ACCOUNT")
    azure_account_key = get_required_env_var("AZURE_STORAGE_KEY")

    raw_container_name = os.getenv("AZURE_RAW_CONTAINER_NAME") or DEFAULT_RAW_CONTAINER_NAME
    hibob_container_name = os.getenv("AZURE_HIBOB_CONTAINER_NAME") or DEFAULT_HIBOB_CONTAINER_NAME

    client = HiBobClient(hibob_service_user, hibob_token)
    blob_service_client = BlobServiceClient(
        account_url=f"https://{azure_account_name}.blob.core.windows.net",
        credential=azure_account_key,
    )

    print("\n" + "=" * 80)
    print("FETCHING EMPLOYEE DATA FROM HIBOB")
    print("=" * 80)
    df_employees = client.get_all_employees(
        ["firstName", "surname", "email", "work.department", "work.title"]
    )
    print(f"\nFetched {len(df_employees)} employees")
    print(df_employees.head())

    run_context = create_run_context()

    raw_blob_name = build_blob_name(run_context, f"employees_data_{run_context['run_time']}.csv")
    transformed_blob_name = build_blob_name(run_context, f"employees_transformed_{run_context['run_time']}.csv")

    print("\n" + "=" * 80)
    print("UPLOADING ORIGINAL EMPLOYEES CSV")
    print("=" * 80)
    upload_dataframe(
        df_employees,
        blob_service_client,
        raw_container_name,
        raw_blob_name,
        label="original employee snapshot",
    )

    normalized = transform_for_hibob(df_employees)
    print("\n" + "=" * 80)
    print("UPLOADING NORMALIZED EMPLOYEES CSV")
    print("=" * 80)
    upload_dataframe(
        normalized,
        blob_service_client,
        hibob_container_name,
        transformed_blob_name,
        label="normalized employee snapshot",
    )


if __name__ == "__main__":
    main()
