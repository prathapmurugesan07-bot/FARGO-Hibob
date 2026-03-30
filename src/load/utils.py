from __future__ import annotations

from datetime import datetime
from io import BytesIO
import os
import re
from typing import Dict

import pandas as pd
from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv

from extract import HiBobClient

load_dotenv()

DEFAULT_RAW_CONTAINER_NAME = "raw"
DEFAULT_HIBOB_CONTAINER_NAME = "hibob"
EMPLOYEE_BLOB_PREFIX = "hibob/employees_data"
EMPLOYEE_FIELDS = [
    "id",
    "firstName",
    "surname",
    "email",
    "work.department",
    "work.title",
    "/work/department.value",
    "/work/title.value",
]


def get_required_env_var(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value.strip()


def choose_container(env_var: str, default: str) -> str:
    value = os.getenv(env_var)
    if value:
        return value.strip()
    fallback = os.getenv("AZURE_CONTAINER_NAME")
    if fallback:
        return fallback.strip()
    return default


def create_run_context() -> Dict[str, str]:
    started = datetime.utcnow()
    return {
        "year": started.strftime("%Y"),
        "month": started.strftime("%m"),
        "day": started.strftime("%d"),
        "run_time": started.strftime("%Y%m%d_%H%M%S"),
    }


def build_blob_prefix(run_context: Dict[str, str]) -> str:
    return (
        f"{EMPLOYEE_BLOB_PREFIX}/{run_context['year']}/{run_context['month']}/"
        f"{run_context['day']}"
    )


def build_blob_name(run_context: Dict[str, str], suffix: str) -> str:
    return f"{build_blob_prefix(run_context)}/{suffix}"


def normalize_column_name(column_name: object) -> str:
    normalized = str(column_name).replace(".", "_").replace("/", "_")
    normalized = re.sub(r"[^a-zA-Z0-9_]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.lower().strip("_")


def transform_for_hibob(dataframe: pd.DataFrame) -> pd.DataFrame:
    staged = dataframe.copy()
    staged.columns = [normalize_column_name(name) for name in staged.columns]
    return staged


def create_blob_service_client(account: str, key: str) -> BlobServiceClient:
    return BlobServiceClient(
        account_url=f"https://{account}.blob.core.windows.net",
        credential=key,
    )


def prepare_resources() -> Dict[str, object]:
    hibob_service_user = get_required_env_var("HIBOB_SERVICE_USER")
    hibob_token = get_required_env_var("HIBOB_TOKEN")
    azure_account_name = get_required_env_var("AZURE_STORAGE_ACCOUNT")
    azure_account_key = get_required_env_var("AZURE_STORAGE_KEY")

    return {
        "client": HiBobClient(hibob_service_user, hibob_token),
        "blob_service_client": create_blob_service_client(azure_account_name, azure_account_key),
        "raw_container": choose_container("AZURE_RAW_CONTAINER_NAME", DEFAULT_RAW_CONTAINER_NAME),
        "hibob_container": choose_container("AZURE_HIBOB_CONTAINER_NAME", DEFAULT_HIBOB_CONTAINER_NAME),
    }


def fetch_employee_data(client: HiBobClient) -> pd.DataFrame:
    print("Fetching employee data from HiBob")
    dataframe = client.get_all_employees(EMPLOYEE_FIELDS)
    print(f"Fetched {len(dataframe)} rows")
    return dataframe


def upload_dataframe(
    dataframe: pd.DataFrame,
    blob_service_client: BlobServiceClient,
    container_name: str,
    blob_name: str,
    label: str,
) -> None:
    if dataframe is None or dataframe.empty:
        print(f"{label} is empty; skipping upload")
        return

    print(f"Uploading {label} to {container_name}/{blob_name}")
    csv_bytes = dataframe.to_csv(index=False).encode("utf-8")
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(
        BytesIO(csv_bytes),
        length=len(csv_bytes),
        overwrite=True,
        content_settings=ContentSettings(content_type="text/csv; charset=utf-8"),
    )
    print(f"Finished uploading {label} ({len(dataframe)} rows)")


def run_azure_raw_ingestion(
    resources: Dict[str, object] | None = None,
    run_context: Dict[str, str] | None = None,
    dataframe: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, Dict[str, str]]:
    resources = resources or prepare_resources()
    client: HiBobClient = resources["client"]
    blob_service_client: BlobServiceClient = resources["blob_service_client"]
    raw_container: str = resources["raw_container"]

    df = dataframe or fetch_employee_data(client)
    context = run_context or create_run_context()
    blob_name = build_blob_name(context, f"employees_data_{context['run_time']}.csv")
    upload_dataframe(
        df,
        blob_service_client,
        raw_container,
        blob_name,
        label="raw employee snapshot",
    )
    return df, context


def run_azure_staging_ingestion(
    resources: Dict[str, object] | None = None,
    run_context: Dict[str, str] | None = None,
    dataframe: pd.DataFrame | None = None,
) -> pd.DataFrame:
    resources = resources or prepare_resources()
    client: HiBobClient = resources["client"]
    blob_service_client: BlobServiceClient = resources["blob_service_client"]
    hibob_container: str = resources["hibob_container"]

    df = dataframe or fetch_employee_data(client)
    context = run_context or create_run_context()
    normalized = transform_for_hibob(df)
    blob_name = build_blob_name(context, f"employees_transformed_{context['run_time']}.csv")
    upload_dataframe(
        normalized,
        blob_service_client,
        hibob_container,
        blob_name,
        label="normalized employee snapshot",
    )
    return normalized


def run_azure_raw_and_staging_ingestion() -> None:
    resources = prepare_resources()
    df, context = run_azure_raw_ingestion(resources=resources)

    run_azure_staging_ingestion(
        resources=resources,
        run_context=context,
        dataframe=df,
    )
