from hibob_client import HiBobClient
from azure.storage.blob import BlobServiceClient
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get credentials from environment variables
hibob_service_user = os.getenv("HIBOB_SERVICE_USER")
hibob_token = os.getenv("HIBOB_TOKEN")

azure_account_name = os.getenv("AZURE_STORAGE_ACCOUNT")
azure_account_key = os.getenv("AZURE_STORAGE_KEY")
container_name = os.getenv("AZURE_CONTAINER_NAME")

# Initialize HiBob client
client = HiBobClient(hibob_service_user, hibob_token)

# Initialize Azure client
blob_service_client = BlobServiceClient(account_url=f"https://{azure_account_name}.blob.core.windows.net", credential=azure_account_key)

# Get all employees
print("\n" + "="*80)
print("FETCHING EMPLOYEE DATA FROM HIBOB")
print("="*80)
df_employees = client.get_all_employees([
    "firstName", "surname", "email", "work.department", "work.title"
])
print(f"\nFetched {len(df_employees)} employees")
print(df_employees.head())

# Upload to Azure
print("\n" + "="*80)
print("UPLOADING TO AZURE BLOB STORAGE")
print("="*80)

try:
    blob_name = "hibob/employees_data/employees.csv"
    csv_data = df_employees.to_csv(index=False)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(csv_data, overwrite=True)
    print(f"\n✅ Successfully uploaded to: {blob_name}")
    print(f"   Records: {len(df_employees)}")
except Exception as e:
    print(f"\n❌ Failed to upload: {str(e)}")

print("\n" + "="*80)
