from hibob_client import HiBobClient
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get credentials from environment variables
hibob_service_user = os.getenv("HIBOB_SERVICE_USER")
hibob_token = os.getenv("HIBOB_TOKEN")

# Initialize HiBob client
client = HiBobClient(hibob_service_user, hibob_token)

# Get all employees
print("\n" + "="*80)
print("=== Getting Employee Data ===")
print("="*80)
df_employees = client.get_all_employees([
    "firstName", "surname", "email", "work.department", "work.title"
])
print(df_employees.head())
print(f"\nTotal employees: {df_employees.shape[0]}")
print("="*80)

