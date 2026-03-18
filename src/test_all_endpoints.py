import requests
import base64
import pandas as pd
import json

service_user = os.getenv("HIBOB_SERVICE_USER")
token = os.getenv("HIBOB_TOKEN")
credentials = f"{service_user}:{token}"
encoded_credentials = base64.b64encode(credentials.encode()).decode()

headers = {
    "Authorization": f"Basic {encoded_credentials}",
    "Content-Type": "application/json"
}

base_url = "https://api.hibob.com/v1"

# Dictionary of endpoints to test
endpoints = {
    "People Search": {
        "methods": ["POST"],
        "url": f"{base_url}/people/search",
        "payload": {"fields": ["firstName", "surname", "email"], "page": 1, "pageSize": 10}
    },
    "Departments": {
        "methods": ["GET", "POST"],
        "url": f"{base_url}/company/departments",
        "payload": {}
    },
    "Teams": {
        "methods": ["GET", "POST"],
        "url": f"{base_url}/company/teams",
        "payload": {}
    },
    "Holidays": {
        "methods": ["GET", "POST"],
        "url": f"{base_url}/company/holidays",
        "payload": {}
    },
    "Leave Types": {
        "methods": ["GET", "POST"],
        "url": f"{base_url}/company/leave-types",
        "payload": {}
    },
    "Working Hours": {
        "methods": ["GET", "POST"],
        "url": f"{base_url}/company/working-hours",
        "payload": {}
    },
    "Time Off Requests": {
        "methods": ["GET", "POST"],
        "url": f"{base_url}/timeoff/requests",
        "payload": {}
    },
    "Attendance": {
        "methods": ["GET", "POST"],
        "url": f"{base_url}/attendance",
        "payload": {}
    },
}

# Test each endpoint
results = []

print("\n" + "="*100)
print("HIBOB API ENDPOINT DISCOVERY TEST")
print("="*100)

for endpoint_name, endpoint_config in endpoints.items():
    print(f"\n📌 Testing: {endpoint_name}")
    print(f"   URL: {endpoint_config['url']}")
    
    for method in endpoint_config["methods"]:
        try:
            if method == "GET":
                response = requests.get(endpoint_config["url"], headers=headers, timeout=10)
            else:
                response = requests.post(endpoint_config["url"], headers=headers, json=endpoint_config.get("payload"), timeout=10)
            
            status_code = response.status_code
            status_text = "✅" if status_code == 200 else "❌"
            
            # Try to parse response
            try:
                data = response.json()
                if isinstance(data, dict):
                    keys = list(data.keys())
                    record_count = len(data.get(keys[0], [])) if keys else 0
                elif isinstance(data, list):
                    record_count = len(data)
                else:
                    record_count = "Unknown"
                
                response_preview = f"Keys: {keys[:3]} | Records: {record_count}"
            except:
                response_preview = f"Response: {response.text[:100]}" if response.text else "Empty Body"
            
            result = {
                "Endpoint": endpoint_name,
                "Method": method,
                "Status": f"{status_text} {status_code}",
                "Response Preview": response_preview
            }
            results.append(result)
            
            print(f"   {method:4} → {status_text} {status_code} | {response_preview}")
            
        except Exception as e:
            result = {
                "Endpoint": endpoint_name,
                "Method": method,
                "Status": f"❌ Error",
                "Response Preview": str(e)[:80]
            }
            results.append(result)
            print(f"   {method:4} → ❌ Error: {str(e)[:60]}")

# Summary
print("\n" + "="*100)
print("SUMMARY - WORKING ENDPOINTS (✅200)")
print("="*100)

working = [r for r in results if "✅" in r["Status"]]
if working:
    df = pd.DataFrame(working)
    print(df.to_string(index=False))
else:
    print("No working endpoints found")

print("\n" + "="*100)
print("SUMMARY - FAILED ENDPOINTS (❌)")
print("="*100)

failed = [r for r in results if "❌" in r["Status"]]
if failed:
    df = pd.DataFrame(failed)
    print(df.to_string(index=False))
else:
    print("All endpoints working!")
