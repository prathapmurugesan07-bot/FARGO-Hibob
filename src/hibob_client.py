import requests
import base64
import pandas as pd
import time
import logging

class HiBobClient:
    """
    A reusable HiBob API client with:
    - Pagination Support
    - Retry Logic
    - Json -> pandas.DataFrame conversion
    """

    def __init__(self,service_user:str,token:str,base_url:str="https://api.hibob.com/v1"):
        self.base_url = base_url

        #Encode Credentials
        credentials = f"{service_user}:{token}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()

        self.headers = {
            "Authorization":f"Basic {encoded_credentials}",
            "Content-Type": "application/json"
        }

        # Setup simple logging
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
        self.logger = logging.getLogger(__name__)
    
    def _request(self, method:str, endpoint:str, payload:dict=None, params:dict=None, retries:int=3)->dict:
        """
        Generic request handler
        """
        url=f"{self.base_url}{endpoint}"

        for attempt in range(retries):
            try:
                response=requests.request(
                    method=method,
                    url=url,
                    headers=self.headers,
                    json=payload,
                    params=params,
                    timeout=30
                )
                if response.status_code==200:
                    return response.json()
                
                elif response.status_code==429:
                    self.logger.warning(f"Rate limited. Retrying in {2 ** attempt} seconds...")
                    time.sleep(2 ** attempt)

                else:
                    self.logger.error(f"Request failed [{response.status_code}]: {response.text}")
                    time.sleep(2)
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request exception: {e}")
                time.sleep(2)
        raise Exception(f"Failed after {retries} retries for endpoint: {endpoint}")
    
    def paginate(self, endpoint: str, payload: dict = None, data_key: str = "employees") -> list:
        """
        Generic pagination for endpoints that support page & pageSize
        """
        all_data = []
        page = 1

        while True:
            self.logger.info(f"Fetching page {page} for endpoint {endpoint}...")

            if payload is None:
                payload = {}

            payload.update({
                "page": page,
                "pageSize": 200  # safe max page size
            })

            data = self._request("POST", endpoint, payload=payload)
            batch = data.get(data_key, [])

            if not batch:
                break

            all_data.extend(batch)

            # Stop if last page
            if len(batch) < payload["pageSize"]:
                break

            page += 1

        return all_data

    def get_all_employees(self, fields: list) -> pd.DataFrame:
        """
        Fetch all employees with selected fields
        """
        payload = {"fields": fields}
        data = self.paginate("/people/search", payload=payload, data_key="employees")
        return pd.json_normalize(data)


