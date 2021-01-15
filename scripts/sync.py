import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import json
import os
import urllib
import pprint

# AUTH MODE – LOGIN\PASSWORD (USERS AND WEB SERVICES KEY)
USERNAME = os.getenv("USERNAME")  # username
SERVICE_KEY = os.getenv("SERVICE_KEY")   # Service KEY
BASE_URL = os.getenv("BASE_URL", "https://api.businesscentral.dynamics.com/v2.0")
ENDPOINT = os.getenv("ENDPOINT")
SERVICE = os.getenv("SERVICE")
COMPANIES = os.getenv("COMPANIES", "")

def main():
    data = []
    for company in get_companies():
        res = requests.get(
            get_url(company),
            auth=HTTPBasicAuth(USERNAME, SERVICE_KEY),
            headers={
                "OData-MaxVersion": "4.0",
                "OData-Version": "4.0",
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8",
                "Prefer": "odata.include-annotations=OData.Community.Display.V1.FormattedValue"
            }
        )
        res_data = res.json().get("value")
        df = pd.DataFrame.from_dict(res_data)
        df["company"] = company
        if len(list(df.T.to_dict().values())) > 0:
            data = data + list(df.T.to_dict().values())
    pprint.pprint(data)



def get_url(company):
    return f"{BASE_URL}/{ENDPOINT}/Company('{urllib.parse.quote(company)}')/{SERVICE}"

def get_companies():
    return COMPANIES.split(",")

if __name__ == "__main__":
    main()
