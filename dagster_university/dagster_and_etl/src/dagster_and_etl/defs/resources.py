# src/dagster_and_etl/defs/resources.py
from urllib import response

import dagster as dg
import requests
from dagster_duckdb import DuckDBResource

from pathlib import Path

class NASAResource(dg.ConfigurableResource):
    api_key: str

    def get_near_earth_asteroids(self, start_date: str, end_date: str):
        url = "https://api.nasa.gov/neo/rest/v1/feed"
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "api_key": self.api_key,
        }

        resp = requests.get(url, params=params)
        return resp.json()["near_earth_objects"][start_date]


# Assurua API Resource
class AssuraAPIResource(dg.ConfigurableResource):
    api_url: str
    username: str
    password: str

    def _get_token(self):

        login_url = f"{self.api_url}/System/login"

        response = requests.post(
            login_url,
            data={
                "Username": self.username,
                "Password": self.password,
                "AuthType": "Session",
            },
        )

        # Check if the request was successful
        response.raise_for_status()

        login_response = response.json()

        if login_response["Status"]["StatusCode"] != 1:
            raise Exception(
                f"Assura login failed: {login_response['Status']['Message']}"
            )

        return login_response["Data"]["Token"]

    def get_workflow_list(self):

        token = self._get_token()

        workflow_url = f"{self.api_url}/Workflow/GetWorkflowList"

        response = requests.post(
            workflow_url,
            data={
                "WorkflowType": "Event",
                "Token": token
            },
            timeout=30
        )

        response.raise_for_status()

        return response.json()
    
# Register resources for Dagster
@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "nasa": NASAResource(
                api_key=dg.EnvVar("NASA_API_KEY"),
            ),
            "database": DuckDBResource(
                database="data/staging/data.duckdb",
            ),
            "assura": AssuraAPIResource(
                api_url=dg.EnvVar("ASSURA_API_URL"),
                username=dg.EnvVar("ASSURA_USERNAME"),
                password=dg.EnvVar("ASSURA_PASSWORD"),
            ),
        },
    )