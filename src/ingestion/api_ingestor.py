"""
- Handles Ingestion from API and normalization of JSON data
"""
import requests 
from utilities.logger import get_logger
import pandas as pd

logger = get_logger(__name__)

class APIIngestion:
    def __init__(self,base_url,season,headers=None):
        self.base_url = base_url.rstrip('/')
        self.season = str(season)
        self.headers = headers
    
    def build_url(self,endpoint, team_id=None,subresource=None):
        if team_id:
            url = (f"{self.base_url}/{endpoint.strip('/')}/{team_id}")
            if subresource:
                url += f"/{subresource.strip('/')}"
            return url
        return f"{self.base_url}/{endpoint.strip('/')}"      
        
    def fetch(self, endpoint,team_id=None,subresource=None,params=None):
        """
        Builds full request URL and returns normalized DataFrame from API response.
        Example: endpoint='epl/v1'
        """
        logger.info("API Ingestion started...")
        url = self.build_url(endpoint,team_id, subresource)

        if params is None:
            params = {}
        if "season" not in params:
            params["season"] = self.season 

        logger.debug(f"Request URL: {url}")
        
        try:
            response = requests.get(url, headers = self.headers, params=params)

            if response.status_code == 200:
                logger.info("API Validation Successful!")
                data = response.json()

                if "season_stats" in (subresource or ""):
                    logger.debug(f"Full season_stats response: {data}")
                    items = data.get("data", [])
                    if isinstance(items, list):
                        stats_row = {item["name"]: item["value"] for item in items}
                        stats_row["team_id"] = team_id
                        stats_row["season"] = self.season 
                        return pd.DataFrame([stats_row])  
                    else:
                        logger.warning("Unexpected format in season_stats data.")
                        return None
                return pd.json_normalize(data["data"])
                            
            else:
                logger.warning(f"Error: {response.status_code}")
                logger.warning(f"Response Body: {response.text}")
        except Exception as e:
            logger.exception(f"Exception during API ingestion: {e}")

        logger.info("API Ingestion Complete!")
        return None

