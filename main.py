"""
main.py

This script runs an end-to-end ETL pipeline:
- Ingests EPL team and player data from API
- Uploads raw JSON to AWS S3
- Cleans data
- Uploads cleaned data to PostgreSQL
"""

#needed for relative imports from src/
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

#utility and wrapper imports
from ingestion.ingest_players import fetch_and_store_all_players_raw
from ingestion.ingest_teams import ingest_teams_and_store_all_teams
from ingestion.api_ingestor import APIIngestion

from transformation.clean_players import clean_players
from transformation.clean_teams import clean_teams

from loading.run_pg_upload import run_pg_upload
from loading.postgres_wrapper import PostgresWrapper
from loading.s3_wrapper import S3Wrapper

from utilities.logger import get_logger


# #library imports
# import dotenv
# import pandas as pd

# #setting up logger and loading .env
# logger = get_logger(__name__)
# dotenv.load_dotenv()


# def main():
#     # Instantiate wrappers (api, s3, pg)
#     api = APIIngestion(
#         base_url='https://api.balldontlie.io/',
#         season=2024,
#         headers= {"Authorization": f"Bearer {os.getenv('API_KEY')}"}                                   
#         )

#     s3 = S3Wrapper(
#         aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
#         aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
#         region=os.getenv('AWS_REGION'),
#         bucket='t1-de-prep')
    
#     pg = PostgresWrapper(
#         db_name=os.getenv('POSTGRES_DB'),
#         user=os.getenv('POSTGRES_USER'),
#         password=os.getenv('POSTGRES_PASSWORD'),
#         host='localhost',
#         port=5432
#         )

#     # Ingest teams
#     teams_df = ingest_teams_and_store_all_teams(api, s3)
#     cleaned_teams = clean_teams(teams_df)
#     run_pg_upload(pg, cleaned_teams, "epl_datapipeline.epl_teams")

#     # Ingest players
#     team_ids = cleaned_teams["id"].tolist()
#     players_df = fetch_and_store_all_players_raw(api, s3, team_ids)
#     cleaned_players = clean_players(players_df)
#     run_pg_upload(pg, cleaned_players, "epl_datapipeline.epl_team_players")

#     # Close connection
#     pg.close()
#     logger.info("ETL pipeline completed successfully!")

# if __name__ == "__main__":
#     main()