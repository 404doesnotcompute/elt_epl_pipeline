from utilities.logger import get_logger

logger = get_logger(__name__)
import pandas as pd
import time 

def ingest_and_store_all_fixtures(api,s3):
    """
    This function does two main things:
    
    1.  Gets all team fixtures via 'epl/v1/games' endpoint, loops through each week(38 total) and then 
        stores the results in a dataframe.

    2.  Then uploads the raw json from the API call into s3 for storage.
    """
    try:
        all_fixtures = []
        for week in range(1,39):
            fixtures_df = api.fetch("epl/v1/games",params={"week": week})
            if fixtures_df is not None and not fixtures_df.empty:
                all_fixtures.append(fixtures_df)
                               
                s3.s3_upload_raw_json(
                    data=fixtures_df.to_dict(orient="records"),
                    s3_key=f"raw/json/epl/fixtures/week_{week}.json",
                    include_ts=False
                )
                logger.info(f"Fetched and uploaded fixture data for week: {week}")
            else:
                logger.warning(f"No data returned for week: {week}")
            # time.sleep(12)  # obey free-tier API limit #uncomment this line if you are using the free tier

    except Exception as e:
        logger.exception(f"Error during fixture ingestion loop: {e}")
       
    
    if not all_fixtures:
        logger.warning("No fixture data was fetched for any week.")
        return pd.DataFrame()
    
    logger.info("Fixture ingestion complete!")
    return pd.concat(all_fixtures, ignore_index=True)