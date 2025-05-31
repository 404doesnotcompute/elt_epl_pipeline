from utilities.logger import get_logger

logger = get_logger(__name__)


def ingest_and_store_all_team_standings(api,s3):
    """
    This function does two main things:
    
    1.  Gets all team standings via 'epl/v1/standings' endpoint, stores the results in a dataframe.

    2.  Then uploads the raw json from the API call into s3 for storage.
    """
    logger.info("Teams standings starting....")
    try:
        standings_df = api.fetch('epl/v1/standings')
        if standings_df is not None and not standings_df.empty:
            s3.s3_upload_raw_json(
                data=standings_df.to_dict(orient="records"),
                s3_key='raw/json/teams/2024_standings.json',
                include_ts=False
            )
            logger.info(f"Fetched and uploaded all team standings data to s3")
        else:
            logger.warning(f"No team standings returned or DataFrame is empty")
    except Exception as e:
        logger.warning(f"Error processing team standings data: {e}") 
    
    logger.info("Teams ingestion complete!")

    return standings_df
        