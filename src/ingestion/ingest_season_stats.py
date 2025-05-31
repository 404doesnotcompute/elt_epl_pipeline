from utilities.logger import get_logger

logger = get_logger(__name__)
import pandas as pd

def ingest_and_store_all_team_stats(api,s3,team_ids):
    """
    This function does two main things:
    
    1.  Gets all team stats via 'epl/v1/teams' endpoint, loops through each team_id and then 
        stores the results in a dataframe.

    2.  Then uploads the raw json from the API call into s3 for storage.

    """
    try:
        all_stats = []
        for team_id in team_ids:
            stats_df = api.fetch("epl/v1/teams", team_id=team_id, subresource = "season_stats")
            if stats_df is not None and not stats_df.empty:
                stats_df_only = stats_df.copy()
                stats_df_only["team_id"] = team_id
                all_stats.append(stats_df_only)
                               
                s3.s3_upload_raw_json(
                    data=stats_df.to_dict(orient="records"),
                    s3_key=f"raw/json/epl/teams_stats/{team_id}_stats.json",
                    include_ts=False
                )
                logger.info(f"Fetched and uploaded team data for team ID: {team_id}")
            else:
                logger.warning(f"No data returned to team ID: {team_id}")
            # time.sleep(12)  # uncomment this line to obey free-tier API limit 

    except Exception as e:
        logger.exception(f"Error during team stats ingestion loop: {e}")
       
    
    if not all_stats:
        logger.warning("No team stats data was fetched.")
        return pd.DataFrame()
    
    logger.info("Team stats ingestion complete!")
    return pd.concat(all_stats, ignore_index=True)
