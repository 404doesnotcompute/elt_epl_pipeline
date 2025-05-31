from utilities.logger import get_logger

logger = get_logger(__name__)

def run_pg_upload(pg,df,table):
    try:
        if df is None or df.empty:
            logger.warning("Input DataFrame is empty or None. Skipping upload.")
            return False
        
        success = pg.copy_from_df(df,table)

        if success:
            logger.info(f"Data successfully uploaded to: {table}")
        else:
            logger.warning(f"No data uploaded to: {table}")

        return success
    
    except Exception as e:
        logger.exception(f"Teams was not uploaded to: {table}.")
        return False
