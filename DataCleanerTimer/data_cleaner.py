import logging
import azure.functions as func
from shared.db_utils import get_db_connection, db_session
from shared.email_utils import send_email_alert

bp = func.Blueprint()

@bp.timer_trigger(arg_name="timer", schedule="0 0 2 * * 0")
def DataCleaner(timer: func.TimerRequest) -> None:
    logging.info("DataCleaner timer trigger started.")
    
    try:
        # SQL logic to delete records outside the most recent 200 per coin
        # using a Common Table Expression (CTE) with ROW_NUMBER()
        cleanup_sql = """
            WITH CTE AS (
                SELECT 
                    RecordID,
                    ROW_NUMBER() OVER (
                        PARTITION BY CoinSymbol 
                        ORDER BY PriceDateTime DESC
                    ) as RowNum
                FROM FourHour
            )
            DELETE FROM FourHour 
            WHERE RecordID IN (
                SELECT RecordID FROM CTE WHERE RowNum > 200
            )
        """
        
        with db_session() as cursor:
            cursor.execute(cleanup_sql)
            deleted_count = cursor.rowcount
        
        logging.info(f"Database cleanup complete. Deleted {deleted_count} records.")
        
        # Send notification
        subject = "Database Cleanup Complete"
        body = f"The weekly DataCleaner function has successfully run.\n\nTotal records removed from FourHour table: {deleted_count}\nEach coin now maintains a maximum of 200 most recent records."
        send_email_alert(subject, body, "CleanupDone")
        
    except Exception as e:
        logging.error(f"Error during DataCleaner execution: {e}")
        send_email_alert("DataCleaner Error", f"An error occurred during the weekly database cleanup: {e}", "SystemError")
