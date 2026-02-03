import logging

from workflow import TaxiColor, etl_process


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def process_month_backfill(
    taxi_color: TaxiColor,
    month: int,
    year: int
):
    try:
        etl_process(taxi_color, month, year)
    except Exception:
        pass


def green_taxi_backfill(start_month: int = 1, end_month: int = 7, year: int = 2021):
    for month in range(start_month, end_month + 1):
        process_month_backfill(TaxiColor.GREEN, month, year)
    

def yellow_taxi_backfill(start_month: int = 1, end_month: int = 7, year: int = 2021):
    for month in range(start_month, end_month + 1):
        process_month_backfill(TaxiColor.YELLOW, month, year)
    

def complete_backfill_workflow(start_month: int = 1, end_month: int = 7, year: int = 2021):
    # Run both backfills sequentially
    green_taxi_backfill(start_month, end_month, year)
    yellow_taxi_backfill(start_month, end_month, year)
    

if __name__ == "__main__":
    complete_backfill_workflow(end_month=12, year=2020)
