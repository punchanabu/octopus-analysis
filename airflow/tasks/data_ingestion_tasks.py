from src.data.scopus_loader import ScopusLoader
from src.data.scraper import DataScraper
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

def load_scopus_data(**context):
    try:
        loader = ScopusLoader()
        data = loader.load_data()
        context['task_instance'].xcom_push(key='scopus_data', value=data)
        return "Scopus data loaded successfully"
    except Exception as e:
        logger.error(f"Error loading Scopus data: {str(e)}")
        raise
    
# TODO: scrape this shit
def scrape_data(**context):
    return 


    
