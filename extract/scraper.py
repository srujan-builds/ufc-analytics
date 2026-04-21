import concurrent.futures
from datetime import datetime
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
from events import EventData

MAX_NUM_OF_WORKERS = 8

class Scraper:
            
    def extract_data(self, transform_funcn, urls, type):
        self.all_scraped_data = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_NUM_OF_WORKERS) as executor:
            results = list(
                tqdm(
                    executor.map(transform_funcn, urls),
                    total=len(urls),
                    desc=f"Scarping UFC {type}"
                )
            )

        # return results

        for result in results:
            self.all_scraped_data.extend(result)

        return self.all_scraped_data
    

