from bs4 import BeautifulSoup
from datetime import datetime
import requests
import os
from db_connector import SnowflakeConnector

class EventData:
    def __init__(self):
        db_connection = SnowflakeConnector()
        db_connection.establish_connection()
        self.max_event_db_date = db_connection.get_max_event_date()
        db_connection.close_connection()


    def get_latest_urls(self):
        completed_ufc_events_url = "http://www.ufcstats.com/statistics/events/completed"

        try:
            response = requests.get(url=completed_ufc_events_url, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            events = soup.find_all('tr', class_='b-statistics__table-row')
            event_urls = []

            for event in events[2:]:
                str_date = event.find('span').text.strip()
                event_date = datetime.strptime(str_date, '%B %d, %Y').date()

                if event_date > self.max_event_db_date:
                    a_tag = event.find('a')
                    event_url = a_tag.get('href')
                    event_urls.append(event_url)
                else:
                    break
            return event_urls
            
        except Exception as e:
            print(f"Error connecting to url-{completed_ufc_events_url}: {e}")

            



