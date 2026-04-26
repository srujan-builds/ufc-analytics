from scraper import Scraper
from events import EventData
from transform import TransformData
from db_connector import SnowflakeConnector
from datetime import datetime
from string import ascii_lowercase
from snowflake.connector.pandas_tools import write_pandas

batch_extract_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def read_transform():
    scraper = Scraper()
    events = EventData()
    transform = TransformData()

    event_urls = events.get_latest_urls()
    events_data = scraper.extract_data(transform_funcn=transform.raw_events_data, urls=event_urls, type='events')
    events_df = transform.data_transform(events_data)
    print(f"Latest UFC fights extracted: {len(events_df )}")


    fighter_urls = [f"http://www.ufcstats.com/statistics/fighters?char={letter}&page=all" for letter in ascii_lowercase]
    fighters_data = scraper.extract_data(transform_funcn=transform.raw_fighters_data, urls=fighter_urls, type='fighters')
    fighters_df = transform.data_transform(fighters_data)
    print(f"UFC fighters: {len(fighters_df)}")

    return {'event_df':events_df, 'fighters_df':fighters_df}

def write_to_snowflake(events_df, fighters_df):

    events_df.columns = [col.upper() for col in events_df.columns]
    fighters_df.columns = [col.upper() for col in fighters_df.columns]

    # RAW tables
    events_table = 'RAW_UFC_EVENTS'
    fighters_table = 'RAW_UFC_FIGHTERS'

    db = SnowflakeConnector()
    db.establish_connection()

    print(f"Appending {len(events_df)} rows to {events_table}")
    write_pandas(
        conn=db.conn,
        df=events_df,
        table_name=events_table,
        auto_create_table=True,
        overwrite=False
    )
    print("Events sucessfully appended")

    print(f"Overwriting {len(fighters_df)} rows in {fighters_table}")
    write_pandas(
        conn=db.conn, 
        df=fighters_df, 
        table_name=fighters_table, 
        auto_create_table=True, 
        overwrite=True         
    )
    print("Fighters successfully overwritten")

    db.close_connection()

if __name__ == '__main__':
    transformed_data = read_transform()

    events = transformed_data['event_df']
    fighters = transformed_data['fighters_df']

    write_to_snowflake(events_df=events, fighters_df=fighters)



    







# print(len(event_results))
# print(len(transform.get_fighter_urls()))
# print(type(event_results))
# print(event_results[1])
# for event in event_results:
#     # print(event)
#     print(type(event))

# loading data to sf
#trigering dbt to check