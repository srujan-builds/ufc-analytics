from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd


class TransformData:
    def __init__(self):
        self.batch_extract_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # self.fighter_urls = []

    def raw_events_data(self, url):
        belt_img_url = 'http://1e49bc5171d173577ecd-1323f4090557a33db01577564f60846c.r80.cf1.rackcdn.com/belt.png'\
        
        events_fight_data = []
        

        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

        try:
            response = requests.get(url, headers=headers, timeout=10)
            # response = session.get(url, timeout=5)
            soup = BeautifulSoup(response.text, 'html.parser')

            event_name = soup.find('h2', class_='b-content__title').text.strip()

            events_venue = soup.find_all('li', class_='b-list__box-list-item')
            event_date, event_location = [li.text.strip().split('\n')[-1].strip() for li in events_venue]

            rows = soup.find_all('tr', class_='b-fight-details__table-row')

            for row in rows[1:]:
                cols = row.find_all('td', class_='b-fight-details__table-col')

                
                fighter_1, fighter_2 = [p.text.strip() for p in cols[1].find_all('p')]
                fighter_1_url, fighter_2_url = [a['href'] for a in cols[1].find_all('a')]

                # col[0] --> fight details
                fight_details = [p for p in cols[0].find_all('p')]
                req_details = [{'fight_url':detail.find('a')['href'], 'status_flag':detail.find('i', class_='b-flag__text').text.strip()} for detail in fight_details]
                status_flag = req_details[0]['status_flag']
                fight_url = req_details[0]['fight_url'] # for fact_fights id
                
                match_outcome = status_flag

                if match_outcome == 'win':
                    winer_name = fighter_1
                    winner_url = fighter_1_url
                else:
                    winer_name = status_flag
                    winner_url = None


                # result = cols[1].find_all('p')[0].text.strip()
                # result_url = fighter_1_url

                # knockdown
                kd_1, kd_2 = [p.text.strip() for p in cols[2].find_all('p')]
                kd = f"{kd_1}-{kd_2}"

                # strikes
                str_1, str_2 = [p.text.strip() for p in cols[3].find_all('p')]
                strikes = f"{str_1}-{str_2}"

                # takedown
                td_1, td_2 = [p.text.strip() for p in cols[4].find_all('p')]
                td = f"{td_1}-{td_2}"

                # submission
                sub_1, sub_2 = [p.text.strip() for p in cols[5].find_all('p')]
                sub = f"{sub_1}-{sub_2}"

                weight_class = cols[6].find('p').text.strip()
                belt_tag = cols[6].find('img', src=belt_img_url)
                championship_bout = bool(belt_tag)
                
                method_list = [method.text.strip() for method in cols[7].find_all('p')]
                method = "-".join(method_list)

                round_num = cols[8].find('p').text.strip()
                fight_time = cols[9].find('p').text.strip()

                event_data = {
                    'event_name':event_name,
                    # 'event_date':event_date,
                    'event_date':datetime.strptime(event_date, '%B %d, %Y').date(),
                    'event_location':event_location,
                    'event_url':url,
                    'fight_url':fight_url,
                    'championship_bout':championship_bout,
                    'match_outcome':match_outcome,
                    'fighter_1':fighter_1,
                    'fighter_1_url':fighter_1_url,
                    'fighter_2':fighter_2,
                    'fighter_2_url':fighter_2_url,
                    'winner_name':winer_name,
                    'winner_url':winner_url,
                    'kd':kd,
                    'strikes':strikes,
                    'td':td,
                    'sub':sub,
                    'weight_class':weight_class,
                    'method':method,
                    'round_num':round_num,
                    'fight_time':fight_time,
                    'extracted_at':self.batch_extract_time
                }
                
                events_fight_data.append(event_data)
                # self.fighter_urls.extend([fighter_1_url, fighter_2_url])

        except Exception as e:
            print(f"\nFailed on {url}: {e}")
            pass

        return events_fight_data
    

    # def get_fighter_urls(self):
    #     return list(set(self.fighter_urls))

    
    def raw_fighters_data(self, fighter_url):
        all_fighters_data = []

        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

        try:
            response = requests.get(fighter_url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')

            rows = soup.find_all(class_='b-statistics__table-row')

            for row in rows[2:]:
                cols = row.find_all(class_='b-statistics__table-col')

                first = cols[0].text.strip()
                last = cols[1].text.strip()
                nickname = cols[2].text.strip()
                a_tag = cols[0].find('a')
                fighter_url = a_tag.get('href') if a_tag else "No URL"
                ht = cols[3].text.strip()
                wt = cols[4].text.strip()
                reach = cols[5].text.strip()
                stance = cols[6].text.strip()
                w = cols[7].text.strip()
                l = cols[8].text.strip()
                d = cols[9].text.strip()
                belt = cols[10].text.strip()
                belt_img = cols[10].find('img')
                belt = "Yes" if belt_img else "No"

                fighter_data = {
                    'first_name':first,
                    'last_name':last,
                    'nickname':nickname,
                    'fighter_url':fighter_url,
                    'height':ht,
                    'weight':wt,
                    'reach':reach,
                    'stance':stance,
                    'win':w,
                    'lose':l,
                    'draw':d,
                    'belt':belt,
                    'extracted_at':self.batch_extract_time
                }

                all_fighters_data.append(fighter_data)

        except Exception as e:
            print(f"\nFailed on {fighter_url}: {e}")
            pass

        return all_fighters_data
    
    def data_transform(self, raw_data):
        transformed_df = pd.DataFrame(raw_data)
        return transformed_df
