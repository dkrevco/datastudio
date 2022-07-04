import pygsheets
import json
import requests
import os
import datetime
from pusher import GoogleSheetWriter
import pandas as pd

class Topvisor:

    def __init__(self):

        print('Authorizing Topvisor')
        self._initializing_dicts()
        self._get_credentials()
        self._set_dates()
        self._make_directories_for_json()
        self.headers = {'Content-type': 'application/json', 'User-Id': self.user, 'Authorization': f'bearer {self.key}'}
        self.server = 'https://api.topvisor.com'

        self.work_book_id = '10bQ3R1LvWd3QQW55ALaNOtdxLrZWdrz57Uhrhnu1bRw'
        self.service_file_path = 'pysheets-347309-9629095400b4.json'

        self.summary_chart_api_url = '/v2/json/get/positions_2/summary_chart'

    def _initializing_dicts(self):

        self.se_region_index = {"yandex": 3, "google": 6}
        self.tags = {"commercial": 2}
        self.folders = {"iphone": 12}
        self.metrics = ["avg", "visibility"]
        return self.se_region_index, self.tags, self.folders, self.metrics

    def _set_dates(self):

        self.date_today = str(datetime.date.today())
        self.dates = ['2022-01-01', self.date_today]

        return self.dates, self.date_today

    def _get_credentials(self):

        self.user = ''
        self.key = ''
        self.project_id = ''

        with open('login_data.txt', 'r', encoding='utf-8') as file:

            oauth_data = {}
            lines = file.read().splitlines()
            for line in lines:
                oauth_data[line.split(': ')[0]] = line.split(': ')[1]

            self.user = oauth_data['user']
            self.key = oauth_data['key']
            self.project_id = oauth_data['project_id']

            file.close()

        return self.user, self.key, self.project_id

    def _make_directories_for_json(self):

        if not os.path.exists('topvisor/'):
            os.mkdir('topvisor/')

        if not os.path.exists('topvisor/charts/'):
            os.mkdir('topvisor/charts/')


    def get_summary_chart(self):

        for se, se_index in self.se_region_index.items():

            self._get_response('base', se)
            self._save_response_to_json()

        df = self._reformat_response_for_summary_dataframe()
        sheet_name = 'summary_test'
        sender = GoogleSheetWriter(self.service_file_path, self.work_book_id, sheet_name)

        sender.run(df)

    def _get_response(self, type, search_engine, tag=0, folder=0):

        print(f'Requesting {type} summary chart for {search_engine}: {folder} : {tag}')
        self._payload_generator(type, search_engine)
        self.response = requests.post(f'{self.server}{self.summary_chart_api_url}', headers=self.headers, data=json.dumps(self.payload))
        self.se = search_engine
        self.folder = folder
        self.type = type
        self.tag = tag

        return self.response, self.se, self.folder, self.type, self.tag

    def _payload_generator(self, type, search_engine, tag=0, folder=0):

        if type == 'base':
            self.payload = {
                "project_id": self.project_id,
                "region_index": self.se_region_index[search_engine],
                "date1": self.dates[0],
                "date2": self.dates[-1],
                "type_range": 0,
                "show_visibility": True,
                "show_avg": True,
                "show_tops": True
            }
            return self.payload
        elif type == 'folder':
            self.payload =  {
                "project_id": 	self.project_id,
                "region_index": self.se_region_index[search_engine],
                "date1" : self.dates[0],
                "date2" : self.dates[-1],
                "type_range": 0,
                "show_visibility": True,
                "show_avg": True,
                "show_tops": True,
                "filters": [
                    {
                        "name": "group_folder_id",
                        "operator": "EQUALS",
                        "values": [
                            str(folder)
                        ]
                    }
                ],
                "group_folder_id_depth": "1"
            }
            return self.payload
        elif type == 'tag':
            self.payload = {
                "project_id": self.project_id,
                "region_index": self.se_region_index[search_engine],
                "date1": self.dates[0],
                "date2": self.dates[-1],
                "type_range": 0,
                "show_visibility": True,
                "show_avg": True,
                "show_tops": True,
                "filters": [
                    {
                        "name": "tags",
                        "operator": "IN",
                        "values": [
                            str(tag)
                        ]
                    }
                ],
                "group_folder_id_depth": "1"
            }
            return self.payload

    def _save_response_to_json(self):

        if type == 'base':
            print(f'Saving /topvisor/charts/{self.date_today}-{self.se}-summary-chart.json')
            with open(f'topvisor/charts/{self.date_today}-{self.se}-summary-chart.json', 'w', encoding='utf-8') as file:
                json.dump(self.response.json(), file, indent=4, ensure_ascii=False)
                file.close()
        elif type == 'folder':
            print(f'Saving /topvisor/charts/{self.date_today}-{self.se}-{self.folder}-summary-chart.json')
            with open(f'topvisor/charts/{self.date_today}-{self.se}-summary-chart.json', 'w', encoding='utf-8') as file:
                json.dump(self.response.json(), file, indent=4, ensure_ascii=False)
                file.close()
        elif type == 'tag':
            print(f'Saving /topvisor/charts/{self.date_today}-{self.se}-{self.tag}-summary-chart.json')
            with open(f'topvisor/charts/{self.date_today}-{self.se}-summary-chart.json', 'w', encoding='utf-8') as file:
                json.dump(self.response.json(), file, indent=4, ensure_ascii=False)
                file.close()

    def _reformat_response_for_summary_dataframe(self):

        yandex = pd.read_json(f'topvisor/charts/{self.date_today}-yandex-summary-chart.json')
        google = pd.read_json(f'topvisor/charts/{self.date_today}-google-summary-chart.json')

        self.summary_frame = {}

        for x in range(len(google["result"]["dates"])):

            date = google["result"]["dates"][x]

            self.summary_frame[x] = [{"date": date}]

            for metric in self.metrics:
                    yandex_metric = {}
                    yandex_value = yandex["result"]["seriesByProjectsId"][str(self.project_id)][metric][x]
                    yandex_metric[f'yandex_{metric}'] = str(yandex_value).replace('.', ',')
                    self.summary_frame[x].append(yandex_metric)

                    google_metric = {}
                    google_value = google["result"]["seriesByProjectsId"][str(self.project_id)][metric][x]
                    google_metric[f'google_{metric}'] = str(google_value).replace('.', ',')
                    self.summary_frame[x].append(google_metric)

        # self.table = json.dumps(self.summary_frame, indent=4, ensure_ascii=False)
        # self.table = pd.read_json(self.table, orient='index')
        self.table = pd.DataFrame.from_dict(self.summary_frame, orient='index')

        print(self.table)
        return self.table


def main():

    tv = Topvisor()
    tv.get_summary_chart()



if __name__ == '__main__':

    main()
