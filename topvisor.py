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
        self.base_path = os.path.dirname(os.path.abspath(__file__))
        self._initializing_dicts()
        self._get_credentials()
        self._set_dates()
        self._make_directories_for_json()
        self._initializing_response_frames()


        self.headers = {'Content-type': 'application/json', 'User-Id': self.user, 'Authorization': f'bearer {self.key}'}
        self.server = 'https://api.topvisor.com'

        self.work_book_id = '10bQ3R1LvWd3QQW55ALaNOtdxLrZWdrz57Uhrhnu1bRw'
        self.service_file_path = 'pysheets-347309-9629095400b4.json'

        self.summary_chart_api_url = '/v2/json/get/positions_2/summary_chart'
        self.keywords_api_url = '/v2/json/get/keywords_2/folders'

    def _initializing_dicts(self):

        self.se_region_index = {"yandex": 3, "google": 6}
        self.tags = {"commercial": 2}
        self.folders_dict = {
                  "iphone": 861539,
                  "ipad": 861590,
                  "mac": 861565,
                  "watch": 861584
                }
        self.metrics = ["avg", "visibility"]
        self.tops = ['all', '1_3', '1_10', '11_30']
        return self.se_region_index, self.tags, self.folders_dict, self.metrics

    def _initializing_response_frames(self):

        self.base_dataframe = {
         }
        self.folder_dataframe = {
        }

        return self.base_dataframe, self.folder_dataframe

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

        if not os.path.exists(f'{self.base_path}/charts/'):
            os.mkdir(f'{self.base_path}/charts/')

        if not os.path.exists(f'{self.base_path}/charts/base/'):
            os.mkdir(f'{self.base_path}/charts/base/')
        if not os.path.exists(f'{self.base_path}/charts/folder/'):
            os.mkdir(f'{self.base_path}/charts/folder/')
        if not os.path.exists(f'{self.base_path}/charts/tag/'):
            os.mkdir(f'{self.base_path}/charts/tag/')

    def get_groups_id(self):

        print(f'Requesting group folders id chart for')

        payload = {
            "project_id": self.project_id
        }
        self.response = requests.post(f'{self.server}{self.keywords_api_url}', headers=self.headers, data=json.dumps(payload))

        with open(f'topvisor/group_id.json', 'w', encoding='utf-8') as file:
            json.dump(self.response.json(), file, indent=4, ensure_ascii=False)
            file.close()

    def _payload_generator(self, search_engine, type, folder='', tag=''):

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
        elif type == 'folder':
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
                        "name": "group_folder_id",
                        "operator": "EQUALS",
                        "values": [
                            str(self.folders_dict[folder])
                        ]
                    }
                ],
                "group_folder_id_depth": "1"
            }
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

    def _get_response(self, search_engine, type, folder='', tag=''):
        """

        :type search_engine: object
        """
        print(f'Requesting {type} summary chart for {search_engine}: {folder} : {tag}')
        self.response = requests.post(f'{self.server}{self.summary_chart_api_url}', headers=self.headers, data=json.dumps(self.payload))

        return self.response

    def _save_response_to_json(self, search_engine, type, folder='', tag=''):

        if type == 'base':
            print(f'Saving /topvisor/charts/base/{self.date_today}-{search_engine}.json')
            with open(f'{self.base_path}/charts/base/{self.date_today}-{search_engine}.json', 'w', encoding='utf-8') as file:
                json.dump(self.response.json(), file, indent=4, ensure_ascii=False)
                file.close()
        elif type == 'folder':
            print(f'Saving /topvisor/charts/folder/{self.date_today}-{search_engine}-{folder}.json')
            with open(f'{self.base_path}/charts/folder/{self.date_today}-{search_engine}-{folder}.json', 'w', encoding='utf-8') as file:
                json.dump(self.response.json(), file, indent=4, ensure_ascii=False)
                file.close()
        elif type == 'tag':
            print(f'Saving /topvisor/charts/tag/{self.date_today}-{search_engine}-{tag}.json')
            with open(f'{self.base_path}/charts/tag/{self.date_today}-{search_engine}.json', 'w', encoding='utf-8') as file:
                json.dump(self.response.json(), file, indent=4, ensure_ascii=False)
                file.close()

    def _produce_base_charts(self):

        self.base_charts = {}
        type = 'base'
        for search_engine in self.se_region_index:
            self._payload_generator(search_engine, type)
            response = self._get_response(search_engine, type)
            self.base_charts[f'{search_engine}'] = response.json()
            self._save_response_to_json(search_engine, type)


        return self.base_charts

    def _reformat_base_charts(self):

        self._produce_base_charts()

        self.base_dataframe['date'] = self.base_charts['google']["result"]["dates"]

        for search_engine in self.se_region_index:

            for metric in self.metrics:

                self.base_dataframe[f'{search_engine}_{metric}'] = self.base_charts[f'{search_engine}']["result"]["seriesByProjectsId"][self.project_id][metric]
                for x, item in enumerate(self.base_dataframe[f'{search_engine}_{metric}']):
                    self.base_dataframe[f'{search_engine}_{metric}'][x] = str(item).replace(".", ",")

            for top in self.tops:

                self.base_dataframe[f'{search_engine}_{top}'] = self.base_charts[f'{search_engine}']["result"]["seriesByProjectsId"][self.project_id]["tops"][top]

        self.base_dataframe = pd.DataFrame(self.base_dataframe)
        return self.base_dataframe

    def push_base_dataframe(self):

        df = self._reformat_base_charts()
        sheet_name = 'summary_test_2'
        pusher = GoogleSheetWriter(self.service_file_path, self.work_book_id, sheet_name)

        pusher.run(df)

    def _produce_folder_charts(self):

        self.folder_charts = {}
        type = 'folder'

        for search_engine in self.se_region_index:
            for folder in self.folders_dict:
                self._payload_generator(search_engine, type)
                response = self._get_response(search_engine, type)
                self.folder_charts[f'{search_engine}_{folder}'] = response.json()
                self._save_response_to_json(search_engine, type)

        return self.folder_charts

    def _reformat_folder_charts(self):

        self._produce_folder_charts()

        self.folder_dataframe['date'] = self.base_charts['google']["result"]["dates"]

        for search_engine in self.se_region_index:
            for folder in self.folders_dict:

                for metric in self.metrics:
                    self.base_dataframe[f'{search_engine}_{folder}_{metric}'] = self.base_charts[f'{search_engine}']["result"]["seriesByProjectsId"][self.project_id][metric]
                    for x, item in enumerate(self.base_dataframe[f'{search_engine}_{metric}']):
                        self.base_dataframe[f'{search_engine}_{folder}_{metric}'][x] = str(item).replace(".", ",")

                for top in self.tops:

                    self.base_dataframe[f'{search_engine}_{top}'] = self.base_charts[f'{search_engine}']["result"]["seriesByProjectsId"][self.project_id]["tops"][top]

        self.folder_dataframe = pd.DataFrame(self.base_dataframe)

        return self.base_dataframe

def main():

    tv = Topvisor()
    tv.push_base_dataframe()
    # tv.get_folders_summary_chart()


if __name__ == '__main__':

    main()
