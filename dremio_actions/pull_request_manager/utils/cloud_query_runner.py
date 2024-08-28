import datetime
import json
import logging
import urllib
from urllib import request, parse
import time


class CloudQuery:
    def __init__(self, project_id: str, region: str, token: str, poll_limit:int = 90, pause_time: int = 2):
        self.project_id = project_id
        self.base_url = CloudQuery.generate_base_url_for_region(region)
        self.post_url = self.generate_post_url()
        self.rest_job_poll_limit = poll_limit
        self.headers = CloudQuery.generate_headers(token)
        self.pause_time = pause_time  # typically 1, depends on workload
        self.payload_key_value = 'sql'

    def generate_post_url(self):
        return '{base_url}/{project}/sql'.format(base_url=self.base_url, project=self.project_id)

    @staticmethod
    def generate_headers(token):
        formatted_headers = {
            'Authorization': 'Bearer {token}'.format(token=token),
            'Content-Type': 'application/json'
        }

        return formatted_headers

    @staticmethod
    def generate_base_url_for_region(region):
        print('the region! ' , region)
        if (region.lower() == 'eu') or (region.lower() == 'emea'):
            return 'https://api.eu.dremio.cloud/v0/projects'

        return 'https://api.dremio.cloud/v0/projects'

    def generate_get_url(self, job_id):

        return '{base_url}/{project_id}/job/{job_id}/results'.format(base_url=self.base_url, job_id=job_id,
                                                                     project_id=self.project_id)

    def submit_query_and_poll_for_response(self, query_text):
        query_payload = {self.payload_key_value: query_text}
        print('query_payload! --> ', query_payload)
        data = json.dumps(query_payload).encode('utf-8')
        req = request.Request(self.post_url, data=data, headers=self.headers)
        print('self.post_url ', self.post_url)

        with urllib.request.urlopen(req) as response:
            response_data = json.loads(response.read().decode('utf-8'))

        job_id = response_data['id']
        print('job_id of query ' + job_id)

        get_url = self.generate_get_url(job_id)

        counter = 0
        response_status_data = {}
        while counter < self.rest_job_poll_limit:
            time.sleep(self.pause_time)
            req_status = urllib.request.Request(get_url, headers=self.headers)

            try:
                with urllib.request.urlopen(req_status) as response_status:
                    response_status_data = json.loads(response_status.read().decode('utf-8'))
                    print('response in the poll while loop ', response_status_data)
                    break
            except urllib.error.HTTPError as e:
                print('******')
                print(f'Polling for response on the query {query_payload} raised an error {e} on the {counter}th try, '
                      f'but don\'t worry we still have {self.rest_job_poll_limit-counter} tries left!')
                print('******')

            counter += 1

        return response_status_data
