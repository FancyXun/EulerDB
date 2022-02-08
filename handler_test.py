from unittest import TestCase
import requests
import json


class TestPostHandler(TestCase):

    def test_handler_query_limit(self):
        content = {
            'host': '127.0.0.1', 'db': 'points',
            'user': 'root', 'password': 'root',
            'query': 'select * from user limit 10'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_create_table(self):
        pass

    def test_handler_query_where(self):
        pass
