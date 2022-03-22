import unittest
import json
import requests

sql_list = [
    "select S.studentNo,studentName,score  from Student S join Score on S.studentNo=Score.studentNo  join Course on "
    "Course.courseNo=Score.courseNo join (select AVG(score) gg ,courseNo  from Score join student H on "
    "Score.studentNo=H.studentNo    group by courseNo) G on G.courseNo=Course.courseNo where score>gg "
]

db_host = '127.0.0.1'
db = 'points'
user = 'root'
password = 'root'
port = 3306

content = {
    'host': db_host, 'db': db,
    'user': user, 'password': password,
    'port': port
}


class MyTestCase(unittest.TestCase):
 def test_sql(self):
  for sql in sql_list:
   content['query'] = sql
   json_data = json.dumps(content)
   try:
    resp = requests.post('http://localhost:8888/query', json_data)
    if 'result' in resp.json():
     print(resp.json()['result'])
   except Exception as e:
    print(e)
    continue


if __name__ == '__main__':
 unittest.main()
