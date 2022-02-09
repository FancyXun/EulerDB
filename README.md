# EulerDB

## ENV
```shell script
python 3.6
mysql
```

## Install

### Download repo
```shell script
git clone git@github.com:FancyXun/EulerDB.git
```
### Install pip requirements
```shell script
cd EulerDB
pip install -r requirements.txt
```
## Start

Before start, you must have a database created in mysql.
the database info(include user and password) could be set in [handler_test.py](https://github.com/FancyXun/EulerDB/blob/d0de441dc580af476be498e52c0aef5602198d0e/handler_test.py#L7)

### Start server
```shell script
python service.py
```

### Test
```shell script
python handler_test.py
```
