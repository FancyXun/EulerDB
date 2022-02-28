# EulerDB

## ENV
```shell script
python 3.6
mysql
```

## Install

### Download repo
```shell script
$ git clone git@github.com:FancyXun/EulerDB.git
```
### Install pip requirements
```shell script
$ cd EulerDB
$ pip install -r requirements.txt
```
## Start

Before start, you must have a database created in mysql.
the database info(include user and password) could be set in [handler_test.py](https://github.com/FancyXun/EulerDB/blob/d0de441dc580af476be498e52c0aef5602198d0e/handler_test.py#L7)

### Start server
```shell script
$ python service.py
```

### Test
```shell script
$ python single_table_test.py
```

## Sql-editor
[Poli](https://github.com/FancyXun/poli)

## Development 

### TODO LIST

SQL Type                    | Status     | SQL Type            | Status                                                                                                                                           
----------------------------- |------------|---------------------| --------------
**SQL select**                 | ```DONE``` | **SQL unique**      | TODO(low) 
**SQL distinct**                 | ```DONE``` | **SQL drop**        | ```DONE``` 
**SQL where**                 | ```DONE``` | **SQL avg**         | ```DONE```
**SQL and & or**                | ```DONE``` | **SQL count**       | ```DONE``` 
**SQL order by**               | ```DONE``` | **SQL max**         | ```DONE```
**SQL insert**               | ```DONE``` | **SQL min**         | ```DONE``` 
**SQL update**                 | ```DONE``` | **SQL sum**         | ```DONE``` 
**SQL delete**                 | ```DONE``` | **SQL having**      | TODO 
**SQL top(SQL Server)**                  | TODO(low)  | **SQL group by**    | TODO 
**SQL like**                 | ```DONE``` | **SQL round**       | TODO 
**SQL in**                 | TODO       | **SQL len**         | TODO 
**SQL between**                 | TODO       | **SQL nulls**       | TODO 
**SQL join**                 | ```DONE```        | **SQL now**         | TODO 
**SQL inner join**                 | ```DONE```       | **SQL first**       | TODO 
**SQL left join**                 | ```DONE```        | **SQL last**        | TODO 
**SQL right join**                 | ```DONE```       | **SQL limit**       | ```DONE``` 
**SQL full join**                 | TODO       | **SQL alter**       | ```DONE(not support)``` 
**SQL union**                 | TODO       | **SQL primary key** | TODO 
**SQL create db**                 | TODO(low)  | **SQL foreign key** | TODO 
**SQL create table**                 | ```DONE``` | **SQL select into** | TODO 
