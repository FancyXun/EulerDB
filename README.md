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
$ python handler_test.py
```

## Development 

### TODO LIST

SQL Type                    | Status                       | SQL Type            | Status                                                                                                                                           
----------------------------- |------------------------------|---------------------| --------------
**SQL select**                 | <font color=blue>DONE</font> | **SQL unique**      | TODO 
**SQL distinct**                 | TODO                         | **SQL drop**        | TODO 
**SQL where**                 | <font color=blue>DONE</font>                         | **SQL avg**         | TODO 
**SQL and & or**                | <font color=blue>DONE</font>                         | **SQL count**       | TODO 
**SQL order by**               | <font color=blue>DONE</font>                         | **SQL max**         | <font color=blue>DONE</font> 
**SQL insert**               | <font color=blue>DONE</font>                         | **SQL min**         | <font color=blue>DONE</font> 
**SQL update**                 | TODO                         | **SQL sum**         | TODO 
**SQL delete**                 | TODO                         | **SQL having**      | TODO 
**SQL top**                 | TODO                         | **SQL group by**    | TODO 
**SQL like**                 | <font color=blue>DONE</font>                         | **SQL round**       | TODO 
**SQL in**                 | TODO                         | **SQL len**         | TODO 
**SQL between**                 | TODO                         | **SQL select**      | TODO 
**SQL join**                 | TODO                         | **SQL now**         | TODO 
**SQL inner join**                 | TODO                         | **SQL fisrt**       | TODO 
**SQL left join**                 | TODO                         | **SQL last**        | TODO 
**SQL right join**                 | TODO                         | **SQL limit**       | <font color=blue>DONE</font> 
**SQL full join**                 | TODO                         | **SQL alter**       | TODO 
**SQL union**                 | TODO                         | **SQL primary key** | TODO 
**SQL create db**                 | TODO                         | **SQL foreign key** | TODO 
**SQL create table**                 | <font color=blue>DONE</font>                         | **SQL select into** | TODO 
