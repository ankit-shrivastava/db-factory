# db-factory

Database factory is used to manage/create database connection with execute queries using the connection.
The concept of having single source to connect various databases and perform database operations.

User need not to worry on the crafting the connection string and to identify the methods for the database operations.
db-factory supports DML / DDL executions and have support of Pandas DataFrame to create or replace existing tables.

## Getting Started

### Setup
------

Assuming that you have Python and virtualenv installed, set up your environment:

#### Setup virtual environment
```
$ mkdir $HOME/db-factory
$ cd db-factory
$ virtualenv venv
```
```
$ . venv/bin/activate
```

#### Setup from source:
```
$ git clone https://github.com/ankit-shrivastava/db-factory.git
$ cd db-factory
$ python -m pip install -e .
```

#### Setup from Github Repository using Pip:
```
$ pip install git+https://github.com/ankit-shrivastava/db-factory.git@master
```

#### Setup using build:
```
$ git clone https://github.com/ankit-shrivastava/db-factory.git
$ cd db-factory
$ python setup.py bdist_wheel
$ pip install dist/*
```

### Using db-factory
-----
```
from db_factory.manager import DatabaseManager
db = DatabaseManager(engine_type="sqlite", database="test_db", sqlite_db_path="/tmp")
db.create_session()

db.execute("create table test (id int PRIMARY KEY)")
db.execute("insert into test values (1)")
db.execute("insert into test values (2)")

rows = db.execute("select * from test")
if rows:
  print(rows)
```

## Appendix
### Supported database type:
----
```
*   sqlite `default`
*   postgres
*   mysql
*   mariadb
*   snowflake
```

### Connection parameters for sqlite:
-----
```
* engine_type: sqlite
* database: <name of database>
* sqlite_db_path: <path where database will be created>
```

### Connection parameters for postgres:
-----
```
* engine_type: postgres
* database: <name of database>
* username: <postgres user>
* password: <user password>
* host: <host of postgres service>
* port: <port of postgres service>
```

### Connection parameters for mysql:
-----
```
* engine_type: mysql
* database: <name of database>
* username: <mysql user>
* password: <user password>
* host: <host of mysql service>
* port: <port of mysql servic\>
```

### Connection parameters for mariadb:
-----
```
* engine_type: mariadb
* database: <name of database>
* username: <mariadb user>
* password: <user password>
* host: <host of mariadb service>
* port: <port of mariadb service>
```

### Connection parameters for snowflake:
-----
```
* engine_type: snowflake
* database: <name of database>
* username: <snowflake user>
* password: <user password>
* schema: <schema name>
* snowflake_role: <snowflake role>
* snowflake_warehouse: <snowflake warehouse>
* snowflake_account: <snowflake account>
```

### Connection parameters for bigquery:
-----
```
* engine_type: bigquery
* database: <name of database>
```

### Getting connection properties from AWS / GCP Secret Manager Service:
-----
Note:
* GCP: 
   * On Cloud Server:
       * Set server to execute the all cloud api services
       * Attach following permissions
          * Project Viewer
          * Secret Manager Secret Accessor
   * On Premises:
       * Attach following permissions to user service account and download service account file for authentication:
          * Project Viewer
          * Secret Manager Secret Accessor
       * Set environment variable "GOOGLE_APPLICATION_CREDENTIALS" pointing to service account file.
* AWS:
   * On Cloud Server:
      * Set execution profile with "secretsmanager:GetSecretValue" policy
   * On Premises:
      * AWS should be configured
      * User should have permissions of "secretsmanager:GetSecretValue" policy.

```
* engine_type: bigquery
* database: <name of database>
* secret_id: <Secret name of AWS / GCP Secret Manager Service>
* secrete_manager_cloud: <aws or gcp as per cloud>
* aws_region: <aws region: default=> us-east-1>
```
