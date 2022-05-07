#!/usr/bin/env python
import snowflake.connector
import os

# Gets the version
ctx = snowflake.connector.connect(
    user='albertomartinezgarcia',
    password='25108-Snowflake8',
    account='pzb06830',
    region='us-east-1'
    )
cs = ctx.cursor()
if False:
    try:
        cs.execute("CREATE DATABASE IF NOT EXISTS testdb")
        cs.execute("USE DATABASE testdb")
        cs.execute("CREATE SCHEMA IF NOT EXISTS testschema")
        cs.execute("create or replace stage teststage file_format = (type = 'CSV' field_delimiter = ',')")
    finally:
        cs.close()

if False:
    try:
        cs.execute("USE DATABASE testdb")
        cs.execute("""create or replace file format my_csv_format
                      type = csv
                      field_delimiter = ','
                      null_if = ('NULL', 'null')
                      empty_field_as_null = true
                      compression = gzip;""")
    finally:
        cs.close()

if False:
    try:
        cwd = os.getcwd()
        cwd_data = os.path.join(cwd,'data')
        query = "PUT file://{}/*.csv @teststage;".format(cwd_data)
        cs.execute("USE DATABASE testdb")
        cs.execute("USE SCHEMA testschema")
        cs.execute(query)
    finally:
        cs.close()

if False:
    try:
        cs.execute("USE DATABASE testdb")
        cs.execute("USE SCHEMA testschema")
        cs.execute("LIST @teststage")
        for (name,size,md5,last_modified) in cs:
            print('{},{},{},{}'.format(name,size,md5,last_modified))
    finally:
        cs.close()
        
if False:
    try:
        cs.execute("USE DATABASE testdb")
        cs.execute("USE SCHEMA testschema")
        cs.execute("select * from table(infer_schema(LOCATION => '@teststage', FILE_FORMAT => 'my_csv_format'));")
    finally:
        cs.close()
        
if False:
    try:
        cs.execute("USE DATABASE testdb")
        cs.execute("USE SCHEMA testschema")
        cs.execute( """create or replace table testtable (
                       a float ,
                       b float ,
                       c float ,
                       d float ,
                       e float ,
                       f float ,
                       g float );""")
    finally:
        cs.close()

if True:
    try:
        cs.execute("USE DATABASE testdb")
        cs.execute("USE SCHEMA testschema")
        cs.execute("alter warehouse compute_wh resume if suspended;")
        cs.execute("copy into testtable from @teststage;")
    finally:
        cs.close()

        
        
       

ctx.close()
