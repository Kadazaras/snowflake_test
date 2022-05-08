#!/usr/bin/env python
import snowflake.connector
import os
import threading
import random
import time

# Gets the version
ctx = snowflake.connector.connect(
    user='albertomartinezgarcia',
    password='25108-Snowflake8',
    account='pzb06830',
    region='us-east-1',
    client_session_keep_alive=True
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
                      SKIP_HEADER = 1
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

if True:
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

if False:
    try:
        cs.execute("USE DATABASE testdb")
        cs.execute("USE SCHEMA testschema")
        cs.execute("alter warehouse compute_wh resume if suspended;")
        cs.execute("copy into testtable from @teststage FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');")
    finally:
        cs.close()

if False:
    try:
        cs.execute("USE DATABASE testdb")
        cs.execute("USE SCHEMA testschema")
        cs.execute("alter warehouse compute_wh resume if suspended;")
        cs.execute("remove @teststage pattern='.*.csv.gz';")
        for (name,result) in cs:
            print('{},{}'.format(name,result))
    finally:
        cs.close()

class ConcurrentQuery (threading.Thread):
    def __init__(self,name,file):
        threading.Thread.__init__(self, name=name)
        self.file = file
        self.ctx = snowflake.connector.connect(
            user='albertomartinezgarcia',
            password='25108-Snowflake8',
            account='pzb06830',
            region='us-east-1',
            client_session_keep_alive=True
        )
        self.cs = self.ctx.cursor()
    def run(self):
        query = "copy into testtable from @{} FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');".format(self.file)
        print('Copying {}'.format(self.file))
        self.cs.execute(query)





if False:
    cs.execute("USE DATABASE testdb")
    cs.execute("USE SCHEMA testschema")
    cs.execute("alter warehouse compute_wh resume if suspended;")
    cs.execute("LIST @teststage")
    filelist = []
    for (name,size,md5,last_modified) in cs:
        filelist.append(name)


    clusters = 4
    print_lock = threading.Lock()
    threads = []

    for i in range(clusters):
        thread = ConcurrentQuery(name=str(i),file=filelist.pop(0))
        threads.append(thread)

    for thread in threads:
        thread.start()

    while len(filelist)>0:
        for thread in threads:
            if thread.is_alive():
                pass
            else:
                with print_lock:
                    print('Thread {} finished: restart new thread'.format(thread.name))
                thread.__init__(name=thread.name,file=filelist.pop(0))
                thread.start()
    while True:
        checklist = []
        for thread in threads:
            if thread.is_alive():
                checklist.append(False)
            else:
                checklist.append(True)
        if all(checklist):
            break
        time.sleep(1)
ctx.close()
