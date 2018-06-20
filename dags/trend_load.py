import json
import os
import io
import pandas as pd
import logging
from pandas.io.json import json_normalize
from airflow.hooks.mysql_hook import MySqlHook

from sqlalchemy import create_engine

import boto3



# checks to see if JSON exists in current table in SQL DB
# def json_check(js):
#     hook = MySqlHook(mysql_conn_id = 'local_mysql')
#     cnx = hook.get_conn()
#     cursor = cnx.cursor()
#     logging.info("SELECT COUNT(*) FROM quality_trend WHERE path = '{0}'".format(js))
#     cursor.execute("SELECT COUNT(*) FROM quality_trend WHERE path = '{0}'".format(js))
#     count = cursor.fetchall()
#
#     # single element tuple to number
#     count = count[0][0]
#     # if the s3 path already exists
#     if count is not 0:
#         # these files already exists, don't download them
#         print('true')
#         return True
#
#     # count is zero
#     else:
#         # this is a new file to the table, download them
#         print('false')
#         return False

def all_paths():
    # Get all paths in table
    hook = MySqlHook(mysql_conn_id = 'local_mysql')
    cnx = hook.get_conn()
    cursor = cnx.cursor()
    cursor.execute("SELECT distinct path FROM quality_trend")
    results = cursor.fetchall()
    results = [result for (result, cow) in results]
    return results

# Starts the process and chooses which files to download/not download & process
def get_json(**kwargs):
    s3 = boto3.client('s3')
    prefix = 'enigma-deid/quality/output'
    bucket = 's3'
    all_objects = s3.list_objects(Bucket = 'modmed', Prefix = prefix)
    cow = all_paths()
    print('COW',cow)
    # for each_json in all_objects['Contents']:
    #     # check if it's a json
    #     current_key = each_json['Key']
    #     # if the key is a .json file and it does not exist in the DB already
    #     if '.json' in current_key and json_check(current_key) is False:
    #         # download it
    #         s3 = boto3.resource('s3')
    #         obj = s3.Object('modmed', each_json['Key'])
    #         # extract the JSON
    #         byte_arr = obj.get()['Body'].read()
    #         js = byte_arr.decode('UTF-8')
    #         js = json.loads(js)
    #         # send it over for processing
    #         process(js, current_key)

# iterates through JSON object to pull all data into table array
#   i.e tab = [{row1},{row2},{row3}, etc..]
def process(data, s3_path):
    # object to represent a table of data
    valid_obj = []

    # checks rule result for data type and inserts data type info into row
    def check_rule(row_obj, rule_obj):
        # check for the right keys
        if 'pass' in rule_obj.keys():
            row_obj['result'] = int(rule_obj['pass'])
            row_obj['result_type'] = "boolean"

        elif 'result' in rule_obj.keys():
            the_result = rule_obj['result']
            row_obj['result'] = the_result
            # is this result a decimal i.e 1.0
            if isinstance(the_result, float):
              row_obj['result_type'] = "float"

            elif isinstance(the_result, int):
              row_obj['result_type'] = "integer"

            else:
                row_obj['result_type'] = None

        elif 'message' in rule_obj.keys():
            row_obj['result'] = rule_obj['message']
            row_obj['result_type'] = "string"

        else:
            print('CANNOT FIND PASS OR RESULT KEY IN RULE OBJECT:')
            second_key = list(rule_obj.keys())[1]
            row_obj['result'] = second_key
            row_obj['result_type'] = None

    # loop through JSON
    print(data)

    for each_table in data['tables']:
      # pull off table rules
      for each_rule in each_table['rules']:
          # for each rule, create a row
          # one "row" in the table with the appr. names
          tab_row = {}
          # pull data
          tab_row['path'] = s3_path
          tab_row['job_name'] = data['name']
          tab_row['execution'] = data['datetime']
          tab_row['table_name'] = each_table['name']
          # col_name is NULL because these are rules for table
          tab_row['col_name'] = None
          tab_row['rule'] = each_rule['rule']
          # check rule object for pass key
          check_rule(tab_row, each_rule)
          # append to table object
          valid_obj.append(tab_row)


      # pull off col rules
      for each_col in each_table['columns']:
          for each_rule in each_col['rules']:
              # for each rule, create a column row in the table
              col_row = {}
              # pull data
              col_row['path'] = s3_path
              col_row['job_name'] = data['name']
              col_row['execution'] = data['datetime']
              col_row['table_name'] = each_table['name']
              # has a col name since this is a col rule
              col_row['col_name'] = each_col['name']
              col_row['rule'] = each_rule['rule']
              # check for the right keys
              check_rule(col_row, each_rule)
              # append to table object
              valid_obj.append(col_row)

    create_df(valid_obj)

# throws table object into dataframe to inject into DB
def create_df(tab_obj):
    df = pd.DataFrame(tab_obj)
    sql_query(df, 'append')

# injects the dataframe into DB
def sql_query(frame, x_mode):
    try:
        engine = dbconnect()
        frame.to_sql(con = engine, name = 'quality_trend', if_exists=x_mode, chunksize = 100000000)

    except Exception as e:
        print(e)


# Begin getting the JSON
# get_json()
