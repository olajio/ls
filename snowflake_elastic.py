import snowflake.connector as snow
from datetime import datetime
from decimal import Decimal
import json
import argparse
import uuid
import json_log_format as jlf

#trace.id
uuid = str(uuid.uuid4())

#Creating and Configuring the JSON Logger
#---------------------------------------
jlf.service_name = "snowflake-logs"
jlf.service_type = "Snowflake"
jlf.trace_id = uuid
jlf.json_logging.init_non_web(custom_formatter=jlf.CustomJSONLog, enable_json=True)
logger = jlf.logging.getLogger(__name__)
logger.setLevel(jlf.logging.INFO)
logger.addHandler(jlf.logging.StreamHandler(jlf.sys.stdout))
#---------------------------------------

# Create a connection to Snowflake

def fetch_snowflake_data(user,sf_keypair, account, warehouse, sql_query):
 try:
    connection = snow.connect(
        account = account,
        user = user,
        sf_keypair = sf_keypair,
        warehouse = warehouse,
        database="SNOWFLAKE"
    )

    # Define a cursor
    # Pull the warehouse into ARGUMENT, same as sql query => sql1,sql2,sql3 etc. 
    
    cursor = connection.cursor()
    cursor.execute(f"USE WAREHOUSE {warehouse};")
    cursor.execute(sql_query)
    results = cursor.fetchall()
    #print("Raw:",results.code)
    columns = [col[0] for col in cursor.description]
    cursor.close()
    connection.close()

    data = [dict(zip(columns, row)) for row in results]
    return data
 except:
     print('ERROR: cannot authenticate to Snowflake')
     
   
    #snowflake_meta = {"service": {"name": "Snowflake", "account": f"{args.account}", "Warehouse": f"{args.warehouse}"}}

def format_results(data,warehouse,account):
    formatted_data = []
    
    for record in data:
        formatted_record = {}
        for key, value in record.items():
            if isinstance(value, Decimal):
                formatted_record[key] = float(value)
            elif isinstance(value, datetime):
                formatted_record[key] = value.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_record[key] = value
        formatted_record['warehouse'] = warehouse
        formatted_record['account'] = account
        formatted_record['execution_timestamp'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
 
        formatted_data.append(formatted_record)
    return formatted_data

def main():

    parser = argparse.ArgumentParser(description="Fetch data from Snowflake.")
    parser.add_argument('--user', type=str, required= False, help="Snowflake username")
    parser.add_argument('--sf_keypair', type=str, required= False, help="Snowflake sf_keypair")
    parser.add_argument('--account', type=str, required= False, help="Snowflake account")
    parser.add_argument('--warehouse', type=str, required= False, help="Snowflake warehouse")
    parser.add_argument('--sql_query', type=str, required= False, help="SQL to be executed")
    args = parser.parse_args()

    
    sql_queries ={
    'sql_warehouse_load_history': """
    SELECT START_TIME, END_TIME, AVG_RUNNING, AVG_QUEUED_LOAD
     FROM table(information_schema.warehouse_load_history(DATE_RANGE_START => TIMEADD(minute, -20, CURRENT_TIMESTAMP()),WAREHOUSE_NAME=>'BLOOMBERG_AWS_WH'));""",

    'sql_check_version': """ SELECT current_version()"""
    }

    sql_query = sql_queries[args.sql_query]
    try:
       
        data = fetch_snowflake_data(args.user,args.sf_keypair,args.account,args.warehouse,sql_query)

# Display the version information
        formatted_data = format_results(data,args.warehouse,args.account)
        print(json.dumps(formatted_data))
    except:
        print("ERROR: Executing snowflake.py script failed")
        return -1   
    return 0


if __name__ == "__main__":
   main()
