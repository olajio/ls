#!/usr/bin/env python3

import snowflake.connector as snow
from datetime import datetime
from decimal import Decimal
import json
import argparse
import uuid
import sys
import os
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# Import your json_log_format if available, otherwise use standard logging
try:
    import json_log_format as jlf
    #trace.id
    trace_uuid = str(uuid.uuid4())
    
    #Creating and Configuring the JSON Logger
    #---------------------------------------
    jlf.service_name = "snowflake-logs"
    jlf.service_type = "Snowflake"
    jlf.trace_id = trace_uuid
    jlf.json_logging.init_non_web(custom_formatter=jlf.CustomJSONLog, enable_json=True)
    logger = jlf.logging.getLogger(__name__)
    logger.setLevel(jlf.logging.INFO)
    logger.addHandler(jlf.logging.StreamHandler(jlf.sys.stdout))
    #---------------------------------------
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

def load_private_key_from_file(keypair_path, passphrase=None):
    """
    Load private key from PEM file for Snowflake authentication
    
    Args:
        keypair_path (str): Path to the PEM file containing the private key
        passphrase (str, optional): Passphrase for encrypted private keys
    
    Returns:
        Private key object suitable for Snowflake connection
    """
    try:
        # Check if file exists
        if not os.path.exists(keypair_path):
            raise FileNotFoundError(f"Private key file not found: {keypair_path}")
        
        # Read the private key file
        with open(keypair_path, 'rb') as key_file:
            private_key_data = key_file.read()
        
        # Load the private key
        private_key = serialization.load_pem_private_key(
            private_key_data,
            password=passphrase.encode() if passphrase else None,
            backend=default_backend()
        )
        
        logger.info(f"Successfully loaded private key from {keypair_path}")
        return private_key
        
    except FileNotFoundError as e:
        logger.error(f"Private key file not found: {e}")
        raise
    except ValueError as e:
        logger.error(f"Invalid private key format or wrong passphrase: {e}")
        raise
    except Exception as e:
        logger.error(f"Error loading private key: {e}")
        raise

def fetch_snowflake_data(user, keypair_path, account, warehouse, sql_query, passphrase=None):
    """
    Fetch data from Snowflake using private key authentication
    
    Args:
        user (str): Snowflake username
        keypair_path (str): Path to private key PEM file
        account (str): Snowflake account identifier
        warehouse (str): Snowflake warehouse name
        sql_query (str): SQL query to execute
        passphrase (str, optional): Private key passphrase if encrypted
    
    Returns:
        List of dictionaries containing query results
    """
    connection = None
    cursor = None
    
    try:
        # Load the private key from file
        private_key = load_private_key_from_file(keypair_path, passphrase)
        
        # Create connection to Snowflake
        logger.info(f"Connecting to Snowflake account: {account}")
        connection = snow.connect(
            account=account,
            user=user,
            private_key=private_key,  # Use private_key instead of sf_keypair
            warehouse=warehouse,
            database="SNOWFLAKE"
        )
        
        logger.info("Successfully connected to Snowflake")
        
        # Execute the query
        cursor = connection.cursor()
        cursor.execute(f"USE WAREHOUSE {warehouse};")
        logger.info(f"Executing query: {sql_query}")
        cursor.execute(sql_query)
        
        # Fetch results
        results = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        
        # Convert to list of dictionaries
        data = [dict(zip(columns, row)) for row in results]
        logger.info(f"Query executed successfully, returned {len(data)} rows")
        
        return data
        
    except snow.errors.DatabaseError as e:
        logger.error(f"Snowflake database error: {e}")
        print(json.dumps({
            "error": "database_error",
            "message": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }))
        return None
        
    except snow.errors.ProgrammingError as e:
        logger.error(f"Snowflake programming error: {e}")
        print(json.dumps({
            "error": "programming_error", 
            "message": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }))
        return None
        
    except Exception as e:
        logger.error(f"Unexpected error connecting to Snowflake: {e}")
        print(json.dumps({
            "error": "connection_error",
            "message": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }))
        return None
        
    finally:
        # Clean up connections
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            logger.info("Snowflake connection closed")

def format_results(data, warehouse, account):
    """
    Format query results for JSON output
    
    Args:
        data (list): Raw query results
        warehouse (str): Snowflake warehouse name
        account (str): Snowflake account identifier
    
    Returns:
        List of formatted dictionaries
    """
    if not data:
        return []
    
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
                
        # Add metadata
        formatted_record['warehouse'] = warehouse
        formatted_record['account'] = account
        formatted_record['execution_timestamp'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        
        formatted_data.append(formatted_record)
    
    return formatted_data

def main():
    """Main execution function"""
    
    parser = argparse.ArgumentParser(description="Fetch data from Snowflake using private key authentication.")
    parser.add_argument('--user', type=str, required=True, help="Snowflake username")
    parser.add_argument('--sf_keypair', type=str, required=True, help="Path to Snowflake private key PEM file")
    parser.add_argument('--account', type=str, required=True, help="Snowflake account identifier")
    parser.add_argument('--warehouse', type=str, required=True, help="Snowflake warehouse name")
    parser.add_argument('--sql_query', type=str, required=True, help="SQL query identifier to execute")
    parser.add_argument('--passphrase', type=str, required=False, help="Private key passphrase (if encrypted)")
    
    args = parser.parse_args()
    
    # Define available SQL queries
    sql_queries = {
        'sql_warehouse_load_history': """
        SELECT START_TIME, END_TIME, AVG_RUNNING, AVG_QUEUED_LOAD
        FROM table(information_schema.warehouse_load_history(
            DATE_RANGE_START => TIMEADD(minute, -20, CURRENT_TIMESTAMP()),
            WAREHOUSE_NAME => %s
        ));
        """,
        'sql_check_version': "SELECT current_version() AS version;",
        'sql_warehouse_usage': """
        SELECT warehouse_name, start_time, end_time, credits_used
        FROM information_schema.warehouse_metering_history
        WHERE start_time >= TIMEADD(hour, -24, CURRENT_TIMESTAMP())
        ORDER BY start_time DESC
        LIMIT 100;
        """
    }
    
    # Validate SQL query identifier
    if args.sql_query not in sql_queries:
        error_response = {
            "error": "invalid_query",
            "message": f"SQL query '{args.sql_query}' not found. Available queries: {list(sql_queries.keys())}",
            "timestamp": datetime.utcnow().isoformat()
        }
        print(json.dumps(error_response))
        return 1
    
    # Get the SQL query
    sql_query = sql_queries[args.sql_query]
    
    # For parameterized queries, substitute warehouse name
    if args.sql_query == 'sql_warehouse_load_history':
        sql_query = sql_query.replace('%s', f"'{args.warehouse}'")
    
    try:
        # Validate input parameters
        if not os.path.exists(args.sf_keypair):
            error_response = {
                "error": "file_not_found",
                "message": f"Private key file not found: {args.sf_keypair}",
                "timestamp": datetime.utcnow().isoformat()
            }
            print(json.dumps(error_response))
            return 1
        
        # Fetch data from Snowflake
        logger.info("Starting Snowflake data fetch operation")
        data = fetch_snowflake_data(
            user=args.user,
            keypair_path=args.sf_keypair,
            account=args.account,
            warehouse=args.warehouse,
            sql_query=sql_query,
            passphrase=args.passphrase
        )
        
        # Check if data fetch was successful
        if data is None:
            logger.error("Data fetch failed, check previous error messages")
            return 1
        
        # Format and output results
        formatted_data = format_results(data, args.warehouse, args.account)
        
        # Output as JSON (one object per line for logstash json_lines codec)
        for record in formatted_data:
            print(json.dumps(record))
        
        logger.info(f"Successfully processed {len(formatted_data)} records")
        return 0
        
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        return 1
        
    except Exception as e:
        logger.error(f"Unexpected error in main execution: {e}")
        error_response = {
            "error": "execution_failed",
            "message": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }
        print(json.dumps(error_response))
        return 1

if __name__ == "__main__":
    sys.exit(main())
