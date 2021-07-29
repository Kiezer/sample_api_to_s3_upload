import json
import time
from timeit import default_timer as timer
import boto3
from boto3.dynamodb.types import TypeDeserializer
import logging
import os


# DynamoDB boto3 handles
dynamoDB_client = boto3.client('dynamodb')
dynamodb_resource = boto3.resource('dynamodb')

# c2c_config_table to get all the lambda configuration.
dyn_c2c_config_table_name = os.environ['config_table_name']
c2c_pipeline_config = dynamodb_resource.Table(dyn_c2c_config_table_name)

# number of retries
RETRY_COUNT = 10

# Sleep time for retries
SLEEP_TIME = 10

# Get the athena client handle
athena_client = boto3.client('athena')

# Get the s3 resource handle
s3_resource = boto3.resource('s3')


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Response body
response_body = {}


# A util Lambda
addQuote = lambda x: f"'{x}'"

# Converts Dynmo object to json
def from_dynamodb_to_json(item):
    d = TypeDeserializer()
    return {k: d.deserialize(value=v) for k, v in item.items()}


# Funciton to get the config from Dynamo table based on file_type and date
def get_dynamo_table_data(file_type, load_date):

    response = dynamoDB_client.query(
        TableName=dyn_c2c_config_table_name,
        KeyConditionExpression='file_type = :f and load_date = :sd ',
        ExpressionAttributeValues={
            ':f': {'S': file_type},
            ':sd' : {'S': load_date}
        }
    )
    
    return response['Items']


def update_schd_status(file_type, load_date):
    
    logger.info("updating the status of the DynamoDB table")
    # Get the day's schedule list
    schedule_before = from_dynamodb_to_json(get_dynamo_table_data(file_type,load_date)[0])['schd_day']
    
    # update the schd list to update the status of the schd
    schedule = sorted(schedule_before, key = lambda items: items['schd_id'] if items['status']=='q' else 9999999)[0]
    schedule_before.remove(schedule)
    schedule['status'] = 's'
    processing_flag = 'N' if schedule['schd_id'] == 96 or schedule['schd_id'] == 24  else 'Y'
    schedule_after = [schedule]
    [schedule_after.append(i) for i in schedule_before]
    
    # API call to update the Dynamo table config
    response = c2c_pipeline_config.update_item(
    Key={
        'file_type': file_type,
        'load_date': load_date
    },
    UpdateExpression='SET schd_day = :val, processing_flag = :pf',
    ExpressionAttributeValues={
        ':val': schedule_after,
        ':pf': processing_flag
    }
    )
    
    logger.info(response)
    
    return True


def terminate_query(query_execution_id):
    query_execution_status = get_status(athena_client, query_execution_id)
    athena_client.stop_query_execution(QueryExecutionId=query_execution_id)
    

def status_check(query_execution_id):
    
    global RETRY_COUNT
    
    while(RETRY_COUNT > 0):
        logger.info(f"RETRY_COUNT: {RETRY_COUNT}")
        logger.info(f"Sleeping for {str(SLEEP_TIME)} secs before checking the status!!")
        time.sleep(SLEEP_TIME)
        logger.info("Checking the status now!!")
        
        # Get query_execution_status
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status['QueryExecution']['Status']['State']
        query_status_reason = query_status['QueryExecution']['Status'].get('StateChangeReason', 'No StateChangeReason')
        
        if (query_execution_status == 'QUEUED' or query_execution_status == 'RUNNING'):
            # Increment the retry count
            RETRY_COUNT -= 1

        elif query_execution_status == 'FAILED':
            exception_message ="query_execution_id - " + query_execution_id + " Failed!! " + "\nReason: " + query_status_reason
            raise Exception(exception_message)
        elif query_execution_status == 'SUCCEEDED':
            logger.info("The Query Succeeded")
            return query_execution_status
        else:
            logger.info("None of the conditions met terminating the query!!")
            terminate_query(query_execution_id)
            return "TERMINATED"


def athena_query_runner(query, database):
    
    logger.info("Submitting the query!!")
    logger.info(f"Query: {query} \nDatabase: {database}")
    query_execution_id = None
    # Execution
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            }
        )
        query_execution_id = response['QueryExecutionId']
    except InternalServerException as e:
        exception_message = "Encountered InternalServerException!!"
        logger.info(f"exception_message : {exception_message}, The query_execution_id will remain None")
    finally:
        logger.info(f"query_execution_id: {query_execution_id}")
        return query_execution_id
        

def lambda_handler(event, context):
    # TODO implement

    body = event['responsePayload']['body']
    
    file_type = body['file_type']
    load_date = body['load_date']
    hour = body['hour']
    
    response_body['file_type'] = file_type
    response_body['load_date'] = load_date
    response_body['hour'] = hour
    
    for_logging = {
            "file_type":body['file_type'],
            "load_date": body['load_date'],
            "hour": body['hour'],
            "query_flag" : body['query_flag']
        }
    
    if body['query_flag'] == 'N':
        logger.info(f"{for_logging}")
        logger.info("Recieved No go for query run.")
        logger.info("Gonna Do Nothing!!")
        response_body['update_status_flag'] = 'N'
        response_body['trigger_glue_job_flag'] = 'N'
        
    else:
        
        logger.info("Recieved signal to proceed")
        logger.info(f"{for_logging}")
        # Get the config from the dynamo table c2c_pipeline_config
        file_type_config = from_dynamodb_to_json(get_dynamo_table_data(file_type, '2999-12-31')[0])
        
        
        source_table = file_type_config['source_table']
        target_table = file_type_config['target_table']
        insert_query = file_type_config['insert_sql'].format(source_table=source_table, target_table=target_table, load_date=addQuote(load_date), hour=addQuote(hour))
        # source_drop_partition_query = file_type_config['drop_partition'].format(table_name=source_table, load_date=addQuote(load_date), hour=addQuote(hour))
        source_add_partition_query = file_type_config['add_partition'].format(table_name=source_table, load_date=addQuote(load_date), hour=addQuote(hour))
        # target_add_partition_query = file_type_config['add_partition'].format(table_name=target_table, load_date=addQuote(load_date), hour=addQuote(hour))
        database = file_type_config['database']
        
        queries = [source_add_partition_query, insert_query]
        
        for query in queries:
            query_execution_id = athena_query_runner(query, database)
            status = status_check(query_execution_id)
            response_body['STATUS'] = status


        if file_type_config.get('glue_export_flag'):
            response_body['update_status_flag'] = 'Y'
            response_body['trigger_glue_job_flag'] = 'Y'
        else:
            update_schd_status(file_type, load_date)
            response_body['update_status_flag'] = 'N'
        
        logger.info(f"{response_body}")

        return {
            'statusCode': 200,
            'body': response_body
        }
