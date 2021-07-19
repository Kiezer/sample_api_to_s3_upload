import requests
import os
import json
from config import RESOURCE_PATH
from requests.auth import AuthBase
import logging
import boto3.session
import sys
import uuid
import time


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

    # input_param = {
    #     "SCE": ["782002","782001","782003","778134"],
    #     "LADWP": ["787778","787779","776519","776518"],
    #     "SDG&E": ["787775","787776","787777"],
    #     "PG&E": ["787770", "787771", "787772"]
    # }




config = {
    "ACCESS_KEY": "THE accesss key",
    "SECRET_KEY": "The Secret key",
    "BUCKET_NAME": 'athena-query-result-720863956745',
    "KEY": 'userdata/power_analytics/{file_type}/user_name={user_name}',
    "API_TOKEN": "The token number"
}

my_session = boto3.session.Session(aws_access_key_id=f"{config['ACCESS_KEY']}",aws_secret_access_key=f"{config['SECRET_KEY']}")


def write_file_to_s3(put_object, user_name, file_type):

    ACCESS_KEY = config['ACCESS_KEY']
    SECRET_KEY = config['SECRET_KEY']
    BUCKET_NAME = config['BUCKET_NAME']
    KEY = config['KEY'].format(file_type=file_type, user_name=user_name) + f'/{file_type}_{uuid.uuid1()}'

    my_session = boto3.session.Session(aws_access_key_id=f"{ACCESS_KEY}",aws_secret_access_key=f"{SECRET_KEY}")
    s3_client = my_session.client('s3')

    response = s3_client.put_object(ACL='bucket-owner-full-control',Body=put_object, Bucket=BUCKET_NAME, Key=KEY)
    return response


def make_api_call(utility, meters=[], file_type='bills'):

    api_token = config['API_TOKEN']

    api_value = ""

    for meter in meters:
        response = requests.get(f'https://utilityapi.com/api/v2/{file_type}?meters={meter}&utility={utility}', 
            headers={'Authorization': f'Bearer {api_token}'})
            
        if response.status_code != 404:
            try:
                if response.json()[f'{file_type}']:
                    api_value +=  json.dumps(response.json()) + "\n"
            except KeyError as e:
                print(e)
    
    return api_value


def athena_query_runner(database, table_name, user_name):

    query = f"ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION (user_name='{user_name}');"
   
    logger.info("Submitting the query!!")
    logger.info(f"Query: {query} \nDatabase: {database}")

    athena_client = my_session.client('athena',region_name = 'us-east-1')

    # Inner function to check the status of the query.
    def status_check(query_execution_id):
    
        RETRY_COUNT = 5
        SLEEP_TIME = 20
    
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
            elif query_execution_status == 'SUCCEEDED':
                logger.info("The Query Succeeded")
                return query_execution_status
            elif query_execution_status == 'FAILED':
                logger.info(f"query_execution_id - {query_execution_id} Failed!! \nReason: {query_status_reason}")
                return "FAILED"
            else:
                logger.info("Did not found a valid status..")
                return "INVALID"
    

    # Execution
    response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': 's3://athena-query-result-720863956745/query_result/'
            }
        )
    # print(response['QueryExecutionId'])
    return status_check(response['QueryExecutionId'])


if __name__ == "__main__":

    file_types = ['bills', 'intervals']

    for file_type in file_types:
        # Make Api call to get the object
        put_object = make_api_call("SCE", ["782002","782001","782003","778134"], file_type=file_type)

        # Write file to the s3 bucket
        print(write_file_to_s3(put_object, 'test@test.com', file_type))

        print(athena_query_runner('sampledb', 'bills_table', 'test@test.com'))
