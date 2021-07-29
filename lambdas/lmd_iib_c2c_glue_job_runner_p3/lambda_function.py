import json
import boto3
import logging
import time
from boto3.dynamodb.types import TypeDeserializer

response_body = {}

# DynamoDB boto3 handles
dynamoDB_client = boto3.client('dynamodb')
dynamodb_resource = boto3.resource('dynamodb')

# c2c_config_table to get all the lambda configuration.
dyn_c2c_config_table_name = os.environ['config_table_name']
c2c_pipeline_config = dynamodb_resource.Table(dyn_c2c_config_table_name)

# number of retries
RETRY_COUNT = 10

# Sleep time for retries
SLEEP_TIME = 80

# Import Boto 3 for AWS Glue
glue_client = boto3.client('glue')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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


def glue_status_check(job_run_id, glue_job_name):
    
    global RETRY_COUNT
    
    while (RETRY_COUNT > 0):
        logger.info(f"RETRY_COUNT: {RETRY_COUNT}")
        time.sleep(SLEEP_TIME)
        logger.info(f"Sleeping for {str(SLEEP_TIME)} secs before checking the status!!")
        logger.info("Checking the status now!!")
        # Get glue_execution_status
        response = glue_client.get_job_run(JobName=glue_job_name, RunId=job_run_id, PredecessorsIncluded=False)
        job_status = response['JobRun']['JobRunState']
        
        logger.info(f"response: {response}")
        
        if (job_status == 'RUNNING' or job_status == 'STARTING'):
            # Increment the retry count
            RETRY_COUNT -= 1
            # Call the status_check again
            status = job_status
        elif (job_status == 'FAILED' or job_status == 'STOPPING' or job_status == 'STOPPED' or job_status == 'TIMEOUT'):
            status = job_status
            raise Exception(f"job_run_id - {job_run_id} Failed or Stopped or Timedout!!\nReason: {response['JobRun']['ErrorMessage']}")
        elif job_status == 'SUCCEEDED':
            logger.info(f"The job_run_id - {job_run_id} has {job_status}")
            status = job_status
            break
        else:
            status = job_status
            raise Exception("None of the conditions met!!")
    
    return status
        

def trigger_glue_job(file_type_config, load_date, hour):
    
    # 'source_db','mapping_list','target_table','source_table','target_db'
    
    glue_job_name = file_type_config.get('glue_job_name')
    athena_source_db = file_type_config.get('athena_source_db')
    rds_target_db = file_type_config.get('rds_target_db')
    glue_mapping = file_type_config.get('glue_mapping')
    source_table = file_type_config.get('athena_source_table')
    target_table = file_type_config.get('rds_target_table')
    
    glue_args = {
        "glue_job_name" : glue_job_name,
        "athena_source_db": athena_source_db,
        "rds_target_db" : rds_target_db,
        "glue_mapping": glue_mapping,
        "source_table": source_table,
        "target_table": target_table,
        "load_date": load_date,
        "hour": hour
    }
    
    logger.info(f'Triggering the Glue_job: {glue_job_name}')
    logger.info(f'glue_job_args: {glue_args}')

    response = glue_client.start_job_run(JobName = glue_job_name,
    Arguments={
        '--athena_source_db': athena_source_db,
        '--rds_target_db': rds_target_db,
        '--source_table': source_table,
        '--target_table': target_table,
        '--glue_mapping': str(glue_mapping),
        '--load_date': load_date,
        '--hour': hour}
        )
    
    logger.info('## STARTED GLUE JOB: ' + glue_job_name)
    logger.info('## GLUE JOB RUN ID: ' + response['JobRunId'])
    return response
    
    
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
    
    
def lambda_handler(event, context):
    # TODO implement
    
    logger.info(event)

    body = event['responsePayload']['body']
    file_type = body['file_type']
    load_date = body['load_date']
    hour = body['hour']
    
    response_body['file_type'] = file_type
    response_body['load_date'] = load_date
    response_body['hour'] = hour
    
    trigger_glue_job_flag = body.get('trigger_glue_job_flag', 'N')
    
    
    for_logging = {
            "file_type":file_type,
            "load_date": load_date,
            "hour": hour,
            "trigger_glue_job_flag" : trigger_glue_job_flag
    }
    
    logger.info(for_logging)
    
    if trigger_glue_job_flag == 'N':
        logger.info("received no-go signal!!")
        logger.info("Do Nothing")
        
        return {
            'statusCode': 404,
            'body': {
                "STATUS": "NO-SHOW",
                "file_type": file_type,
                "load_date" : load_date,
                "hour": hour,
                "trigger_glue_job_flag": trigger_glue_job_flag
            }
        }
    
    else:
        
        logger.info("Received signal to proceed!!")
        
        # Get the config from DynamoDb.
        
        file_type_config = from_dynamodb_to_json(get_dynamo_table_data(file_type, '2999-12-31')[0])
        
        # trigger the glue job
        glue_response = trigger_glue_job(file_type_config, load_date, hour)
        
        # Check the status
        status = glue_status_check(glue_response['JobRunId'],file_type_config['glue_job_name'])
        
        # Update the status
        update_schd_status(file_type, load_date)
        
        
        response_body['STATUS'] = status
    
    return {
        'statusCode': 200,
        'body': json.dumps(response_body)
    }
