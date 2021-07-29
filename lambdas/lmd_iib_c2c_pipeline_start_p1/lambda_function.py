import json
import datetime
from datetime import timezone
import logging
import boto3
import os
from boto3.dynamodb.types import TypeDeserializer

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

response_body = {}

# client for s3
s3_client = boto3.client('s3')

# DynamoDB boto3 handles
dyn_c2c_config_table_name = os.environ['config_table_name']
dynamoDB_client = boto3.client('dynamodb')
dynamodb_resource = boto3.resource('dynamodb')
c2c_pipeline_config = dynamodb_resource.Table(dyn_c2c_config_table_name)


# Util Functions
addZero = lambda x : '0' + x if len(x) == 1 else x

# Converts Dynmo object to json
def from_dynamodb_to_json(item):
    d = TypeDeserializer()
    return {k: d.deserialize(value=v) for k, v in item.items()}


# compares 2 datetime and return true or false. Used to get latest file to process 
def compare_dates(event_date, schedule_datetime):
    
    # convert schedule_datetime to event_date timezone
    new_schedule_datetime = schedule_datetime.replace(tzinfo=schedule_datetime.tzinfo).astimezone(tz=event_date.tzinfo)
    
    return event_date > new_schedule_datetime
    
    
# Funciton to get the config from Dynamo table based on file_type and date
def get_dynamo_table_data(file_type, load_date=None):
    
    response = dynamoDB_client.query(
        TableName=dyn_c2c_config_table_name,
        KeyConditionExpression='file_type = :f and load_date = :sd',
        FilterExpression = 'processing_flag = :pf',
        ExpressionAttributeValues={
            ':f': {'S': file_type},
            ':sd' : {'S': load_date},
            ':pf' : {'S': 'Y'}
        }
    )
    
    return response['Items']


def get_processing_data(file_type,pf='Y'):

    response = dynamoDB_client.query(
        TableName=dyn_c2c_config_table_name,
        KeyConditionExpression='file_type = :f',
        FilterExpression = 'processing_flag = :pf',
        ProjectionExpression = 'load_date',
        ExpressionAttributeValues={
            ':f': {'S': file_type},
            ':pf' : {'S': pf}
        }
    )
    
    return response



# Creates all 96 schedule for the day
def get_schd_day_list(frequency):
    schd_list = []
    id = 0
    if frequency != None and frequency == 'Hourly':
        for hour in range(0,24):
            hr = addZero(str(hour))
            time = hr + ':59:00'
            id += 1
            dict_name = dict()
            dict_name['schd_id'] = int(id)
            dict_name['schd_time'] = time
            dict_name['status'] = 'q'
            schd_list.append(dict_name)
    else:
        for hour in range(0,24):
            min_list = ['15:00', '30:00', '45:00', '59:59']
            for i in min_list:
                hr = addZero(str(hour))
                time = hr + ':' + i
                id += 1
                dict_name = dict()
                dict_name['schd_id'] = int(id)
                dict_name['schd_time'] = time
                dict_name['status'] = 'q'
                schd_list.append(dict_name)
    return schd_list


# Use DynamoDB boto3's put_item API to add the schedule to the config table
def put_item_dynamodb(file_type, load_date, frequency=None):
    
    dynamoDb_put_item = {
        "file_type" : file_type,
        "load_date" : load_date,
        "processing_flag" : "Y",
        "schd_day" : get_schd_day_list(frequency)
    }
    
    c2c_pipeline_config.put_item(Item = dynamoDb_put_item)

    return dynamoDb_put_item
    

# returns a response body
def get_resp_body(**kwargs):
    resp_body = {}
    for k,v in kwargs.items():
        resp_body[k] = v
    return resp_body


def lambda_handler(event, context):
    # TODO implement
    
    try:
        file_type = json.loads(event)["file_type"]
        schd_start_date = str(json.loads(event)["schd_start_datetime"]).split("T")[0]
        event_date = str(json.loads(event)["event_datetime"]).split("T")[0]
        event_datetime = str(json.loads(event)["event_datetime"])
        frequency = json.loads(event).get("frequency",None)
    except TypeError as t:
        file_type = event["file_type"]
        schd_start_date = str(event["schd_start_datetime"]).split("T")[0]
        event_date = str(event["event_datetime"]).split("T")[0]
        event_datetime = str(event["event_datetime"])
        frequency = event.get("frequency",None)
    
        
    logger_info = {
        "file_type" : file_type,
        "schd_start_date" : schd_start_date,
        "event_date" : event_date,
        "event_datetime" : event_datetime,
        "frequency" : frequency
    }
    
    logger.info(f"{json.dumps(logger_info)}")
    
    response_body['file_type'] = file_type
    
    # Check if its the latest schedule to be processed
    dynamo_resp = get_processing_data(file_type,pf='Y')
    
    logger.info(f"dynamo_resp: {dynamo_resp}")
    
    # Initialize file_config
    file_config = {}
    
    if dynamo_resp.get('Count') == 0:
        logger.info("Getting the latest date to process...")
        
        pf_N_resp = get_processing_data(file_type,pf='N')
        if pf_N_resp.get('Items'):
            items = pf_N_resp.get('Items')
            load_date_deserde = [from_dynamodb_to_json(x) for x in items]
            logger.info(f"load_date_deserde : {load_date_deserde}")
            # Get the previous process date that was processed
            last_process_date = sorted(load_date_deserde, key = lambda i : datetime.datetime.strptime(i['load_date'], '%Y-%m-%d'),reverse=True)[0]['load_date']
            # Add one to the previous process date
            latest_date = datetime.datetime.strptime(last_process_date, '%Y-%m-%d') + datetime.timedelta(days=1)
            logger.info(f"The latest processing date: {str(latest_date.date())}")
            file_config = put_item_dynamodb(file_type,str(latest_date.date()), frequency=frequency)
        else:
            logger.info(f"Creating first schedule for file_type: {file_type}")
            file_config = put_item_dynamodb(file_type,schd_start_date, frequency=frequency)
    elif dynamo_resp.get('Count') == 1:
        processing_date = from_dynamodb_to_json(dynamo_resp.get('Items')[0]).get('load_date')
        logger.info(f"Get the latest schedule from processing_date: {processing_date}")
        file_config = from_dynamodb_to_json(get_dynamo_table_data(file_type, load_date=processing_date)[0])
        
    
    # Get the latest schedule from the schd_day in the config
    logger.info(f"schd_day: {file_config['schd_day']}")
    schd_day = file_config['schd_day']
    # Get the Latest Schedule from the item with Status queued
    schedule = sorted(schd_day, key = lambda items: items['schd_id'] if items['status']=='q' else 999999)[0]
    logger.info(f"Latest Schdule: {schedule}")
            
        

    
    logger.info(f"schdule_datetime: {file_config['load_date']}T{schedule['schd_time']}Z")
    schdule_datetime = datetime.datetime.strptime(file_config['load_date'] + 'T' + schedule['schd_time'] + 'Z', "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    
    logger.info(f"event_datetime: {event_datetime}")
    event_datetime = datetime.datetime.strptime(event_datetime, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    
    compare_datetime = lambda x,y : x > y.replace(tzinfo=y.tzinfo).astimezone(tz=x.tzinfo)
    hour = addZero(str(schdule_datetime.hour))
    
    if compare_datetime(event_datetime, schdule_datetime):
        # Get the filenames from the work bucket
        response_body['query_flag'] = 'Y'
        response_body['hour'] = hour
        response_body['load_date'] = file_config['load_date']
    else:
        logger.info("Schedule is running ahead")
        logger.info("Process nothing")
        response_body['query_flag'] = 'N'
        response_body['hour'] = hour
        response_body['load_date'] = file_config['load_date']

    
    logger.info(f"response_body: {response_body}")
    
    
    return {
        'statusCode': 200,
        'body': response_body
    }
