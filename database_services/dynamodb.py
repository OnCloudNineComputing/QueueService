import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
from datetime import datetime
import time
import uuid
from middleware import context
import string


def get_db_connection():
    # get keys from aws credentials
    db_info = context.get_dynamo_info()
    dynamodb = boto3.resource('dynamodb',
                              aws_access_key_id=db_info['aws_access_key_id'],
                              aws_secret_access_key=db_info['aws_secret_access_key'],
                              aws_session_token=db_info['aws_session_token'],
                              region_name=db_info['region_name'])

    return dynamodb


def get_all_items(table_name):
    dynamodb = get_db_connection()
    table = dynamodb.Table(table_name)
    response = table.scan()
    return (response['Items'])


# query for partition key, will auto sort on sort key if present
def query_table(table_name, key=None, value=None):
    dynamodb = get_db_connection()
    table = dynamodb.Table(table_name)

    if key and value:
        filtering_exp = Key(key).eq(value)
        return table.query(KeyConditionExpression=filtering_exp)

    raise ValueError('Parameters missing or invalid')


# get one item from table
def get_item(table_name, partition_key, partition_value, sort_key=None, sort_value=None):
    dynamodb = get_db_connection()
    table = dynamodb.Table(table_name)
    if sort_key and sort_value:
        response = table.get_item(Key={
            partition_key: partition_value,
            sort_key: sort_value
        })
    else:
        response = table.get_item(Key={
            partition_key: partition_value
        })
    return response


def put_item(table_name, item):
    dynamodb = get_db_connection()
    table = dynamodb.Table(table_name)
    res = table.put_item(Item=item)
    return res


def add_item(table_name, item, partition_key, partition_val, sort_key=None, sort_val=None):
    item[partition_key] = partition_val

    # only add sort key for Students table
    if sort_key and sort_val:
        item[sort_key] = sort_val

    res = put_item(table_name, item=item)

    return res


def update_item(table_name, data, partition_key, partition_val, sort_key=None, sort_val=None):
    dynamodb = get_db_connection()
    alpha = list(string.ascii_lowercase)

    counter = 0
    update_expression = []
    expression_values = {}
    for key in data:
        letter = alpha[counter]
        update_expression.append("{0}=:{1}".format(key, letter))
        expression_values[":" + letter] = data[key]
    update_expression =  "set " + ', '.join(update_expression)

    if sort_key:
        key = {partition_key: partition_val, sort_key: sort_val}
    else:
        key = {partition_key: partition_val}

    table = dynamodb.Table(table_name)

    response = table.update_item(
        Key=key,
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_values,
        ReturnValues="UPDATED_NEW"
    )
    return response


# add item to Students table
def test_add_item_students():
    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    item = {
        "Name": "Name3",
        "StudentUNI": "aaa123",
        "IfTaken": 0,
        "Notes": "Help with first question"
    }
    add_item('Students', item, 'QueueId', '1', 'Timestamp', timestamp)


# add item to Queue table
def test_add_item_queue():
    item = {
        "OfficeHoursID": "234455",
        "CourseID": "q43224",
        "Zoom": "fakeLink",
        "Building": "Mudd",
        "VersionID": str(uuid.uuid4())

    }
    add_item('Queues', item, 'QueueId', str(uuid.uuid4()))


# query students table by QueueId
def test_query_students_id():
    res = query_table('Students', 'QueueId', '1')
    print("Result = \n", json.dumps(res, indent=4, default=str))


# query students table by QueueId and Timestamp
def test_get_student_item():
    res = get_item('Students', 'QueueId', '1', 'Timestamp', "2021-12-05 06:14:11.515")
    print("Result = \n", json.dumps(res, indent=4, default=str))


# query queue table by QueueId
def test_query_queue_id():
    res = query_table('Queues', 'QueueId', "64572841-89e5-4c42-89a2-b963a689d862")
    print("Result = \n", json.dumps(res, indent=4, default=str))


# change student "IsTaken" field to 1 or 0
def test_student_taken():
    update_student_taken('Students', "QueueId", "beba33cc-2856-457a-b50e-9fedcefef8d7", "Timestamp",
                         "2021-12-05 05:55:46.666")


##### unused functions from template that could be helpful

def find_by_template(table_name, template):
    dynamodb = get_db_connection()
    fe = ' AND '.join(['{0}=:{0}'.format(k) for k, v in template.items()])
    ea = {':{}'.format(k): v for k, v in template.items()}

    tbl = dynamodb.Table(table_name)
    result = tbl.scan(
        FilterExpression=fe,
        ExpressionAttributeValues=ea
    )
    return result


def find_by_tag(tag):
    dynamodb = get_db_connection()
    table = dynamodb.Table("comments")

    expressionAttributes = dict()
    expressionAttributes[":tvalue"] = tag
    filterExpression = "contains(tags, :tvalue)"

    result = table.scan(FilterExpression=filterExpression,
                        ExpressionAttributeValues=expressionAttributes)
    return result


def do_a_scan(table_name, filter_expression=None, expression_attributes=None, projection_expression=None,
              expression_attribute_names=None):
    dynamodb = get_db_connection()
    table = dynamodb.Table(table_name)

    if filter_expression is not None and projection_expression is not None:
        if expression_attribute_names is not None:
            response = table.scan(
                FilterExpression=filter_expression,
                ExpressionAttributeValues=expression_attributes,
                ProjectionAttributes=projection_expression,
                ExpressionAttributeNames=expression_attribute_names
            )
        else:
            response = table.scan(
                FilterExpression=filter_expression,
                ExpressionAttributeValues=expression_attributes,
                ProjectionAttributes=projection_expression)
    elif filter_expression is not None:
        if expression_attribute_names is not None:
            response = table.scan(
                FilterExpression=filter_expression,
                ExpressionAttributeValues=expression_attributes,
                ExpressionAttributeNames=expression_attribute_names
            )
        else:
            response = table.scan(
                FilterExpression=filter_expression,
                ExpressionAttributeValues=expression_attributes
            )
    elif projection_expression is not None:
        if expression_attribute_names is not None:
            response = table.scan(
                ProjectionExpression=projection_expression,
                ExpressionAttributeNames=expression_attribute_names
            )
        else:
            response = table.scan(
                ProjectionExpression=projection_expression
            )
    else:
        response = table.scan()

    return response["Items"]