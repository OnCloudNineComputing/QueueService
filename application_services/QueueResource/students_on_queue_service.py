from application_services.base_application_resource import BaseRDBApplicationResource
import database_services.dynamodb as dynamodb
from integrity_services.StudentsOnQueueIntegrityResource import StudentsOnQueueIntegrity
from datetime import datetime
import uuid

DATABASE_LIMIT = 5


class StudentsOnQueueResource(BaseRDBApplicationResource):

    def __init__(self):
        super().__init__()

    @classmethod
    def get_all_students(cls, queue_id):
        table_name, partition_key, sort_key = cls.get_data_resource_info()
        result = dynamodb.query_table(table_name, key=partition_key, value=queue_id)
        for res in result:
            res['links'] = cls.get_links(res)
        return StudentsOnQueueIntegrity.oh_get_responses(result)

    @classmethod
    def create(cls, queue_id, data):
        table_name, partition_key, sort_key = cls.get_data_resource_info()
        col_validation = StudentsOnQueueIntegrity.column_validation(data.keys())
        if col_validation[0] == 200:
            validation = StudentsOnQueueIntegrity.input_validation(data)
            if validation[0] == 200:
                timestamp = datetime.utcnow().strftime('%H:%M:%S.%f')
                db_result = dynamodb.add_item(table_name, data, partition_key, queue_id, sort_key=sort_key, sort_val=timestamp)
                rsp = StudentsOnQueueIntegrity.post_responses(validation, db_result=cls.get_links(data, queue_id=queue_id, timestamp=timestamp))
                return rsp
            else:
                res = validation
        else:
            res = col_validation
        rsp = StudentsOnQueueIntegrity.post_responses(res)
        return rsp

    @classmethod
    def get_by_timestamp(cls, queue_id, timestamp):
        table_name, partition_key, sort_key = cls.get_data_resource_info()

        res = dynamodb.get_item(table_name, partition_key, queue_id, sort_key=sort_key, sort_value=timestamp)
        res['links'] = cls.get_links(res)
        return StudentsOnQueueIntegrity.oh_get_responses(res)

    @classmethod
    def update_by_timestamp(cls, queue_id, timestamp, data):
        if_id_exits = StudentsOnQueueResource.get_by_timestamp(queue_id, timestamp)

        if if_id_exits.status_code == 200:
            col_validation = StudentsOnQueueIntegrity.column_validation(data.keys())
            if col_validation[0] == 200:
                validation = StudentsOnQueueIntegrity.type_validation(data)
                if validation[0] == 200:
                    table_name, partition_key, sort_key = cls.get_data_resource_info()
                    res = dynamodb.update_item(table_name, data, partition_key, queue_id, sort_key=sort_key, sort_val=timestamp)
                else:
                    res = validation
            else:
                res = col_validation
        else:
            res = None
        return StudentsOnQueueIntegrity.oh_put_responses(res)

    @classmethod
    def get_links(cls, data, queue_id=None, timestamp=None):
        if queue_id is None:
            queue_id = data['QueueId']
        if timestamp is None:
            timestamp = data['Timestamp']
        links = {
            'self': '/queue/{0}/students/{1}'.format(queue_id, timestamp)
        }
        return links

    @classmethod
    def get_data_resource_info(cls):
        return 'Students', 'QueueId', 'Timestamp'
