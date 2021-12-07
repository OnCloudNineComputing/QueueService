from application_services.base_application_resource import BaseRDBApplicationResource
import database_services.dynamodb as dynamodb
from integrity_services.QueueIntegrityResource import QueueIntegrity
from datetime import datetime
import uuid

DATABASE_LIMIT = 5


class QueueResource(BaseRDBApplicationResource):

    def __init__(self):
        super().__init__()

    @classmethod
    def get_all(cls):
        table_name, partition_key = cls.get_data_resource_info()
        items = dynamodb.get_all_items(table_name)
        for item in items:
            item['links'] = cls.get_links(item)
        return QueueIntegrity.oh_get_responses(items)

    @classmethod
    def create(cls, data):
        table_name, partition_key = cls.get_data_resource_info()
        col_validation = QueueIntegrity.column_validation(data.keys())
        if col_validation[0] == 200:
            validation = QueueIntegrity.input_validation(data)
            if validation[0] == 200:
                queue_id = str(uuid.uuid4())
                dynamodb.add_item(table_name, data, partition_key, queue_id)
                data['QueueId'] = queue_id
                rsp = QueueIntegrity.post_responses(validation, db_result={'links': cls.get_links(data, queue_id)})
            else:
                res = validation
                rsp = QueueIntegrity.post_responses(res)

        else:
            res = col_validation
            rsp = QueueIntegrity.post_responses(res)

        return rsp

    @classmethod
    def get_by_queue_id(cls, queue_id):
        table_name, partition_key = cls.get_data_resource_info()

        res = dynamodb.get_item(table_name, partition_key, queue_id)
        res['links'] = cls.get_links(res)
        return QueueIntegrity.oh_get_responses(res)

    @classmethod
    def update_by_queue_id(cls, queue_id, data):
        if_id_exits = QueueResource.get_by_queue_id(queue_id)

        if if_id_exits.status_code == 200:
            col_validation = QueueIntegrity.column_validation(data.keys())
            if col_validation[0] == 200:
                validation = QueueIntegrity.type_validation(data)
                if validation[0] == 200:
                    table_name, partition_key = cls.get_data_resource_info()
                    res = dynamodb.update_item(table_name, data, partition_key, queue_id)
                else:
                    res = validation
            else:
                res = col_validation
        else:
            res = None
        return QueueIntegrity.oh_put_responses(res)

    @classmethod
    def get_links(cls, data, queue_id=None):
        if queue_id is None:
            queue_id = data['queue_id']
        links = {
            'self': '/queue/{0}'.format(queue_id),
            'students': '/queue/{0}/students'.format(queue_id),
        }
        return links

    @classmethod
    def get_data_resource_info(cls):
        return 'Queues', 'queue_id'
