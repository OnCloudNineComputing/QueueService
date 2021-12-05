from application_services.base_application_resource import BaseRDBApplicationResource
import database_services.dynamodb as dynamodb
from integrity_services.StudentsOnQueueIntegrityResource import StudentsOnQueueIntegrity
from datetime import datetime
import uuid

DATABASE_LIMIT = 5


class StudentsOnQueueResource(BaseRDBApplicationResource):

    def __init__(self):
        super().__init__()


    def get_all_students(cls, queue_id):
        table_name, partition_key, sort_key = cls.get_data_resource_info()
        result = dynamodb.query_table(table_name, key=partition_key, value=queue_id)
        return StudentsOnQueueIntegrity.oh_get_responses(result)

    @classmethod
    def create(cls, queue_id, data):
        table_name, partition_key, sort_key = cls.get_data_resource_info()
        col_validation = StudentsOnQueueIntegrity.column_validation(data.keys())
        if col_validation[0] == 200:
            validation = StudentsOnQueueIntegrity.input_validation(data)
            if validation[0] == 200:
                timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
                db_result = dynamodb.add_item(table_name, data, partition_key, queue_id, sort_key=sort_key, sort_val=timestamp)
            else:
                res = validation
        else:
            res = col_validation
        rsp = StudentsOnQueueIntegrity.post_responses(res, db_result=db_result)
        return rsp

    @classmethod
    def get_by_timestamp(cls, queue_id, timestamp):
        table_name, partition_key, sort_key = cls.get_data_resource_info()

        res = dynamodb.get_item(table_name, partition_key, queue_id, sort_key=sort_key, sort_value=timestamp)
        return StudentsOnQueueIntegrity.oh_get_responses(res)

    @classmethod
    def update_by_timestamp(cls, queue_id, timestamp, data):
        if_id_exits = StudentsOnQueueResource.get_by_queue_id(queue_id)

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
    def get_links(cls, resource_data, inputs):
        links = []
        path_args = []
        next_path_args = []
        prev_path_args = []

        path = inputs.path
        next_path = inputs.path
        prev_path = inputs.path
        if inputs.args:
            input_args = inputs.args
            for k, v in input_args.items():
                input_args[k] = v.replace(" ", "%20")
            path_args.append("&".join(["=".join([k, str(v)]) for k, v in input_args.items()]))
            next_path_args.append("&".join(["=".join([k, str(v)]) for k, v in input_args.items()]))
            prev_path_args.append("&".join(["=".join([k, str(v)]) for k, v in input_args.items()]))
        if inputs.fields:
            path_args.append("fields=" + inputs.fields)
            next_path_args.append("fields=" + inputs.fields)
            prev_path_args.append("fields=" + inputs.fields)
        if inputs.order_by:
            path_args.append("order_by=" + inputs.order_by)
            next_path_args.append("order_by=" + inputs.order_by)
            prev_path_args.append("order_by=" + inputs.order_by)
        else:
            path_args.append("order_by=id")
            next_path_args.append("order_by=id")
            prev_path_args.append("order_by=id")
        limit = 5
        if inputs.limit:
            if int(inputs.limit) < limit:
                limit = int(inputs.limit)
        path_args.append("limit=" + str(limit))
        next_path_args.append("limit=" + str(limit))
        prev_path_args.append("limit=" + str(limit))
        offset = 0
        if inputs.offset:
            offset = int(inputs.offset)
        path_args.append("offset=" + str(offset))
        next_path_args.append("offset=" + str(offset + limit))
        if offset != 0:
            prev_path_args.append("offset=" + str(offset - limit))

        if path_args:
            path += "?" + "&".join(path_args)
        if next_path_args:
            next_path += "?" + "&".join(next_path_args)
        if prev_path_args:
            prev_path += "?" + "&".join(prev_path_args)

        self_link = {"rel": "self", "href": path}
        links.append(self_link)
        next_link = {"rel": "next", "href": next_path}
        links.append(next_link)
        if offset != 0:
            prev_link = {"rel": "prev", "href": prev_path}
            links.append(prev_link)

        links_dict = {"links": links}
        resource_data.append(links_dict)

        return resource_data

    @classmethod
    def get_data_resource_info(cls):
        return 'Students', 'QueueId', 'Timestamp'
