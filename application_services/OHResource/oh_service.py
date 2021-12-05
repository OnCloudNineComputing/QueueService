from application_services.base_application_resource import BaseRDBApplicationResource
from database_services.RDBService import RDBService
from integrity_services.OHIntegrityResource import OHIntegrity
from datetime import datetime

DATABASE_LIMIT = 5


class OHResource(BaseRDBApplicationResource):

    def __init__(self):
        super().__init__()

    @classmethod
    def get_by_template(cls, template, inputs, order_by=None, limit=None, offset=None, field_list=None):
        if field_list is not None and not OHIntegrity.field_validation(field_list.split(',')):
            return OHIntegrity.generate_response(404, "Invalid Field Selectors")
        if order_by is not None and not OHIntegrity.field_validation([order_by]):
            return OHIntegrity.generate_response(404, "Column not found")
        if limit is not None and int(limit) > DATABASE_LIMIT:
            limit = 5
        res = super().get_by_template(template, order_by=order_by, limit=limit, offset=offset, field_list=field_list)
        if res:
            res = OHResource.get_links(res, inputs)
        return OHIntegrity.oh_get_responses(res)

    @classmethod
    def delete_by_template(cls, template):
        res = super().delete_by_template(template)

    @classmethod
    def update_by_template(cls, data_template, where_template):
        res = super().update_by_template(data_template, where_template)

    @classmethod
    def create(cls, data):
        col_validation = OHIntegrity.column_validation(data.keys())
        if col_validation[0] == 200:
            validation = OHIntegrity.input_validation(data)
            if validation[0] == 200:
                data["start_time"] = datetime.strptime(data["start_time"], "%I:%M %p").time()
                data["end_time"] = datetime.strptime(data["end_time"], "%I:%M %p").time()
                data["start_date"] = datetime.strptime(data["start_date"], "%m/%d/%y").date()
                data["end_date"] = datetime.strptime(data["end_date"], "%m/%d/%y").date()
                data["id"] = None
                res = super().create(data)
            else:
                res = validation
        else:
            res = col_validation
        rsp = OHIntegrity.post_responses(res)
        return rsp

    @classmethod
    def get_by_oh_id(cls, oh_id, order_by=None, field_list=None):
        if field_list is not None and not OHIntegrity.field_validation(field_list.split(',')):
            return OHIntegrity.generate_response(404, "Invalid Field Selectors")
        if order_by is not None and not OHIntegrity.field_validation([order_by]):
            return OHIntegrity.generate_response(404, "Column not found")
        db_name, table_name = OHResource.get_data_resource_info()
        template = {"id": oh_id}
        res = RDBService.find_by_template(db_name, table_name, template, order_by=order_by, field_list=field_list)
        return OHIntegrity.oh_get_responses(res)

    @classmethod
    def delete_by_oh_id(cls, oh_id):
        if_id_exits = OHResource.get_by_oh_id(oh_id)
        if if_id_exits.status_code == 200:
            db_name, table_name = OHResource.get_data_resource_info()
            template = {"id": oh_id}
            res = RDBService.delete_by_template(db_name, table_name, template)
        else:
            res = None

        return OHIntegrity.oh_delete_responses(res)

    @classmethod
    def update_by_oh_id(cls, oh_id, data):
        if_id_exits = OHResource.get_by_oh_id(oh_id)

        if if_id_exits.status_code == 200:
            col_validation = OHIntegrity.column_validation(data.keys())
            if col_validation[0] == 200:
                validation = OHIntegrity.type_validation(data)
                if validation[0] == 200:
                    db_name, table_name = OHResource.get_data_resource_info()
                    template = {"id": oh_id}
                    res = RDBService.update_by_template(db_name, table_name, data, template)
                else:
                    res = validation
            else:
                res = col_validation
        else:
            res = None
        return OHIntegrity.oh_put_responses(res)

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
        return 'ohdb', 'OfficeHours'
