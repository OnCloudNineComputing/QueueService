from flask import Flask, Response, request
from flask_cors import CORS
import json
import logging
import re
from datetime import datetime

import utils.rest_utils as rest_utils

from integrity_services.BaseIntegrityResource import BaseIntegrityResource, ValidationFunction


def time_validation(format):
    def time_valid(time_str):
        try:
            datetime.strptime(time_str, format).time()
        except ValueError:
             return False
        return True
    return time_valid


class QueueIntegrity(BaseIntegrityResource):

    def __init__(self):
        super(QueueIntegrity, self).__init__()

    field_to_type = {
        'office_hours_info': dict,
        'queue_id': int,
        'ta_email': str,
        'ta_firstname': str,
        'ta_lastname': str,
        'location': str,
        'course_name': str,
        'course_number': str,
        'zoom_link': str,
        'start_time': str,
        'end_time': str
    }

    required_fields = []

    field_to_validation_fn = {
    }


    @classmethod
    def get_responses(cls, res):
        if res:
            return 200
        else:
            return 404

    @classmethod
    def field_validation(cls, fields):
        for field in fields:
            if field not in QueueIntegrity.field_to_type:
                return False
        return True

    @classmethod
    def column_validation(cls, fields):
        if not QueueIntegrity.field_validation(fields):
            return 400, {"fields": "Column Does Not Exist"}
        else:
            return 200, {}

    @classmethod
    def type_validation(cls, data):
        input_fields = list(data.keys())
        errors = {}

        for k in data.keys():
            if k not in QueueIntegrity.field_to_type:
                errors["fields"] = "Invalid Data Fields Provided"

        if errors:
            return 400, errors

        for field in data.keys():
            required_type = QueueIntegrity.field_to_type[field]
            if type(data[field]) != required_type:
                errors[field] = "Invalid {0} provided, must be of type {1}".format(field, str(type(required_type)))

            elif field in QueueIntegrity.field_to_validation_fn and not QueueIntegrity.field_to_validation_fn[field].validate(data[field]):
                errors[field] = QueueIntegrity.field_to_validation_fn[field].error_msg

        if errors:
            return 400, errors
        else:
            return 200, "Data Types Validated"

    @classmethod
    def input_validation(cls, data):
        input_fields = list(data.keys())
        errors = {}

        try:
            for r in QueueIntegrity.required_fields:
                if r not in input_fields:
                    raise ValueError("Missing required data fields; {0} required".format(", ".join(QueueIntegrity.required_fields)))
        except ValueError as v:
            errors["required_fields"] = str(v)

        type_errors = QueueIntegrity.type_validation(data)

        if type_errors[0] == 400:
            errors.update(type_errors[1])

        if errors:
            return 400, errors

        return 200, "Input Validated"

    @classmethod
    def post_responses(cls, res, db_result=None):
        rsp = ""
        if res == 422:
            rsp = Response("OfficeHours already exists!", status=422,
                           content_type="text/plain")
        elif type(res) == tuple:
            if res[0] == 400:
                rsp = Response(json.dumps(res[1], default=str), status=res[0], content_type="application/json")
            else:
                rsp = Response(json.dumps(db_result), status=200,
                         content_type="text/plain")
        elif res is not None:
            rsp = Response(json.dumps(db_result), status=201,
                           content_type="text/plain")
        else:
            rsp = Response("Failed! Unprocessable entity.",
                           status=422, content_type="text/plain")

        return rsp

    @classmethod
    def put_responses(cls, res):
        if res == 422:
            return 422
        elif type(res) == tuple and len(res) == 2:
            if res[0] == 400:
                return res[0]
        elif res is not None:
            return 200
        else:
            return 404

    @classmethod
    def delete_responses(cls, res):
        if res is not None:
            return 204
        else:
            return 404

    @classmethod
    def oh_get_responses(cls, res):
        status = QueueIntegrity.get_responses(res)
        if status == 200:
            rsp = Response(json.dumps(res, default=str), status=status, content_type="application/json")
        else:
            rsp = Response("No data found!", status=status, content_type="text/plain")

        return rsp

    @classmethod
    def oh_put_responses(cls, res):
        status = QueueIntegrity.put_responses(res)
        rsp = ""
        if status == 422:
            rsp = Response("Update violates data integrity!", status=status,
                           content_type="text/plain")
        elif status == 400:
            rsp = Response(json.dumps(res[1], default=str), status=status, content_type="application/json")
        elif status == 200:
            rsp = Response("Success! The given data for the Queue " +
                           "that matched was updated as requested.", status=status,
                           content_type="text/plain")
        elif status==404:
            rsp = Response("No data found!", status=status, content_type="text/plain")
        else:
            rsp = Response("Failed! Matching Queue not found or unexpected error.",
                           status=422, content_type="text/plain")

        return rsp

    @classmethod
    def oh_delete_responses(cls, res):
        status = QueueIntegrity.delete_responses(res)
        if status == 204:
            rsp = Response("Success!",
                           status=status, content_type="text/plain")
        elif status==404:
            rsp = Response("No data found!", status=status, content_type="text/plain")
        else:
            rsp = Response("Failed! Could not delete all courses.",
                           status=status, content_type="text/plain")

        return rsp

