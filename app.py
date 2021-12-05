from flask import Flask, Response, request
from flask_cors import CORS
import json
import logging
from datetime import datetime
import utils.rest_utils as rest_utils

from middleware.notification import NotificationMiddlewareHandler
from application_services.QueueResource.queue_service import QueueResource
from application_services.QueueResource.students_on_queue_service import StudentsOnQueueResource

from database_services.RDBService import RDBService as RDBService

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

app = Flask(__name__)
CORS(app)

##################################################################################################################

# DFF TODO A real service would have more robust health check methods.
# This path simply echoes to check that the app is working.
# The path is /health and the only method is GETs
@app.route("/health", methods=["GET"])
def health_check():
    rsp_data = {"status": "healthy", "time": str(datetime.now())}
    rsp_str = json.dumps(rsp_data)
    rsp = Response(rsp_str, status=200, content_type="app/json")
    return rsp


@app.route('/queue', methods=['GET', 'POST'])
def oh_collection():
    """
    1. HTTP GET return all users.
    2. HTTP POST with body --> create a user, i.e --> database.
    :return:
    """
    inputs = rest_utils.RESTContext(request)
    rest_utils.log_request("queue_collection", inputs)

    if inputs.method == "GET":
        rsp = QueueResource.get_all()
    elif inputs.method == "POST":
        rsp = QueueResource.create(inputs.data)

    return rsp


@app.route('/queue/<queue_id>', methods=['GET', 'PUT'])
def specific_oh(queue_id):
    """
    1. Get a specific one by ID.
    2. Update body and update.
    3. Delete would ID and delete it.
    :param user_id:
    :return:
    """
    inputs = rest_utils.RESTContext(request)
    rest_utils.log_request("queue_by_id", inputs)
    if inputs.method == "GET":
        rsp = QueueResource.get_by_queue_id(queue_id)

    elif inputs.method == 'PUT':
        rsp = QueueResource.update_by_queue_id(queue_id, inputs.data)

    return rsp


@app.route('/queue/<queue_id>/students', methods=['GET','POST'])
def specific_oh(queue_id):
    """
    1. Get a specific one by ID.
    2. Update body and update.
    3. Delete would ID and delete it.
    :param user_id:
    :return:
    """
    inputs = rest_utils.RESTContext(request)
    rest_utils.log_request("students", inputs)
    if inputs.method == "GET":
        rsp = StudentsOnQueueResource.get_all_students(queue_id)

    elif inputs.method == 'POST':
        rsp = StudentsOnQueueResource.create(queue_id, inputs.data)

    return rsp


@app.route('/queue/<queue_id>/students/<timestamp>', methods=['GET', 'PUT'])
def specific_oh(queue_id, timestamp):
    """
    1. Get a specific one by ID.
    2. Update body and update.
    3. Delete would ID and delete it.
    :param user_id:
    :return:
    """
    inputs = rest_utils.RESTContext(request)
    rest_utils.log_request("students_get_by_timestamp", inputs)
    if inputs.method == "GET":
        rsp = StudentsOnQueueResource.get_by_timestamp(queue_id, timestamp)

    elif inputs.method == 'PUT':
        rsp = StudentsOnQueueResource.get_by_timestamp(queue_id, timestamp, inputs.data)

    return rsp


if __name__ == '__main__':
    app.run()
