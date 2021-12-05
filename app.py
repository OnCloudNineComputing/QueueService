from flask import Flask, Response, request
from flask_cors import CORS
import json
import logging
from datetime import datetime
import utils.rest_utils as rest_utils

from middleware.notification import NotificationMiddlewareHandler
from application_services.OHResource.oh_service import OHResource
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


# TODO Remove later. Solely for explanatory purposes.
# The method take any REST request, and produces a response indicating what
# the parameters, headers, etc. are. This is simply for education purposes.
#
@app.route("/api/demo/<parameter1>", methods=["GET", "POST", "PUT", "DELETE"])
@app.route("/api/demo/", methods=["GET", "POST", "PUT", "DELETE"])
def demo(parameter1=None):
    """
    Returns a JSON object containing a description of the received request.

    :param parameter1: The first path parameter.
    :return: JSON document containing information about the request.
    """

    # DFF TODO -- We should wrap with an exception pattern.
    #

    # Mostly for isolation. The rest of the method is isolated from the specifics of Flask.
    inputs = rest_utils.RESTContext(request, {"parameter1": parameter1})

    # DFF TODO -- We should replace with logging.
    r_json = inputs.to_json()
    msg = {
        "/demo received the following inputs": inputs.to_json()
    }
    print("/api/demo/<parameter> received/returned:\n", msg)

    rsp = Response(json.dumps(msg), status=200, content_type="application/json")
    return rsp



@app.route('/')
def hello_world():
    return '<u>Hello World!</u>'


@app.route('/officehours', methods=['GET', 'POST'])
def oh_collection():
    """
    1. HTTP GET return all users.
    2. HTTP POST with body --> create a user, i.e --> database.
    :return:
    """
    inputs = rest_utils.RESTContext(request)
    rest_utils.log_request("oh_collection", inputs)

    if inputs.method == "GET":
        rsp = OHResource.get_by_template(inputs.args, inputs, order_by=inputs.order_by, limit=inputs.limit, offset=inputs.offset,
                                              field_list=inputs.fields)
    elif inputs.method == "POST":
        rsp = OHResource.create(inputs.data)

    return rsp

@app.route('/officehours/<oh_id>', methods=['GET', 'PUT', 'DELETE'])
def specific_oh(oh_id):
    """
    1. Get a specific one by ID.
    2. Update body and update.
    3. Delete would ID and delete it.
    :param user_id:
    :return:
    """
    inputs = rest_utils.RESTContext(request)
    rest_utils.log_request("oh_by_id", inputs)
    if inputs.method == "GET":
        rsp = OHResource.get_by_oh_id(oh_id)

    elif inputs.method == 'PUT':
        rsp = OHResource.update_by_oh_id(oh_id, inputs.data)

    elif inputs.method == 'DELETE':
        rsp = OHResource.delete_by_oh_id(oh_id)

    return rsp

@app.after_request
def send_notifications(rsp):
    res, e = NotificationMiddlewareHandler.notify(request, rsp)
    if not res:
        app.logger.error("Couldn't send notif: " + str(e))
    return rsp

if __name__ == '__main__':
    app.run()
