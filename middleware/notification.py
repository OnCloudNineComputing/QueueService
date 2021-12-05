
import requests
import json
import boto3
import middleware.context as context


class NotificationMiddlewareHandler:
    sns_client = None

    def __init__(self):
        pass

    @classmethod
    def get_sns_client(cls):

        if NotificationMiddlewareHandler.sns_client is None:
            NotificationMiddlewareHandler.sns_client = sns = boto3.client("sns",
                                                                          region_name="us-east-1")
        return NotificationMiddlewareHandler.sns_client

    @classmethod
    def get_sns_topics(cls):
        s_client = NotificationMiddlewareHandler.get_sns_client()
        result = response = s_client.list_topics()
        topics = result["Topics"]
        return topics

    @classmethod
    def send_sns_message(cls, sns_topic, message):
        import json
        import boto3

        s_client = NotificationMiddlewareHandler.get_sns_client()
        response = s_client.publish(
            TargetArn=sns_topic,
            Message=json.dumps({'default': json.dumps(message)}),
            MessageStructure='json'
        )
        print("Publish response = ", json.dumps(response, indent=2))

    @staticmethod
    def notify(request, response):

        subscriptions = context.get_notifications()
        for sub in subscriptions:
            if request.method == sub.method and request.path == sub.path:
                try:
                    data = request.get_json()
                    data['start_date'] = str(data['start_date'])
                    data['start_time'] = str(data['start_time'])
                    data['end_date'] = str(data['end_date'])
                    data['end_time'] = str(data['end_time'])
                    NotificationMiddlewareHandler.send_sns_message(sub.topic, request.get_json())
                except Exception as e:
                    print("No notification sent")
                    return False, e
        return True, ""

