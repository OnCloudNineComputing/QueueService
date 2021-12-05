from abc import ABC, abstractmethod
from flask import Flask, Response

class BaseIntegrityException(Exception):

    def __init__(self):
        pass


class BaseIntegrityResource(ABC):

    def __init__(self):
        pass

    @classmethod
    @abstractmethod
    def get_responses(cls, res):
        pass

    @classmethod
    @abstractmethod
    def input_validation(cls, data):
        pass

    @classmethod
    @abstractmethod
    def post_responses(cls, res):
        pass

    @classmethod
    @abstractmethod
    def put_responses(cls, res):
        pass

    @classmethod
    @abstractmethod
    def delete_responses(cls, res):
        pass

    @classmethod
    def generate_response(cls, status, msg):
        return Response(msg, status=status, content_type="text/plain")


class ValidationFunction():
    def __init__(self, fn, error_msg):
        self.fn = fn
        self.error_msg = error_msg

    def validate(self, input):
        return self.fn(input)


