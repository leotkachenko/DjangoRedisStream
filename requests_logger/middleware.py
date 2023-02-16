from os import environ
from redis import Redis
from uuid import uuid4

producer = 'test-user'
stream_key = 'test-redis'


class RequestLoggerMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
        hostname = environ.get("REDIS_HOSTNAME", "127.0.0.1")
        port = environ.get("REDIS_PORT", 6379)
        self.redis_connection = Redis(hostname, port, retry_on_timeout=True)

    def __call__(self, request):
        data = {
            "producer": producer,
            "some_id": uuid4().hex,  # Just some random data
            "call": 'call',
        }
        self.redis_connection.xadd(stream_key, data, id='*')
        return self.get_response(request)
