"""
It reads the REDIS STREAM events
Using the xread, it gets 1 event per time (from the oldest to the last one)

Usage:
  python consumer.py
"""
from os import environ
from redis import Redis, ResponseError
from redisearch import TextField, IndexDefinition, Client, aggregation

stream_key = environ.get("STREAM", "test-redis")


def connect_to_redis():
    hostname = environ.get("REDIS_HOSTNAME", "localhost")
    port = environ.get("REDIS_PORT", 6379)
    redis = Redis(host=hostname, port=port)

    client = Client("test-redis", host=hostname, port=port)
    SCHEMA = (
        TextField("producer"),
        TextField("some_id"),
        TextField("call")
    )
    definition = IndexDefinition(prefix=['doc:'])
    try:
        client.info()
    except ResponseError:
        client.create_index(SCHEMA, definition=definition)
    return client, redis


def get_data(client_connection, redis_connection):
    last_id = 0
    sleep_ms = 5000
    while True:
        try:
            resp = redis_connection.xread(
                {stream_key: last_id}, count=1, block=sleep_ms
            )
            if resp:
                key, messages = resp[0]
                last_id, data = messages[0]
                unidict = {k.decode('utf8'): v.decode('utf8') for k, v in data.items()}
                client_connection.add_document(f'doc:{last_id.decode("utf8")}',
                                    producer=unidict["producer"], some_id=unidict["some_id"], call=unidict["call"])
                print('\nsearching for new objects', client.search("test"))
                print("\nREDIS ID: ", last_id.decode("utf8"))
                print("      --> ", unidict)
            print('\nsearching for all objects', client.search("user"))
        except ConnectionError as e:
            print("ERROR REDIS CONNECTION: {}".format(e))
        except ResponseError as error:
            print('\nsearching for existing objects', client.search("test"))



if __name__ == "__main__":
    client, redis = connect_to_redis()
    get_data(client, redis)
