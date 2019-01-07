#!/usr/bin/env python


from optparse import OptionParser
import json
import sys
import asyncio
import websockets
import logging
from kafka import KafkaProducer

# TODO move wss_socket into it's own module
class wss_socket(object):

    stdout_params = ['wslink', 'wslink_param', 'out']
    kafka_params = stdout_params + ['topic', 'kafka_broker']

    ERR_MSG_CONFIG = 'Error: Invalid Config File'
    def __init__(self, **kwargs):
        self.file = kwargs.get('filename')
        self.config = self.config_check()
        if self.config:
            loop = asyncio.get_event_loop()
            asyncio.ensure_future(self.connect())
            loop.run_forever()
            loop.close()

    # @TODO make into static method
    def config_check(self):
        with open(self.file) as f:
            config = json.load(f)

        try:
            if config['out']:
                # Standard Out
                if config['out'] == 'stdout':
                    if all([i in config.keys() for i in wss_socket.stdout_params]):
                        return config
                    else:
                        raise ValueError(ERR_MSG_CONFIG)

                # Kafka Out
                if config['out'] == 'kafka':
                    if all([i in config.keys() for i in wss_socket.kafka_params]):
                        return config
                    else:
                        raise ValueError(ERR_MSG_CONFIG)
            else:
                raise ValueError(ERR_MSG_CONFIG)
        except ValueError as exp:
            sys.exit(exp)


    async def connect(self):
        # Connects to websocket and routes stream to Kafka topic
        # @TODO move logging options into to decorator function

        logging.info(self.config["wslink"])
        logging.info(json.dumps(self.config["wslink_param"]))
        logging.info(self.config["out"])

        async with websockets.connect(self.config["wslink"]) as websocket:
            if self.config["wslink_param"] != "None":
                await websocket.send(json.dumps(self.config["wslink_param"]))

            if self.config["out"] == "stdout":
                while True:
                    response = await websocket.recv()
                    print(response)

            elif self.config["out"] == "kafka":
                producer = KafkaProducer(bootstrap_servers=self.config['kafka_broker'])
                while True:
                    response = await websocket.recv()
                    producer.send(self.config['topic'], response.encode())
                    producer.flush()


def main():
    parser = OptionParser(usage="wssout -f <config file>", version="0.1")
    parser.add_option("-f", "--config", action="store_true", default=False)

    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.error("wrong number of arguments")

    ws = wss_socket(filename=args[0])


if __name__ == "__main__":

    main()
