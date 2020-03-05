import argparse
import pika

parser = argparse.ArgumentParser()
parser.add_argument('--id', default=0, type=int, help='node id to send message to')
arguments = parser.parse_args()

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.basic_publish(exchange='', routing_key='queue' + str(arguments.id), body='fail now')

connection.close()
