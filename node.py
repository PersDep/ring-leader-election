import argparse
import pika
import sys
import time


class process:
    def __init__(self, args):
        self.id = args.id
        self.leader_id = 0
        self.amount = args.amount
        self.next = (self.id + 1) % self.amount
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='queue'+str(self.id), auto_delete=True)
        self.channel.confirm_delivery()
        print('Node', self.id, 'started')
        if self.id == self.leader_id:
            self.channel.queue_declare(queue='queue' + str(self.next), auto_delete=True)
            self.send('basic message running around')
            self.send('healthcheck ' + str(self.id))
        self.channel.basic_consume(queue='queue'+str(self.id), on_message_callback=self.callback)
        self.channel.start_consuming()

    def send(self, msg):
        try:
            self.channel.basic_publish(exchange='', routing_key='queue' + str(self.next), body=msg, mandatory=True)
        except pika.exceptions.UnroutableError:
            prev = self.next
            self.next = (self.next + 1) % self.amount
            print('Node', self.id, 'detected fail of node', prev, 'rerouting to node', self.next)
            if prev == self.leader_id:
                print('Failed node', prev, 'was leader, starting election')
                self.send('elect ' + str(self.id))
            self.send(msg)

    def callback(self, ch, method, properties, body):
        time.sleep(1)
        print('Node', self.id, 'received', body.decode('utf-8'))
        getattr(self, body.split()[0].decode('utf-8'))(body)

    def basic(self, msg):
        print('Node', self.id, 'leader', self.leader_id)
        self.send(msg)

    def elect(self, msg):
        candidates = msg.decode('utf-8').split(' ')[1:]
        if str(self.id) in candidates:
            min_id = self.amount
            for candidate in candidates:
                candidate = int(candidate)
                if candidate < min_id:
                    min_id = candidate
            self.send('leader ' + str(min_id) + ' by ' + str(self.id))
        else:
            self.send(msg + (' ' + str(self.id)).encode('utf-8'))

    def leader(self, msg):
        self.leader_id = int(msg.decode('utf-8').split(' ')[1])
        if self.id != int(msg.decode('utf-8').split(' ')[3]):
            self.send(msg)

    def fail(self, msg):
        print('Node', self.id, 'failed!')
        sys.exit()

    def healthcheck(self, msg):
        nodelist = msg.decode('utf-8').split(' ')[1:]
        if len(nodelist) == self.amount:
            if str(self.leader_id) not in nodelist:
                print('Detected ill leader', self.leader_id, 'starting election')
                self.send('elect ' + str(self.id))
            self.send('healthcheck ' + str(self.id))
        else:
            self.send(msg + (' ' + str(self.id)).encode('utf-8'))


parser = argparse.ArgumentParser()
parser.add_argument('--id', default=0, type=int, help='node id')
parser.add_argument('--amount', default=1, type=int, help='nodes amount')
arguments = parser.parse_args()

process(arguments)
