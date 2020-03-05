import argparse
import subprocess


parser = argparse.ArgumentParser()
parser.add_argument('--amount', default=10, type=int, help='nodes amount')
arguments = parser.parse_args()

nodes = []
for i in range(arguments.amount):
    proc = subprocess.Popen('python3 node.py --id=' + str(i) + ' --amount=' + str(arguments.amount), shell=True)
    nodes.append(proc)

for node in nodes:
    node.wait()
