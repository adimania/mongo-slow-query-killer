#!/usr/bin/env python

from pymongo import MongoClient
from optparse import OptionParser
from types import NoneType
import logging

logging.basicConfig(filename='mongo-query-killer.log',level=logging.DEBUG, format='%(asctime)s %(message)s')

parser = OptionParser()
parser.add_option("-m", "--mongohost", dest="host", help="Mongodb address to be connected", metavar="MONGO_HOST")
parser.add_option("-p", "--mongoport", dest="port", help="Mongodb port to be connected", metavar="MONGO_PORT")
parser.add_option("-t", "--timeout", dest="timeout", help="Time threshold in seconds for killing the query", metavar="TIMEOUT", type="int")
(options, args) = parser.parse_args()

if type(options.host) is NoneType:
	host = "127.0.0.1"
else:
	host = options.host
if type(options.port) is NoneType:
	port = "27017"
else:
	port = options.port	
if type(options.timeout) is NoneType:
	timeout = 300
else:
	timeout = options.timeout

logging.info('Connecting to %s:%s. Timeout set to %d seconds' % (host,port,timout))
client = MongoClient('mongodb://%s:%s/' % (host, port))
db = client.test_database
ops = db.current_op()
for op in ops['inprog']:
	if op['op'] == 'query':
		if op['secs_running'] > timeout:
                        logging.info('Trying to kill '+str(op))     
			db['$cmd.sys.killop'].find_one({'op':op['opid']})
