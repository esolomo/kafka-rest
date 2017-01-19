#!/bin/python2.7

import random
import sys
from datetime import datetime
from kafka import KafkaConsumer
import random
import threading
import time
import urllib
import requests
import os
import commands
import time
import dateutil.parser
import json
import logging
import random
import binascii

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s (%(threadName)-2s) %(message)s',
                    )

class BdLService(object):
    def __init__(self):
        self.pserie = []
        self.headers = "Content-Type: application/vnd.kafka.binary.v1+json"
        self.data = "{\"records\":[{\"value\":\"S2Fma2E=\"}]}"
        self.fqdn = "localhost"
        self.port = 8082
        self.topic = "BDLIN"
        self.url = "http://"+self.fqdn+":"+str(self.port)+"/topics/"+self.topic
        self.cmd = "curl -s -X POST -H '" + self.headers + "'  --data '"+ self.data + "' " + self.url        
        self.cserie = []
        self.pserie = []
        self.iteration = int(sys.argv[1]) if sys.argv.__len__() > 1 else 1000
        self.frequency = float(sys.argv[2]) if sys.argv.__len__() > 2 else 0.0001

    def httpConsumer(self):
        groupid = binascii.b2a_hex(os.urandom(5))
        self.consumer = KafkaConsumer('BDLIN',group_id='enxg-'+str(groupid), bootstrap_servers=['1lxesolomo-pad.oad.exch.int'])
        for message in self.consumer:                    
            entry = dict(rx_timestamp=datetime.now().isoformat(), topic=message.topic, partition=message.partition, offset=message.offset, key=message.key,msg=message.value)
            self.cserie.append(entry)

    def httpProducer(self):
        print "Running "+ str(self.iteration)  +" requests"
        for x in range(0, self.iteration):            
            print "Iteration : ",x
            date = datetime.now().isoformat()
            status, output = commands.getstatusoutput(self.cmd)
            rdata = json.loads(output)
            partition = rdata['offsets'][0]['partition']
            offset = rdata['offsets'][0]['offset']
            error_code = rdata['offsets'][0]['error_code']
            entry = dict(tx_timestamp=date, status=status, partition=partition, offset=offset, error_code=error_code)
            self.pserie.append(entry)
            time.sleep(self.frequency)

    def Consumer(self):
        groupid = binascii.b2a_hex(os.urandom(5))
        self.consumer = KafkaConsumer('BDLIN',group_id='enxg-'+str(groupid), bootstrap_servers=['1lxesolomo-pad.oad.exch.int'])
        for message in self.consumer:                    
            entry = dict(rx_timestamp=datetime.now().isoformat(), topic=message.topic, partition=message.partition, offset=message.offset, key=message.key,msg=message.value)
            self.cserie.append(entry)

    def Producer(self):
        print "Running "+ str(self.iteration)  +" requests"
        for x in range(0, self.iteration):            
            print "Iteration : ",x
            date = datetime.now().isoformat()
            status, output = commands.getstatusoutput(self.cmd)
            rdata = json.loads(output)
            partition = rdata['offsets'][0]['partition']
            offset = rdata['offsets'][0]['offset']
            error_code = rdata['offsets'][0]['error_code']
            entry = dict(tx_timestamp=date, status=status, partition=partition, offset=offset, error_code=error_code)
            self.pserie.append(entry)
            time.sleep(self.frequency)

    def Process(self):
        print "Processing data"
        results = []
        for x in range(0, self.iteration):
            tx = self.pserie[x]['tx_timestamp']
            rx = self.cserie[x]['rx_timestamp']
            diff=dateutil.parser.parse(rx) - dateutil.parser.parse(tx) 
            latency=diff.total_seconds()        
            entry=dict(txtimestamp=tx,rxtimestamp=rx,latency=latency,tx_offset=self.pserie[x]['offset'],rx_offset=self.cserie[x]['offset'],topic=self.cserie[x]['topic'],msg=self.cserie[x]['msg'],partition=self.cserie[x]['partition'])
            results.append(entry)
                #entry['@timestamp'] = tx                        
        idx = { 'index': { '_index': 'mdgstats', '_type': 'stats', '_id': 0, }}
        jsonoutput = open("./output.txt" , 'w+')
        print "Copying Data"
        for r in  results:
            entry = {}
            newidx = idx
            newidx['index']['_id'] = binascii.b2a_hex(os.urandom(8))
            #json.dump(newidx, jsonoutput)
            #jsonoutput.write("\n")
            json.dump(r, jsonoutput)
            jsonoutput.write("\n")

if __name__ == '__main__':
    condition = threading.Condition()
    BdL = BdLService() 
    consumer = threading.Thread(name='consumer', target=BdL.Consumer, args=())
    consumer.daemon = True
    consumer.start()
    time.sleep(3)
    BdL.httpProducer()
    while BdL.cserie.__len__() < BdL.pserie.__len__():
        print "**************************Consumer array DATA length: ",BdL.cserie.__len__(),"***************************************"
        print "**************************Producer array DATA length: ",BdL.pserie.__len__(),"***************************************"
        time.sleep(3)

    time.sleep(3)
    print "**************************Consumer array DATA length: ",BdL.cserie.__len__(),"***************************************"
    print "**************************Producer array DATA length: ",BdL.pserie.__len__(),"***************************************"    
    #results =  Process(BdL.iteration, BdL.pserie, BdL.cserie)
    BdL.Process()
    


