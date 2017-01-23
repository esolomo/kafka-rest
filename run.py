#!/usr/bin/python2.7

import random
import sys
from datetime import datetime
from kafka import KafkaConsumer
from kafka import KafkaProducer
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
import base64


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s (%(threadName)-2s) %(message)s',
                    )


class BdLService(object):
    def __init__(self, topic="BLDIN", filePath="95K.msg"):
        self.pserie = []
        self.headers = "Content-Type: application/vnd.kafka.binary.v1+json"
        #self.data = "{\"records\":[{\"value\":\"S2Fma2E=\"}]}"
        self.buffer = "Read buffer:\n"
        self.buffer += open(filePath, 'rU').read()        
        encoded_msg = base64.b64encode(self.buffer)
        msgsize =  sys.getsizeof(encoded_msg)
        self.encoded_msg = encoded_msg
        self.msgsize = msgsize
        self.data = "{\"records\":[{\"value\":\""+ encoded_msg + "\"}]}"
        self.fqdn = "localhost"
        self.port = 8082
        self.topic = topic
        self.url = "http://"+self.fqdn+":"+str(self.port)+"/topics/"+self.topic
        self.cmd = "curl -s -X POST -H '" + self.headers + "'  --data '"+ self.data + "' " + self.url        
        self.cserie = []
        self.pserie = []
        self.iteration = int(sys.argv[1]) if sys.argv.__len__() > 1 else 1000
        self.frequency = float(sys.argv[2]) if sys.argv.__len__() > 2 else 0.0001




    def httpConsumerRemove(self):
        instanceid = self.consumerid['instance_id']
        requestConsumerDeleteCmd = "curl -X DELETE http://localhost:8082/consumers/my_avro_consumer/instances/" + instanceid
        status, output = commands.getstatusoutput(requestConsumerDeleteCmd)
        print "Delete successfull for instalid: " + instanceid

    def httpConsumer(self):
        groupid = binascii.b2a_hex(os.urandom(5))
        print "groupid: ",groupid
        requestConsumerCmd = "curl -s -X POST -H 'Content-Type: application/vnd.kafka.v1+json' --data '{\"format\": \"binary\", \"auto.offset.reset\": \"smallest\"}' 'http://localhost:8082/consumers/" + groupid + "\'"
        status, output = commands.getstatusoutput(requestConsumerCmd)
        self.consumerid = json.loads(output)
        requestDataCmd = "curl -s -X GET -H 'Accept: application/vnd.kafka.binary.v1+json' " + self.consumerid['base_uri'] + "/topics/" + self.topic
        status, output = commands.getstatusoutput(requestDataCmd)
        #requestOffsetCmd = "curl -s -X GET -H 'Accept: application/vnd.kafka.binary.v1+json' " + self.consumerid['base_uri'] + "/offsets"
        #status, output = commands.getstatusoutput(requestOffsetCmd)
        #self.httpConsumerResults = []
        self.httpcserie = []
        while True:            
            #statusoffset, outputoffset = commands.getstatusoutput(requestOffsetCmd)            
            status, output = commands.getstatusoutput(requestDataCmd)            
            messages = json.loads(output)
            if messages.__len__() > 0:
                date = datetime.now().isoformat()
                for message in messages:
                    entry = dict(rx_timestamp=date, msg=message['value'], offset=message['offset'], partition=message['partition'], key=message['key'], topic=self.topic, rx_grouptimestamp=date)
                    self.httpcserie.append(entry)

    def httpProducer(self):
        print "Running "+ str(self.iteration)  +" requests"
        for x in range(0, self.iteration):            
            print "Iteration : ",x
            date = datetime.now().isoformat()
            status, output = commands.getstatusoutput(self.cmd)
            if status != 0:
                print "Too Many Errors, failed to send command"
                exit (0)
            rdata = json.loads(output)
            partition = rdata['offsets'][0]['partition']
            offset = rdata['offsets'][0]['offset']
            error_code = rdata['offsets'][0]['error_code']
            entry = dict(tx_timestamp=date, status=status, partition=partition, offset=offset, error_code=error_code)
            self.pserie.append(entry)
            time.sleep(self.frequency)

    def Consumer(self):
        groupid = binascii.b2a_hex(os.urandom(5))
        self.consumer = KafkaConsumer(self.topic,group_id='enxg-'+str(groupid), bootstrap_servers=['172.17.42.1'])
        for message in self.consumer:                    
            entry = dict(rx_timestamp=datetime.now().isoformat(), topic=message.topic, partition=message.partition, offset=message.offset, key=message.key,msg=message.value)
            self.cserie.append(entry)

    def Producer(self):
        print "Running Kafka Producer "+ str(self.iteration)  +" requests"
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092',compression_type='gzip')
        for x in range(0, self.iteration):            
            print "Iteration : ",x
            date = datetime.now().isoformat()
            output = self.producer.send(self.topic, self.encoded_msg).get(timeout=60)
            partition = output.partition 
            offset = output.offset
            error_code = 'None'
            entry = dict(tx_timestamp=date, status='OK', partition=partition, offset=offset, error_code=error_code)
            self.pserie.append(entry)
            time.sleep(self.frequency)

    def Process(self, pserie, cserie, output="./output.txt"):
        print "Processing data"
        results = []
        for x in range(0, self.iteration):
            tx = pserie[x]['tx_timestamp']
            rx = cserie[x]['rx_timestamp']
            diff=dateutil.parser.parse(rx) - dateutil.parser.parse(tx) 
            latency=diff.total_seconds()
            #entry=dict(txtimestamp=tx,rxtimestamp=rx,latency=latency,tx_offset=pserie[x]['offset'],rx_offset=cserie[x]['offset'],topic=cserie[x]['topic'],msg=cserie[x]['msg'],partition=cserie[x]['partition'],msgsize=self.msgsize)
            entry=dict(txtimestamp=tx,rxtimestamp=rx,latency=latency,tx_offset=pserie[x]['offset'],rx_offset=cserie[x]['offset'],topic=cserie[x]['topic'],partition=cserie[x]['partition'],msgsize=self.msgsize)
            #entry=dict(txtimestamp=tx,rxtimestamp=rx,latency=latency,rx_offset=cserie[x]['offset'],topic=cserie[x]['topic'],partition=cserie[x]['partition'],msgsize=self.msgsize)
            results.append(entry)
                #entry['@timestamp'] = tx                        
        idx = { 'index': { '_index': 'mdgstats', '_type': 'stats', '_id': 0, }}
        jsonoutput = open(output , 'w+')
        print "Copying Data"
        for r in  results:
            entry = {}
            newidx = idx
            newidx['index']['_id'] = binascii.b2a_hex(os.urandom(8))
            #json.dump(newidx, jsonoutput)
            #jsonoutput.write("\n")
            json.dump(r, jsonoutput)
            jsonoutput.write("\n")


class Orchestrate(object):
    def __init__(self):
        self.id = 1

    def httptoKafka(self):
        condition = threading.Condition()
        BdL = BdLService(topic='httptoKafka') 
        consumer = threading.Thread(name='consumer', target=BdL.Consumer, args=())
        consumer.daemon = True
        consumer.start()
        time.sleep(5)
        BdL.httpProducer()
        while BdL.cserie.__len__() < BdL.pserie.__len__():
            print "**************************Consumer array DATA length: ",BdL.cserie.__len__(),"***************************************"
            print "**************************Producer array DATA length: ",BdL.pserie.__len__(),"***************************************"
            time.sleep(3)

            time.sleep(3)
            print "**************************Consumer array DATA length: ",BdL.cserie.__len__(),"***************************************"
            print "**************************Producer array DATA length: ",BdL.pserie.__len__(),"***************************************"    
            #results =  Process(BdL.iteration, BdL.pserie, BdL.cserie)
        BdL.Process(BdL.pserie, BdL.cserie, "./httptoKafka_output.txt")


    def kafkatoHttp(self):
        condition = threading.Condition()
        BdL = BdLService(topic="kafkatoHttp") #BdLService(topic="kafkatoHttp", filePath="700K.msg") 
        consumer = threading.Thread(name='consumer', target=BdL.httpConsumer, args=())
        consumer.daemon = True
        consumer.start()
        print "Waiting before running producer"
        time.sleep(5)
        #BdL.httpProducer()
        BdL.Producer()

        while BdL.httpcserie.__len__() < BdL.pserie.__len__():
            print "**************************Consumer array DATA length: ",BdL.httpcserie.__len__(),"***************************************"
            print "**************************Producer array DATA length: ",BdL.pserie.__len__(),"***************************************"
            time.sleep(3)

        print "Removing consumer"
        BdL.httpConsumerRemove()
        BdL.Process(BdL.pserie, BdL.httpcserie, "./kafkatoHttp_output.txt")





if __name__ == '__main__':
    print "Running httptoKafka test suite"
    Orchestrate().httptoKafka()
    print "httptoKafka test suite: completed"
    time.sleep(10)
    print "Running kafkatoHttp test suite"    
    Orchestrate().kafkatoHttp()
    print "kafkatoHttp test suite: completed"
    


