import paramiko
import time
import sys
import socket
import requests
import json
# app = Flask(__name__)



# @app.get('/message')
# def messegae():
while True:
    url = "https://database-query.v3.microgen.id/api/v1/fb6db565-2e6c-41eb-bf0f-66f43b2b75ae/KafkaConnect?$select[0]=connector&$select[1]=createdBy"
    lists = requests.get(url)
    n = len(lists.json())
    for i in range(0, n):
        def ssh_con (ip, un, pw):
            global client
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            # print ("Connecting to device/VM: %s" % ip)
            client.connect(ip, username=un, password=pw,timeout=10)


        def cmd_io (command):
            global client_cmd
            client_cmd.send("%s \n" %command)
            time.sleep(1)
            output = client_cmd.recv(10000).decode("utf-8")
            print (output)

        ip = '10.10.65.5'
        un = 'sapujagad'
        pw = 'kayangan'

        ssh_con(ip,un,pw)
        client_cmd = client.invoke_shell()
        command = "sudo docker exec -it ubuntu-kafka /opt/confluent/bin/kafka-avro-console-consumer --topic "+lists.json()[i]['connector']+"-XE-redo-log"+" --bootstrap-server 10.10.65.5:9092 --from-beginning" 
        cmd_io (command)
        client_cmd.settimeout(60.0)
        progress = True
        messegae = 0
        urlen = "https://database-query.v3.microgen.id/api/v1/fb6db565-2e6c-41eb-bf0f-66f43b2b75ae/Kafka_Messages?$select[0]=topic&topic="+lists.json()[i]['connector']+""
        response = requests.get(urlen)
        lenmessage = len(response.json())
        while progress:
            try:
                output = client_cmd.recv(100000).decode("utf-8")
                messegae += 1
                print (messegae)
                if lenmessage == messegae or lenmessage == 0:
                    url = "https://database-query.v3.microgen.id/api/v1/fb6db565-2e6c-41eb-bf0f-66f43b2b75ae/Kafka_Messages"
                    jsons = {"topic":lists.json()[i]['connector'],"messages":output,"createdBy":lists.json()[i]['createdBy']['email']}
                    response = requests.post(url,json=jsons)
                    print (response.status_code)
            except socket.timeout:
                progress = False
                messegae = 0
                pass
    client.close()
