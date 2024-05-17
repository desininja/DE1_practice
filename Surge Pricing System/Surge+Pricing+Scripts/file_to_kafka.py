import json
from time import sleep
from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# reading data from file
file_data = open("Course4Dataset-data.csv", "r")
for line in file_data:
	words = line.replace("\n","").split(",")
	data = json.dumps({"id":words[0],"lat":words[1],"long":words[2],"ts":words[3],"type":words[4]}).encode('utf-8')
	producer.send('surge_pricing_demo', value=data)

  
