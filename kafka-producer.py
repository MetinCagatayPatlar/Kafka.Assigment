import time 
import json 
from datetime import datetime
from kafka import KafkaProducer


# Messages will be serialized as JSON 
def serializer(message):
    return json.dumps(message).encode('utf-8')


# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer
)


if __name__ == '__main__':
    # Infinite loop - runs until you kill the program
    idx=0
    while True:
        # Generate a message
        dummy_message = f"message_{idx}"
        
        # Send it to our 'messages' topic
        print(f'Producing message @ {datetime.now()} | Message = {dummy_message}')
        producer.send('messages', dummy_message)
        
        idx += 1
        time.sleep(1)