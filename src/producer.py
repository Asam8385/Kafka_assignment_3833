"""
Kafka Order Producer with Avro Serialization
Produces random order messages to Kafka topic with Avro schema
"""

import time
import random
import logging
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OrderProducer:
    """Producer for generating and sending order messages"""
    
    def __init__(self, bootstrap_servers='localhost:9092', schema_registry_url='http://localhost:8081'):
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.topic = 'orders'
        
        # Load Avro schema
        with open('schemas/order.avsc', 'r') as f:
            schema_str = f.read()
        
        # Initialize Schema Registry client
        schema_registry_conf = {'url': schema_registry_url}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        # Create Avro Serializer
        avro_serializer = AvroSerializer(
            schema_registry_client,
            schema_str,
            lambda order, ctx: order
        )
        
        # Producer configuration
        producer_conf = {
            'bootstrap.servers': bootstrap_servers,
            'key.serializer': StringSerializer('utf_8'),
            'value.serializer': avro_serializer,
            'acks': 'all',
            'retries': 3,
            'max.in.flight.requests.per.connection': 1
        }
        
        self.producer = SerializingProducer(producer_conf)
        self.order_counter = 1000
        
        # Product catalog with base prices
        self.products = [
            'Laptop', 'Smartphone', 'Headphones', 'Keyboard', 
            'Mouse', 'Monitor', 'Tablet', 'Smartwatch',
            'Camera', 'Printer', 'Speaker', 'Router'
        ]
        
        logger.info(f"Producer initialized. Bootstrap: {bootstrap_servers}, Schema Registry: {schema_registry_url}")
    
    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')
    
    def generate_order(self):
        """Generate a random order"""
        self.order_counter += 1
        order = {
            'orderId': str(self.order_counter),
            'product': random.choice(self.products),
            'price': round(random.uniform(10.0, 2000.0), 2)
        }
        return order
    
    def produce_order(self):
        """Produce a single order message"""
        order = self.generate_order()
        
        try:
            self.producer.produce(
                topic=self.topic,
                key=order['orderId'],
                value=order,
                on_delivery=self.delivery_report
            )
            self.producer.poll(0)
            return order
        except Exception as e:
            logger.error(f"Error producing message: {e}")
            return None
    
    def produce_batch(self, count=10, interval=1):
        """Produce multiple orders with interval"""
        produced_orders = []
        for i in range(count):
            order = self.produce_order()
            if order:
                produced_orders.append(order)
                logger.info(f"Produced order {i+1}/{count}: {order}")
            time.sleep(interval)
        
        # Wait for all messages to be delivered
        self.producer.flush()
        return produced_orders
    
    def close(self):
        """Close the producer"""
        self.producer.flush()
        logger.info("Producer closed")


def main():
    """Main function for standalone producer testing"""
    producer = OrderProducer()
    
    try:
        print("Starting order production...")
        print("Press Ctrl+C to stop\n")
        
        while True:
            order = producer.produce_order()
            if order:
                print(f"✓ Produced: Order #{order['orderId']} - {order['product']} - ${order['price']:.2f}")
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()


if __name__ == '__main__':
    main()
