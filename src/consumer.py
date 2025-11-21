"""
Kafka Order Consumer with Real-time Aggregation, Retry Logic, and DLQ
Consumes order messages and calculates running average of prices
Implements retry logic and Dead Letter Queue for failed messages
"""

import time
import logging
from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from collections import deque
import json
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OrderConsumer:
    """Consumer for processing order messages with aggregation and error handling"""
    
    def __init__(self, bootstrap_servers='localhost:9092', schema_registry_url='http://localhost:8081', 
                 group_id='order-consumer-group'):
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.topic = 'orders'
        self.dlq_topic = 'orders-dlq'
        self.retry_topic = 'orders-retry'
        
        # Load Avro schema
        with open('schemas/order.avsc', 'r') as f:
            schema_str = f.read()
        
        # Initialize Schema Registry client
        schema_registry_conf = {'url': schema_registry_url}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        # Create Avro Deserializer
        avro_deserializer = AvroDeserializer(
            schema_registry_client,
            schema_str,
            lambda order, ctx: order
        )
        
        # Consumer configuration
        consumer_conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'key.deserializer': StringDeserializer('utf_8'),
            'value.deserializer': avro_deserializer,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000
        }
        
        self.consumer = DeserializingConsumer(consumer_conf)
        self.consumer.subscribe([self.topic, self.retry_topic])
        
        # Initialize DLQ producer
        avro_serializer = AvroSerializer(
            schema_registry_client,
            schema_str,
            lambda order, ctx: order
        )
        
        producer_conf = {
            'bootstrap.servers': bootstrap_servers,
            'key.serializer': StringSerializer('utf_8'),
            'value.serializer': avro_serializer
        }
        
        self.dlq_producer = SerializingProducer(producer_conf)
        
        # Aggregation state
        self.price_history = deque(maxlen=100)  # Keep last 100 prices
        self.running_average = 0.0
        self.total_orders = 0
        self.failed_orders = 0
        self.retry_count = {}
        self.max_retries = 3
        
        # Statistics
        self.stats = {
            'total_consumed': 0,
            'successful': 0,
            'retried': 0,
            'sent_to_dlq': 0,
            'running_avg': 0.0,
            'min_price': float('inf'),
            'max_price': 0.0,
            'last_order': None
        }
        
        logger.info(f"Consumer initialized. Group: {group_id}, Topics: {self.topic}, {self.retry_topic}")
    
    def update_aggregation(self, price):
        """Update running average with new price"""
        self.price_history.append(price)
        self.running_average = sum(self.price_history) / len(self.price_history)
        
        # Update statistics
        self.stats['running_avg'] = self.running_average
        self.stats['min_price'] = min(self.stats['min_price'], price)
        self.stats['max_price'] = max(self.stats['max_price'], price)
    
    def process_order(self, order):
        """
        Process order with simulated failure for demonstration
        Returns True if successful, False if should retry, raises exception for DLQ
        """
        try:
            # Simulate random processing failures (10% chance for demo purposes)
            import random
            if random.random() < 0.1:  # 10% failure rate
                raise Exception("Simulated temporary processing failure")
            
            # Process order successfully
            price = order['price']
            self.update_aggregation(price)
            self.total_orders += 1
            
            logger.info(f"Processed order: {order['orderId']} - {order['product']} - ${price:.2f}")
            logger.info(f"Running average: ${self.running_average:.2f}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing order {order['orderId']}: {e}")
            return False
    
    def send_to_dlq(self, order, error_msg):
        """Send failed message to Dead Letter Queue"""
        try:
            # Add error metadata
            dlq_order = order.copy()
            dlq_order['error'] = error_msg if isinstance(error_msg, str) else str(error_msg)
            dlq_order['timestamp'] = datetime.now().isoformat()
            
            self.dlq_producer.produce(
                topic=self.dlq_topic,
                key=order['orderId'],
                value=order  # Send original order without metadata
            )
            self.dlq_producer.flush()
            
            self.failed_orders += 1
            self.stats['sent_to_dlq'] += 1
            logger.warning(f"Order {order['orderId']} sent to DLQ after {self.max_retries} retries")
            
        except Exception as e:
            logger.error(f"Failed to send order to DLQ: {e}")
    
    def handle_retry(self, order):
        """Handle retry logic for failed messages"""
        order_id = order['orderId']
        retry_count = self.retry_count.get(order_id, 0)
        
        if retry_count < self.max_retries:
            # Retry processing
            self.retry_count[order_id] = retry_count + 1
            self.stats['retried'] += 1
            logger.info(f"Retrying order {order_id} (attempt {retry_count + 1}/{self.max_retries})")
            
            # Exponential backoff
            time.sleep(2 ** retry_count)
            return True
        else:
            # Max retries exceeded, send to DLQ
            self.send_to_dlq(order, f"Max retries ({self.max_retries}) exceeded")
            del self.retry_count[order_id]
            return False
    
    def consume_messages(self, timeout=1.0):
        """Consume and process messages"""
        try:
            msg = self.consumer.poll(timeout)
            
            if msg is None:
                return None
            
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                return None
            
            order = msg.value()
            self.stats['total_consumed'] += 1
            self.stats['last_order'] = order
            
            # Process order with retry logic
            success = self.process_order(order)
            
            if success:
                # Commit offset on success
                self.consumer.commit(msg)
                self.stats['successful'] += 1
                
                # Clear retry count if exists
                if order['orderId'] in self.retry_count:
                    del self.retry_count[order['orderId']]
                    
                return order
            else:
                # Handle retry
                should_retry = self.handle_retry(order)
                if should_retry:
                    # Process will be retried in next poll
                    pass
                else:
                    # Sent to DLQ, commit offset
                    self.consumer.commit(msg)
                
                return None
                
        except Exception as e:
            logger.error(f"Error consuming message: {e}")
            return None
    
    def get_stats(self):
        """Get current consumer statistics"""
        return {
            **self.stats,
            'total_orders': self.total_orders,
            'failed_orders': self.failed_orders,
            'active_retries': len(self.retry_count)
        }
    
    def close(self):
        """Close consumer and producer"""
        self.consumer.close()
        self.dlq_producer.flush()
        logger.info("Consumer closed")


def main():
    """Main function for standalone consumer testing"""
    consumer = OrderConsumer()
    
    try:
        print("Starting order consumption...")
        print("Press Ctrl+C to stop\n")
        
        while True:
            order = consumer.consume_messages()
            if order:
                stats = consumer.get_stats()
                print(f"\n✓ Consumed: Order #{order['orderId']} - {order['product']} - ${order['price']:.2f}")
                print(f"Running Average: ${stats['running_avg']:.2f}")
                print(f"Total Orders: {stats['total_orders']} | Failed: {stats['failed_orders']}")
            
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
