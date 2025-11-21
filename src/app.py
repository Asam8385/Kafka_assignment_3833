"""
Flask Web Application for Kafka Order Processing System
Provides real-time web UI for monitoring producer, consumer, and system metrics
"""

from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
import threading
import time
from datetime import datetime
import os
import sys

# Add parent directory to path and change to project root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
os.chdir(project_root)

from src.producer import OrderProducer
from src.consumer import OrderConsumer

app = Flask(__name__)
app.config['SECRET_KEY'] = 'kafka-order-system-secret'
socketio = SocketIO(app, cors_allowed_origins="*")

# Global state
system_state = {
    'running': False,
    'start_time': None,
    'producer': None,
    'consumer': None,
    'producer_thread': None,
    'consumer_thread': None,
    'metrics': {
        'producer': {
            'total_produced': 0,
            'last_order': None,
            'rate': 0.0,
            'orders': []
        },
        'consumer': {
            'total_consumed': 0,
            'successful': 0,
            'retried': 0,
            'sent_to_dlq': 0,
            'running_avg_price': 0.0,
            'recent_orders': []
        }
    }
}


def producer_worker():
    """Background worker for producing orders"""
    while system_state['running']:
        try:
            order = system_state['producer'].produce_order()
            if order:
                system_state['metrics']['producer']['total_produced'] += 1
                system_state['metrics']['producer']['last_order'] = order
                system_state['metrics']['producer']['orders'].insert(0, {
                    'orderId': order['orderId'],
                    'product': order['product'],
                    'price': f"${order['price']:.2f}",
                    'timestamp': datetime.now().strftime('%H:%M:%S')
                })
                # Keep only last 10 orders
                system_state['metrics']['producer']['orders'] = \
                    system_state['metrics']['producer']['orders'][:10]
                
                # Emit to frontend
                socketio.emit('producer_update', system_state['metrics']['producer'])
        except Exception as e:
            print(f"Producer error: {e}")
        
        time.sleep(2)  # Produce every 2 seconds


def consumer_worker():
    """Background worker for consuming orders"""
    while system_state['running']:
        try:
            order = system_state['consumer'].consume_messages()
            if order:
                system_state['metrics']['consumer']['total_consumed'] += 1
                system_state['metrics']['consumer']['successful'] += 1
                
                # Get stats from consumer
                stats = system_state['consumer'].get_stats()
                system_state['metrics']['consumer']['running_avg_price'] = stats['running_avg']
                system_state['metrics']['consumer']['retried'] = stats['retried']
                system_state['metrics']['consumer']['sent_to_dlq'] = stats['sent_to_dlq']
                
                # Add to recent orders
                system_state['metrics']['consumer']['recent_orders'].insert(0, {
                    'orderId': order.get('orderId', 'N/A'),
                    'product': order.get('product', 'N/A'),
                    'price': f"${order.get('price', 0):.2f}",
                    'status': 'success',
                    'timestamp': datetime.now().strftime('%H:%M:%S')
                })
                # Keep only last 10 orders
                system_state['metrics']['consumer']['recent_orders'] = \
                    system_state['metrics']['consumer']['recent_orders'][:10]
                
                # Emit to frontend
                socketio.emit('consumer_update', system_state['metrics']['consumer'])
        except Exception as e:
            print(f"Consumer error: {e}")
        
        time.sleep(0.1)


@app.route('/')
def index():
    """Render main dashboard page"""
    return render_template('index.html')


@app.route('/api/status')
def get_status():
    """Get current system status"""
    uptime = 0
    if system_state['running'] and system_state['start_time']:
        uptime = int((datetime.now() - system_state['start_time']).total_seconds())
    
    return jsonify({
        'running': system_state['running'],
        'uptime': uptime,
        'metrics': system_state['metrics']
    })


@socketio.on('start_system')
def handle_start_system():
    """Start the Kafka producer and consumer"""
    if not system_state['running']:
        try:
            # Initialize producer and consumer
            system_state['producer'] = OrderProducer(
                bootstrap_servers='localhost:9092',
                schema_registry_url='http://localhost:8081'
            )
            
            system_state['consumer'] = OrderConsumer(
                bootstrap_servers='localhost:9092',
                schema_registry_url='http://localhost:8081',
                group_id='order-consumer-group'
            )
            
            # Reset metrics
            system_state['metrics'] = {
                'producer': {
                    'total_produced': 0,
                    'last_order': None,
                    'rate': 0.0,
                    'orders': []
                },
                'consumer': {
                    'total_consumed': 0,
                    'successful': 0,
                    'retried': 0,
                    'sent_to_dlq': 0,
                    'running_avg_price': 0.0,
                    'recent_orders': []
                }
            }
            
            # Start workers
            system_state['running'] = True
            system_state['start_time'] = datetime.now()
            
            system_state['producer_thread'] = threading.Thread(target=producer_worker, daemon=True)
            system_state['consumer_thread'] = threading.Thread(target=consumer_worker, daemon=True)
            
            system_state['producer_thread'].start()
            system_state['consumer_thread'].start()
            
            emit('system_status', {'status': 'started', 'message': 'System started successfully!'})
        except Exception as e:
            emit('system_status', {'status': 'error', 'message': f'Failed to start: {str(e)}'})
    else:
        emit('system_status', {'status': 'warning', 'message': 'System is already running!'})


@socketio.on('stop_system')
def handle_stop_system():
    """Stop the Kafka producer and consumer"""
    if system_state['running']:
        system_state['running'] = False
        
        # Wait for threads to finish
        if system_state['producer_thread']:
            system_state['producer_thread'].join(timeout=2)
        if system_state['consumer_thread']:
            system_state['consumer_thread'].join(timeout=2)
        
        # Close connections
        if system_state['producer']:
            system_state['producer'].close()
        if system_state['consumer']:
            system_state['consumer'].close()
        
        emit('system_status', {'status': 'stopped', 'message': 'System stopped successfully!'})
    else:
        emit('system_status', {'status': 'warning', 'message': 'System is not running!'})


@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    print('Client connected')
    emit('system_status', {'status': 'connected', 'message': 'Connected to server'})


@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    print('Client disconnected')


if __name__ == '__main__':
    print("=" * 80)
    print("🚀 KAFKA ORDER PROCESSING SYSTEM - WEB UI 🚀")
    print("=" * 80)
    print("\nStarting Flask server...")
    print("Open your browser and navigate to: http://localhost:5000")
    print("\nPress Ctrl+C to stop the server\n")
    print("=" * 80)
    
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, allow_unsafe_werkzeug=True)
