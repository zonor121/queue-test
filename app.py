import os
import json
import threading
import requests
import pika
from dotenv import load_dotenv
from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit

load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = 'dev-secret-key'
socketio = SocketIO(app, async_mode='threading', cors_allowed_origins="*")

QUEUE_NAME = 'web_demo_queue'
CLOUDAMQP_URL = os.environ.get('CLOUDAMQP_URL', 'amqp://guest:guest@localhost:5672/%2f')

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
TABLE_NAME = 'queue_messages'

def get_rabbitmq_connection():
    params = pika.URLParameters(CLOUDAMQP_URL)
    params.socket_timeout = 5
    return pika.BlockingConnection(params)

def save_message(content: str, direction: str) -> dict:
    from supabase import create_client
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    response = supabase.table(TABLE_NAME).insert({
        'content': content,
        'direction': direction
    }).execute()
    return response.data[0] if response.data else None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/publish', methods=['POST'])
def publish():
    data = request.get_json()
    message = data.get('message', 'Empty message')
    
    try:
        record = save_message(message, 'sent')
        
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)
        )
        connection.close()
        
        # Уведомляем всех клиентов через WebSocket
        socketio.emit('new_message', record, namespace='/')
        
        return jsonify({'status': 'ok', 'record': record})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/messages')
def get_messages():
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}'
    }
    url = f'{SUPABASE_URL}/rest/v1/{TABLE_NAME}?select=*&order=created_at.asc&limit=50'
    
    try:
        response = requests.get(url, headers=headers)
        data = response.json()
        return jsonify(data if isinstance(data, list) else [])
    except Exception as e:
        return jsonify([])

def rabbitmq_consumer():
    while True:
        try:
            connection = get_rabbitmq_connection()
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.basic_qos(prefetch_count=1)

            def callback(ch, method, properties, body):
                content = body.decode()
                with app.app_context():
                    record = save_message(content, 'received')
                    if record:
                        # Отправляем в WebSocket из фонового потока
                        socketio.emit('new_message', record, namespace='/')
                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
            print('[*] Background consumer started...')
            channel.start_consuming()
        except Exception as e:
            print(f'[!] Consumer error: {e}. Reconnecting in 5s...')
            import time
            time.sleep(5)

if __name__ == '__main__':
    if not os.environ.get('WERKZEUG_RUN_MAIN'):
        consumer_thread = threading.Thread(target=rabbitmq_consumer, daemon=True)
        consumer_thread.start()
    
    # Запускаем через socketio.run вместо app.run
    socketio.run(app, debug=True, port=5000, allow_unsafe_werkzeug=True)