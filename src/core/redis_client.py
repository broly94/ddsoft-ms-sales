import redis
import json
import threading
import os
import traceback

class RedisMicroservice:
    def __init__(self, host='redis', port=6379, queue='sales_service'):
        self.redis_client = redis.Redis(host=host, port=port, decode_responses=True)
        self.queue = queue
        self.handlers = {}
        self.is_running = False

    def on(self, pattern):
        """Decorator to register a handler for a specific NestJS pattern."""
        def decorator(handler):
            self.handlers[json.dumps(pattern) if isinstance(pattern, (dict, list)) else pattern] = handler
            return handler
        return decorator

    def listen(self):
        print(f"[*] Redis Microservice listening on queue: {self.queue}")
        self.is_running = True
        while self.is_running:
            try:
                # NestJS Microservices use LPUSH/BRPOP or similar for queues
                # For Redis Transport, it usually uses a 'message' pattern
                # But NestJS standard Redis transport uses Pub/Sub or Queues depending on config.
                # In ddsoft, based on Gateway config, it seems to be standard Redis transport.
                # Standard NestJS Redis transport uses Pub/Sub for 'emit' and 'send'.
                
                # Let's use Pub/Sub as it's the default for NestJS Redis Transport
                pubsub = self.redis_client.pubsub()
                pubsub.subscribe(self.queue)
                
                for message in pubsub.listen():
                    if message['type'] == 'message':
                        self._handle_message(message['data'])
            except Exception as e:
                print(f"[!] Redis error: {e}")
                traceback.print_exc()

    def _handle_message(self, raw_data):
        try:
            packet = json.loads(raw_data)
            pattern = packet.get('pattern')
            data = packet.get('data')
            packet_id = packet.get('id') # Only for 'send' (request-response)

            key = json.dumps(pattern) if isinstance(pattern, (dict, list)) else pattern
            
            if key in self.handlers:
                result = self.handlers[key](data)
                
                # If it's a request-response (has id), send response back
                if packet_id:
                    response = json.dumps({
                        "id": packet_id,
                        "response": result,
                        "isDisposed": True
                    })
                    # Response is sent back to a specific response channel
                    self.redis_client.publish(f"{self.queue}.res", response)
            else:
                print(f"[?] No handler for pattern: {pattern}")
        except Exception as e:
            print(f"[!] Error handling message: {e}")
            traceback.print_exc()

    def start(self):
        self.thread = threading.Thread(target=self.listen, daemon=True)
        self.thread.start()
