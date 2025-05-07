# Server code for the Kafka mock broker
import socket
import threading
from app.parser import parse_header
from app.handlers.api_versions import handle_api_versions

def run_server():
    print("Kafka mock broker listening on port 9092...")
    with socket.create_server(("localhost", 9092)) as server:
        while True:
            conn, addr = server.accept()
            print(f"Accepted connection from {addr}")
            # Spawn a thread for each client
            thread = threading.Thread(target=handle_connection, args=(conn,))
            thread.daemon = True
            thread.start()

def handle_connection(conn):
    with conn:
        try:
            while True:
                request = recv_kafka_request(conn)
                if not request:
                    break
                response = handle_api_versions(request)
                if response:
                    send_kafka_response(conn, response)
        except Exception as e:
            print("Error handling client:", e)

def recv_kafka_request(conn):
    length_bytes = conn.recv(4)
    if not length_bytes:
        return None
    length = int.from_bytes(length_bytes, 'big')
    data = b''
    while len(data) < length:
        chunk = conn.recv(length - len(data))
        if not chunk:
            return None
        data += chunk
    return length_bytes + data  # Full Kafka message (length + payload)

def send_kafka_response(conn, response):
    conn.sendall(response)  # Already includes the length from build_full_response