# Server code for the Kafka mock broker
import socket
from app.parser import parse_header
from app.handlers.api_versions import handle_api_versions

def run_server():
    print("Kafka mock broker listening on port 9092...")
    with socket.create_server(("localhost", 9092)) as server:
        while True:
            conn, _ = server.accept()
            with conn:
                data = conn.recv(1024)
                if not data:
                    continue
                response = handle_api_versions(data)
                if response:
                    conn.sendall(response)