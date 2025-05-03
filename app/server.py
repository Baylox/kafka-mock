# Server code for the Kafka mock broker
import socket


def run_server():
    print("Kafka mock broker listening on port 9092...")
    with socket.create_server(("localhost", 9092)) as server:
        while True:
            conn, _ = server.accept()
            with conn:
                data = conn.recv(1024)
