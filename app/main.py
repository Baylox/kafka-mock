# TCP server that simulates a Kafka broker.
import socket 


def main():
    print("Kafka mock broker listening on port 9092...")

    # Create a TCP server on localhost:9092
    with socket.create_server(("localhost", 9092)) as server:
        while True:
            conn, addr = server.accept()
            with conn:
                print(f"Connection from {addr}")

                # The request can be read here
                _ = conn.recv(1024)  

                response = b"\x00\x00\x00\x04" + b"\x00\x00\x00\x07"
                conn.sendall(response)
                print("Sent response with correlation_id = 7")


if __name__ == "__main__":
    main()