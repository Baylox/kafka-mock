def build_full_response(correlation_id: bytes, body: bytes) -> bytes:
    """
    Assemble the Kafka response: message size (4 bytes) + correlation ID (4 bytes) + body.
    """
    message_size = len(correlation_id + body).to_bytes(4, "big")
    print("Full response hex:", (message_size + correlation_id + body).hex())
    return message_size + correlation_id + body
    