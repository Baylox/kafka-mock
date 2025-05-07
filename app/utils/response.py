def build_full_response(correlation_id: int, body: bytes) -> bytes:
    """
    Kafka response format:
    - message_length: int32 (number of bytes after this field)
    - correlation_id: int32 (matches the request's correlation ID)
    - response_body: bytes (the actual response data)
    """

    # Encode the correlation ID as 4 bytes (big-endian)
    correlation_bytes = correlation_id.to_bytes(4, "big")
    # Compute the total message length (excluding the first 4 bytes for the length itself)
    message_size = len(correlation_bytes + body).to_bytes(4, "big")
    # Construct the full response: length + correlation ID + body
    full = message_size + correlation_bytes + body
    print("Full response hex:", full.hex())
    return full
    