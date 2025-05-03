UNSUPPORTED_VERSION = 35

# Build a full with the message size, correlation ID, and body
def build_full_response(correlation_id: bytes, body: bytes):
    message_size = len(body).to_bytes(4, "big") 
    return message_size + correlation_id + body