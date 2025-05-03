from app.parser import parse_header
from app.handlers.utils import build_full_response

def handle_api_versions(data: bytes):
    parsed = parse_header(data)
    if not parsed:
        return None

    _, correlation_id = parsed

    error_code = (0).to_bytes(2, "big")

    entry = (
        (18).to_bytes(2, "big") +  # ApiKey
        (0).to_bytes(2, "big") +   # MinVersion
        (4).to_bytes(2, "big")     # MaxVersion
    )
    # CORRECT → send \x01 to mean 1 item (len-1 = 0)
    compact_array_len = b'\x01'
    api_key_array = compact_array_len + entry

    tag_buffer = b'\x00'  # Empty TAG_BUFFER

    body = error_code + api_key_array + tag_buffer

    return build_full_response(correlation_id, body)