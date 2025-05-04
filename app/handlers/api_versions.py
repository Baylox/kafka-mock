# app/handlers/api_versions.py
from app.parser import parse_header
from app.utils.encoding import encode_compact_array, encode_unsigned_varint
from app.utils.response import build_full_response

def handle_api_versions(data: bytes):
    parsed = parse_header(data)
    if not parsed:
        return None

    api_version, correlation_id = parsed

    # --- Response fields ---

    # 1. Error code: 0 = No error, 35 = UNSUPPORTED_VERSION
    error_code_value = 0 if 0 <= api_version <= 4 else 35
    error_code = error_code_value.to_bytes(2, "big")

    # 2. Supported API keys list: one entry (ApiVersions, v0 to v4)
    entry = (
        (18).to_bytes(2, "big") +  # API Key (ApiVersions = 18)
        (0).to_bytes(2, "big") +   # MinVersion
        (4).to_bytes(2, "big") +   # MaxVersion
        encode_unsigned_varint(0)  # Tagged fields (empty)
    )
    api_key_array = encode_compact_array([entry])

    # 3. throttle_time_ms: 0 (4 bytes)
    throttle_time_ms = (0).to_bytes(4, "big")

    # 4. Final tagged fields (empty)
    final_tag_buffer = encode_unsigned_varint(0)

    # Corrected Kafka v3+ response order
    body = error_code + api_key_array + throttle_time_ms + final_tag_buffer

    return build_full_response(correlation_id, body)