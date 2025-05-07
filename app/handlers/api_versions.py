from app.parser import parse_header
from app.utils.response import build_full_response
from app.utils.encoding import encode_unsigned_varint, encode_compact_array

def handle_api_versions(data: bytes):
    """
    Handles an ApiVersions request (Kafka API key 18).
    If the API version is not supported it returns an error response.
    """
    # Parse the Kafka request header to extract API version and correlation ID
    parsed = parse_header(data)
    if not parsed:
        return None

    api_version, correlation_id = parsed
    correlation_id = int.from_bytes(correlation_id, "big")  # Convert bytes to int

    # If the version is invalid, respond with error code 35 (UNSUPPORTED_VERSION)
    if api_version < 0 or api_version > 4:
        error_code = (35).to_bytes(2, "big")
    else:
        error_code = (0).to_bytes(2, "big")  # 0 = No Error

    # Define the supported API keys and their version ranges
    api_versions = [
        (18, 0, 4),  # API_VERSIONS
        (0, 0, 11),  # PRODUCE
        (1, 0, 13),  # FETCH
    ]

    # Encode each API key entry with min/max version and empty tagged fields
    entries = [
        api_key.to_bytes(2, 'big') +
        min_v.to_bytes(2, 'big') +
        max_v.to_bytes(2, 'big') +
        encode_unsigned_varint(0)  # Empty tagged fields for each entry
        for api_key, min_v, max_v in api_versions
    ]
    # Encode the list of API versions using Kafka's compact array format
    api_versions_bytes = encode_compact_array(entries)

    # Throttle time is set to 0 (no delay)
    throttle_time_ms = (0).to_bytes(4, 'big')
    
    # Final tagged fields 
    final_tagged_fields = encode_unsigned_varint(0)

    # Construct the complete response body
    body = error_code + api_versions_bytes + throttle_time_ms + final_tagged_fields

    # Wrap the body with correlation ID and message size
    return build_full_response(correlation_id, body)