def parse_header(data: bytes):
    if len(data) < 12:
        return None
    api_version = int.from_bytes(data[6:8], "big")
    correlation_id = data[8:12]
    return api_version, correlation_id