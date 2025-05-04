def encode_unsigned_varint(n: int) -> bytes:
    """Encode an integer as an unsigned varint, per Kafka protocol."""
    result = bytearray()
    while True:
        to_write = n & 0x7F
        n >>= 7
        if n:
            result.append(to_write | 0x80)  # Set MSB to indicate continuation
        else:
            result.append(to_write)        # Final byte
            break
    return bytes(result)


def encode_compact_array(elements: list[bytes]) -> bytes:
    """
    Kafka compact array:
    - First: unsigned varint = number of elements + 1
    - Then: concatenated elements
    """
    return encode_unsigned_varint(len(elements) + 1) + b''.join(elements)



def encode_compact_string(s: str) -> bytes:
    """
    Encode a UTF-8 string with Kafka's compact format:
    - Varint length = len(string bytes) + 1
    - Then the UTF-8 string bytes
    """
    encoded = s.encode("utf-8")
    return encode_unsigned_varint(len(encoded) + 1) + encoded