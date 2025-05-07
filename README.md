# Kafka Mock Broker - ApiVersions v3 Implementation

This project is part of a low-level Kafka broker implementation challenge, focusing specifically on crafting a correct response to an `ApiVersions` request using the Kafka v3+ binary protocol.

## Objective

Correctly respond to a Kafka client sending an `ApiVersions` request (version 4) with a valid binary-encoded response, including fields such as error code, supported API keys, throttle time, and tagged fields. Support for unknown versions must return a corresponding error.

## Core Implementation Highlights

* Extraction and decoding of the `correlation_id` and `api_version` from the request header
* Dynamic error code assignment depending on the `api_version` support
* Construction of a valid Kafka-encoded compact array for supported API keys
* Appending of all required tagged field buffers (even if empty)
* Order of fields respected per Kafka v3+ response requirements

## Structure of Kafka ApiVersions Response (v3)

| Field                 | Type    | Description                             |
| --------------------- | ------- | --------------------------------------- |
| message\_length       | int32   | Total message length (excluding itself) |
| correlation\_id       | int32   | Echoed from request                     |
| error\_code           | int16   | 0 = OK, 35 = Unsupported version        |
| api\_keys\_count+1    | uvarint | Compact array size indicator            |
| api\_key              | int16   | 18 for ApiVersions                      |
| min\_version          | int16   | Usually 0                               |
| max\_version          | int16   | Usually 4                               |
| api\_key\_tag\_buffer | uvarint | Empty (0)                               |
| throttle\_time\_ms    | int32   | Usually 0                               |
| final\_tag\_buffer    | uvarint | Empty (0)                               |

## Example (hexadecimal, valid response)

```bash
# ApiVersions v3 encoded response example (19 bytes total):
00000013  # message length: 19 bytes
663b41eb  # correlation ID
0000      # error code = 0 (NO_ERROR)
02        # compact array length (1 item + 1)
0012      # API key = 18
0000      # min_version = 0
0004      # max_version = 4
00        # tagged fields for API key
00000000  # throttle_time_ms = 0
00        # final tagged fields (empty)
```

## Lessons Learned

* Kafka tagged fields are always prefixed with a `uvarint`, even when empty
* The compact array's length prefix counts bytes, not the number of entries
* Every response field **must** be encoded and appended in strict Kafka v3+ order

## Technologies Used

* Python (low-level socket and binary encoding)
* Kafka Protocol v3+ specification (official docs)

---
