# KafkaMock

| Element          | Description                                   | Size    |
| ---------------- | --------------------------------------------- | ------- |
| `message_size`   | size of the message (excluding itself)        | 4 bytes |
| `correlation_id` | identifier of the client (echoed back)        | 4 bytes |
| `error_code`     | indicates if an error occurred (0 = no error) | 2 bytes |
| `api_key_array`  | list of supported API keys                    | ? bytes |
| `tag_buffer`     | additional info (empty in this case)          | 1 byte  |
