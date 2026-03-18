ALLOWED_RESPONSE_TYPES = {
    "simple_string",
    "bulk_string",
    "null",
    "integer",
    "error",
    "array",
}


class ProtocolResponseError(ValueError):
    """Raised when an internal response dict cannot be encoded to wire format."""


def encode_response(result: dict) -> str:
    """Encode internal response dict to a RESP-like wire string."""

    if "type" not in result or "value" not in result:
        raise ProtocolResponseError("response must include 'type' and 'value'")

    response_type = result["type"]
    value = result["value"]

    if response_type not in ALLOWED_RESPONSE_TYPES:
        raise ProtocolResponseError(f"unsupported response type: {response_type}")

    if response_type == "simple_string":
        if not isinstance(value, str):
            raise ProtocolResponseError("simple_string value must be str")
        return f"+{value}\r\n"

    if response_type == "bulk_string":
        if not isinstance(value, str):
            raise ProtocolResponseError("bulk_string value must be str")
        return f"${len(value)}\r\n{value}\r\n"

    if response_type == "null":
        if value is not None:
            raise ProtocolResponseError("null value must be None")
        return "$-1\r\n"

    if response_type == "integer":
        if not isinstance(value, int):
            raise ProtocolResponseError("integer value must be int")
        return f":{value}\r\n"

    if response_type == "error":
        if not isinstance(value, str):
            raise ProtocolResponseError("error value must be str")
        return f"-{value}\r\n"

    if response_type == "array":
        if not isinstance(value, list):
            raise ProtocolResponseError("array value must be list")

        encoded = f"*{len(value)}\r\n"
        for item in value:
            if item is None:
                encoded += "$-1\r\n"
            elif isinstance(item, str):
                encoded += f"${len(item)}\r\n{item}\r\n"
            else:
                raise ProtocolResponseError("array item must be str or None")
        return encoded

    raise ProtocolResponseError(f"unreachable response type: {response_type}")
