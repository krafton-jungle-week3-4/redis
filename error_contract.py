"""Shared error message contract for parser/server/core layers."""


ERR_EMPTY_COMMAND = "ERR empty command"
ERR_VALUE_NOT_INTEGER = "ERR value is not an integer or out of range"
ERR_VALUE_NOT_FLOAT = "ERR value is not a valid float"
ERR_WRONG_TYPE_STRING = "ERR wrong type operation against non-string value"
ERR_WRONG_TYPE_SET = "ERR wrong type operation against non-set value"
ERR_WRONG_TYPE_LIST = "ERR wrong type operation against non-list value"
ERR_WRONG_TYPE_HASH = "ERR wrong type operation against non-hash value"
ERR_WRONG_TYPE_ZSET = "ERR wrong type operation against non-zset value"


def err_wrong_number_of_arguments(command_name: str) -> str:
    return f"ERR wrong number of arguments for '{command_name.lower()}' command"


def err_unknown_command(raw_command_name: str) -> str:
    return f"ERR unknown command '{raw_command_name}'"
