from tap_rest_api.schema import Schema

def get_schemas():
    old_schema = {
        "type": "object",
        "properties": {
            "id": {
                "type": "integer",
            },
            "nested": {
                "type": "object",
                "properties": {
                    "id": {
                        "type": "integer",
                    },
                    "name": {
                        "type": "string"
                    }
                }
            }
        }
    }
    new_schema = {
        "type": "object",
        "properties": {
            "id": {
                "type": "string",
            },
            "name": {
                "type": "string"
            },
            "created_at": {
                "type": "string",
                "format": "date-time",
            },
            "nested": {
                "type": "object",
                "properties": {
                    "id": {
                        "type": "integer",
                    },
                    "name": {
                        "type": "string"
                    },
                    "price": {
                        "type": "number"
                    }
                }
            }
        },
    }

    expected_schema = {
        "type": "object",
        "properties": {
            "id": {
                "type": "integer",
            },
            "name": {
                "type": "string"
            },
            "created_at": {
                "type": "string",
                "format": "date-time",
            },
            "nested": {
                "type": "object",
                "properties": {
                    "id": {
                        "type": "integer",
                    },
                    "name": {
                        "type": "string"
                    }
                }
            }
        },
    }
    return old_schema, new_schema, expected_schema


def test_safe_update():
    lock_obj = True
    old_schema, new_schema, expected_schema = get_schemas()
    safe_schema = Schema.safe_update(old_schema, new_schema, lock_obj)
    assert(safe_schema == expected_schema)


def test_safe_update_lock_obj():
    lock_obj = True
    old_schema, new_schema, expected_schema = get_schemas()
    lock_obj = False
    # This time, new_schema's nested.price (=subitem) should be added.
    expected_schema["properties"]["nested"]["properties"]["price"] = {"type": "number"}
    safe_schema = Schema.safe_update(old_schema, new_schema, lock_obj)
    assert(safe_schema == expected_schema)

