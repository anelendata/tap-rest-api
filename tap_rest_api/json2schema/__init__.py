import json, sys, re

# JSON schema follows:
# https://json-schema.org/

def _do_infer_schema(obj):
    schema = dict()
    if type(obj) is dict and obj.keys():
        schema["type"] = ["null", "object"]
        schema["properties"] = dict()
        for key in obj.keys():
            ret = _do_infer_schema(obj[key])
            if ret:
                schema["properties"][key] = ret
    elif type(obj) is list:
        if not obj:
            return None
        # TODO: Check more than the first record
        ret = _do_infer_schema(obj[0])
        if ret:
            schema["type"] = ["null", "array"]
            schema["items"] = ret
    else:
        try:
            float(obj)
        except:
            schema["type"] = ["null", "string"]
            # TODO: This is a very loose regex for date-time.
            if type(obj) is str and re.match("(19|20)\d\d-(0[1-9]|1[012])-([1-9]|0[1-9]|[12][0-9]|3[01])", obj) is not None:
                schema["format"] = "date-time"
        else:
            if type(obj) == float or (type(obj) == str and "." in obj):
                schema["type"] = ["null", "number"]
            else:
                schema["type"] = ["null", "integer"]
    return schema


def _compare_props(prop1, prop2):
    prop = prop2
    t1 = prop1["type"]
    t2 = prop2["type"]
    f1 = prop1.get("format")
    f2 = prop2.get("format")
    if t1[1] == "object":
        assert(t1[1] == t2[1])
        for key in prop["properties"]:
            prop["properties"][key] = _compare_props(prop1["properties"][key],
                                                    prop2["properties"][key])
    if t1[1] == "array":
        assert(t1[1] == t2[1])
        prop["items"] = _compare_props(prop1["items"], prop2["items"])

    numbers = ["integer", "number"]
    if not (t1[1] == t2[1] and f1 == f2):
        if t1[1] in numbers and t2[1] in numbers:
            prop["type"] = ["null", "number"]
        else:
            prop["type"] = ["null", "string"]
            if "format" in prop.keys():
                prop.pop("format")

    return prop


def infer_from_two(schema1, schema2):
    if schema1 is None:
        return schema2
    if schema2 is None:
        return schema1
    schema = schema2
    for key in schema1["properties"]:
        prop1 = schema1["properties"][key]
        prop2 = schema2["properties"][key]
        schema["properties"][key] = _compare_props(prop1, prop2)
    return schema


def infer_schema(obj):
    if type(obj) is not list:
        obj = [obj]
    if type(obj[0]) is not dict:
        raise ValueError("Input must be a dict object.")
    schema = None
    for o in obj:
        cur_schema = _do_infer_schema(o)
        schema = infer_from_two(schema, cur_schema)
    schema["type"] = "object"
    return schema


if __name__ == "__main__":
    with open(sys.argv[1], "r") as f:
        content = f.read()
    sample_data = json.loads(content)

    schema = infer_schema(sample_data)

    print(json.dumps(schema, indent=2))
