import json, sys, re


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
        ret = _do_infer_schema(obj[0])
        if ret:
            schema["type"] = ["null", "array"]
            schema["items"] = ret
    else:
        try:
            float(obj)
        except:
            schema["type"] = ["null", "string"]
            if type(obj) is str and re.match("(19|20)\d\d-(0[1-9]|1[012])-([1-9]|0[1-9]|[12][0-9]|3[01])", obj) is not None:
                schema["format"] = "date-time"
        else:
            schema["type"] = ["null", "number"]
    return schema


def infer_schema(obj):
    if type(obj) is list:
        raise NotImplemented("TODO: Read array of objects to run bettter inference.")
    if type(obj) is not dict:
        raise ValueError("Input must be a dict object.")
    schema = _do_infer_schema(obj)
    schema["type"] = "object"
    return schema


def read_json_file(file_name):
    with open(file_name, "r") as f:
        content = f.read()
    return json.loads(content)


if __name__ == "__main__":
    sample_data = read_json_file(sys.argv[1])
    schema = infer_schema(sample_data)
    print(json.dumps(schema, indent=4))
