import datetime, dateutil, sys, re
from dateutil.tz import tzoffset
import simplejson as json

# JSON schema follows:
# https://json-schema.org/

def _do_infer_schema(obj, record_level=None):
    schema = dict()

    # Go down to the record level if specified
    if record_level:
        for x in record_level.split(","):
            obj = obj[x]

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
            if (type(obj) is datetime.datetime or
                    type(obj) is datetime.date or
                    (type(obj) is str and
                     re.match("(19|20)\d\d-(0[1-9]|1[012])-([1-9]|0[1-9]|[12][0-9]|3[01])",
                              obj) is not None)):
                schema["format"] = "date-time"
        else:
            if type(obj) == bool:
                schema["type"] = ["null", "boolean"]
            elif type(obj) == float or (type(obj) == str and "." in obj):
                schema["type"] = ["null", "number"]
            # Let's assume it's a code such as zipcode if there is a leading 0
            elif type(obj) == int or (type(obj) == str and obj[0] != "0"):
                schema["type"] = ["null", "integer"]
            else:
                schema["type"] = ["null", "string"]
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


def _infer_from_two(schema1, schema2):
    """
    Compare between currently the most conservative and the new record schemas
    and keep the more conservative one.
    """
    if schema1 is None:
        return schema2
    if schema2 is None:
        return schema1
    schema = schema2
    for key in schema1["properties"]:
        prop1 = schema1["properties"][key]
        prop2 = schema2["properties"].get(key, prop1)
        schema["properties"][key] = _compare_props(prop1, prop2)
    return schema


def infer_schema(obj, record_level=None):
    if type(obj) is not list:
        obj = [obj]
    if type(obj[0]) is not dict:
        raise ValueError("Input must be a dict object.")
    schema = None
    # Go through the entire list of objects and find the most safe type assumption
    for o in obj:
        cur_schema = _do_infer_schema(o, record_level)
        # Compare between currently the most conservative and the new record
        # and keep the more conservative.
        schema = _infer_from_two(schema, cur_schema)
    schema["type"] = "object"
    return schema


def _nested_get(input_dict, nested_key):
    internal_dict_value = input_dict
    for k in nested_key:
        internal_dict_value = internal_dict_value.get(k, None)
        if internal_dict_value is None:
            return None
    return internal_dict_value


def _parse_datetime_tz(datetime_str, default_tz_offset=0):
    d = dateutil.parser.parse(datetime_str)
    if not d.tzinfo:
        d = d.replace(tzinfo=tzoffset(None, default_tz_offset))
    return d


def _on_invalid_property(policy, dict_path, obj_type, obj, err_msg):
    if policy == "raise":
        raise Exception(err_msg + " dict_path" + str(dict_path) + " object type: " + obj_type + " object: " + str(obj))
    elif policy == "force":
        filtered = str(obj)
    elif policy == "null":
        filtered = None
    else:
        raise ValueError("Unknown policy: %s" % policy)
    return filtered


def filter_object(obj, schema, dict_path=[], on_invalid_property="raise"):
    """
    Check the object against the schema.
    Convert the fields into the proper object types.
    """
    invalid_actions = ["raise", "null", "force"]
    if not on_invalid_property in invalid_actions:
        raise ValueError("on_invalid_property is not one of %s" % invalid_actions)

    obj_type = _nested_get(schema, dict_path + ["type"])
    obj_format = _nested_get(schema, dict_path + ["format"])

    nullable = False
    if obj_type is None:
        if on_invalid_property == "raise":
            raise ValueError("Unknown property found at: %s" % dict_path)
        return None
    if type(obj_type) is list:
        nullable = (obj_type == "null")
        obj_type = obj_type[1]

    if obj is None:
        if not nullable:
            if on_invalid_property == "raise":
                raise ValueError("Null object given at %s" % dict_path)
            return None

    # Recurse if object or array types
    if obj_type == "object":
        if not (type(obj) is dict and obj.keys()):
            raise KeyError("property type (object) Expected a dict object." +
                           "Got: %s %s" % (type(obj), str(obj)))
        filtered = dict()
        for key in obj.keys():
            ret = filter_object(obj[key], schema, dict_path + ["properties", key], on_invalid_property)
            if ret:
                filtered[key] = ret
    elif obj_type == "array":
        assert(type(obj) is list)
        filtered = list()
        for o in obj:
            ret = filter_object(o, schema, dict_path + ["items"], on_invalid_property)
            if ret:
                filtered.append(ret)
    else:
        if obj_type == "string":
            filtered = str(obj)
            if obj_format == "date-time":
                try:
                    filtered = _parse_datetime_tz(obj, default_tz_offset=0).isoformat()
                except Exception as e:
                    filtered = _on_invalid_property(on_invalid_property, dict_path, obj_type, obj, err_msg=str(e))
        elif obj_type == "number":
            try:
                filtered = float(obj)
            except ValueError as e:
                filtered = _on_invalid_property(on_invalid_property, dict_path, obj_type, obj, err_msg=str(e))
        elif obj_type == "integer":
            try:
                filtered = int(obj)
            except ValueError as e:
                filtered = _on_invalid_property(on_invalid_property, dict_path, obj_type, obj, err_msg=str(e))
        elif obj_type == "boolean":
            if str(obj).lower() == "true":
                filtered = True
            elif str(obj).lower() == "false":
                filtered = False
            else:
                filtered = _on_invalid_property(on_invalid_property, dict_path, obj_type, obj, err_msg=str(e))
    return filtered


if __name__ == "__main__":
    with open(sys.argv[1], "r") as f:
        content = f.read()
    sample_data = json.loads(content)

    schema = infer_schema(sample_data)

    print(json.dumps(schema, indent=2))
