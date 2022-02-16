import json


def find_matching_model(models, filter_dict):
    output = []
    for model in models:
        key = model.pop("key")
        if model == filter_dict:
            output.append(model)
        model["key"] = key  # MUST restore the key

    if len(output) > 1:
        raise Exception(
            f"Found {len(models)} matches in model set but expected only 1 to match filter {json.dumps(filter_dict)}"
        )
    if len(output) == 0:
        return None
    return output[0]
