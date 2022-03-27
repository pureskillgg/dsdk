import itertools


def normalize_instructions(instructions):
    final_instructions = []
    channel_dict = {
        key: list(item)
        for key, item in itertools.groupby(instructions, lambda x: x["channel"])
    }
    for channel, channel_instructions in channel_dict.items():
        column_sets = [
            instruction.get("columns") for instruction in channel_instructions
        ]
        if None in column_sets:
            final_instructions.append({"channel": channel})
        else:
            columns_final = []
            for column_set in column_sets:
                columns_final.extend(column_set)
            columns_final = list(
                dict.fromkeys(columns_final)
            )  # remove dupes keeping order
            final_instructions.append({"channel": channel, "columns": columns_final})
    return final_instructions
