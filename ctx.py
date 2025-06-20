import pandas as pd


def ctx_decode(text: str):
    """
    Creativyst Table Exchange (CTX) decoder. Note that this is not a general decoder for this format BUT
    only for this specific application on the GOVI KV8 data.
    :param text: CTX data as string
    :return: pandas DataFrame
    """
    lines = text.split('\n')
    for line in lines:
        if line.startswith((r'\G', r'\T')):
            continue

        elif line.startswith(r'\L'):
            columns = line.removeprefix(r'\L').split(r'|')
            data = {k: list() for k in columns}

        else:
            if line == '':
                continue
            split_line = line.split(r'|')
            for key, value in zip(columns, split_line):
                data[key].append(value)

    data.pop('OperatorCode', None)
    return pd.DataFrame(data)
