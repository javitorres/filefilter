import requests
from Logger import Logger
import duckdb
import json

log = Logger("DEBUG")

def restFilter(row, actionConfig):
    host = actionConfig['host']
    path = actionConfig['path']
    params = {}
    param_names = actionConfig['queryParamNames'].split(',')
    param_values = actionConfig['queryParamValues'].split(',')
    for name, value in zip(param_names, param_values):
        # if Value is surrounded by {} remove them, else use the value as is
        if value.startswith('{') and value.endswith('}'):
            params[name] = row[value[1:-1]]
        else:
            params[name] = value

    method = actionConfig.get('method', 'GET')
    # Replace in method every {value} appearance with the value of the row with the same name
    path = path.format(**row)

    url = f"{host}/{path}"
    if actionConfig['urlencodeParams']:
        response = requests.request(method, url, params=params)
    else:
        url += '?' + '&'.join([f"{k}={v}" for k, v in params.items()])
        if actionConfig.get('logHttpRequests', False):
            log.debug(f"\t\tHttp request: {url}")
        response = requests.request(method, url)

    if response.status_code == 200:
        if actionConfig.get('logHttpResponses', False):
            log.debug("\t\tResponse:" + str(json.dumps(response.json())))
        # Add full json as new columns to the row
        row_dict = row.to_dict()
        row_dict['response'] = json.dumps(response.json())
        return row_dict
    else:
        log.debug(f"\t\tError al hacer la petición REST: {response.status_code} URL: {url}")

def pythonFilter(row, actionConfig):
    codeObject = compile(str(actionConfig['code']), 'sumstring', 'exec')
    try:
        row_dict = row.to_dict()
        res = exec(codeObject, {"row": row_dict})
        return row_dict
    except Exception as e:
        log.debug(f"\t\tError al ejecutar el código: {e}")

def sqlFilter(df, actionConfig):
    sql = actionConfig['sql']
    newDf = duckdb.query(sql).to_df()
    return newDf

def pandasFilter(df, actionConfig):
    codeObject = compile(str(actionConfig['code']), 'sumstring', 'exec')
    try:
        res = exec(codeObject, {"df": df})
        return df
    except Exception as e:
        log.debug(f"\t\tError al ejecutar el código: {e}")