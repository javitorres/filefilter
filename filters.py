
import requests

from Logger import Logger
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
            log.debug("\t\tResponse:" + str(response.json()))
        # Add full json as new columns to the row
        row_dict = row.to_dict()
        row_dict['response'] = response.json()
        return row_dict
    else:
        log.debug(f"\t\tError al hacer la petición REST: {response.status_code}")

def pythonFilter(row, actionConfig):
    #log.debug("Compilando: " + str(actionConfig['code']))
    codeObject = compile(str(actionConfig['code']), 'sumstring', 'exec')
    
    try:
        #log.debug("Running python code: \n{code}")
        row_dict = row.to_dict()
        res = exec(codeObject, {"row": row_dict})
        return row_dict
    except Exception as e:
        log.debug(f"\t\tError al ejecutar el código: {e}")