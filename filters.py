import requests
from Logger import Logger
import duckdb
import json
import yaml
from urllib.parse import quote
from CompiledCodeCache import CompiledCodeCache

log = Logger("DEBUG")

############################################################################
def restFilter(row_dict, actionConfig):
    host = actionConfig['host']
    path = actionConfig['path']
    method = actionConfig.get('method', 'GET')

    queryParams = actionConfig.get('queryParams', "")
    try:
        queryParams = queryParams.format(**row_dict)
    except Exception as e:
        log.debug(f"\t\tError mapping parameter: {e}")
        return None

    # If not all parameters are filled, skip this row
    if "{" in queryParams:
        log.debug(f"\t\tError: Not all parameters are filled in queryParams: {queryParams}")
        return {}

    if (method == 'POST'):
        postBody = actionConfig.get('postBody', "")
        postBody = postBody.format(**row_dict)
        # If not all parameters are filled, skip this row
        if "{" in postBody:
            log.debug(f"\t\tError: Not all parameters are filled in postBody: {postBody}")
            return {}
        postBody = json.dumps(yaml.safe_load(postBody))

    path = path.format(**row_dict)
    url = f"{host}/{path}"

    response = None
    if (method == 'GET'):
        if actionConfig.get('urlencodeParams', False):
            queryParams = quote(queryParams, safe='')

        if actionConfig.get('logHttpRequests', False):
            log.debug("\t\t" + method + " Request: " + url + "?" + queryParams)
        try:
            response = requests.request(method, url, params=queryParams)
        except Exception as e:
            log.debug(f"\t\tError making REST request: {e}")
            return None
    else:
        headers = {'Content-Type': 'application/json'}
        if actionConfig.get('logHttpRequests', False):
            log.debug("\t\t" + method + " Request: " + url + "?" + queryParams + " Body: " + postBody)
            try:
                response = requests.request(method, url, data=postBody, headers=headers)
            except Exception as e:
                log.debug(f"\t\tError making REST request: {e}")
                return None

    if response.status_code == 200:
        if actionConfig.get('logHttpResponses', False):
            log.debug("\t\tResponse:" + str(json.dumps(response.json())))
        # Add full json as new columns to the row
        #row_dict = row.to_dict()
        row_dict[actionConfig.get('newField', 'response')] = json.dumps(response.json())
        result = {}
        result['row'] = row_dict
        result['status_code'] = response.status_code
        return result
    else:
        log.debug(
            f"\t\tError al hacer la petición REST: http {response.status_code} URL: {url}?{queryParams}   ROW: {row_dict}")
        result = {}
        result['row'] = None
        result['status_code'] = response.status_code
        return result


############################################################################
def pythonFilter(filterIndex, row, code):
    CompiledCodeCache().get_compiled_code(filterIndex, code)
    try:
        codeObject = compile(code, 'sumstring', 'exec')
        exec(codeObject, {"row": row})
        return row
    except Exception as e:
        log.debug(f"\t\tError running python code: {e}")

############################################################################
def sqlFilter(filter_):
    sql = filter_['code']
    index = str(filter_['index'])
    duckdb.query("CREATE OR REPLACE TABLE filter" + index +" AS (" + sql + ")")


############################################################################
def pandasFilter(df, actionConfig):
    codeObject = compile(str(actionConfig['code']), 'sumstring', 'exec')
    try:
        res = exec(codeObject, {"df": df})
        return df
    except Exception as e:
        log.debug(f"\t\tError al ejecutar el código: {e}")
