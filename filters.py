import requests
from Logger import Logger
import duckdb
import json
import yaml
from urllib.parse import quote
from CompiledCodeCache import CompiledCodeCache



log = Logger("DEBUG")

############################################################################
def restFilter(row, actionConfig):
    host = actionConfig['host']
    path = actionConfig['path']
    method = actionConfig.get('method', 'GET')

    queryParams = actionConfig.get('queryParams', "")
    queryParams = queryParams.format(**row)

    # If not all parameters are filled, skip this row
    if "{" in queryParams:
        log.debug(f"\t\tError: Not all parameters are filled in queryParams: {queryParams}")
        return {}
    

    postBody = actionConfig.get('postBody', "")
    postBody = postBody.format(**row)
    # If not all parameters are filled, skip this row
    if "{" in postBody:
        log.debug(f"\t\tError: Not all parameters are filled in postBody: {postBody}")
        return {}
    postBody = json.dumps(yaml.safe_load(postBody))

    path = path.format(**row)
    url = f"{host}/{path}"

    if (method == 'GET'):
        if actionConfig.get('urlencodeParams', False):
            queryParams = quote(queryParams, safe='')
        
        if actionConfig.get('logHttpRequests', False):
            log.debug("\t\t" + method + " Request: " + url + "?" + queryParams)
        response = requests.request(method, url, params=queryParams)
    else:
        headers = {'Content-Type': 'application/json'}
        if actionConfig.get('logHttpRequests', False):
            log.debug("\t\t" + method + " Request: " + url + "?" + queryParams + " Body: " + postBody)
        response = requests.request(method, url, data=postBody, headers=headers)

    if response.status_code == 200:
        if actionConfig.get('logHttpResponses', False):
            log.debug("\t\tResponse:" + str(json.dumps(response.json())))
        # Add full json as new columns to the row
        row_dict = row.to_dict()
        row_dict[actionConfig.get('newField', 'response')] = json.dumps(response.json())
        return row_dict
    else:
        log.debug(f"\t\tError al hacer la petición REST: http {response.status_code} URL: {url}?{queryParams}   ROW: {row}")

############################################################################
def pythonFilter(filterIndex, row, code):
    CompiledCodeCache().get_compiled_code(filterIndex, code)
    try:
        codeObject = compile(code, 'sumstring', 'exec')
        row_dict = row.to_dict()
        res = exec(codeObject, {"row": row_dict})
        return row_dict
    except Exception as e:
        log.debug(f"\t\tError running python code: {e}")
############################################################################
def sqlFilter(df, actionConfig):
    sql = actionConfig['sql']
    newDf = duckdb.query(sql).to_df()
    return newDf
############################################################################
def pandasFilter(df, actionConfig):
    codeObject = compile(str(actionConfig['code']), 'sumstring', 'exec')
    try:
        res = exec(codeObject, {"df": df})
        return df
    except Exception as e:
        log.debug(f"\t\tError al ejecutar el código: {e}")