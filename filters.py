import requests
from Logger import Logger
import duckdb
import json

log = Logger("DEBUG")

############################################################################
def restFilter(row, actionConfig):
    host = actionConfig['host']
    path = actionConfig['path']
    method = actionConfig.get('method', 'GET')

    # queryParamNames example: param1,param2
    queryParamNames = actionConfig.get('queryParamNames', "")
    # queryParamValues example: {paramValue1},{paramVale2}_{paramValue3}
    queryParamValues = actionConfig.get('queryParamValues', "")
    queryParamValues = queryParamValues.format(**row)

    # Build query params merging names and values
    queryParams = dict(zip(queryParamNames.split(","), queryParamValues.split(",")))
    # Build queryString
    queryParamString = "&".join([f"{key}={value}" for key, value in queryParams.items()])

    #log.debug("queryParamString:" + str(queryParamString))
    #queryParams = queryParams.format(**row)
    path = path.format(**row)

    url = f"{host}/{path}"

    if actionConfig.get('logHttpRequests', False):
        log.debug("\t\tRequest: " + url + "?" + queryParamString)

    if actionConfig.get('urlencodeParams', False):
        response = requests.request(method, url, params=queryParamString)
    else:
        response = requests.request(method, url, params=queryParamString)

    if response.status_code == 200:
        if actionConfig.get('logHttpResponses', False):
            log.debug("\t\tResponse:" + str(json.dumps(response.json())))
        # Add full json as new columns to the row
        row_dict = row.to_dict()
        row_dict['response'] = json.dumps(response.json())
        return row_dict
    else:
        log.debug(f"\t\tError al hacer la petición REST: {response.status_code} URL: {url}?{queryParamString}")

############################################################################
def pythonFilter(row, actionConfig):
    codeObject = compile(str(actionConfig['code']), 'sumstring', 'exec')
    try:
        row_dict = row.to_dict()
        res = exec(codeObject, {"row": row_dict})
        return row_dict
    except Exception as e:
        log.debug(f"\t\tError al ejecutar el código: {e}")
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