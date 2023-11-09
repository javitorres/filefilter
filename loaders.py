import duckdb
import pandas as pd
import dask.dataframe as dd

def read_csv_with_dask(config_file, input_file):
    config = load_config(config_file, False)
    log.info("Reading CSV file into dataframe 'df' with dask")
    if config.get('inDelimiter', None) is None:
        df = dd.read_csv(input_file, header=0, sep=',', encoding='utf-8', dtype=str)
    elif config['inDelimiter'] == 'TAB' or config['inDelimiter'] == 'tab' or config['inDelimiter'] == '\t':
        df = dd.read_csv(input_file, header=0, sep='\t', encoding='utf-8', dtype=str)
    else:
        df = dd.read_csv(input_file, header=0, sep=config['inDelimiter'], encoding='utf-8', dtype=str)
    
    log.debug("df content:\n" + str(df.head(5)))
    return df.compute()

def read_csv_with_pandas(config_file, input_file):
    config = load_config(config_file, False)
    log.info("Reading CSV file into dataframe 'df' with pandas")
    if config.get('inDelimiter', None) is None:
        df = pd.read_csv(input_file, header=0, sep=',', encoding='utf-8', dtype=str)
    elif config['inDelimiter'] == 'TAB' or config['inDelimiter'] == 'tab' or config['inDelimiter'] == '\t':
        df = pd.read_csv(input_file, header=0, sep='\t', encoding='utf-8', dtype=str)
    else:
        df = pd.read_csv(input_file, header=0, sep=config['inDelimiter'], encoding='utf-8', dtype=str)
    
    log.debug("df content:\n" + str(df.head(5)))
    return df

def read_csv_with_duckdb(config_file, input_file):
    config = load_config(config_file, False)
    if config.get('inDelimiter', None) is None:
        sqlCommand = "SELECT * FROM read_csv_auto('" + input_file + "', HEADER=TRUE, SAMPLE_SIZE=1000000)"
    elif config['inDelimiter'] == 'TAB' or config['inDelimiter'] == 'tab' or config['inDelimiter'] == '\t':
        sqlCommand = "SELECT * FROM read_csv_auto('" + input_file + "', HEADER=TRUE, SAMPLE_SIZE=1000000, DELIM='\t')"
    else:
        sqlCommand = "SELECT * FROM read_csv_auto('" + input_file + "', HEADER=TRUE, SAMPLE_SIZE=1000000, DELIM='"+ config['inDelimiter'] +"')"
    
    log.info("Reading CSV file into dataframe 'df' " + input_file + ": Running: " + sqlCommand)
    df=duckdb.query(sqlCommand).to_df()
    log.debug("df content:\n" + str(df.head(5)))
    return df