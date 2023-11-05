import duckdb
import yaml
import sys
from filters import *
from Logger import Logger
log = Logger("DEBUG")

def read_csv_with_duckdb(input_file):
    log.info("Reading CSV file into dataframe 'df' " + input_file + ": Running: SELECT * FROM read_csv_auto('" + input_file + "', HEADER=TRUE, SAMPLE_SIZE=1000000)")
    df=duckdb.query("SELECT * FROM read_csv_auto('" + input_file + "', HEADER=TRUE, SAMPLE_SIZE=1000000)").to_df()
    log.debug(df.head(5))
    return df

def apply_transformations(config_file, df):
    config = load_config(config_file, True)

    # Aplicar transformaciones
    log.debug("Applying transformations...")
    # Loop over each filter in config:
    for filter_ in config.get('filters', []):
        if filter_.get('disabled', False):
            log.debug(f"\tFilter {filter_.get('name', 'unnamed')} is disabled, skipping...")
            break
        
        # This kind of filters loops over each record of df pandas dataframe:
        if filter_.get('actionType') == 'python' or filter_.get('actionType') == 'rest':
            for index, row in df.iterrows():
                
                if (config.get('sampleLines', 0)!=0 and index > config.get('sampleLines', 0)):
                    log.warn("Reached sampleLines (" + str(config.get('sampleLines', 0)) + ") limit, stopping...")
                    break

                # Reload config every reloadConfigEvery lines
                if (config.get('reloadConfigEvery', 0)!=0 and index % config.get('reloadConfigEvery', 0) == 0):
                    log.debug("Reloading config file " + config_file + "...")
                    config = load_config(config_file, False)

                if filter_.get('actionType') == 'python':
                    log.debug("Processing row " + str(index) + " with " + filter_.get('actionType', 'unknown') + " filter '" + filter_.get('name', 'unnamed') + "'")
                    modified_row_dict = pythonFilter(row, filter_.get('actionConfig'))
                    # Aplicar los cambios al DataFrame
                    for col, value in modified_row_dict.items():
                        try:
                            df.at[index, col] = value
                        except Exception as e:
                            log.debug(f"\t\tError appliying change: {e}")
                elif filter_.get('actionType') == 'rest':
                    log.debug("Processing row " + str(index) + " with " + filter_.get('actionType', 'unknown') + " filter '" + filter_.get('name', 'unnamed') + "'")
                    modified_row_dict = restFilter(row, filter_.get('actionConfig'))
                    if 'response' not in df.columns:
                        df['response'] = None
                    # Aplicar los cambios al DataFrame
                    for col, value in modified_row_dict.items():
                        try:
                            df.at[index, col] = value
                        except Exception as e:
                            log.debug(f"\t\tError appliying change: {e}. index: {index}, col: {col}, value: {short(value, 100)}")          
                else:
                    log.debug(f"Action type unknown: {filter_.get('actionType')}")
        # This kind of filters act over the whole df pandas dataframe:
        elif filter_.get('actionType') == 'sql' or filter_.get('actionType') == 'pandas':
            if filter_.get('actionType') == 'sql':
                log.debug("Processing df with " + filter_.get('actionType', 'unknown') + " filter '" + filter_.get('name', 'unnamed') + "'")
                newDf = sqlFilter(df, filter_.get('actionConfig'))
                df = newDf
            elif filter_.get('actionType') == 'pandas':
                log.debug("Processing df with " + filter_.get('actionType', 'unknown') + " filter '" + filter_.get('name', 'unnamed') + "'")
                newDf = pandasFilter(df, filter_.get('actionConfig'))
                df = newDf
    return df

def short(value, length):
    if len(str(value)) > length:
        return str(value)[:length] + "..."
    else:
        return str(value)

def write_output(transformed_data, config_file, output_file):
    config = load_config(config_file, False)
    log.info("Writing df to output file " + output_file + "...:\n" + str(transformed_data.head(5)))
    if config['outDelimiter'] == 'TAB' or config['outDelimiter'] == 'tab' or config['outDelimiter'] == '\t':
        transformed_data.to_csv(output_file, sep='\t', index=True)
    else:
        transformed_data.to_csv(output_file, sep=config['outDelimiter'], index=True)

def load_config(config_file, logConfig):
    with open(config_file, 'r') as f:
        if logConfig: log.debug("Loading config file " + config_file + "...")
        config = yaml.safe_load(f)
        if logConfig: log.debug("Loaded config:\n" +yaml.dump(config, default_flow_style=False, sort_keys=False))
    return config    

def main(input_file, config_file, output_file):
    # Leer el archivo CSV a un Dataframe con DuckDB
    df = read_csv_with_duckdb(input_file)

    # Aplicar transformaciones
    transformed_data = apply_transformations(config_file, df)
    # Escribir el archivo de salida
    write_output(transformed_data, config_file, output_file)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        log.info("Uso: filefilter <fichero de entrada> <configuracion.yml> <fichero de salida>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    config_file = sys.argv[2]
    output_file = sys.argv[3]
    
    main(input_file, config_file, output_file)