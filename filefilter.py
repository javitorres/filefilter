import duckdb
import yaml
import sys
from filters import *
from Logger import Logger
log = Logger("DEBUG")

def read_csv_with_duckdb(input_file):
    log.info("Reading CSV file " + input_file + ": Running: CREATE TABLE df AS (SELECT * FROM read_csv_auto('" + input_file + "', HEADER=TRUE, SAMPLE_SIZE=1000000))")
    duckdb.query("CREATE TABLE df AS (SELECT * FROM read_csv_auto('" + input_file + "', HEADER=TRUE, SAMPLE_SIZE=1000000))")
    log.info("Loading data into dataframe")
    df = duckdb.query("SELECT * FROM df").to_df()  # this is needed to load the data into memory
    log.debug(df.head(5))
    return df

def apply_transformations(config, df):
    # Aplicar transformaciones
    log.debug("Applying transformations...")
        
    # Loop over each filter in config:
    for filter_ in config.get('filters', []):
        if filter_.get('disabled', False):
            log.debug(f"\tFilter {filter_.get('name', 'unnamed')} is disabled, skipping...")
            break
        else:
            log.debug(f"\tApplying '{filter_.get('name', 'unnamed')}'")
        # Loop over each record of df pandas dataframe:
        for index, row in df.iterrows():
            
            if (config.get('sampleLines', 0)!=0 and index > config.get('sampleLines', 0)):
                log.warn("Reached sampleLines (" + str(config.get('sampleLines', 0)) + ") limit, stopping...")
                break
            log.debug("Processing row " + str(index) + "...")

            if filter_.get('actionType') == 'python':
                modified_row_dict = pythonFilter(row, filter_.get('actionConfig'))
                # Aplicar los cambios al DataFrame
                for col, value in modified_row_dict.items():
                    try:
                        df.at[index, col] = value
                    except Exception as e:
                        log.debug(f"\t\tError al aplicar el cambio: {e}")

            elif filter_.get('actionType') == 'rest':
                modified_row_dict = restFilter(row, filter_.get('actionConfig'))
                if 'response' not in df.columns:
                    df['response'] = None
                # Aplicar los cambios al DataFrame
                for col, value in modified_row_dict.items():
                    try:
                        df.at[index, col] = value
                    except Exception as e:
                        log.debug(f"\t\tError al aplicar el cambio: {e}. index: {index}, col: {col}, value: {short(value, 100)}")
            else:
                log.debug(f"Tipo de acción desconocido: {filter_.get('actionType')}")
    return df

def short(value, length):
    if len(str(value)) > length:
        return str(value)[:length] + "..."
    else:
        return str(value)

def write_output(data, config, output_file):
    log.info("Writing output file " + output_file + "...")
    if config['outDelimiter'] == 'TAB' or config['outDelimiter'] == 'tab' or config['outDelimiter'] == '\t':
        data.to_csv(output_file, sep='\t', index=True)
    else:
        data.to_csv(output_file, sep=config['outDelimiter'], index=True)

def load_config(config_file):
    with open(config_file, 'r') as f:
        log.debug("Loading config file " + config_file + "...")
        config = yaml.safe_load(f)
        log.debug("Loaded config:\n" +yaml.dump(config, default_flow_style=False, sort_keys=False))
    return config    

def main(input_file, config_file, output_file):
    # Leer el archivo CSV con DuckDB
    df = read_csv_with_duckdb(input_file)
    # Leer configuracion.yml
    config = load_config(config_file)
    # Leer configuracion.yml y aplicar transformaciones
    transformed_data = apply_transformations(config, df)
    # Escribir el archivo de salida
    write_output(transformed_data, config, output_file)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        log.info("Uso: filefilter <fichero de entrada> <configuracion.yml> <fichero de salida>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    config_file = sys.argv[2]
    output_file = sys.argv[3]
    
    main(input_file, config_file, output_file)