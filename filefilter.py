import pandas as pd
import yaml
import sys
from filters import *
from Logger import Logger
import os
from tqdm import tqdm
import psutil

log = Logger("DEBUG")
global totalRows
totalRows=0

def applyRowFilter(index, row, filter_, df):
    #log.debug("\t\tProcessing row " + str(index) + " with " + filter_.get('actionType', 'unknown') + " filter '" + filter_.get('name', 'unnamed') + "'")
    if filter_.get('actionType') == 'python':
        modified_row_dict = pythonFilter(row, filter_.get('actionConfig'))
        if modified_row_dict is None:
            log.error("\t\tError executing python code, skipping row " + str(index) + ". Row:" + str(row))
        else: 
            for col, value in modified_row_dict.items():
                try:
                    df.at[index, col] = value
                except Exception as e:
                    log.debug(f"\t\tError applying change: {e}")
    elif filter_.get('actionType') == 'rest':
        modified_row_dict = restFilter(row, filter_.get('actionConfig'))
        if 'response' not in df.columns:
            df['response'] = None
        # Aplicar los cambios al DataFrame
        if modified_row_dict is None:
            log.error("\t\tError executing rest filter, skipping row " + str(index) + ". Row:" + str(row))
        else:
            for col, value in modified_row_dict.items():
                try:
                    df.at[index, col] = value
                except Exception as e:
                    log.debug(f"\t\tError appliying change: {e}. index: {index}, col: {col}, value: {short(value, 100)}")          
    else:
        log.debug(f"Action type unknown: {filter_.get('actionType')}")

def applyDfFilter(df, filter_):
    #log.debug("Processing df with " + filter_.get('actionType', 'unknown') + " filter '" + filter_.get('name', 'unnamed') + "'")
    if filter_.get('actionType') == 'sql':
        newDf = sqlFilter(df, filter_.get('actionConfig'))
        df = newDf
    elif filter_.get('actionType') == 'pandas':
        newDf = pandasFilter(df, filter_.get('actionConfig'))
        df = newDf

def apply_transformations(config_file, df):
    global totalRows
    config = load_config(config_file, True)
    
    # Loop over each filter in config:
    for filter_ in config.get('filters', []):
        if filter_.get('disabled', False):
            #log.debug(f"\tFilter '{filter_.get('name', 'unnamed')}' is disabled, skipping...")
            continue
        # This kind of filters loops over each record of df pandas dataframe:
        if filter_.get('actionType') == 'python' or filter_.get('actionType') == 'rest':
            with tqdm(total=df.shape[0], desc="Applying row filter '"+ filter_.get('name', 'unknown') +"' of type "+ filter_.get('actionType', 'unknown') +" ", unit="row") as pbar:
                for index, row in df.iterrows():
                    totalRows += 1
                    
                    if (config.get('sampleLines', 0)!=0 and index > config.get('sampleLines', 0)):
                        log.warn("Reached sampleLines (" + str(config.get('sampleLines', 0)) + ") limit, stopping...")
                        break

                    # Reload config every reloadConfigEvery lines
                    if (config.get('reloadConfigEvery', 0)!=0 and index % config.get('reloadConfigEvery', 0) == 0):
                        log.debug("Reloading config file " + config_file + "...")
                        config = load_config(config_file, False)

                    applyRowFilter(index, row, filter_, df)
                    pbar.update(1)

        # This kind of filters act over the whole df pandas dataframe:
        elif filter_.get('actionType') == 'sql' or filter_.get('actionType') == 'pandas':
            applyDfFilter(df, filter_)
    return df

def short(value, length):
    if len(str(value)) > length:
        return str(value)[:length] + "..."
    else:
        return str(value)

def write_output(transformed_data, config_file, output_file):
    config = load_config(config_file, False)
    #log.info("Writing df to output file " + output_file + "...:\n" + str(transformed_data.head(5)))
    if config['outDelimiter'] == 'TAB' or config['outDelimiter'] == 'tab' or config['outDelimiter'] == '\t':
        transformed_data.to_csv(output_file, sep='\t', index=True)
    else:
        transformed_data.to_csv(output_file, sep=config['outDelimiter'], index=True)

def load_config(config_file, logConfig):
    with open(config_file, 'r') as f:
        #if logConfig: log.debug("Loading config file " + config_file + "...")
        config = yaml.safe_load(f)
        #if logConfig: log.debug("Loaded config:\n" +yaml.dump(config, default_flow_style=False, sort_keys=False))
    return config    

# Function to show memory usage if interactive is True
def show_memory_usage(interactive):
    if interactive:
        tqdm.write(f"Memory usage: {psutil.Process().memory_info().rss / 1024 ** 2:.2f} MB")

def main(input_file, config_file, output_file):
    global totalRows
    config=load_config(config_file, True)
    chunkSize = config.get('chunkSize', 100000)
    log.debug("Chunk size: " + str(chunkSize))
    tmp_files = []
    with tqdm(pd.read_csv(input_file, header=0, sep=';', encoding='utf-8', chunksize=chunkSize), unit="chunk") as pbar:
        for index, chunk in enumerate(pbar):
            
            pbar.set_description(f"Processing chunk {index}")
            #log.debug("Applying transformations to chunk ("+ str(chunkSize) +") " + str(index) + ". Total rows processed: " + str(totalRows))
            transformed_data = apply_transformations(config_file, chunk)
            tmp_file_name = f"{output_file}_{index}.tmp"
            tmp_files.append(tmp_file_name)
            write_output(transformed_data, config_file, tmp_file_name)

    # Concatenar todos los archivos temporales en un DataFrame
    df_list = [pd.read_csv(tmp_file, sep='\t' if 'TAB' in tmp_file else ';', encoding='utf-8') for tmp_file in tmp_files]
    combined_df = pd.concat(df_list, ignore_index=True)

    # Escribir el DataFrame combinado en el archivo de salida final
    final_output_file_name = f"{output_file}.csv"
    combined_df.to_csv(final_output_file_name, sep=';', index=False, encoding='utf-8')

    # Eliminar archivos temporales
    for tmp_file in tmp_files:
        os.remove(tmp_file)


if __name__ == "__main__":
    if len(sys.argv) < 4:
        log.info("Uso: filefilter <fichero de entrada> <configuracion.yml> <fichero de salida>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    config_file = sys.argv[2]
    output_file = sys.argv[3]
    
    main(input_file, config_file, output_file)