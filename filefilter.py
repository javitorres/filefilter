import pandas as pd
import sys
from filters import *
import os
from tqdm import tqdm
import psutil
import asyncio
from concurrent.futures import ThreadPoolExecutor
from utils import load_config
from utils import short
from Database import Database
import time
import logging as log
import queue
from ConsumerManager import ConsumerManager

global rowsProcessed
rowsProcessed = 0

global sampleLinesReached
sampleLinesReached = False

KILL = object()

def applyRowFilter(rowIndex, row, filter_, df):
    #log.debug("\t\tProcessing row " + str(index) + " with " + filter_.get('actionType', 'unknown') + " filter '" + filter_.get('name', 'unnamed') + "'")
    #log.debug("\t\t\tRow: " + str(row))
    if filter_.get('actionType') == 'python':
        #print("Filter: " + str(filter_.get('code')))
        modified_row_dict = pythonFilter(filter_.get('filterIndex'), row, filter_.get('code'))
        if modified_row_dict is None:
            log.error("\t\tError executing python code, skipping row " + str(rowIndex) + ". Row:\n" + str(row) + "\nCode:\n" + str(filter_.get('code')))
        else: 
            for col, value in modified_row_dict.items():
                try:
                    df.at[rowIndex, col] = value
                except Exception as e:
                    log.debug(f"\t\tError applying change: {e}")
    elif filter_.get('actionType') == 'rest':
        modified_row_dict = restFilter(row, filter_.get('actionConfig'))
        if 'response' not in df.columns:
            df['response'] = None
        # Aplicar los cambios al DataFrame
        if modified_row_dict is None:
            log.error("\t\tError executing rest filter, skipping row " + str(rowIndex) + ". Row:" + str(row))
        else:
            for col, value in modified_row_dict.items():
                try:
                    df.at[rowIndex, col] = value
                except Exception as e:
                    log.debug(f"\t\tError appliying change: {e}. index: {rowIndex}, col: {col}, value: {short(value, 100)}")
    else:
        log.debug(f"Action type unknown: {filter_.get('actionType')}")

def applyRowFilter2(rowIndex, row, filter_):
    #log.debug("\t\tProcessing row " + str(index) + " with " + filter_.get('actionType', 'unknown') + " filter '" + filter_.get('name', 'unnamed') + "'")
    #log.debug("\t\t\tRow: " + str(row))
    if filter_.get('actionType') == 'python':
        #print("Filter: " + str(filter_.get('code')))
        modified_row_dict = pythonFilter(filter_.get('filterIndex'), row, filter_.get('code'))
        if modified_row_dict is None:
            log.error("\t\tError executing python code, skipping row " + str(rowIndex) + ". Row:\n" + str(row) + "\nCode:\n" + str(filter_.get('code')))
        else:
            return modified_row_dict

    elif filter_.get('actionType') == 'rest':
        modified_row_dict = restFilter(row, filter_.get('actionConfig'))
        if 'response' not in row.columns:
            row['response'] = None
        if modified_row_dict is None:
            log.error("\t\tError executing rest filter, skipping row " + str(rowIndex) + ". Row:" + str(row))
        else:
            return modified_row_dict

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

def apply_filters_to_chunk(config_file, df, chunkIndex, use_tqdm=False):
    global rowsProcessed
    global sampleLinesReached
    config = load_config(config_file, True)

    filterIndex = 0
    # Loop over each filter in config:
    for filter_ in config.get('filters', []):
        # set filterIndex to the index of the filter in the list
        filter_['index'] = filterIndex
        if filter_.get('disabled', False):
            # log.debug(f"\tFilter '{filter_.get('name', 'unnamed')}' is disabled, skipping...")
            continue

        # This kind of filters (python and rest) loops over each record of df pandas dataframe:
        if filter_.get('actionType') == 'python' or filter_.get('actionType') == 'rest':
            with ThreadPoolExecutor(max_workers=filter_.get('filterThreads', 1)) as executor:
                log.info("Max threads: " + str(filter_.get('filterThreads', 1)))
                loop = asyncio.get_event_loop()
                tasks = []
            
                if use_tqdm:
                    pbar = tqdm(total=df.shape[0], desc="Chunk " + str(chunkIndex) + " Applying row filter '"+ filter_.get('name', 'unknown') +"' of type "+ filter_.get('actionType', 'unknown') +" ", unit="row")
                else:
                    pbar = None

                for rowIndex, row in df.iterrows():
                    rowsProcessed += 1

                    if (config.get('sampleLines', 0)!=0 and rowsProcessed > config.get('sampleLines', 0)):
                        log.warn("Reached sampleLines (" + str(config.get('sampleLines', 0)) + ") limit, stopping...")
                        break

                    if (config.get('reloadConfigEvery', 0)!=0 and (rowIndex % config.get('reloadConfigEvery', 0) == 0)):
                        #log.debug("Reloading config file " + config_file + "(index="+ str(index)+")...")
                        config = load_config(config_file, False)

                    #applyRowFilter(index, row, filter_, df)
                    #print ("Executor: " + str(executor))
                    task = loop.run_in_executor(executor, applyRowFilter, rowIndex, row, filter_, df)
                    tasks.append(task)
                    if use_tqdm:
                        pbar.update(1)

                if use_tqdm:
                    pbar.close()
                
                asyncio.gather(*tasks)

        # This kind of filters act over the whole df pandas dataframe:
        elif filter_.get('actionType') == 'sql' or filter_.get('actionType') == 'pandas':
            applyDfFilter(df, filter_)

        log.info("DF status: " + str(df.shape))
        if(config.get('saveIntermediateTo', "") != ""):
            df.to_csv(config['saveIntermediateTo'] + "_"+ str(filterIndex) +".csv", sep=';', index=True)
        filterIndex += 1
    return df


def write_output(transformed_data, config_file, output_file):
    config = load_config(config_file, False)
    #log.info("Writing df to output file " + output_file + "...:\n" + str(transformed_data.head(5)))
    if config['outDelimiter'] == 'TAB' or config['outDelimiter'] == 'tab' or config['outDelimiter'] == '\t':
        transformed_data.to_csv(output_file, sep='\t', index=True)
    else:
        transformed_data.to_csv(output_file, sep=config['outDelimiter'], index=True)


# Function to show memory usage if interactive is True

def getMemoryUsage():
    return f"Memory usage: {psutil.Process().memory_info().rss / 1024 ** 2:.2f} MB"

def main(input_file, config_file, output_file, use_tqdm=False):
    global rowsProcessed
    config = load_config(config_file, True)
    chunkSize = config.get('chunkSize', 100000)
    # if sampleLines is set, use only une chunk
    if config.get('sampleLines', 0) != 0:
        ## Set to max int
        chunkSize = 999999999 
    log.debug("Chunk size: " + str(chunkSize))
    tmp_files = []
    # Definir una función que procesa los chunks
    def process_chunks(chunks):
        for chunkIndex, chunk in enumerate(chunks):
            if use_tqdm:
                chunks.set_description(f"Processing chunk {chunkIndex}")
            # log.debug("Applying transformations to chunk (" + str(chunkSize) + ") " + str(index) + ". Total rows processed: " + str(totalRows))
            transformed_data = apply_filters_to_chunk(config_file, chunk, chunkIndex, use_tqdm )
            tmp_file_name = f"{output_file}_{chunkIndex}.tmp"
            tmp_files.append(tmp_file_name)
            write_output(transformed_data, config_file, tmp_file_name)

    # Leer el archivo de entrada y procesar los chunks con o sin tqdm
    if use_tqdm:
        with tqdm(pd.read_csv(input_file, header=0, sep=config.get('inDelimiter', ','), encoding='utf-8', chunksize=chunkSize), unit="chunk") as pbar_chunks:
            process_chunks(pbar_chunks)
    else:
        chunks = pd.read_csv(input_file, header=0, sep=config.get('inDelimiter', ','), encoding='utf-8', chunksize=chunkSize)
        process_chunks(chunks)
    
    # Concatenar todos los archivos temporales en un DataFrame
    df_list = [pd.read_csv(tmp_file, sep='\t' if 'TAB' in tmp_file else ';', encoding='utf-8') for tmp_file in tmp_files]
    combined_df = pd.concat(df_list, ignore_index=True)

    # Escribir el DataFrame combinado en el archivo de salida final
    final_output_file_name = f"{output_file}"
    combined_df.to_csv(final_output_file_name, sep=config.get('outDelimiter', ','), index=False, encoding='utf-8')

    # Eliminar archivos temporales
    for tmp_file in tmp_files:
        os.remove(tmp_file)

def consumer(idConsumer, jobQueue):
    while True:
        job = jobQueue.get()
        if job is KILL:
            log.info("Stopping consumer " + idConsumer + "...")
            break  # Detener este hilo consumidor

        if job is None:
            log.info("Consumer " + idConsumer + " got None job, skipping...")
            time.sleep(1)
            continue

        log.info("Row type: " + str(type(job['row'])))
        log.info("Consumer " + idConsumer + " processing row: " + str(job['row']) + " with filter: " + str(job['filter']) + "...")
        # print row type

        row = job['row']
        filter_ = job['filter']
        rowIndex = job['rowIndex']
        modified_row_dict = applyRowFilter2(rowIndex, row, filter_)
        if modified_row_dict is None:
            log.error("\t\tError executing python code, skipping row " + str(rowIndex) + ". Row:\n" + str(row) + "\nCode:\n" + str(filter_.get('code')))
        else:
            log.info("Consumer " + idConsumer + " processed row:" + str(modified_row_dict))
        time.sleep(5)
def main2(input_file, config_file, output_file, use_tqdm=False):
    config = load_config(config_file, True)
    chunkSize = config.get('chunkSize', 100000)
    if config.get('sampleLines', 0) != 0:
        chunkSize = 999999999

    # Init DB
    fileName = os.path.basename(input_file)
    db = Database(fileName + ".db")
    # set tableName as fileName replacing . with _
    table_name = fileName.replace(".", "_")

    log.info(getMemoryUsage())
    # Load data
    startTime = int(round(time.time() * 1000))
    db.loadTable(table_name, input_file)
    rowsLoaded = db.runQuery("SELECT count(*) as rows FROM " + table_name, False)
    log.info("Loaded table with " + str(rowsLoaded['rows'][0]) + " records in " + str(int(round(time.time() * 1000)) - startTime) + " ms " + str(getMemoryUsage()) )

    cursor = db.getCursor()
    cursor.execute("SELECT * FROM " + table_name)
    chunkIndex = 0
    jobQueue = queue.Queue()
    manager = ConsumerManager(jobQueue)

    filterIndex = 0
    # For each filter
    for filter_ in config.get('filters', []):
        filter_['index'] = filterIndex
        if filter_.get('disabled', False):
            continue

        # This kind of filters (python and rest) loops over each record of df pandas dataframe:
        if (filter_.get('actionType') == 'python' or
                filter_.get('actionType') == 'rest'):

            rows_pending = True
            while rows_pending:
                rowChunk = cursor.fetchmany(chunkSize)
                log.info("Loaded chunk " + str(chunkIndex) + "  " + str(getMemoryUsage()))

                if not rowChunk:
                    rows_pending = False
                    break

                filterThreads = filter_.get('filterThreads', 1)
                log.info("Max threads: " + str(filterThreads))
                while (manager.getActiveConsumers() < filterThreads):
                    log.info("Starting consumer...")
                    manager.start_consumer(consumer)


                rowIndex = 0
                for row in rowChunk:
                    while (jobQueue.qsize() > filterThreads * 5):
                        log.info("Waiting for queue to be consumed...")
                        time.sleep(0.5)

                    job = {}
                    job['row'] = row
                    job['rowIndex'] = rowIndex
                    job['filter'] = filter_
                    jobQueue.put(job)

                    log.info("Queue size: %s",str(jobQueue.qsize()) + " Consumers: " + str(manager.getActiveConsumers()))
                    time.sleep(0.5)
                    rowIndex += 1

                chunkIndex += 1
            log.info("No more rows")
            log.info("Queue size: %s",str(jobQueue.qsize()) + " Consumers: " + str(manager.getActiveConsumers()))
                # This kind of filters act over the whole df pandas dataframe:
        elif (filter_.get('actionType') == 'sql' or
              filter_.get('actionType') == 'pandas' or
              filter_.get('actionType') == 'udf'):
            # TODO
            pass



if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    # Format with line:
    format = "%(asctime)s: %(message)s (%(filename)s:%(lineno)d)"
    log.basicConfig(format=format, level=log.INFO, datefmt="%H:%M:%S")

    if len(sys.argv) < 4:
        log.info("Usage: filefilter <data input file> <configuration.yml> <output file> [use_tqdm]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    config_file = sys.argv[2]
    output_file = sys.argv[3]
    use_tqdm = sys.argv[4] if len(sys.argv) > 4 else False
    
    
    #main(input_file, config_file, output_file, use_tqdm)
    main2(input_file, config_file, output_file, use_tqdm)