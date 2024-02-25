import argparse
from filters import *
import os
import psutil
from utils import load_config
from Database import Database
import time
import logging as log
import queue
from ConsumerManager import ConsumerManager
import pandas as pd
import tracemalloc
import gc

lastStatusPrint = 0
KILL = object()

tracemalloc.start()

def applyDfFilter(df, filter_):
    # log.debug("Processing df with " + filter_.get('actionType', 'unknown') + " filter '" + filter_.get('name', 'unnamed') + "'")
    if filter_.get('actionType') == 'sql':
        newDf = sqlFilter(df, filter_.get('actionConfig'))
        df = newDf
    elif filter_.get('actionType') == 'pandas':
        newDf = pandasFilter(df, filter_.get('actionConfig'))
        df = newDf

# Function to show memory usage if interactive is True
def getMemoryUsage():
    return f"Memory usage: {psutil.Process().memory_info().rss / 1024 ** 2:.2f} MB"


def applyRowFilter(rowIndex, row_dict, filter_):
    log.info("actionType: " + filter_.get('actionType', 'unknown') + " filter '" + filter_.get('name', 'unnamed') + "'")
    if filter_.get('actionType') == 'python':
        # print("Filter: " + str(filter_.get('code')))
        modified_row_dict = pythonFilter(filter_.get('filterIndex'), row_dict, filter_.get('code'))
        if modified_row_dict is not None:
            return modified_row_dict

    elif filter_.get('actionType') == 'rest':
        log.info("Processing row " + str(rowIndex) + " with " + filter_.get('actionType', 'unknown') + " filter '" + filter_.get('name', 'unnamed') + "'")
        modified_row_dict = restFilter(row_dict, filter_.get('actionConfig'))
        #if 'response' not in row_dict.columns:
        #    row_dict['response'] = None
        if modified_row_dict is None:
            log.error("\t\tError executing rest filter, skipping row " + str(rowIndex) + ". Row:" + str(row_dict))
        else:
            return modified_row_dict

    else:
        log.debug(f"Action type unknown: {filter_.get('actionType')}")


def consumer(idConsumer, jobQueue, outPutQueue):
    while True:
        job = jobQueue.get()
        if job is KILL:
            log.info("Stopping consumer " + idConsumer + "...")
            break  # Detener este hilo consumidor

        if job is None:
            log.info("Consumer " + idConsumer + " got None job, skipping...")
            time.sleep(1)
            continue

        # Zip columns array and row tuple
        row_dict = dict(zip(job['columns'], job['row']))
        modified_row_dict = applyRowFilter(job['rowIndex'], row_dict, job['filter'])
        if modified_row_dict is None:
            log.error(
                "\t\tError executing python code, skipping row " + str(job['rowIndex']) + ". Row:\n" + str(row_dict))
        else:
            # log.info("Consumer " + idConsumer + " processed row:" + str(modified_row_dict))
            outPutQueue.put(modified_row_dict)


def printStatus(manager, chunkIndex, rowIndex, totalRows, filter_, interactive=False):
    filterThreads = filter_.get('filterThreads', 1)
    # sleep
    # time.sleep(0.2)
    global lastStatusPrint
    message = "Filter " + str(filter_["index"]) + " (" + filter_["name"] + "): " + " Processed rows: " + str(
        totalRows) + " Chunk: " + str(chunkIndex) + " Last row index: " + str(
        rowIndex) + " Queue size:" + str(manager.getQueueSize()) + " Consumers: " + str(
        manager.getActiveConsumers()) + " Threads: " + str(filterThreads) + " " + getMemoryUsage()
    if interactive:
        # Print only every 100 ms
        if lastStatusPrint + 100 < int(round(time.time() * 1000)):
            # Delete console line
            print(
                "\033[A                                                                                                                    \033[A")
            print(message)
            lastStatusPrint = int(round(time.time() * 1000))
    else:
        log.info(message)

''' Reset the number of threads for each filter '''
def setNewThreads(config, newConfig):
    changed = False
    for newFilterConfig in newConfig.get('filters', []):
        for filter in config.get('filters', []):
            if newFilterConfig['name'] == filter['name']:
                if newFilterConfig['filterThreads'] != filter['filterThreads']:
                    log.info("Changing threads for filter " + newFilterConfig['name'] + ": " + str(filter['filterThreads']) + " -> " + str(newFilterConfig['filterThreads']))
                    filter['filterThreads'] = newFilterConfig['filterThreads']
                    changed = True
    return changed

def main():
    input_file = args.input_file
    config_file = args.config_file
    output_file = args.output_file
    interactive = args.interactive

    log.info("Starting filefilter...interactive:" + str(interactive))
    config = load_config(config_file, True)
    lastConfigLoaded = int(round(time.time() * 1000))
    chunkSize = config.get('chunkSize', 100000)
    limitClause = ""
    # If we are sampling, use only one chunk
    sampleLines = config.get('sampleLines', 0)
    if sampleLines != 0:
        chunkSize = 999999999
        limitClause = " LIMIT " + str(sampleLines)

    # Init DB
    fileName = os.path.basename(input_file)
    db = Database(fileName + ".db")
    # set tableName as fileName replacing . with _
    table_name = fileName.replace(".", "_")

    log.info(getMemoryUsage())
    # Load data
    startTime = int(round(time.time() * 1000))
    db.loadTable(table_name, input_file, sampleLines)
    rowsLoaded = db.runQuery("SELECT count(*) as rows FROM " + table_name, False)
    log.info(
        "Loaded table with " + str(rowsLoaded['rows'][0]) + " records (sample lines " + str(sampleLines) + ") in " + str(
            int(round(time.time() * 1000)) - startTime) + "ms " + str(getMemoryUsage()))

    filterIndex = 0
    # For each filter
    for filter_ in config.get('filters', []):
        if filter_.get('disabled', False):
            log.info("Filter " + str(filterIndex) + "(" + filter_.get('name', 'NoName') + ") is disabled, skipping...")
            continue

        totalRows = 0
        # Load or Reload data
        cursor = db.getCursor()
        cursor.execute("SELECT * FROM " + table_name + limitClause)
        chunkIndex = 0
        columns = [description[0] for description in cursor.description]
        log.info("Column names: " + str(columns))

        log.info("Processing filter " + str(filterIndex) + " (" + filter_.get('name', 'NoName') + ")...")
        filterThreads = filter_.get('filterThreads', 1)
        log.info("Max threads: " + str(filterThreads))
        manager = ConsumerManager(queue.Queue(), filterThreads)
        filter_['index'] = filterIndex


        # LINE FILTER: This kind of filters (python and rest) loops over each record of df pandas dataframe:
        if (filter_.get('actionType') == 'python' or
                filter_.get('actionType') == 'rest'):

            rows_pending = True
            # Load chunk
            while rows_pending:
                rowChunk = cursor.fetchmany(chunkSize)
                log.info("Loaded chunk " + str(chunkIndex) + "  " + str(getMemoryUsage()))

                if not rowChunk:
                    rows_pending = False
                    break

                rowIndex = 0
                # For each row in chunk
                for row in rowChunk:
                    if config.get('reloadConfigEverySeconds') != 0 and (lastConfigLoaded + config.get(
                            'reloadConfigEverySeconds') * 1000 < int(round(time.time() * 1000))):
                        log.debug("Reloading config file " + config_file + "...")
                        newConfig = load_config(config_file, False)
                        changed = setNewThreads(config, newConfig)
                        if changed:
                            filterThreads = filter_.get('filterThreads', 1)
                            manager.setMaxConsumers(filterThreads)
                        filterThreads = filter_.get('filterThreads', 1)
                        lastConfigLoaded = int(round(time.time() * 1000))

                    if manager.getActiveConsumers() < filterThreads:
                        log.info("Starting consumer " + str(manager.getActiveConsumers()+1) + " objetive: " + str(
                            filterThreads))
                        manager.start_consumer(consumer)
                        time.sleep(0.10)
                    elif manager.getActiveConsumers() > filterThreads:
                        log.info("Stopping consumer " + str(manager.getActiveConsumers()) + " objetive: " + str(
                            filterThreads))
                        manager.stop_consumer()

                    while manager.getQueueSize() > filterThreads * 5:
                        # log.info("Queue has " + str(manager.getQueueSize()) + " jobs. Waiting before loading more jobs. Consumers: " + str(manager.getActiveConsumers()))
                        time.sleep(0.5)

                    job = {'columns': columns, 'row': row, 'rowIndex': rowIndex, 'filter': filter_}
                    manager.putJob(job)

                    rowIndex += 1
                    totalRows += 1
                    printStatus(manager, chunkIndex, rowIndex, totalRows, filter_, interactive)

                mem_db = db.runQuery("PRAGMA database_size", False)
                log.info("Database size: " + str(mem_db) + " bytes")

                # Save chunk
                outPutChunk = manager.getOutput()
                newPd = pd.DataFrame(outPutChunk)
                exists = True
                try:
                    db.runQuery("SELECT * FROM filter" + str(filterIndex) + " LIMIT 1", True)
                except:
                    exists = False

                if exists:
                    log.info("Table filter" + str(filterIndex) + " exists, inserting...")
                    db.runQuery("INSERT INTO filter" + str(filterIndex) + " SELECT * FROM newPd", True)
                else:
                    log.info("Table filter" + str(filterIndex) + " does not exist, creating...")
                    db.runQuery("CREATE TABLE filter" + str(filterIndex) + " AS SELECT * FROM newPd", True)
                # We set the table name to the new table for next filter
                table_name = "filter" + str(filterIndex)
                limitClause = ""

                # https://docs.python.org/es/3/library/tracemalloc.html
                snapshot = tracemalloc.take_snapshot()
                top_stats = snapshot.statistics('lineno')

                print("[ Top 10 ]")
                for stat in top_stats[:10]:
                    print(stat)
                chunkIndex += 1

            log.info("No more rows")
            printStatus(manager, chunkIndex, 0, totalRows, filter_, interactive)
            log.info("Stopping consumers...")
            consumersStopped = 0
            while manager.getActiveConsumers() > 0:
                manager.stop_consumer()
                consumersStopped += 1
            log.info("Stopped " + str(consumersStopped) + " consumers")
            cursor.close()
            # END LINE FILTER

        # This kind of filters act over the whole df pandas dataframe:
        elif (filter_.get('actionType') == 'sql' or
              filter_.get('actionType') == 'pandas' or
              filter_.get('actionType') == 'udf'):
            # TODO
            pass
            # END DF FILTER

        filterIndex += 1


    # END FOR EACH FILTER

    # Save to output file
    log.info("Saving to output file " + output_file + "...")
    df = db.runQuery("COPY (SELECT * FROM filter" + str(filterIndex-1) + ") TO '" + output_file + "' (FORMAT CSV, DELIMITER '"+ config['outDelimiter'] +"')", True)




if __name__ == "__main__":
    format = "%(asctime)s %(filename)s:%(lineno)d - %(message)s "
    log.basicConfig(format=format, level=log.INFO, datefmt="%H:%M:%S")

    # Crear el parser
    parser = argparse.ArgumentParser(
        description="Procesa un archivo de datos usando filtros definidos en un archivo de configuración.")

    parser.add_argument("input_file", help="Archivo de entrada de datos.")
    parser.add_argument("config_file", help="Archivo de configuración en formato YAML.")
    parser.add_argument("output_file", help="Archivo de salida donde se escribirán los datos procesados.")
    parser.add_argument("-i", "--interactive", type=bool, default=False, help="Modo interactivo (True o False).")
    args = parser.parse_args()

    main()
