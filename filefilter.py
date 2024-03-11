from tabulate import tabulate
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
import StatsManager as statsManager

statsManager = statsManager.StatsManager()

lastStatusPrint = 0
KILL = object()
tracemalloc.start()

def applyDfFilter(df, filter_):
    log.debug("Processing df with " + filter_.get('actionType', 'unknown') + " filter '" + filter_.get('name', 'unnamed') + "'")

    if filter_.get('actionType') == 'pandas':
        newDf = pandasFilter(df, filter_.get('actionConfig'))
        df = newDf
        return df

# Function to show memory usage if interactive is True
def getMemoryUsage():
    return f"Memory usage: {psutil.Process().memory_info().rss / 1024 ** 2:.2f} MB"

#  duckdb data/fullExample.txt.db "drop table if exists filter0;drop table if exists filter1; drop table if exists filter2;drop view if exists df"
#  python3 filefilter.py  examples/fullExample/fullExample.txt examples/fullExample/fullExample.yml /tmp/example.csv


def applyRowFilter(rowIndex, row_dict, filter_):
    if filter_.get('actionType') == 'python':
        # print("Filter: " + str(filter_.get('code')))
        modified_row_dict = pythonFilter(filter_.get('filterIndex'), row_dict, filter_.get('code'))
        result = {}
        if modified_row_dict is not None:
            result['row'] = modified_row_dict
            result['error'] = False
        else:
            result['row'] = row_dict
            result['error'] = True
            filter_['errors'] += filter_.get('errors', 0) + 1
        # result['row'] and result['error']
        return result

    elif filter_.get('actionType') == 'rest':
        result = restFilter(row_dict, filter_.get('actionConfig'))
        # result['row'] and result['status_code']
        if result.get('status_code')/100 == 2:
            filter_['20X'] = filter_.get('20X', 0) + 1
        if result.get('status_code')/100 == 3:
            filter_['30X'] = filter_.get('30X', 0) + 1
        if result.get('status_code')/100 == 4:
            filter_['40X'] = filter_.get('40X', 0) + 1
        if result.get('status_code')/100 == 5:
            filter_['50X'] = filter_.get('50X', 0) + 1


        return result

    else:
        log.debug(f"Action type unknown: {filter_.get('actionType')}")


def consumer(idConsumer, jobQueue, outPutQueue):
    global statsManager
    while True:
        job = jobQueue.get()
        if job is KILL:
            break  # Detener este hilo consumidor

        if job is None:
            log.info("Consumer " + idConsumer + " got None job, skipping...")
            time.sleep(1)
            continue

        # Zip columns array and row tuple
        row_dict = dict(zip(job['columns'], job['row']))
        start_time  = int(round(time.time() * 1000))
        result = {'row': None}
        try:
            result = applyRowFilter(job['rowIndex'], row_dict, job['filter'])
        except Exception as e:
            log.error("Error processing line, skipping row " + str(job['rowIndex']) + ". Row:\n" + str(row_dict))

        end_time = int(round(time.time() * 1000))
        statsManager.register(end_time - start_time)

        if result.get('row') is None:
            log.error(
                "\t\tError executing python code, skipping row " + str(job['rowIndex']) + ". Row:\n" + str(row_dict))
        else:
            #log.info("Filter errors:" + str(job['filter'].get('errors',0)) + " 20X:" + str(job['filter'].get('20X',0)) + " 30X:" + str(job['filter'].get('30X',0)) + " 40X:" + str(job['filter'].get('40X',0)) + " 50X:" + str(job['filter'].get('50X',0)))
            #log.info("Consumer " + idConsumer + " processed row")
            outPutQueue.put(result.get('row'))
    log.info("Stopped consumer " + idConsumer + "...")

def printStatus(manager, chunkIndex, totalChunks, rowIndex, rowsInChunk, totalRows, filter_, interactive=False, force=False):
    global statsManager
    filterThreads = filter_.get('filterThreads', 1)
    # sleep
    # time.sleep(0.2)
    global lastStatusPrint
    message = "Filter " + str(filter_["index"]) + " (" + filter_["name"] + "): " + " Total rows:" + str(
        totalRows) + " Chunk:" + str(chunkIndex) + "/" + str(totalChunks) + " Row:" + str(
        rowIndex) + "/" + str(rowsInChunk) + " Queue:" + str(manager.getQueueSize()) + " Workers:" + str(
        manager.getActiveConsumers()) + " avgT:"+ str(int(statsManager.avg_time(100))) + " ETA:" + str(statsManager.get_eta(rowsInChunk-rowIndex, manager.getActiveConsumers() )) + " " + getMemoryUsage()
    if (filter_.get('actionType') == 'rest'):
        message += " 20X:" + str(filter_.get('20X',0)) + " 30X:" + str(filter_.get('30X',0)) + " 40X:" + str(filter_.get('40X',0)) + " 50X:" + str(filter_.get('50X',0))
    else:
        message += " Errors:" + str(filter_.get('errors', 0))
    if interactive:
        # Print only every 100 ms
        if force or lastStatusPrint + 100 < int(round(time.time() * 1000)):
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
    delete = args.delete

    log.info("Starting filefilter...")
    log.info("Input file: " + input_file)
    log.info("Config file: " + config_file)
    log.info("Output file: " + output_file)
    log.info("Interactive mode: " + str(interactive))
    log.info("Delete previous process data: " + str(delete))

    config = load_config(config_file, True)
    lastConfigLoaded = int(round(time.time() * 1000))

    limitClause = ""
    # If we are sampling, use only one chunk
    sampleLines = config.get('sampleLines', 0)
    if sampleLines != 0:
        limitClause = " LIMIT " + str(sampleLines)

    # Init DB
    fileName = os.path.basename(input_file)
    db = Database(fileName + ".db", delete)
    # set tableName as fileName replacing . with _
    table_name = fileName.replace(".", "_")

    log.info(getMemoryUsage())
    # Load data
    startTime = int(round(time.time() * 1000))
    db.loadTable(table_name, input_file, sampleLines)
    rowsLoaded = db.getQueryResult("SELECT count(*) as rows FROM " + table_name, False)
    rowsLoaded = rowsLoaded['rows'][0]
    log.info(
        "Loaded table with " + str(rowsLoaded) + " records (sample lines " + str(sampleLines) + ") in " + str(
            int(round(time.time() * 1000)) - startTime) + "ms " + getMemoryUsage())

    filterIndex = 0
    # Last filter that generated data
    lastFilterIndex = 0
    # For each filter
    for filter_ in config.get('filters', []):
        if filter_.get('disabled', False):
            log.info("Filter " + str(filterIndex) + "(" + filter_.get('name', 'NoName') + ") is disabled, skipping...")
            filterIndex += 1
            continue

        filter_['chunkSize'] = filter_.get('chunkSize', 10000)
        chunkSize = filter_['chunkSize']

        log.info("Chunk size: " + str(chunkSize) + " records")
        totalChunks = int(rowsLoaded / chunkSize) + 1

        totalRows = 0
        # Load or Reload data
        cursor = db.getCursor()
        cursor.execute("SELECT * FROM " + table_name + limitClause)
        chunkIndex = 0
        columns = [description[0] for description in cursor.description]
        log.info("Column names: " + str(columns))

        log.info("Processing filter " + str(filterIndex) + " (" + filter_.get('name', 'NoName') + ")...")
        filter_['index'] = filterIndex

        # LINE FILTER: This kind of filters (python and rest) loops over each record of df pandas dataframe:
        if (filter_.get('actionType') == 'python' or
                filter_.get('actionType') == 'rest'):

            try:
                # Define object with fields: chunkIndex, chunkSize, columns, config, config_file, cursor, db,,filterIndex, filter_, interactive, lastConfigLoaded, limitClause, output_file, table_name, totalChunks, totalRows
                process_status = {}
                process_status['chunkIndex'] = chunkIndex
                process_status['totalChunks'] = totalChunks
                process_status['chunkSize'] = chunkSize

                process_status['totalRows'] = totalRows

                process_status['columns'] = columns
                process_status['config'] = config
                process_status['config_file'] = config_file
                process_status['cursor'] = cursor
                process_status['db'] = db
                #process_status['filterIndex'] = filterIndex
                process_status['filter_'] = filter_
                process_status['interactive'] = interactive
                process_status['lastConfigLoaded'] = lastConfigLoaded
                process_status['limitClause'] = limitClause
                process_status['output_file'] = output_file
                process_status['table_name'] = table_name



                limitClause, table_name = line_filter(chunkIndex, chunkSize, columns, config, config_file, cursor, db,
                                                  filterIndex, filter_, interactive, lastConfigLoaded, limitClause,
                                                  output_file, table_name, totalChunks, totalRows)
            except Exception as e:
                log.error("Error processing filter " + str(filterIndex) + " (" + filter_.get('name', 'NoName') + "): " + str(e))
                raise e

        # This kind of filters act over the whole df pandas dataframe:
        elif (filter_.get('actionType') == 'sql'):

            log.info("Processing df with " + filter_.get('actionType', 'unknown') + " filter '" + filter_.get('name', 'unnamed') + "'")
            db.executeQuery("CREATE OR REPLACE TABLE filter" + str(filterIndex) + " AS (" + filter_.get('code') + ")")

        elif (filter_.get('actionType') == 'pandas' or
            filter_.get('actionType') == 'udf'):
            log.info("PENDING")
        else:
            log.debug("Action type unknown: " + filter_.get('actionType'))
            # END DF FILTER

        # Create or replace view df from filter table. This view is to be used in the next filter as "df"
        db.executeQuery("CREATE OR REPLACE VIEW df AS SELECT * FROM filter" + str(filterIndex), True)
        lastFilterIndex = filterIndex

        # Show data example
        df = db.getQueryResult("SELECT * FROM df LIMIT 5", False)
        if df is not None:
            if filter_.get('showSampleOnFinish', False):
                log.info("Show example data from current data")
                log.info("\n" + tabulate(df, headers='keys', tablefmt='fancy_grid'))
            else:
                log.info(df)
        else:
            log.info("No data to show")

        filterIndex += 1
    # END FOR EACH FILTER

    # Save to output file
    log.info("Saving to output file " + output_file + "...")
    db.executeQuery("COPY (SELECT * FROM filter" + str(lastFilterIndex) + ") TO '" + output_file + "' (FORMAT CSV, DELIMITER '"+ config['outDelimiter'] +"')", True)


def line_filter(chunkIndex, chunkSize, columns, config, config_file, cursor, db, filterIndex, filter_, interactive,
                lastConfigLoaded, limitClause, output_file, table_name, totalChunks, totalRows):
    filterThreads = filter_.get('filterThreads', 1)
    log.info("Max threads: " + str(filterThreads))
    manager = ConsumerManager(queue.Queue(), filterThreads)
    rows_pending = True
    # Load chunk
    while rows_pending:
        rowChunk = cursor.fetchmany(chunkSize)
        # get number of rows of the chunk
        rowsInChunk = len(rowChunk)

        if not rowChunk:
            rows_pending = False
            break
        log.info("Loaded chunk " + str(chunkIndex) + " with " + str(len(rowChunk)) + " records. " + getMemoryUsage())

        rowIndex = 0
        # For each row in chunk
        rowIndex, totalRows = processChunk(chunkIndex, columns, config, config_file, filterThreads, filter_,
                                           interactive, lastConfigLoaded, manager, rowChunk, rowIndex,
                                           rowsInChunk, totalChunks, totalRows)

        while manager.getQueueSize() > 0:
            time.sleep(0.5)
            log.info("Waiting for consumers to finish chunk queue (" + str(manager.getQueueSize()) + ")...")

        printStatus(manager, chunkIndex, totalChunks, rowIndex, rowsInChunk, totalRows, filter_, interactive, True)

        mem_db = db.getQueryResult("PRAGMA database_size", False)
        mem_db_dict = mem_db.to_dict()
        log.info("Database memory_usage: " + str(mem_db_dict['memory_usage']) + " bytes")

        # Save chunk
        outPutChunk = manager.getOutput()
        newPd = pd.DataFrame(outPutChunk)
        exists = True
        try:
            db.getQueryResult("SELECT * FROM filter" + str(filterIndex) + " LIMIT 1", True)
        except:
            exists = False

        # show newPd
        log.info("Saving chunk data: " + str(newPd.shape))

        if exists:
            log.info("Table filter" + str(filterIndex) + " exists, inserting...")
            db.executeQuery("INSERT INTO filter" + str(filterIndex) + " SELECT * FROM newPd", True)
        else:
            log.info("Table filter" + str(filterIndex) + " does not exist, creating...")
            try:
                db.executeQuery("CREATE TABLE filter" + str(filterIndex) + " AS SELECT * FROM newPd", True)
            except Exception as e:
                log.error("Error creating table: " + str(e) + " trying to storing file and load from there...")
                newPd.to_csv(output_file, index=False)
                try:
                    db.executeQuery("CREATE TABLE filter" + str(
                        filterIndex) + " AS SELECT * FROM read_csv_auto('" + output_file + "')", True)
                except Exception as e:
                    log.error("Error creating table from file: " + str(
                        e) + " Exiting because this is an unrecoverable error...")
                    # Raise exception with text: "Error creating table from file: " + str(e) + " Exiting because this is an unrecoverable error..."
                    raise Exception("Error creating table from file: " + str(e) + " Exiting because this is an unrecoverable error...")



        # We set the table name to the new table for next filter
        table_name = "filter" + str(filterIndex)
        limitClause = ""
        chunkIndex += 1
    log.info("No more chunks")
    printStatus(manager, chunkIndex, totalChunks, 0, 0, totalRows, filter_, interactive, True)
    log.info("Stopping consumers...")
    consumersStopped = 0
    while manager.getActiveConsumers() > 0:
        manager.stop_consumer()
        consumersStopped += 1
    log.info("Stopped " + str(consumersStopped) + " consumers")
    cursor.close()
    # END LINE FILTER
    return limitClause, table_name


def processChunk(chunkIndex, columns, config, config_file, filterThreads, filter_, interactive, lastConfigLoaded,
                manager, rowChunk, rowIndex, rowsInChunk, totalChunks, totalRows):
    for row in rowChunk:
        log.debug("Processing row " + str(rowIndex) + " of chunk " + str(chunkIndex) + "...")
        if config.get('reloadConfigEverySeconds', 0) != 0 and (lastConfigLoaded + config.get(
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
            # log.info("Starting consumer " + str(manager.getActiveConsumers()+1) + " objetive: " + str(filterThreads))
            manager.start_consumer(consumer)
            time.sleep(0.10)
        elif manager.getActiveConsumers() > filterThreads:
            # log.info("Stopping consumer " + str(manager.getActiveConsumers()) + " objetive: " + str(filterThreads))
            manager.stop_consumer()

        while manager.getQueueSize() > filterThreads * 5:
            # log.info("Queue has " + str(manager.getQueueSize()) + " jobs. Waiting before loading more jobs. Consumers: " + str(manager.getActiveConsumers()))
            time.sleep(0.5)

        job = {'columns': columns, 'row': row, 'rowIndex': rowIndex, 'filter': filter_}
        # log.info("Putting job "+ str(rowIndex) +" " + str(job) + " in queue...")
        manager.putJob(job)

        rowIndex += 1
        totalRows += 1
        printStatus(manager, chunkIndex, totalChunks, rowIndex, rowsInChunk, totalRows, filter_, interactive, False)
    return rowIndex, totalRows


if __name__ == "__main__":
    format = "%(asctime)s %(filename)s:%(lineno)d - %(message)s "
    log.basicConfig(format=format, level=log.INFO, datefmt="%H:%M:%S")

    # Crear el parser
    parser = argparse.ArgumentParser(
        description="Process a file using preconfigured filters")

    parser.add_argument("input_file", help="Input file")
    parser.add_argument("config_file", help="Configuration file in YAML format")
    parser.add_argument("output_file", help="Output file")
    parser.add_argument("-i", "--interactive", action="store_true", help="Interactive mode")
    parser.add_argument("-d", "--delete", action="store_true", help="Previous process data")
    args = parser.parse_args()

    main()
