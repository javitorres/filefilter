# FileFilter: A tool for filtering and transforming data files using Python, SQL, and REST APIs.

import gc
import typer
from tabulate import tabulate
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
from rich.console import Console
from rich.logging import RichHandler
import sys
from constants import KILL

console = Console()
statsManager = statsManager.StatsManager()

lastStatusPrint = 0
tracemalloc.start()

manager: ConsumerManager = None
#######################################################################
def getMemoryUsage():
    """Show current memory usage."""
    return f"Memory usage: {psutil.Process().memory_info().rss / 1024 ** 2:.2f} MB"
#######################################################################
def applyRowFilter(rowIndex, row_dict, filter_):
    """Apply a row-level filter (e.g. python code execution, REST call)."""
    actionType = filter_.get('actionType')
    if actionType == 'python':
        try:
            modified_row_dict = pythonFilter(filter_.get('filterIndex'), row_dict, filter_.get('code'))
        except Exception as e:
            log.error(f"Error executing python filter code: {e}")
            raise e

        result = {}
        if modified_row_dict is not None:
            result['row'] = modified_row_dict
            result['error'] = False
        else:
            result['row'] = row_dict
            result['error'] = True
            filter_['errors'] = filter_.get('errors', 0) + 1
        return result

    elif actionType == 'rest':
        result = restFilter(row_dict, filter_.get('actionConfig'))
        if result is None:
            log.error("restFilter returned None. Using default status_code 500.")
            result = {'status_code': 500}
        status_code = result.get('status_code', 0)
        if status_code // 100 == 2:
            filter_['20X'] = filter_.get('20X', 0) + 1
        elif status_code // 100 == 3:
            filter_['30X'] = filter_.get('30X', 0) + 1
        elif status_code // 100 == 4:
            filter_['40X'] = filter_.get('40X', 0) + 1
        elif status_code // 100 == 5:
            filter_['50X'] = filter_.get('50X', 0) + 1
        return result

    else:
        log.debug(f"Action type unknown: {actionType}")
        return {'row': row_dict}
#######################################################################
# This function is executed by each consumer thread.
# It will be called by the ThreadPoolExecutor.
# It will receive the job from the job queue and process it.
# The result will be put in the output queue.
##########################################################################
def consumerFunction(idConsumer, jobQueue, outPutQueue):
    global statsManager
    global manager
    while True:
        # Wait for a job to be available. ATTENTION: BLOCKING!
        log.debug(f"Consumer_{idConsumer} waiting (AND BLOCKED) for job...")
        job = manager.getJob()

        if job is KILL:
            log.debug(f"Consumer_{idConsumer} received KILL signal")
            manager.consumer_finish_signal(idConsumer, "KILLED")
            break  # Stop this consumer thread

        log.debug(f"Consumer_{idConsumer} received job {job['rowIndex']}")

        row_dict = dict(zip(job['columns'], job['row']))
        start_time = int(round(time.time() * 1000))
        try:
            result = applyRowFilter(job['rowIndex'], row_dict, job['filter'])
        except Exception as e:
            log.error(f"consumerFunction -> Error processing line: {e}")
            manager.consumer_finish_signal(idConsumer, "EXCEPTION")
            break

        end_time = int(round(time.time() * 1000))
        statsManager.register(end_time - start_time)

        if result.get('row') is None:
            log.error(f"\t\tNew row is None, skipping row {job['rowIndex']}. Row:\n{row_dict}")
        else:
            outPutQueue.put(result.get('row'))
            log.debug(f"Consumer_{idConsumer} finished job {job['rowIndex']}!")

#######################################################################
def printStatus(text, chunkIndex, totalChunks, rowIndex, rowsInChunk, totalRows, filter_, interactive=False, force=False):
    global statsManager
    global lastStatusPrint

    message = (
        f"{text} "
        f"Filter {filter_['index']} ({filter_['name']}): "
        f"Total rows:{totalRows} Chunk:{chunkIndex}/{totalChunks} Row:{rowIndex}/{rowsInChunk} "
        f"Queue:{manager.getQueueSize()} Workers:{manager.getActiveConsumers()} "
        f"Worker stats: {manager.consumer_stats} "
        f"avgT:{int(statsManager.avg_time(100))} "
        f"ETA:{statsManager.get_eta(rowsInChunk - rowIndex, manager.getActiveConsumers())} "
        f"{getMemoryUsage()}"
    )

    if filter_.get('actionType') == 'rest':
        message += (f" 20X:{filter_.get('20X',0)} 30X:{filter_.get('30X',0)} "
                    f"40X:{filter_.get('40X',0)} 50X:{filter_.get('50X',0)}")
    else:
        message += f" Errors:{filter_.get('errors', 0)}"

    if interactive:
        # Print only every 100 ms
        if force or (lastStatusPrint + 100 < int(round(time.time() * 1000))):
            console.print(message, end="\r")
            lastStatusPrint = int(round(time.time() * 1000))
    else:
        log.info(message)
#######################################################################
def setNewThreads(config, newConfig):
    """Reset the number of threads for each filter if changed in the config file."""
    changed = False
    for newFilterConfig in newConfig.get('filters', []):
        for filter_ in config.get('filters', []):
            if newFilterConfig['name'] == filter_['name']:
                if newFilterConfig['filterThreads'] != filter_['filterThreads']:
                    log.info(f"Changing threads for filter {newFilterConfig['name']}: "
                             f"{filter_['filterThreads']} -> {newFilterConfig['filterThreads']}")
                    filter_['filterThreads'] = newFilterConfig['filterThreads']
                    changed = True
    return changed
#######################################################################
def processChunk(chunkIndex, columns, config, config_file, filterThreads, filter_, interactive, lastConfigLoaded,
                 rowChunk, rowIndex, rowsInChunk, totalChunks, totalRows):
    """Process a single chunk of rows for a line-based filter."""
    global manager
    #log.debug(f"Entrando a processChunk con {len(rowChunk)} filas")

    for row in rowChunk:
        # Reload config if needed
        if config.get('reloadConfigEverySeconds', 0) != 0 and (
            lastConfigLoaded + config.get('reloadConfigEverySeconds') * 1000 < int(round(time.time() * 1000))
        ):
            log.debug("Reloading config file " + config_file + "...")
            newConfig = load_config(config_file, False)
            changed = setNewThreads(config, newConfig)
            if changed:
                filterThreads = filter_.get('filterThreads', 1)
                manager.setMaxConsumers(filterThreads)
            lastConfigLoaded = int(round(time.time() * 1000))

        # Adjust consumers if needed
        if manager.getActiveConsumers() < filterThreads:
            try:
                manager.start_consumer(consumerFunction)
            except Exception as e:
                log.error(f"Error starting consumer: {e}")
                raise e
            log.debug(f"Launched consumer: {manager.getActiveConsumers()} active")
            time.sleep(0.10)
        elif manager.getActiveConsumers() > filterThreads:
            manager.send_kill_signal_to_consumer()

        # Prevent queue from getting too large
        while manager.getQueueSize() > filterThreads * 5:
            time.sleep(0.5)

        job = {'columns': columns, 'row': row, 'rowIndex': rowIndex, 'filter': filter_}
        # Add job to queue
        manager.putJob(job)

        rowIndex += 1
        totalRows += 1
        printStatus("chunk", chunkIndex, totalChunks, rowIndex, rowsInChunk, totalRows, filter_, interactive, False)

    # Free memory after processing chunk
    del rowChunk
    gc.collect()
    return rowIndex, totalRows
#######################################################################
def line_filter(chunkIndex, chunkSize, columns, config, config_file, cursor, db, filterIndex, filter_, interactive,
                lastConfigLoaded, limitClause, output_file, table_name, totalChunks, totalRows):

    """Process a line-based filter (python/rest) that requires iteration over each row."""
    filterThreads = filter_.get('filterThreads', 1)
    log.debug("Max threads: " + str(filterThreads))
    global manager
    manager = ConsumerManager(queue.Queue(), filterThreads)

    rows_pending = True
    while rows_pending:
        rowChunk = cursor.fetchmany(chunkSize)
        rowsInChunk = len(rowChunk)

        if not rowChunk:
            rows_pending = False
            break

        log.debug(f"Loaded chunk {chunkIndex} with {rowsInChunk} records. {getMemoryUsage()}")
        rowIndex = 0
        rowIndex, totalRows = processChunk(
            chunkIndex, columns, config, config_file, filterThreads, filter_,
            interactive, lastConfigLoaded, rowChunk, rowIndex,
            rowsInChunk, totalChunks, totalRows
        )

        # Wait for consumers to finish processing
        log.debug("Waiting for all consumers to finish. Stop on error count: " + str(config.get('stopOnErrorCount', -1)))
        try:
            manager.wait_until_all_consumers_idle(config.get('stopOnErrorCount', -1))
        except Exception as e:
            log.error(f"Critical errors occurred in consumers: {e}")
            raise e

        log.debug("All consumers finished.")

        # Force status print at the end of the chunk
        printStatus("Consumers finished", chunkIndex, totalChunks, rowIndex, rowsInChunk, totalRows, filter_, interactive, True)

        mem_db = db.getQueryResult("PRAGMA database_size", False)
        mem_db_dict = mem_db.to_dict()
        #log.info(f"Database memory_usage: {mem_db_dict['memory_usage']} bytes")

        # Save results for this chunk
        outPutChunk = manager.getOutput()
        newPd = pd.DataFrame(outPutChunk)
        exists = db.checkIfTableExists("filter" + str(filterIndex))

        log.debug(f"Saving chunk data: {newPd.shape}")
        db.register('newPd', newPd)

        if exists:
            # Si la tabla existe, insertamos
            db.executeQuery("INSERT INTO filter" + str(filterIndex) + " SELECT * FROM newPd", True)
        else:
            # Si no existe, la creamos
            log.debug(f"Table filter{filterIndex} does not exist, creating...")
            try:
                db.executeQuery(f"CREATE TABLE filter{filterIndex} AS (SELECT * FROM newPd)", True)
                #log.info(f"Table filter{filterIndex} created")
            except Exception as e:
                log.error(f"Error creating table: {e} trying to store file and load from there...")
                newPd.to_csv(output_file, index=False)
                try:
                    db.executeQuery(f"CREATE TABLE filter{filterIndex} AS SELECT * FROM read_csv_auto('{output_file}')", True)
                except Exception as e2:
                    log.error("Error creating table from file: " + str(e2) +
                              " Exiting because this is an unrecoverable error...")
                    raise Exception("Error creating table from file: " + str(e2) +
                                    " Exiting because this is an unrecoverable error...")

        table_name = "filter" + str(filterIndex)
        limitClause = ""
        chunkIndex += 1

    #log.info("No more chunks")
    printStatus("No more chunks", chunkIndex, totalChunks, 0, 0, totalRows, filter_, interactive, True)
    log.debug("Stopping consumers...")

    consumersStopped = 0
    while manager.getActiveConsumers() > 0:
        log.debug(f"Stopping consumer {consumersStopped + 1} of {manager.getActiveConsumers()}")
        manager.send_kill_signal_to_consumer()
        consumersStopped += 1
    log.debug(f"Stopped {consumersStopped} consumers")

    cursor.close()
    return limitClause, table_name, totalRows
#######################################################################
def mainProcess(input_file: str, config_file: str, output_file: str, interactive: bool = False, delete: bool = False):
    console.log("[bold green]Starting...[/bold green]")
    config = load_config(config_file, True)
    lastConfigLoaded = int(round(time.time() * 1000))

    sampleLines = config.get('sampleLines', 0)
    limitClause = f" LIMIT {sampleLines}" if sampleLines != 0 else ""

    fileName = os.path.basename(input_file)
    db = Database(fileName + ".db", delete)
    table_name = fileName.replace(".", "_")
    inDelimiter = config.get('inDelimiter', None)

    log.info(getMemoryUsage())
    startTime = int(round(time.time() * 1000))
    db.loadTable(table_name, input_file, sampleLines, inDelimiter)
    db.executeQuery(f"CREATE OR REPLACE VIEW df AS SELECT * FROM {table_name}", True)

    rowsLoaded = db.getQueryResult("SELECT count(*) as rows FROM df", True)
    rowsLoaded = rowsLoaded['rows'][0]
    log.info(
        f"Loaded table with {rowsLoaded} records (sample lines {sampleLines}) in "
        f"{int(round(time.time() * 1000)) - startTime}ms {getMemoryUsage()}"
    )

    filterIndex = 0
    lastFilterIndex = 0

    for filter_ in config.get('filters', []):
        if filter_.get('disabled', False):
            log.info(f"Filter {filterIndex}({filter_.get('name', 'NoName')}) is disabled, skipping...")
            filterIndex += 1
            continue

        filter_['chunkSize'] = filter_.get('chunkSize', 10000)
        chunkSize = filter_['chunkSize']

        log.debug(f"Chunk size: {chunkSize} records")
        totalChunks = (rowsLoaded // chunkSize) + 1
        totalRows = 0

        # Reload cursor from the current df state
        cursor = db.getCursor()
        cursor.execute("SELECT * FROM df" + limitClause)
        columns = [description[0] for description in cursor.description]

        log.info(f"\n####################################################################################################\nProcessing filter {filterIndex} ({filter_.get('name', 'NoName')})\n####################################################################################################")
        filter_['index'] = filterIndex

        actionType = filter_.get('actionType')

        if actionType in ['python', 'rest']:
            try:
                limitClause, table_name, totalRows = line_filter(
                    0, chunkSize, columns, config, config_file, cursor, db,
                    filterIndex, filter_, interactive, lastConfigLoaded, limitClause,
                    output_file, table_name, totalChunks, 0
                )
            except Exception as e:
                log.error(f"Error processing filter {filterIndex} ({filter_.get('name', 'NoName')}): {e}. Exiting")
                # raise e
                # Exit program with error
                sys.exit(1)

        elif actionType == 'sql':
            log.info(f"Processing df with {actionType} filter '{filter_.get('name', 'unnamed')}'")
            db.executeQuery(f"CREATE OR REPLACE TABLE filter{filterIndex} AS ({filter_.get('code')})")

        elif actionType in ['pandas', 'udf']:
            # PENDING: Implement if needed
            log.info("PENDING")

        else:
            log.debug(f"Action type unknown: {actionType}")

        # Update df view to the new filter result
        db.executeQuery(f"CREATE OR REPLACE VIEW df AS SELECT * FROM filter{filterIndex}", True)
        # Recalculate rowsLoaded from the new df
        rowsLoaded = db.getQueryResult("SELECT count(*) as rows FROM df", True)
        rowsLoaded = rowsLoaded['rows'][0]
        lastFilterIndex = filterIndex

        # Show sample data if requested
        df_sample = db.getQueryResult("SELECT * FROM df LIMIT 5", False)
        if df_sample is not None:
            if filter_.get('showSampleOnFinish', False):
                log.info("Show example data from current data")
                log.info("\n" + tabulate(df_sample, headers='keys', tablefmt='fancy_grid'))
            #else:
            #    pd.option_context('display.max_columns', None)
            #    log.info(df_sample)
                # Print with tabulate
                #log.info("\n" + tabulate(df_sample, headers='keys', tablefmt='fancy_grid'))
        else:
            log.info("No data to show")

        filterIndex += 1

    log.info(f"Saving to output file {output_file}...")
    outDelimiter = config.get('outDelimiter', ',')
    db.executeQuery(
        f"COPY (SELECT * FROM filter{lastFilterIndex}) TO '{output_file}' (FORMAT CSV, DELIMITER '{outDelimiter}')",
        True
    )
#######################################################################
def main(
    input_file: str,
    config_file: str,
    output_file: str,
    interactive: bool = False,
    delete: bool = False,
    verbose: bool = False
):
    logLevel = log.INFO
    if verbose:
        logLevel = log.DEBUG
    format_str = "%(asctime)s %(filename)s - :%(lineno)d (%(funcName)s) - %(message)s "
    #handler = log.StreamHandler()
    log.basicConfig(
        format=format_str,
        level=logLevel,
        datefmt="%H:%M:%S",
        handlers=[RichHandler(rich_tracebacks=False, show_path=False, markup=False)]
        #handlers=[handler]
    )

    log.info(f"Input file: {input_file}")
    log.info(f"Config file: {config_file}")
    log.info(f"Output file: {output_file}")
    log.info(f"Interactive mode: {interactive}")
    log.info(f"Delete previous data: {delete}")
    log.info(f"Verbose mode: {verbose}")

    mainProcess(input_file, config_file, output_file, interactive, delete)
#######################################################################
def run(
    input_file: str = typer.Argument(...),
    config_file: str = typer.Argument(...),
    output_file: str = typer.Argument(...),
    interactive: bool = typer.Option(False, "-i", "--interactive", help="Run in interactive mode"),
    delete: bool = typer.Option(False, "-d", "--delete", help="Delete previous process data"),
    verbose: bool = typer.Option(False, "-v", "--verbose", help="Verbose mode")
):
    main(input_file, config_file, output_file, interactive, delete, verbose)

if __name__ == "__main__":
    typer.run(run)
