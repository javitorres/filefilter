import duckdb
import os
import logging as log


def init(databaseName):
    global db

    if (databaseName is not None):
        log.info("Connecting to database..." + databaseName)
        db = duckdb.connect("data/" + databaseName)
    else:
        log.info("Connecting to in-memory database")
        db = duckdb.connect(':memory:')

    global configLoaded
    configLoaded = True


class Database:
    def __init__(self, databaseName, deleteDatabase=False):
        format = "%(asctime)s %(filename)s:%(lineno)d - %(message)s "
        log.basicConfig(format=format, level=log.INFO, datefmt="%H:%M:%S")
        log.info("Initializing DB...")

        # Check if data folder exists in filesistem and create if not
        log.info("Checking data folder...")
        if not os.path.exists("data"):
            os.makedirs("data")
        # If the database exists and deleteDatabase is True, delete it
        if os.path.exists("data/" + databaseName) and deleteDatabase:
            log.info("Deleting database...")
            os.remove("data/" + databaseName)
        init(databaseName)

        self.serverStatus = {}
        self.serverStatus["databaseReady"] = True

    def get(self):
        return self.serverStatus

    ####################################################
    def getQueryResult(self, query, logQuery=True):
        try:
            if (logQuery):
                log.info("Executing query: " + str(query))

            # Here it seems there is a memory leak
            r = db.query(query)
            if (r is not None):
                return r.df()
            else:
                return None
        except Exception as e:
            log.error("Error running query: " + str(e))
            raise e

    def executeQuery(self, query, logQuery=True):
        try:
            if (logQuery):
                log.info("Executing query: " + str(query))

            db.query(query)

        except Exception as e:
            log.error("Error running query: " + str(e))
            raise e

    ####################################################
    def loadTable(self, table_name, file_name, limit=0):
        try:
            log.info("Loading file into " + table_name + " from " + file_name)
            db.query("DROP TABLE IF EXISTS " + table_name)
            limitClause=""
            if (limit > 0):
                limitClause = " LIMIT " + str(limit)

            if (file_name.startswith("s3://")):
                db.execute("CREATE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN, REGION 'eu-west-1');")
                db.execute("INSTALL httpfs")
                db.execute("LOAD httpfs")

            if file_name.lower().endswith(".csv") or file_name.lower().endswith(".txt") or file_name.lower().endswith(".tsv"):
                createTableQuery = "CREATE TABLE " + table_name + " AS (SELECT * FROM read_csv_auto('" + file_name + "') " + limitClause + ")"
            elif file_name.lower().endswith(".parquet"):
                createTableQuery = "CREATE TABLE " + table_name + " AS (SELECT * FROM read_parquet_auto('" + file_name + "') " + limitClause + ")"
            else:
                raise Exception("File format not supported")

            log.info("Executing query: " + createTableQuery)
            db.query(createTableQuery)

            r = db.sql('SHOW TABLES')
        except Exception as e:
            log.error("Error running query: " + str(e))
            raise e

    def getCursor(self):
        return db.cursor()

