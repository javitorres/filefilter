import duckdb
import os
import logging as log
import pandas as pd


class Database:
    def __init__(self, databaseName, deleteDatabase=False):
        format = "%(asctime)s %(filename)s:%(lineno)d - %(message)s "
        log.basicConfig(format=format, level=log.INFO, datefmt="%H:%M:%S")
        log.info("Initializing DB...")

        # Check if data folder exists in filesystem and create if not
        log.info("Checking data folder...")
        if not os.path.exists("data"):
            os.makedirs("data")

        db_path = os.path.join("data", databaseName)

        # If the database exists and deleteDatabase is True, delete it
        if os.path.exists(db_path) and deleteDatabase:
            log.info("Deleting database...")
            os.remove(db_path)

        # Establish a connection
        if databaseName is not None:
            log.info(f"Connecting to database: {db_path}")
            self.connection = duckdb.connect(database=db_path)
        else:
            log.info("Connecting to in-memory database")
            self.connection = duckdb.connect(database=':memory:')

        self.serverStatus = {"databaseReady": True}

    def get(self):
        return self.serverStatus

    ####################################################
    def getQueryResult(self, query, logQuery=True):
        try:
            if logQuery:
                log.info(f"Executing query: {query}")

            # Ejecutar la consulta y obtener el DataFrame
            result = self.connection.execute(query).fetchdf()
            return result if not result.empty else None
        except Exception as e:
            log.error(f"Error running query: {e}")
            raise e

    ####################################################
    def executeQuery(self, query, logQuery=True):
        try:
            if logQuery:
                log.info(f"Executing query: {query}")

            self.connection.execute(query)
        except Exception as e:
            log.error(f"Error running query: {e}")
            raise e

    ####################################################
    def loadTable(self, table_name, file_name, limit=0, inDelimiter=None):
        try:
            log.info(f"Loading file into {table_name} from {file_name}")
            self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")

            limitClause = f" LIMIT {limit}" if limit > 0 else ""
            delimClause = ", delim='"+ inDelimiter + "'"

            if file_name.startswith("s3://"):
                self.connection.execute("CREATE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN, REGION 'eu-west-1');")
                self.connection.execute("INSTALL httpfs;")
                self.connection.execute("LOAD httpfs;")

            if file_name.lower().endswith((".csv", ".txt", ".tsv")):
                createTableQuery = f"CREATE TABLE {table_name} AS (SELECT * FROM read_csv('{file_name}' {delimClause}) {limitClause})"
            elif file_name.lower().endswith(".parquet"):
                createTableQuery = f"CREATE TABLE {table_name} AS (SELECT * FROM read_parquet('{file_name}') {limitClause})"
            else:
                raise Exception("File format not supported")

            log.info(f"Executing query: {createTableQuery}")
            self.connection.execute(createTableQuery)

            # Opcional: Mostrar tablas cargadas
            tables = self.connection.execute('SHOW TABLES').fetchdf()
            log.info(f"Tables in database: {tables}")
        except Exception as e:
            log.error(f"Error running query: {e}")
            raise e

    ####################################################
    def getCursor(self):
        return self.connection.cursor()

    ####################################################
    def register(self, name, df):
        try:
            log.info(f"Registering DataFrame as table '{name}'")
            self.connection.register(name, df)
            # Verificación inmediata
            test_query = self.connection.execute(f"SELECT * FROM {name} LIMIT 1").fetchdf()
            log.info(f"Registration of '{name}' successful. Sample data:\n{test_query}")
        except Exception as e:
            log.error(f"Error registering DataFrame '{name}': {e}")
            raise e

    ####################################################
    def checkIfTableExists(self, name):
        try:
            query = f"SELECT * FROM information_schema.tables WHERE table_name = '{name}'"
            result = self.connection.execute(query).fetchdf()
            exists = not result.empty
            log.info(f"Table '{name}' exists: {exists}")
            return exists
        except Exception as e:
            log.error(f"Error checking if table '{name}' exists: {e}")
            raise e
