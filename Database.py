import duckdb
import os


def init(databaseName):
    global db

    if (databaseName is not None):
        print("Connecting to database..." + databaseName)
        db = duckdb.connect("data/" + databaseName)
    else:
        print("Connecting to in-memory database")
        db = duckdb.connect(':memory:')

    global configLoaded
    configLoaded = True


class Database:
    def __init__(self, databaseName):
        print("Initializing DB...")

        # Check if data folder existsin filesistem and create if not
        print("Checking data folder...")
        if not os.path.exists("data"):
            os.makedirs("data")

        init(databaseName)

        self.serverStatus = {}
        self.serverStatus["databaseReady"] = True

    def get(self):
        return self.serverStatus

    ####################################################
    def runQuery(self, query, logQuery=True):
        try:
            if (logQuery):
                print("Executing query: " + str(query))

            r = db.query(query)
            if (r is not None):
                return r.df()
        except Exception as e:
            print("Error running query: " + str(e))
            raise e

    ####################################################
    def loadTable(self, table_name, file_name):
        try:
            print("Loading file into " + table_name + " from " + file_name)
            db.query("DROP TABLE IF EXISTS " + table_name)

            db.query("CREATE TABLE " + table_name + " AS (SELECT * FROM read_csv_auto('" + file_name + "', HEADER=TRUE, SAMPLE_SIZE=1000000))")

            r = db.sql('SHOW TABLES')
        except Exception as e:
            print("Error running query: " + str(e))
            raise e

    def getCursor(self):
        return db.cursor()

