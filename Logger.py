# Logger.py

class Logger:
    LEVELS = {
        "DEBUG": 1,
        "INFO": 2,
        "WARN": 3,
        "ERROR": 4
    }

    def __init__(self, log_level="INFO"):
        self.log_level = log_level

    def _log(self, level, message):
        if Logger.LEVELS.get(level, 0) >= Logger.LEVELS.get(self.log_level, 0):
            print(f"[{level}] {message}")

    def debug(self, message):
        self._log("DEBUG", message)

    def info(self, message):
        self._log("INFO", message)

    def warn(self, message):
        self._log("WARN", message)

    def error(self, message):
        self._log("ERROR", message)
