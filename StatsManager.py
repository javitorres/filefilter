from collections import deque
import time

class StatsManager:
    def __init__(self):
        self.times = deque(maxlen=100)  # Almacena los tiempos de las últimas 1000 filas
        self.total_rows = 0
        self.total_time = 0

    def register(self, processing_time):
        self.times.append(processing_time)
        self.total_rows += 1
        self.total_time += processing_time

    def avg_time(self, last_n_rows=None):
        r = None
        if last_n_rows is None or last_n_rows > len(self.times):
            r = sum(self.times) / len(self.times) if self.times else 0
        else:
            r = sum(list(self.times)[-last_n_rows:]) / last_n_rows
        return r


    def get_eta(self, pending_rows):
        avg = self.avg_time()
        milliseconds = avg * pending_rows
        #print("MillisecondsETA: ", int(milliseconds), " HH:MM:SS:", time.strftime('%H:%M:%S', time.gmtime(milliseconds)))
        # Get in HH:MM:SS format
        return time.strftime('%H:%M:%S', time.gmtime(milliseconds))
