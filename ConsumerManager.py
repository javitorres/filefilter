import random
import concurrent.futures
import threading
import queue
import logging as log

KILL = object()
MAX_WORKERS = 500

# Based on https://realpython.com/intro-to-python-threading/

class ConsumerManager:
    def __init__(self, job_queue, max_consumers):
        format = "%(asctime)s %(filename)s:%(lineno)d - %(message)s "
        log.basicConfig(format=format, level=log.DEBUG, datefmt="%H:%M:%S")
        self.jobQueue = job_queue
        self.outPutQueue = queue.Queue()
        self.maxConsumers = max_consumers
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)
        self.active_consumers = 0
        self.lock = threading.Lock()
        self.lastConsumerId = 0

    def start_consumer(self, consumer_func):
        with self.lock:
            if self.active_consumers < self.maxConsumers and self.executor._max_workers > self.active_consumers:
                log.debug(f"Starting consumer {self.lastConsumerId}")
                self.executor.submit(consumer_func, str(self.lastConsumerId), self.jobQueue, self.outPutQueue)
                self.active_consumers += 1
                self.lastConsumerId += 1
            else:
                log.info(f"Max consumers reached, not starting new consumer")

    def stop_consumer(self):
        with self.lock:
            if self.active_consumers > 0:
                self.jobQueue.put(KILL)
                self.active_consumers -= 1
    
    def getActiveConsumers(self):
        return self.active_consumers

    def shutdown(self):
        self.executor.shutdown()

    def getQueueSize(self):
        return self.jobQueue.qsize()

    def putJob(self, job):
        self.jobQueue.put(job)

    def getOutput(self):
        # Returns all dicts in output queue
        output = []
        while not self.outPutQueue.empty():
            output.append(self.outPutQueue.get())
        return output

    def setMaxConsumers(self, max_consumers):
        self.maxConsumers = max_consumers

    def getMaxWorkers(self):
        return self.executor._max_workers



