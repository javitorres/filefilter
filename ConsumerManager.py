######################################################################
#####################  ConsumerManager.py  ########################
#######################################################################

import concurrent.futures
import threading
import queue
import logging as log
import time

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
        #self.consumers = []
        self.jobs_in_progress = 0

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
                log.debug(f"Stopping consumer ")
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

    def wait_until_all_consumers_idle(self):
        while not self.jobQueue.empty() or self.getActiveConsumers() > 0:
            log.debug(f"Waiting for consumers to finish. Active consumers: {self.getActiveConsumers()}. Queue size: {self.getQueueSize()}")
            if self.getActiveConsumers() > 0:
                # Send message to stop consumers as soon as they finish
                if self.getQueueSize() == 0:
                    log.debug(f"Forcing stop of consumers. Active consumers: {self.getActiveConsumers()}. Queue size: {self.getQueueSize()}")
                    self.stop_consumer()

            # Wait for a short time before checking again
            time.sleep(0.2)
        log.debug(f"All consumers finished. Active consumers: {self.getActiveConsumers()}. Queue size: {self.getQueueSize()}")



