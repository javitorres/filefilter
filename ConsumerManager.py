#####################  ConsumerManager.py  ########################

import concurrent.futures
import threading
import queue
import logging as log
import time
from constants import KILL

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

        # Define stats dictionary to store consumer stats. For example OK: 4, ERROR: 2
        self.consumer_stats = {
            "KILLED": 0,
            "EXCEPTION": 0,
        }


    def start_consumer(self, consumer_func):
        with self.lock:
            if self.active_consumers < self.maxConsumers and self.executor._max_workers > self.active_consumers:
                log.debug(f"Starting consumer {self.lastConsumerId}")
                self.executor.submit(consumer_func, str(self.lastConsumerId), self.jobQueue, self.outPutQueue)
                self.active_consumers += 1
                self.lastConsumerId += 1
            else:
                log.info(f"Max consumers reached, not starting new consumer")

    def send_kill_signal_to_consumer(self):
        with self.lock:
            if self.active_consumers > 0:
                log.debug(f"Sending KILL signal to stop a consumer")
                self.jobQueue.put(KILL)


    def consumer_finish_signal(self, consumerId, result):
        log.debug(f"Received finish signal from consumer {consumerId} with result: {result}")
        ## Store the result in the consumer stats dictionary
        if result == "KILLED":
            self.consumer_stats["KILLED"] += 1
        elif result == "EXCEPTION":
            self.consumer_stats["EXCEPTION"] += 1
        else:
            # Store the result in the output queue
            log.error(f"Consumer {consumerId} finished with error: {result}")

        with self.lock:
            if self.active_consumers > 0:
                self.active_consumers -= 1
                log.debug(f"Decreasing active consumers. Active consumers: {self.active_consumers}")
            else:
                log.debug(f"No active consumers to decrease. Active consumers: {self.active_consumers}")
    
    def getActiveConsumers(self):
        return self.active_consumers

    def shutdown(self):
        self.executor.shutdown()

    def getQueueSize(self):
        return self.jobQueue.qsize()

    def putJob(self, job):
        with self.lock:
            log.debug(f"Putting job {job['rowIndex']} in queue")
            self.jobQueue.put(job)

    def getJob(self):
        # Wait and returns next job
        job = self.jobQueue.get()
        if job is KILL:
            log.debug("Getting KILL job from queue")
        else:
            log.debug(f"Getting job {job['rowIndex']} from queue")
        return job

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

    def wait_until_all_consumers_idle(self, MAX_EXCEPTION=-1):
        log.debug(f"Waiting for consumers to finish. Active consumers: {self.getActiveConsumers()}. Queue size: {self.getQueueSize()}")
        while not self.jobQueue.empty() or self.getActiveConsumers() > 0:
            log.debug(f"Stats: {self.consumer_stats}")
            log.debug(f"Queue content: {[item for item in self.jobQueue.queue]}")
            # If EXCEPTION stat is greater than MIN_ERRORS (except it is -1) exit
            if MAX_EXCEPTION != -1 and self.consumer_stats["EXCEPTION"] > MAX_EXCEPTION:
                raise Exception(f"Max exceptions reached. Exiting. Active consumers: {self.getActiveConsumers()}. Queue size: {self.getQueueSize()}")

            if self.getActiveConsumers() > 0:
                # Send message to stop consumers as soon as they finish
                if self.getQueueSize() == 0:
                    log.debug(f"Forcing stop of consumers. Active consumers: {self.getActiveConsumers()}. Queue size: {self.getQueueSize()}")
                    self.send_kill_signal_to_consumer()

            # Wait for a short time before checking again
            log.debug(f"Sleeping for 0.2 seconds. Active consumers: {self.getActiveConsumers()}. Queue size: {self.getQueueSize()}")
            time.sleep(0.2)
        log.debug(f"All consumers finished. Active consumers: {self.getActiveConsumers()}. Queue size: {self.getQueueSize()}" +
                  f"Stats: {self.consumer_stats}")



