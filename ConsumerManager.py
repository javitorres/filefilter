import random
import concurrent.futures
import threading
import queue

KILL = object()

# Based on https://realpython.com/intro-to-python-threading/

class ConsumerManager:
    def __init__(self, jobQueue, maxConsumers):
        self.jobQueue = jobQueue
        self.outPutQueue = queue.Queue()
        self.maxConsumers = maxConsumers
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=maxConsumers)
        self.active_consumers = 0
        self.lock = threading.Lock()
        self.lastConsumerId = 0

    def start_consumer(self, consumerFunc):
        with self.lock:
            if self.active_consumers < self.maxConsumers:
                self.executor.submit(consumerFunc, str(self.lastConsumerId), self.jobQueue, self.outPutQueue)
                self.active_consumers += 1
                self.lastConsumerId += 1

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




def generateJobs(jobQueue):
    for index in range(10):
        message = random.randint(1, 101)
        jobQueue.put(message)

