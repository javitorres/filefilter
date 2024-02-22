import logging as log
import random
import time
import concurrent.futures
import queue
import threading

KILL = object()

class ConsumerManager:
    def __init__(self, jobQueue):
        self.jobQueue = jobQueue
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
        self.active_consumers = 0
        self.lock = threading.Lock()
        self.lastConsumerId = 0

    def start_consumer(self, consumerFunc):
        with self.lock:
            if self.active_consumers < 10:  # Máximo de consumidores
                self.executor.submit(consumerFunc, str(self.lastConsumerId), self.jobQueue)
                self.active_consumers += 1
                self.lastConsumerId += 1

    def stop_consumer(self):
        with self.lock:
            if self.active_consumers > 0:
                self.jobQueue.put(KILL)  # Cada SENTINEL detendrá un consumidor
                self.active_consumers -= 1
    
    def getActiveConsumers(self):
        return self.active_consumers

'''
def consumer(id, jobQueue):
    while True:
        job = jobQueue.get()
        if job is KILL:
            print ("Stopping consumer " + id + "...")
            break  # Detener este hilo consumidor
        #log.info("Consumer " + id + " storing message: %s", job)
        time.sleep(5)
'''
def generateJobs(jobQueue):
    for index in range(10):
        message = random.randint(1, 101)
        jobQueue.put(message)

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    log.basicConfig(format=format, level=log.INFO, datefmt="%H:%M:%S")
    # log.getLogger().setLevel(logging.DEBUG)

    jobQueue = queue.Queue()
    manager = ConsumerManager(jobQueue)

    # Iniciar algunos consumidores
    for _ in range(5):
        manager.start_consumer()


    while True:
        if (jobQueue.qsize()<100):
            generateJobs(jobQueue)
        log.info("Queue size: %s", str(jobQueue.qsize()) + " Consumers: " + str(manager.getActiveConsumers()))
        time.sleep(1)

    # Ejemplo de cómo ajustar los consumidores en tiempo real
    time.sleep(10)
    log.info("Adding 1 more consumer...")
    manager.start_consumer()

    time.sleep(10)  # Simular otra condición
    log.info("Stopping 1 consumer...")
    manager.stop_consumer()  # Detener un consumidor
