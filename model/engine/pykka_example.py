import pykka
import time

# Define a message class for processing data
class ProcessDataMessage:
    def __init__(self, data):
        self.data = data

# Define a worker actor
class WorkerActor(pykka.ThreadingActor):
    def on_receive(self, message):
        """Handles incoming messages"""
        if isinstance(message, ProcessDataMessage):
            time.sleep(1)  # Simulate processing time
            result = sum(message.data)  # Example: Sum the data
            return f"Processed {message.data} -> {result}"  # ✅ Directly return the result

# Define a manager actor that collects results from workers
class ManagerActor(pykka.ThreadingActor):
    def __init__(self, workers):
        super().__init__()
        self.workers = workers

    def on_receive(self, message):
        """Handles messages from workers"""
        if isinstance(message, ProcessDataMessage):
            worker = self.workers[hash(str(message.data)) % len(self.workers)]
            return worker.ask(message)  # ✅ Directly return the result (no `.get()`)

# Create worker actors
workers = [WorkerActor.start() for _ in range(3)]

# Create manager actor
manager = ManagerActor.start(workers)

# Send messages to the manager (which distributes them to workers)
data_chunks = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
futures = [manager.ask(ProcessDataMessage(data)) for data in data_chunks]

# Collect and print results
results = [future for future in futures]
print("Results:", results)  # ✅ Now it prints correct values

# Stop all actors
pykka.ActorRegistry.stop_all()