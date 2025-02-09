import pykka
import time
from config import CONTROLLER_CONFIG, WORKERS_CONFIG
from worker import WorkerActor
from controller import Controller
from utils import send_message_to_controller

# Start Worker Actors
workers = [WorkerActor.start(worker["host"], worker["port"]) for worker in WORKERS_CONFIG]

# Start Controller as an independent server
controller = Controller.start(CONTROLLER_CONFIG["host"], CONTROLLER_CONFIG["port"], WORKERS_CONFIG)

# Send a message to the Controller using the utility function
response = send_message_to_controller(CONTROLLER_CONFIG["host"], CONTROLLER_CONFIG["port"], "Hello Controller!")
print(f"Response from Controller: {response}")

# Wait before shutting down to observe message flow
time.sleep(2)

# Clean up
pykka.ActorRegistry.stop_all()