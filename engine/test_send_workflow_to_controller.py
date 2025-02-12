import os,sys
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(CURRENT_DIR))

import pykka
import time
from config import CONTROLLER_CONFIG, WORKERS_CONFIG
from worker import WorkerActor
from controller import Controller
from utils import send_message_to_controller
from model.texera.TexeraWorkflow import TexeraWorkflow
import pickle
import json

# Start Worker Actors
workers = [WorkerActor.start(worker["host"], worker["port"]) for worker in WORKERS_CONFIG]

# Start Controller as an independent server
controller = Controller.start(CONTROLLER_CONFIG["host"], CONTROLLER_CONFIG["port"], WORKERS_CONFIG)

# Send a message to the Controller using the utility function
content = '{"name":"Untitled workflow","description":null,"content":{"operators":[],"operatorPositions":{},"links":[],"commentBoxes":[],"settings":{"dataTransferBatchSize":400}},"isPublished":1,"readonly":false,"size":109}'
workflow = TexeraWorkflow(json.loads(content), workflow_title = "test")
response = send_message_to_controller(CONTROLLER_CONFIG["host"], CONTROLLER_CONFIG["port"], pickle.dumps(workflow))
print(f"Response from Controller: {response}")

# Wait before shutting down to observe message flow
time.sleep(2)

# Clean up
pykka.ActorRegistry.stop_all()
