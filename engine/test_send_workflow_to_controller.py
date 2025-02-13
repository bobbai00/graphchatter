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
from messages import *
import pickle
import json

# Start Worker Actors
workers = [WorkerActor.start(worker["host"], worker["port"]) for worker in WORKERS_CONFIG]

# Start Controller as an independent server
controller = Controller.start(CONTROLLER_CONFIG["host"], CONTROLLER_CONFIG["port"], WORKERS_CONFIG)

# Send a message to the Controller using the utility function
content = '{"name":"Network Viz","description":null,"content":{"operators":[{"operatorID":"CSVFileScan-operator-adbedb65-69d4-4ca2-bd1b-f3948e26fe41","operatorType":"CSVFileScan","operatorVersion":"N/A","operatorProperties":{"fileEncoding":"UTF_8","customDelimiter":",","hasHeader":true,"limit":10,"fileName":"/paulschatt/Git Dataset/v1/musae_git_edges.csv"},"inputPorts":[],"outputPorts":[{"portID":"output-0","displayName":"","allowMultiInputs":false,"isDynamicPort":false}],"showAdvanced":false,"isDisabled":false,"customDisplayName":"CSV File Scan","dynamicInputPorts":false,"dynamicOutputPorts":false},{"operatorID":"RandomKSampling-operator-8eaa33a9-6e11-4206-9784-b9d3769f68a5","operatorType":"RandomKSampling","operatorVersion":"N/A","operatorProperties":{"random k sample percentage":50},"inputPorts":[{"portID":"input-0","displayName":"","allowMultiInputs":false,"isDynamicPort":false,"dependencies":[]}],"outputPorts":[{"portID":"output-0","displayName":"","allowMultiInputs":false,"isDynamicPort":false}],"showAdvanced":false,"isDisabled":false,"customDisplayName":"Random K Sampling","dynamicInputPorts":false,"dynamicOutputPorts":false}],"operatorPositions":{"CSVFileScan-operator-adbedb65-69d4-4ca2-bd1b-f3948e26fe41":{"x":387,"y":264},"RandomKSampling-operator-8eaa33a9-6e11-4206-9784-b9d3769f68a5":{"x":524,"y":258}},"links":[{"linkID":"link-77862b51-80a3-405d-984c-fd4a316c0f14","source":{"operatorID":"CSVFileScan-operator-adbedb65-69d4-4ca2-bd1b-f3948e26fe41","portID":"output-0"},"target":{"operatorID":"RandomKSampling-operator-8eaa33a9-6e11-4206-9784-b9d3769f68a5","portID":"input-0"}}],"commentBoxes":[],"settings":{"dataTransferBatchSize":400}},"isPublished":1,"readonly":false,"size":1607}'
workflow = TexeraWorkflow(json.loads(content), workflow_title = "test")
response = send_message_to_controller(CONTROLLER_CONFIG["host"], CONTROLLER_CONFIG["port"], pickle.dumps(workflow))
print(f"Response from Controller: {response}")
termination = ControllerTermination()
response = send_message_to_controller(CONTROLLER_CONFIG["host"], CONTROLLER_CONFIG["port"], pickle.dumps(termination))
print(f"Response from Controller: {response}")

# Wait before shutting down to observe message flow
time.sleep(2)

# Clean up
pykka.ActorRegistry.stop_all()
