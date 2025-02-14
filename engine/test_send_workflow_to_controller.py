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

# Send a message to the Controller using the utility functions
content = '''{
               "operators": [
                 {
                   "operatorID": "Chat-operator-391d9b07-3c50-4f6b-96b3-5fa229860831",
                   "operatorType": "Chat",
                   "operatorVersion": "N/A",
                   "operatorProperties": {
                     "question": "Hi!"
                   },
                   "inputPorts": [
                     {
                       "portID": "input-0",
                       "displayName": "",
                       "allowMultiInputs": true,
                       "isDynamicPort": false,
                       "dependencies": []
                     }
                   ],
                   "outputPorts": [
                     {
                       "portID": "output-0",
                       "displayName": "",
                       "allowMultiInputs": false,
                       "isDynamicPort": false
                     }
                   ],
                   "showAdvanced": false,
                   "isDisabled": false,
                   "customDisplayName": "Chat with LLM",
                   "dynamicInputPorts": true,
                   "dynamicOutputPorts": true
                 },
                 {
                   "operatorID": "Chat-operator-66ad92c6-20c3-434e-aaf3-f627f759d378",
                   "operatorType": "Chat",
                   "operatorVersion": "N/A",
                   "operatorProperties": {
                     "question": "# Write your question here"
                   },
                   "inputPorts": [
                     {
                       "portID": "input-0",
                       "displayName": "",
                       "allowMultiInputs": true,
                       "isDynamicPort": false,
                       "dependencies": []
                     }
                   ],
                   "outputPorts": [
                     {
                       "portID": "output-0",
                       "displayName": "",
                       "allowMultiInputs": false,
                       "isDynamicPort": false
                     }
                   ],
                   "showAdvanced": false,
                   "isDisabled": false,
                   "customDisplayName": "Chat with LLM",
                   "dynamicInputPorts": true,
                   "dynamicOutputPorts": true
                 },
                 {
                   "operatorID": "Chat-operator-20c04942-7551-4108-9a0a-ef7b696812ef",
                   "operatorType": "Chat",
                   "operatorVersion": "N/A",
                   "operatorProperties": {
                     "question": "# Write your question here"
                   },
                   "inputPorts": [
                     {
                       "portID": "input-0",
                       "displayName": "",
                       "allowMultiInputs": true,
                       "isDynamicPort": false,
                       "dependencies": []
                     }
                   ],
                   "outputPorts": [
                     {
                       "portID": "output-0",
                       "displayName": "",
                       "allowMultiInputs": false,
                       "isDynamicPort": false
                     }
                   ],
                   "showAdvanced": false,
                   "isDisabled": false,
                   "customDisplayName": "Chat with LLM",
                   "dynamicInputPorts": true,
                   "dynamicOutputPorts": true
                 }
               ],
               "operatorPositions": {
                 "Chat-operator-391d9b07-3c50-4f6b-96b3-5fa229860831": {
                   "x": 431,
                   "y": 215
                 },
                 "Chat-operator-66ad92c6-20c3-434e-aaf3-f627f759d378": {
                   "x": 562,
                   "y": 210
                 },
                 "Chat-operator-20c04942-7551-4108-9a0a-ef7b696812ef": {
                   "x": 768,
                   "y": 231
                 }
               },
               "links": [
                 {
                   "linkID": "link-d9c59fef-5d18-4e70-8fd4-3749b3efcb23",
                   "source": {
                     "operatorID": "Chat-operator-391d9b07-3c50-4f6b-96b3-5fa229860831",
                     "portID": "output-0"
                   },
                   "target": {
                     "operatorID": "Chat-operator-66ad92c6-20c3-434e-aaf3-f627f759d378",
                     "portID": "input-0"
                   }
                 },
                 {
                   "linkID": "72862859-51dc-4f6a-989e-972ac96f737f",
                   "source": {
                     "operatorID": "Chat-operator-66ad92c6-20c3-434e-aaf3-f627f759d378",
                     "portID": "output-0"
                   },
                   "target": {
                     "operatorID": "Chat-operator-20c04942-7551-4108-9a0a-ef7b696812ef",
                     "portID": "input-0"
                   }
                 }
               ],
               "commentBoxes": [],
               "settings": {
                 "dataTransferBatchSize": 400
               }
             }'''


workflow = TexeraWorkflow(json.loads(content), workflow_title = "test")
response = send_message_to_controller(CONTROLLER_CONFIG["host"], CONTROLLER_CONFIG["port"], pickle.dumps(workflow))

print(f"Response from Controller: {response}")
termination = ControllerTermination()
response = send_message_to_controller(CONTROLLER_CONFIG["host"], CONTROLLER_CONFIG["port"], pickle.dumps(termination))
print(f"Response from Controller: {response}")


# Clean up
pykka.ActorRegistry.stop_all()
