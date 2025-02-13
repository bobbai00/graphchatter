import os,sys
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(CURRENT_DIR))

import pykka
import zmq
import threading
import pickle
from messages import WorkerExecutionStart, WorkerAssignment, ExecutionResult

from engine.config import WORKERS_CONFIG


class WorkerActor(pykka.ThreadingActor):
    def __init__(self, host, port):
        super().__init__()
        self.host = host
        self.port = port
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)  # REP socket for replies
        self.socket.bind(f"tcp://{self.host}:{self.port}")  # Bind to host & port

        self.execution_ready = False

        self.operators = {}
        self.execution_ready_ops = []

        #operator id is key so that we know when we can start to execute
        #upstream contain input operators for an operator that haven't sent their result yet
        self.upstreams = {}
        self.downstreams = {}
        self.inputs = {}

        #operator id is key, worker is value, so that we know where to send to
        self.operator_worker_mapping = {}

    def on_start(self):
        threading.Thread(target=self.listen_for_messages, daemon=True).start()

    def listen_for_messages(self):
        while True:
            message = self.socket.recv()
            deserialized_msg = pickle.loads(message)
            self.socket.send_string(f"Worker {self.port} received message {deserialized_msg}")

            #TODO: also consider ports for input/output

            #Three types of messages
            #type1: task assignment message from the controller, deserialize the message and save needed information
            if isinstance(deserialized_msg, WorkerAssignment):
                assignment = deserialized_msg
                opID = assignment.operator.getID()
                if assignment.worker['host'] == self.host and assignment.worker['port'] == self.port:
                    self.operators[opID] = assignment.operator
                    self.upstreams[opID] = assignment.upstreams
                    self.downstreams[opID] = assignment.downstreams
                operator_worker_mapping[opID] = assignment.worker

            #type3: results from the dependant actors, record it locally and see if all dependant messages arrive.
            elif isinstance(deserialized_msg, ExecutionResult):
                input = deserialized_msg
                self.inputs[input.opID].append(input.result)
                print(f"Worker {self.port} received execution result for {opID}")
                if(len(self.inputs[input.opID]) == len(self.upstreams[input.opID])):
                    self.execution_ready_ops.append(input.opID)


            #type2: execution start message from the controller, start to execute (if no dependant nodes, start to execute right away; if having 1+ dependant nodes, wait for their message all arrives and start to execute)
            elif isinstance(deserialized_msg, WorkerExecutionStart):
                self.execution_ready = True
                #Check if there is an execution ready operator
                while self.execution_ready_ops:
                    opID = execution_ready_ops.pop()
                    #TODO actually execute operator
                    print(f"Worker {self.port} started execution")
                    for targetOpID in self.downstreams[opID]:
                        target_worker = self.operator_worker_mapping[targetOpID]
                        result = ExecutionResult(self, opID, "Dummy Result")
                        message = pickle.dumps(result)
                        self.send_to_worker(target_worker, message)
            else:
                print(f"Worker {self.port} was not able to recognize message {deserialized_msg}")

    def send_to_worker(self, target, message):
        try:
            socket = self.context.socket(zmq.REQ)
            socket.connect(f"tcp://{target['host']}:{target['port']}")
            socket.send(message)

            response = socket.recv_string()
            print(f"Worker {self.port} received from Worker {target['port']}: {response}")
            socket.close()
        except Exception as e:
            print(f"Error communicating with Worker {target['port']}: {e}")
            response = f"Failed to reach Worker {target['port']}"

        return response

    def on_stop(self):
        self.socket.close()
        self.context.term()


# ---------------------- Main Execution Block ---------------------- #

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python worker.py <worker_index>")
        sys.exit(1)

    index = int(sys.argv[1])

    if index < 0 or index >= len(WORKERS_CONFIG):
        print(f"Invalid worker index: {index}. Must be between 0 and {len(WORKERS_CONFIG) - 1}.")
        sys.exit(1)

    worker_config = WORKERS_CONFIG[index]
    print(f"Starting Worker {index} at {worker_config['host']}:{worker_config['port']}")

    # Start the Worker Actor
    worker = WorkerActor.start(worker_config["host"], worker_config["port"])

    try:
        while True:
            pass
    except KeyboardInterrupt:
        print(f"\nShutting down Worker {index}...")
        pykka.ActorRegistry.stop_all()