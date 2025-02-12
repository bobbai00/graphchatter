import os,sys
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(CURRENT_DIR))

import pykka
import zmq
import threading

from engine.config import WORKERS_CONFIG


class WorkerActor(pykka.ThreadingActor):
    def __init__(self, host, port):
        super().__init__()
        self.host = host
        self.port = port
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)  # REP socket for replies
        self.socket.bind(f"tcp://{self.host}:{self.port}")  # Bind to host & port
        self.current_operator = None
        self.dependent_inputs = []

    def on_start(self):
        threading.Thread(target=self.listen_for_messages, daemon=True).start()

    def listen_for_messages(self):
        while True:
            message = self.socket.recv()
            deserialized_msg = pickle.loads()


            # TODO: handle two types of messages:
            # TODO:     type1: task assignment message from the controller, deserialize the message and save needed information
            # TODO:     type2: execution start message from the controller, start to execute (if no dependant nodes, start to execute right away; if having 1+ dependant nodes, wait for their message all arrives and start to execute)
            # TODO:     type3: results from the dependant actors, record it locally and see if all dependant messages arrive.
            # TODO: for the execution, for now we can simply come up with a random message for simplicity consideration
            # TODO: to get the topology information, you can refer to the class TexeraWorkflow's DAG variable, which models the whole workflow as the DAG
            if type(deserialized_msg) == 'TexeraOperator':
                if self.current_operator == None:
                    self.current_operator = deserialized_msg
                    for port in self.current_operator.input_ports:
                        self.dependent_inputs.append(port)

            elif type(deserialized_msg) == 'WorkflowExecuteRequest':
                if self.dependent_inputs:
                    self.socket.send_string("Inputs have not been computed yet")
                elif self.current_operator:
                    #TODO execute workflow
                    self.socket.send_string("Task Execution started")
                else:
                    self.socket.send_string("Error: Actor was assigned no operator")
            else:
                pass

            print(f"Worker {self.port} received: {deserialized_msg}")

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