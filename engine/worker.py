import sys

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

    def on_start(self):
        threading.Thread(target=self.listen_for_messages, daemon=True).start()

    def listen_for_messages(self):
        while True:
            message = self.socket.recv_string()
            print(f"Worker {self.port} received: {message}")
            # TODO: handle two types of messages:
            # TODO:     type1: task assignment message from the controller, deserialize the message and save needed information
            # TODO:     type2: execution start message from the controller, start to execute (if no dependant nodes, start to execute right away; if having 1+ dependant nodes, wait for their message all arrives and start to execute)
            # TODO:     type3: results from the dependant actors, record it locally and see if all dependant messages arrive.
            # TODO: for the execution, for now we can simply come up with a random message for simplicity consideration
            # TODO: to get the topology information, you can refer to the class TexeraWorkflow's DAG variable, which models the whole workflow as the DAG
            self.socket.send_string("message received")

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