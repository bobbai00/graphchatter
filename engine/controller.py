import pykka
import zmq
import threading
import pickle
from messages import *

from engine.config import CONTROLLER_CONFIG, WORKERS_CONFIG


class Controller(pykka.ThreadingActor):
    def __init__(self, host, port, workers_config):
        super().__init__()
        self.host = host
        self.port = port
        self.workers_config = workers_config
        self.context = zmq.Context()

        # REP socket to receive messages (Controller as Server)
        self.server_socket = self.context.socket(zmq.REP)
        self.server_socket.bind(f"tcp://{self.host}:{self.port}")

    def on_start(self):
        threading.Thread(target=self.listen_for_requests, daemon=True).start()

    def listen_for_requests(self):
        while True:
            message = self.server_socket.recv()
            workflow = pickle.loads(message)

            print(f"Controller received workflow with WID {workflow.wid}")

            operators = workflow.GetOperators()

            for assignment in self.assign_tasks_to_workers(operators, workflow):
                message = pickle.dumps(assignment)
                self.broadcast_to_workers(message)

            start = WorkerExecutionStart()
            message = pickle.dumps(start)
            self.server_socket.send_string(f"Execution starts")

    def broadcast_to_workers(self, message):
        responses = []
        for worker in self.workers_config:
            try:
                socket = self.context.socket(zmq.REQ)
                socket.connect(f"tcp://{worker['host']}:{worker['port']}")
                socket.send(message)

                response = socket.recv_string()
                print(f"Controller received from Worker {worker['port']}: {response}")
                responses.append(response)

                socket.close()
            except Exception as e:
                print(f"Error communicating with Worker {worker['port']}: {e}")
                responses.append(f"Failed to reach Worker {worker['port']}")
        return responses

    def on_stop(self):
        self.server_socket.close()
        self.context.term()

    def assign_tasks_to_workers(self, operators, workflow):
        #Round Robin
        i = 0
        for operator in operators:
            yield WorkerAssignment(self.workers_config[i%3], operator, workflow)
            i += 1



# ---------------------- Main Execution Block ---------------------- #

if __name__ == "__main__":
    print(f"Starting Controller at {CONTROLLER_CONFIG['host']}:{CONTROLLER_CONFIG['port']}")

    # Start the Controller Actor
    controller = Controller.start(
        CONTROLLER_CONFIG["host"],
        CONTROLLER_CONFIG["port"],
        WORKERS_CONFIG
    )

    try:
        # Keep the controller running indefinitely
        while True:
            pass
    except KeyboardInterrupt:
        print("\nShutting down the Controller...")
        pykka.ActorRegistry.stop_all()