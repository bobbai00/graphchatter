import zmq

def send_message_to_controller(host, port, message):
    """Utility to send a message to the Controller"""
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(f"tcp://{host}:{port}")

    socket.send(message)
    response = socket.recv_string()

    socket.close()
    context.term()

    return response