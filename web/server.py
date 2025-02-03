from fastapi import FastAPI
from model.op_json_loader import *
from web.websocket import websocket_endpoint

app = FastAPI()

# Register the WebSocket endpoint correctly
app.add_api_websocket_route("/wsapi/workflow-websocket", websocket_endpoint)

# ------------------- HTTP API ------------------- #

@app.get("/api/resources/operator-metadata")
async def get_operator_metadata():
    properties = [
        create_operator_property(name="question", prop_type="string", title="Question",
                                 description="User's question", default="# Write your question here",
                                 required=True),
    ]
    # Define a chat operator
    chat_op = create_operator_metadata(
        operator_type="Chat",
        user_friendly_name="Chat with LLM",
        operator_description="A single conversation with LLM",
        operator_group_name="LLM Chatter",
        input_ports=[create_input_port(port_id=1)],
        output_ports=[create_output_port(port_id=2, mode="SET_SNAPSHOT")],
        properties=properties,
        dynamic_input_ports=True,
        dynamic_output_ports=True,
        support_reconfiguration=True,
        allow_port_customization=True,
    )

    # Define an example operator group
    chat_op_group = create_group(
        group_name="LLM Chatter",
        children=[],
    )

    return {"operators": [chat_op], "groups": [chat_op_group]}


# ------------------- Running the Server ------------------- #

if __name__ == "__main__":
    import uvicorn

    print("Server running at http://127.0.0.1:8080")
    uvicorn.run(app, host="localhost", port=8080)