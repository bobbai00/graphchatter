import json

import requests

from config.config import Config
from model.texera.TexeraWorkflow import TexeraWorkflow
from service.texera_logical_plan import convertWorkflowContentToLogicalPlan, parseInputSchemaMapping

TEXERA_API_ENDPOINT = f"{Config.get('texera', 'address')}{Config.get('texera', 'api-endpoint')}"
TEXERA_WORKFLOW_COMPILATION_ENDPOINT = f"{TEXERA_API_ENDPOINT}/compile"

BRAIN_API_ENDPOINT = f"{Config.get('brain', 'address')}{Config.get('brain', 'api-endpoint')}"
BRAIN_USER_LOGIN_ENDPOINT = f"{BRAIN_API_ENDPOINT}/auth/login"

BRAIN_USERNAME = Config.get("brain", "username")
BRAIN_PASSWORD = Config.get("brain", "password")

# Global variable to store the Bearer token
_bearer_token = None


# Function to get the Bearer token
def get_brain_token():
    global _bearer_token

    if _bearer_token is None:
        # Send the login request if the token is not set
        login_payload = {
            "username": BRAIN_USERNAME,
            "password": BRAIN_PASSWORD
        }

        response = requests.post(BRAIN_USER_LOGIN_ENDPOINT, json=login_payload)

        # Check if the request was successful
        if response.status_code != 200:
            raise Exception(f"Failed to login to Brain API: {response.status_code}, {response.text}")

        # Extract and store the token
        _bearer_token = response.json().get("accessToken")
        if not _bearer_token:
            raise Exception("Failed to retrieve access token from Brain API response.")

    return _bearer_token


# Function to check the static Texera workflow
# Function to check the static Texera workflow
def parseRawWorkflowToTexeraWorkflowWithSchema(wid: int, workflowContent: str) -> (TexeraWorkflow | None, bool):
    # token = get_brain_token()
    #
    # # Prepare the headers with the Bearer token
    # headers = {
    #     "Authorization": f"Bearer {token}",
    #     "Content-Type": "application/json",
    # }

    try:
        request = convertWorkflowContentToLogicalPlan(workflowContent)
    except Exception as e:
        print("convert workflow to logical plan failed")
        request = None

    if request == None:
        return None, False

    # Send the request with the dict directly to the 'json' parameter (no need to use json.dumps())
    response = requests.post(f"{TEXERA_WORKFLOW_COMPILATION_ENDPOINT}/{wid}", json=request)

    is_schema_propagated = True
    # Check if the request was successful
    if response.status_code != 200:
        print("request failed: ", response)
        is_schema_propagated = False

    # Extract relevant information from the response
    operator_id_to_input_schema = {}
    operator_id_to_error_mapping = {}

    if is_schema_propagated:
        operator_id_to_input_schema = parseInputSchemaMapping(response.json().get("operatorInputSchemas"))
        operator_id_to_error_mapping = response.json().get("operatorErrors")
        physicalPojo = response.json().get("physicalPlan")

    # Parse the response and return the updated workflow
    workflow_dict = json.loads(workflowContent)
    workflow = TexeraWorkflow(
        workflow_content=workflowContent,
        operator_id_to_port_indexed_input_schemas_mapping=operator_id_to_input_schema,
        operator_id_to_error_mapping=operator_id_to_error_mapping,
        wid=wid,
    )

    return workflow, is_schema_propagated