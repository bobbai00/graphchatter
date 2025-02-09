import json

from model.texera.TexeraWorkflow import TexeraWorkflow
from service.texera_logical_plan import convertWorkflowContentToLogicalPlan, parseInputSchemaMapping

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

    is_schema_propagated = False

    # Extract relevant information from the response
    operator_id_to_input_schema = {}
    operator_id_to_error_mapping = {}

    # Parse the response and return the updated workflow
    workflow_dict = json.loads(workflowContent)
    workflow = TexeraWorkflow(
        workflow_content=workflowContent,
        operator_id_to_port_indexed_input_schemas_mapping=operator_id_to_input_schema,
        operator_id_to_error_mapping=operator_id_to_error_mapping,
        wid=wid,
    )

    return workflow, is_schema_propagated