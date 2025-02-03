import json
import jsonpatch
from typing import List, Tuple, Dict

from config.config import Config
from data.texera.texera_db import getWorkflowByWid, getSession, getEngine, getAllWorkflows, Workflow

def getReversePatches(latestWorkflowContent: str, workflowPatches: List[str]) -> Tuple[str, List[List[dict]]]:
    """
    Reverse the given patches iteratively, return a list of (workflowContent json dict,
    the list of patches to the next version), sorted from the earliest version to the latest.

    :param latestWorkflowContent: the final workflow content as a JSON string
    :param workflowPatches: a list of patches sorted from the latest patch to the earliest patch.
                            Each patch is a JSON string representing a JSON Patch (an array of operations).
    :return: a tuple containing:
             - The oldest version of the workflow content (as a dict),
             - A flattened list of patch operations (each as a dict) that can be applied to
               get back to the latest version, ordered from the earliest to the latest.
    """
    # Parse the input JSON string to a Python dictionary for the latest workflow content
    current_version = json.loads(latestWorkflowContent)

    # This will hold the reverse patches, starting from the earliest to the latest
    reverse_patches = []

    # Define priority for sorting based on the operation type and target path
    def get_patch_priority(patch):
        op_type = patch['op']
        path = patch['path']

        # Assign priority based on operation and path
        if op_type == "add":
            if "operators" in path:
                return 1  # Add operator should be first
            elif "links" in path:
                return 2  # Add link should come after add operator
        elif op_type == "remove":
            if "links" in path:
                return 3  # Remove link should come after add operator and link
            elif "operators" in path:
                return 4  # Remove operator should come last in remove operations
        elif op_type == "replace":
            return 5  # Replace operations should be last for both operators and links

        return 6  # Default priority for any unspecified operations

    # Iterate over patches from the latest to the earliest
    for patch_str in workflowPatches:
        # Parse the patch string into a list of operations (JSON Patch)
        patch_data = json.loads(patch_str)
        patch = jsonpatch.JsonPatch(patch_data)

        # Apply the patch to revert the workflow content to its previous version
        previous_version = patch.apply(current_version, in_place=False)

        # Generate a reverse patch to go from previous_version back to current_version
        reverse_patch = jsonpatch.make_patch(previous_version, current_version)

        # Sort reverse_patch.patch by the defined priority rules
        sorted_reverse_patch = sorted(reverse_patch.patch, key=get_patch_priority)

        # Add the sorted reverse patch as a sublist to reverse_patches (2D list structure)
        reverse_patches.insert(0, sorted_reverse_patch)

        # Update the current version to the previous version for the next iteration
        current_version_str = json.dumps(current_version)
        current_version = previous_version

    # After processing all patches, current_version holds the oldest version
    # reverse_patches now holds a flattened list of all patch operations from the earliest to the latest
    return json.dumps(current_version), reverse_patches


def applyWorkflowVersionPatches(workflowContent: str, patches: List[dict]) -> str:
    """
    Applies a given patch to the workflow content and returns the updated workflow content as a JSON string.

    :param workflowContent: The original workflow content as a JSON string.
    :param patch: A dictionary representing a JSON Patch.
    :return: The updated workflow content as a JSON string after applying the patch.
    """
    # Parse the workflow content JSON string into a Python dictionary
    workflow_dict = json.loads(workflowContent)

    # Create a JsonPatch object from the patch dictionary
    patch_obj = jsonpatch.JsonPatch(patches)

    # Apply the patch to the workflow content
    updated_workflow = patch_obj.apply(workflow_dict, in_place=False)

    # Convert the updated workflow back to a JSON string
    return json.dumps(updated_workflow)

def GetWorkflowToOperationMap(workflows: List[Workflow]) -> Dict[int, None | List[List[dict]]]:
    result = {}

    for workflow in workflows:
        result[workflow.wid] = None
        try:
            # Get the workflow versions
            workflowVersions = workflow.versions

            # Sort workflowVersions by creation_time from latest to earliest
            sorted_workflow_versions = sorted(workflowVersions, key=lambda wv: wv.creation_time, reverse=True)

            # Extract the patch of each version into a list of patches (not content, but actual patch sequences)
            workflow_version_patches = [wv.content for wv in sorted_workflow_versions]  # Assuming these are patches

            # Get the reverse patches, with the oldest version of the workflow content
            oldestVersion, reverse_patches = getReversePatches(workflow.content, workflow_version_patches)

        except Exception as e:
            # Catch errors in getReversePatches
            print(f"Error in getReversePatches for workflow {workflow.wid}: {e}")
            continue

        try:
            # Apply each reverse patch one by one, starting from the oldest version
            recovered_workflow_content = "{}"
            for patches in reverse_patches:
                for patch in patches:
                    recovered_workflow_content = applyWorkflowVersionPatches(recovered_workflow_content, [patch])  # Start with the oldest version
        except Exception as e:
            # Catch errors in applyWorkflowVersionPatches
            print(f"Error in applyWorkflowVersionPatches for workflow {workflow.wid}: {e}")
            continue

        original_workflow_content = workflow.content

        # Compare the equivalence between the recovered and original workflow content
        if json.loads(recovered_workflow_content) == json.loads(original_workflow_content):
            result[workflow.wid] = reverse_patches
            print(f"The recovered workflow content for workflow {workflow.wid} is equivalent to the original content.")
        else:
            result[workflow.wid] = None
            print(
                f"The recovered workflow content for workflow {workflow.wid} is NOT equivalent to the original content.")

    return result

if __name__ == "__main__":
    wid = 1800

    # Get database credentials from config
    user = Config.get('mysql', 'user')
    password = Config.get('mysql', 'password')
    host = Config.get('mysql', 'host')
    port = Config.getint('mysql', 'port')
    db = Config.get('mysql', 'db')

    # Create engine and session
    engine = getEngine(user, password, host, port, db)
    session = getSession(engine)

    workflows = getAllWorkflows(session)
    # workflows = getWorkflowByWid(session, 1921)
    results = GetWorkflowToOperationMap(workflows)

    num_of_replayable_workflow = 0
    for workflow in workflows:
        if results[workflow.wid] != None:
            num_of_replayable_workflow += 1

    print(f"total num of workflows: {len(workflows)}, num of replayable workflows: {num_of_replayable_workflow}")
