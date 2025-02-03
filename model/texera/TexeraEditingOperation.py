from typing import Tuple, List, Dict, Any

import json
import jsonpatch

from config.config import Config
from data.texera.texera_db import getEngine, getSession, getWorkflowByWid, getAllWorkflows
from model.EditingOperation import EditingOperation, EditingOperationType
from model.Operator import Operator
from model.Port import Port
from model.Workflow import Workflow
from model.texera.TexeraOperator import TexeraOperator
from model.texera.TexeraPort import TexeraPort
from model.texera.TexeraWorkflow import TexeraWorkflow
from service.workflow_json_patch import GetWorkflowToOperationMap, applyWorkflowVersionPatches

MiscPaths = ['settings', 'operatorPositions', 'commentBoxes', 'groups']


class TexeraEditingOperation(EditingOperation):
    def __init__(self, workflow_content: str, updated_workflow_content: str, patch: dict):
        # for add operator op
        self.added_operators = None
        # for remove operator op
        self.removed_operator = None
        # for update_operator op
        self.operator_before_change = None
        self.operator_after_change = None
        # for add link op
        self.source_operators = None
        self.source_ports = None
        self.target_operators = None
        self.target_ports = None
        # for remove link op
        self.source_operator = None
        self.source_port = None
        self.target_operator = None
        self.target_port = None
        # for update link op
        self.source_operator_before = None
        self.source_port_before = None
        self.target_operator_before = None
        self.target_port_before = None
        self.source_operator_after = None
        self.source_port_after = None
        self.target_operator_after = None
        self.target_port_after = None

        self.raw_patch = patch
        self.workflow_content = workflow_content
        self.operationType = EditingOperationType.Void
        self.updated_workflow_content = updated_workflow_content
        # self.updated_workflow_content, is_equal = self.apply_patch_and_compare(workflow_content, patch)
        self.is_valid = True

        path = patch['path']
        try:
            if any(p in path for p in MiscPaths):
                self.operationType = EditingOperationType.Misc
            else:
                op_type = patch['op']
                if 'operators' in path:
                    # Handle operator operations
                    if op_type == 'add':
                        self.operationType = EditingOperationType.AddOperator
                        self.handle_add_operator(patch)
                    elif op_type == 'remove':
                        self.operationType = EditingOperationType.RemoveOperator
                        self.handle_remove_operator(path)
                    elif op_type == 'replace' or op_type == 'move':
                        self.operationType = EditingOperationType.UpdateOperator
                        self.handle_update_operator(path, patch)

                elif 'links' in path:
                    # Handle link operations
                    if op_type == 'add':
                        self.operationType = EditingOperationType.AddLink
                        self.handle_add_link(patch)
                    elif op_type == 'remove':
                        self.operationType = EditingOperationType.RemoveLink
                        self.handle_remove_link(path)
                    elif op_type == 'replace':
                        self.operationType = EditingOperationType.UpdateLink
                        self.handle_update_link(path, patch)
        except Exception as e:
            print(f"Error: parsing the {self.operationType} operation failed")
            self.is_valid = False

        # if self.operationType == EditingOperationType.Void:
        #     print("pause here")


    def apply_patch_and_compare(self, workflow_content: str, patch: dict) -> Tuple[str, bool]:
        """Applies the patch to the workflow content and returns updated content and comparison result."""
        original_content = json.loads(workflow_content)
        # Create a JsonPatch object from the patch dictionary
        patch_obj = jsonpatch.JsonPatch([patch])
        patched_content = patch_obj.apply(original_content, in_place=False)
        is_equal = original_content == patched_content
        return json.dumps(patched_content), is_equal

    def handle_add_operator(self, patch: dict) -> None:
        """Extracts added operator(s) as a list."""
        operator = patch['value']  # Extract operator dict from patch value
        if isinstance(operator, list):
            self.added_operators: List[Dict[str, Any]] = operator  # Multiple operators added
        else:
            self.added_operators: List[Dict[str, Any]] = [operator]  # Single operator added

    def handle_remove_operator(self, path: str) -> None:
        """Extracts removed operator(s) as a list from the workflow content."""
        operator_index = self.extract_operator_index_from_path(path)
        workflow = json.loads(self.workflow_content)
        self.removed_operator: Dict[str, Any] = workflow['operators'][operator_index]  # Get operator before removal

    def handle_update_operator(self, path: str, patch: dict) -> None:
        """Extracts modified operator(s) before and after the change."""
        operator_index = self.extract_operator_index_from_path(path)
        workflow = json.loads(self.workflow_content)
        self.operator_before_change: Dict[str, Any] = workflow['operators'][operator_index]  # Before change
        patched_workflow = json.loads(self.updated_workflow_content)
        self.operator_after_change: Dict[str, Any] = patched_workflow['operators'][operator_index]  # After change

    def handle_add_link(self, patch: dict) -> None:
        """Extracts the source and target operator and ports for an added link."""
        self.source_operators = []  # List to store source operators
        self.source_ports = []  # List to store source ports

        self.target_operators = []  # List to store target operators
        self.target_ports = []  # List to store target ports

        link_value = patch['value']  # Extract link dict from patch
        # Ensure links is always a list (single value wrapped into a list if necessary)
        links = [link_value] if not isinstance(link_value, list) else link_value

        # Iterate over each link
        for link in links:
            # Extract source and target operator and port
            source_operator, source_port = self.extract_operator_and_port(link['source'], True)
            target_operator, target_port = self.extract_operator_and_port(link['target'], True)

            # Add to corresponding lists
            self.source_operators.append(source_operator)
            self.source_ports.append(source_port)
            self.target_operators.append(target_operator)
            self.target_ports.append(target_port)

    def handle_remove_link(self, path: str) -> None:
        """Extracts the source and target operator and ports for a removed link."""
        link_index = self.extract_link_index_from_path(path)
        workflow = json.loads(self.workflow_content)
        link = workflow['links'][link_index]  # Get link before removal
        self.source_operator, self.source_port = self.extract_operator_and_port(link['source'], False)
        self.target_operator, self.target_port = self.extract_operator_and_port(link['target'], False)

    def handle_update_link(self, path: str, patch: dict) -> None:
        """Extracts the source and target operator and ports before and after modification."""
        link_index = self.extract_link_index_from_path(path)
        workflow = json.loads(self.workflow_content)
        patched_workflow = json.loads(self.updated_workflow_content)

        link_before = workflow['links'][link_index]  # Before modification
        link_after = patched_workflow['links'][link_index]  # After modification

        self.source_operator_before, self.source_port_before = self.extract_operator_and_port(link_before['source'], False)
        self.target_operator_before, self.target_port_before = self.extract_operator_and_port(link_before['target'], False)

        self.source_operator_after, self.source_port_after = self.extract_operator_and_port(link_after['source'], True)
        self.target_operator_after, self.target_port_after = self.extract_operator_and_port(link_after['target'], True)

    def extract_operator_index_from_path(self, path: str) -> int:
        """Extracts the operator index from the path."""
        # Extract the index from something like '/operators/0'
        return int(path.split('/')[2])

    def extract_link_index_from_path(self, path: str) -> int:
        """Extracts the link index from the path."""
        return int(path.split('/')[2])

    def extract_operator_and_port(self, source_or_target: dict, from_updated_workflow: bool) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Extracts the operator and port dicts from source/target in the link."""
        operator_id = source_or_target['operatorID']
        port_id = source_or_target['portID']

        if from_updated_workflow:
            workflow = json.loads(self.updated_workflow_content)
        else:
            workflow = json.loads(self.workflow_content)

        operator = next(op for op in workflow['operators'] if op['operatorID'] == operator_id)
        if port_id.startswith('output'):
            port = next(port for port in operator['outputPorts'] if port['portID'] == port_id)
        else:
            port = next(port for port in operator['inputPorts'] if port['portID'] == port_id)
        return operator, port

    def GetBaseWorkflow(self) -> Workflow:
        return TexeraWorkflow(
            self.workflow_content,
        )

    def GetBase(self) -> (None
                          | List[Operator]
                          | List[Tuple[Tuple[Operator, Port], Tuple[Operator, Port]]]):
        """
        Return the base of this operation
        add operator(s) -> None
        remove operator(s) -> List[Operator]
        modify operator(s) -> List[Operator]

        add link -> List[Tuple[Tuple[Operator, Port], Tuple[Operator, Port]]]
        remove link -> List[Tuple[Tuple[Operator, Port], Tuple[Operator, Port]]]
        modify link -> List[Tuple[Tuple[Operator, Port], Tuple[Operator, Port]]]
        """

        if self.operationType is EditingOperationType.Misc or self.operationType is EditingOperationType.Unchanged:
            return None
        if self.operationType is EditingOperationType.AddOperator:
            # return nothing
            return None
        if self.operationType is EditingOperationType.RemoveOperator:
            # return the operators being removed
            return [TexeraOperator(op_dict) for op_dict in [self.removed_operator]]
        if self.operationType is EditingOperationType.UpdateOperator:
            # return the operators going to be updated
            return [TexeraOperator(op_dict) for op_dict in [self.operator_before_change]]
        if self.operationType is EditingOperationType.UpdateLink:
            # return list of ((src_op_before, src_port_before), (tgt_op_before, tgt_port_before))
            source_op_and_port_before_list = [(self.source_operator_before, self.source_port_before)]
            target_op_and_port_before_list = [(self.target_operator_before, self.target_port_before)]
            return [((src_operator := TexeraOperator(src_op), TexeraPort(src_port, False, src_operator)),
                     (tgt_operator := TexeraOperator(tgt_op), TexeraPort(tgt_port, True, tgt_operator)))
                    for (src_op, src_port), (tgt_op, tgt_port) in zip(source_op_and_port_before_list, target_op_and_port_before_list)]

        if self.operationType is EditingOperationType.AddLink or self.operationType is EditingOperationType.RemoveLink:
            # return list of ((src_op, src_port), (tgt_op, tgt_port))
            source_op_and_port_list = list(zip(self.source_operators, self.source_ports))
            target_op_and_port_list = list(zip(self.target_operators, self.target_ports))
            return [((src_operator := TexeraOperator(src_op), TexeraPort(src_port, False, src_operator)),
                     (tgt_operator := TexeraOperator(tgt_op), TexeraPort(tgt_port, True, tgt_operator)))
                    for (src_op, src_port), (tgt_op, tgt_port) in
                    zip(source_op_and_port_list, target_op_and_port_list)]

    def GetModification(self) -> (None
                                  | List[Operator]
                                  | List[Tuple[Tuple[Operator, Port], Tuple[Operator, Port]]]):
        """
        Return "what's new" brought by this patch.

        add operator(s) -> List[Operator]
        remove operator(s) -> None
        modify operator(s) -> List[Operator]

        add link -> None
        remove link -> None
        modify link -> List[Tuple[Tuple[Operator, Port], Tuple[Operator, Port]]]
        """
        if self.operationType is EditingOperationType.Misc or self.operationType is EditingOperationType.Unchanged:
            return None
        if self.operationType is EditingOperationType.AddOperator:
            return [TexeraOperator(op_dict) for op_dict in self.added_operators]
        if self.operationType is EditingOperationType.RemoveOperator:
            return None
        if self.operationType is EditingOperationType.UpdateOperator:
            return [TexeraOperator(op_dict) for op_dict in [self.operator_after_change]]
        if self.operationType is EditingOperationType.AddLink or self.operationType is EditingOperationType.RemoveLink:
            return None
        if self.operationType is EditingOperationType.UpdateLink:
            # return list of ((src_op_before, src_port_before), (tgt_op_before, tgt_port_before))
            source_op_and_port_after_list = [(self.source_operator_after, self.source_port_after)]
            target_op_and_port_after_list = [(self.target_operator_after, self.target_port_after)]
            return [((src_operator := TexeraOperator(src_op), TexeraPort(src_port, False, src_operator)),
                     (tgt_operator := TexeraOperator(tgt_op), TexeraPort(tgt_port, True, tgt_operator)))
                    for (src_op, src_port), (tgt_op, tgt_port) in
                    zip(source_op_and_port_after_list, target_op_and_port_after_list)]


    def GetType(self) -> EditingOperationType:
        """
        Return the type of the operation
        """
        return self.operationType

    def GetRawPatch(self) -> dict:
        return self.raw_patch

    def IsValid(self) -> bool:
        return self.is_valid

def getTexeraEditingOperations(patches: list[list[dict]], workflow_initial_content: str = "{}") -> list[TexeraEditingOperation]:
    workflow_content = workflow_initial_content
    workflow_editing_operations = []
    for patch_batch in patches:
        # consider each batch as an atomic patch
        updated_workflow_content = applyWorkflowVersionPatches(workflow_content, patch_batch)
        for patch in patch_batch:
            operation = TexeraEditingOperation(workflow_content, updated_workflow_content, patch)
            workflow_editing_operations.append(operation)
        workflow_content = applyWorkflowVersionPatches(workflow_content, patch_batch)

    return workflow_editing_operations

def getTexeraEditingOperationSummary(wid: int, operations: list[TexeraEditingOperation] | None) -> dict:
    operation_type_counts = {
        EditingOperationType.AddOperator: 0,
        EditingOperationType.RemoveOperator: 0,
        EditingOperationType.UpdateOperator: 0,
        EditingOperationType.AddLink: 0,
        EditingOperationType.RemoveLink: 0,
        EditingOperationType.UpdateLink: 0,
        EditingOperationType.Misc: 0,
        EditingOperationType.Unchanged: 0,
        EditingOperationType.Void: 0
    }

    if operations == None:
        return {
            "wid": wid,
            "is_equivalent": False,
            "total_operations": 0,
            "valid_operations": 0,
            "operation_counts": operation_type_counts,
        }

    # TODO: process the operation types
    # Populate counts based on valid operations
    for op in operations:
        operation_type = op.GetType()
        operation_type_counts[operation_type] += 1

    summary = {
        "wid": wid,
        "is_equivalent": True,
        "total_operations": len(operations),
        "valid_operations": len([op for op in operations if op.IsValid()]),
        "operation_counts": operation_type_counts
    }

    return summary

if __name__ == "__main__":
    # Get database credentials from config
    user = Config.get('mysql', 'user')
    password = Config.get('mysql', 'password')
    host = Config.get('mysql', 'host')
    port = Config.getint('mysql', 'port')
    db = Config.get('mysql', 'db')

    # Create engine and session
    engine = getEngine(user, password, host, port, db)
    session = getSession(engine)

    # workflows = getAllWorkflows(session)
    # workflows = getAllWorkflows(session)
    workflows = getWorkflowByWid(session, 1866)
    parseResult = GetWorkflowToOperationMap(workflows)
    patches = parseResult[1866]

    workflowEditingOperations = getTexeraEditingOperations(patches)

    summary = getTexeraEditingOperationSummary(1866, workflowEditingOperations)
    # Output the summary
    print(json.dumps(summary, indent=4))






