import json
from typing import Tuple, List

from model.EditingOperation import EditingOperationType
from model.Operator import Operator
from model.Port import Port
from model.texera.TexeraEditingOperation import TexeraEditingOperation
from model.texera.TexeraWorkflow import TexeraWorkflow


class TexeraEditingAdditionPair:
    def __init__(self, operation: TexeraEditingOperation):
        self.isAdditionPair = False
        if (operation.GetType() is not EditingOperationType.AddLink and operation.GetType() is not EditingOperationType.UpdateLink) or (
        not operation.is_valid):
            return
        self.operation = operation
        self.isAdditionPair = True

    def GetAdditionPairs(self) -> List[Tuple[Tuple[Operator, Port], Tuple[Operator, Port]]]:
        if not self.isAdditionPair:
            raise Exception("The pair is not addition pair")
        if self.operation.GetType() == EditingOperationType.AddLink:
            return self.operation.GetBase()
        else:
            return self.operation.GetModification()

    def IsValid(self) -> bool:
        return self.isAdditionPair


def getAdditionPairFromEditingOperations(operations: list[TexeraEditingOperation]) -> List[
    Tuple[Tuple[Operator, Port], Tuple[Operator, Port]]]:
    result: List[Tuple[Tuple[Operator, Port], Tuple[Operator, Port]]] = []
    for operation in operations:
        pair = TexeraEditingAdditionPair(operation)
        if pair.IsValid():
            result.extend(pair.GetAdditionPairs())

    return result

def getAdditionPairFromWorkflowDAGs(wid_to_workflow_content: dict[int, str]) -> List[
    Tuple[Tuple[Operator, Port], Tuple[Operator, Port]]]:
    result: List[Tuple[Tuple[Operator, Port], Tuple[Operator, Port]]] = []

    for wid_and_workflow in wid_to_workflow_content.items():
        wid = wid_and_workflow[0]
        workflow_content = wid_and_workflow[1]
        workflow = TexeraWorkflow(
            workflow_content=workflow_content,
            wid = wid
        )
        result.extend(workflow.GetAdditionPairs())
    return result


