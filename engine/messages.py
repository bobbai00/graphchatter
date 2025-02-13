from pydantic import BaseModel


class WorkerExecutionStart(BaseModel):
    type: str = "WorkerExecutionStart"

class WorkerAssignment():
    def __init__(self, worker, operator, workflow):
        self.worker = worker
        self.operator = operator
        self.workflow = workflow


class ExecutionResult():
    def __init__(self, worker, op_id, result):
        worker: dict
        op_id: str
        result: str

class ControllerTermination(BaseModel):
    type:str = "ControllerTermination"