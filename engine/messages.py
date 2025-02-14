from pydantic import BaseModel


class WorkerExecutionStart(BaseModel):
    type: str = "WorkerExecutionStart"

class WorkerAssignment():
    def __init__(self, worker, operator, workflow):
        self.worker = worker
        self.operator = operator
        self.workflow = workflow
        self.upstreams = self.upstreams()
        self.downstreams = self.downstreams()

    def upstreams(self):
        return self.workflow.DAG.predecessors(self.operator.GetId())

    def downstreams(self):
        return self.workflow.DAG.successors(self.operator.GetId())


class ExecutionResult():
    def __init__(self, worker, op_id, result):
        worker: dict
        op_id: str
        result: str

class ControllerTermination(BaseModel):
    type:str = "ControllerTermination"