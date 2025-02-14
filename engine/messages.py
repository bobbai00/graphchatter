from pydantic import BaseModel


class WorkerExecutionStart(BaseModel):
    type: str = "WorkerExecutionStart"

class WorkerAssignment():
    def __init__(self, worker, operator, workflow):
        self.worker = worker
        self.operator = operator
        self.opID = self.operator.GetId()
        self.workflow = workflow
        self.upstreams = self.upstreams()
        self.downstreams = self.downstreams()

    def upstreams(self):
        return list(self.workflow.DAG.predecessors(self.opID))

    def downstreams(self):
        return list(self.workflow.DAG.successors(self.opID))


class ExecutionResult():
    def __init__(self, host, port, op_id, result):
        self.host = host
        self.port = port
        self.op_id = op_id
        self.result = result

class ControllerTermination(BaseModel):
    type:str = "ControllerTermination"