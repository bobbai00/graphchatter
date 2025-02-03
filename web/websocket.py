import asyncio
import json
import logging
from datetime import datetime
from fastapi import WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from typing import Dict, Optional, List, Union

logging.basicConfig(level=logging.INFO)

# In-memory session state storage
session_state: Dict[str, "SessionState"] = {}


# -------------------- WebSocket Request Models --------------------

class TexeraWebSocketRequest(BaseModel):
    type: str


class HeartBeatRequest(TexeraWebSocketRequest):
    type: str = "HeartBeatRequest"


class ResultPaginationRequest(TexeraWebSocketRequest):
    type: str = "ResultPaginationRequest"
    requestID: str
    operatorID: str
    pageIndex: int
    pageSize: int


class ReplayExecutionInfo(BaseModel):
    eid: int
    interaction: str


class WorkflowExecuteRequest(TexeraWebSocketRequest):
    type: str = "WorkflowExecuteRequest"
    executionName: str
    engineVersion: str
    logicalPlan: dict
    replayFromExecution: Optional[ReplayExecutionInfo] = None
    workflowSettings: dict
    emailNotificationEnabled: bool


# -------------------- WebSocket Event Models (Responses) --------------------

class TexeraWebSocketEvent(BaseModel):
    type: str


class HeartBeatResponse(TexeraWebSocketEvent):
    type: str = "HeartBeatResponse"


class WorkflowFatalError(BaseModel):
    message: str
    details: str
    operatorId: str
    workerId: str
    error_type: str
    timestamp: Dict[str, int]  # { "nanos": int, "seconds": int }


class WorkflowErrorEvent(TexeraWebSocketEvent):
    type: str = "WorkflowErrorEvent"
    fatalErrors: List[WorkflowFatalError]


class OperatorAggregatedMetrics(BaseModel):
    operatorState: str
    aggregatedInputRowCount: int
    aggregatedOutputRowCount: int
    numWorkers: int
    aggregatedDataProcessingTime: int
    aggregatedControlProcessingTime: int
    aggregatedIdleTime: int


class OperatorStatisticsUpdateEvent(TexeraWebSocketEvent):
    type: str = "OperatorStatisticsUpdateEvent"
    operatorStatistics: Dict[str, OperatorAggregatedMetrics]


class ClusterStatusUpdateEvent(TexeraWebSocketEvent):
    type: str = "ClusterStatusUpdateEvent"
    numWorkers: int


class PaginatedResultEvent(TexeraWebSocketEvent):
    type: str = "PaginatedResultEvent"
    requestID: str
    operatorID: str
    pageIndex: int
    table: List[dict]
    schema: List[dict]


class ModifyLogicResponse(TexeraWebSocketEvent):
    type: str = "ModifyLogicResponse"
    opId: str
    isValid: bool
    errorMessage: str


# -------------------- Session State --------------------
class SessionState:
    def __init__(self, websocket: WebSocket):
        self.websocket = websocket

    async def send(self, message: dict):
        """Sends a raw JSON message over WebSocket."""
        await self.websocket.send_text(json.dumps(message))

    async def send_event(self, event: TexeraWebSocketEvent):
        """Sends a Texera WebSocket event."""
        await self.send(event.dict())  # Convert event to JSON

# -------------------- WebSocket Connection Handling --------------------

async def websocket_endpoint(websocket: WebSocket):
    """Handles incoming WebSocket connections and requests."""
    await websocket.accept()
    session_id = websocket.headers.get("sec-websocket-key", str(id(websocket)))
    session_state[session_id] = SessionState(websocket)

    logging.info(f"New WebSocket connection established: {session_id}")

    # Send ClusterStatusUpdateEvent with numWorkers = 1
    cluster_status_event = ClusterStatusUpdateEvent(type="ClusterStatusUpdateEvent", numWorkers=1)
    await session_state[session_id].send_event(cluster_status_event)

    try:
        while True:
            message = await websocket.receive_text()
            request_data = json.loads(message)
            request_type = request_data.get("type")

            response = await handle_websocket_request(session_id, request_type, request_data)
            if response:
                await session_state[session_id].send(response)

    except WebSocketDisconnect:
        logging.info(f"WebSocket connection closed: {session_id}")
        session_state.pop(session_id, None)
    except Exception as e:
        logging.error(f"Error handling WebSocket message: {e}")


# -------------------- WebSocket Message Handling --------------------

async def handle_websocket_request(session_id: str, request_type: str, request_data: dict):
    """Processes WebSocket messages based on request type."""
    if request_type == "HeartBeatRequest":
        return HeartBeatResponse().dict()

    elif request_type == "ResultPaginationRequest":
        request = ResultPaginationRequest(**request_data)
        return PaginatedResultEvent(
            requestID=request.requestID,
            operatorID=request.operatorID,
            pageIndex=request.pageIndex,
            table=[],  # Placeholder for result data
            schema=[]  # Placeholder for schema attributes
        ).dict()

    elif request_type == "WorkflowExecuteRequest":
        request = WorkflowExecuteRequest(**request_data)
        print(request)
        # TODO: call the pykka engine
        return {
            "type": "WorkflowExecutionStarted",
            "executionName": request.executionName,
            "engineVersion": request.engineVersion
        }

    return {
        "type": "WorkflowErrorEvent",
        "fatalErrors": [
            {
                "message": "Unknown request type",
                "details": f"Request type {request_type} is not recognized",
                "operatorId": "unknown",
                "workerId": "unknown",
                "error_type": "COMPILATION_ERROR",
                "timestamp": {"nanos": 0, "seconds": int(datetime.utcnow().timestamp())}
            }
        ]
    }
