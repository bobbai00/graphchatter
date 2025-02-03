from typing import Dict
from model.DataSchema import DataSchema
from model.texera.TexeraWorkflow import TexeraWorkflow
from service.dag import *
import os
import csv

class TexeraWorkflowStats:
    def __init__(self,
                 wid: int,
                 is_schema_propagated: bool,
                 is_empty: bool,
                 is_dot: bool,
                 is_chain: bool,
                 is_tree: bool,
                 is_dag: bool,
                 num_operators: int,
                 num_edges: int):
        self.wid = wid
        self.is_schema_propagated = is_schema_propagated
        self.is_empty = is_empty
        self.is_dot = is_dot
        self.is_chain = is_chain
        self.is_tree = is_tree
        self.is_dag = is_dag
        self.num_operators = num_operators
        self.num_edges = num_edges

    def GetTopologyType(self) -> str:
        if self.is_empty:
            return "Empty"
        elif self.is_dot:
            return "Dot"
        elif self.is_chain:
            return "Chain"
        elif self.is_tree:
            return "Tree"
        elif self.is_dag:
            return "DAG"
        else:
            return "Misc"

    def __eq__(self, other) -> bool:
        if not isinstance(other, TexeraWorkflowStats):
            return False
        return (self.is_empty == other.is_empty and
                self.is_dot == other.is_dot and
                self.is_chain == other.is_chain and
                self.is_tree == other.is_tree and
                self.is_dag == other.is_dag)

    def __hash__(self) -> int:
        return hash((self.is_empty,
                     self.is_dot,
                     self.is_chain,
                     self.is_tree,
                     self.is_dag))

    def __str__(self) -> str:
        return (f"TexeraWorkflowStats("
                f"is_empty={self.is_empty}, "
                f"is_dot={self.is_dot}, "
                f"is_chain={self.is_chain}, "
                f"is_tree={self.is_tree}, "
                f"is_dag={self.is_dag}, "
                f"num_operators={self.num_operators}, "
                f"num_edges={self.num_edges})")

class TexeraStats:
    def __init__(self, total_workflows: int):
        self.total_workflows = total_workflows
        self.num_of_broken_json = 0
        self.workflowIdToStat: Dict[int, TexeraWorkflowStats] = {}
        self.operatorTypeToNextOperatorType: Dict[TexeraWorkflowStats, Dict[str, Dict[str, int]]] = {}
        self.schemaToNextOperatorType: Dict[TexeraWorkflowStats, Dict[DataSchema, Dict[str, int]]] = {}

    def exportAsCSVs(self):
        # Create the results directory
        os.makedirs('results', exist_ok=True)

        # Create and export the workflow_overview.csv file under the results directory
        with open(os.path.join('results', 'workflow_overview.csv'), 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Wid', 'TopologyType', 'NumberOfOperators', 'NumberOfEdges'])
            for wid, stats in self.workflowIdToStat.items():
                writer.writerow([wid, stats.GetTopologyType(), stats.num_operators, stats.num_edges])

        # Create directories for the second and third set of CSV files under the results directory
        os.makedirs(os.path.join('results', 'operators_type_to_operators_type'), exist_ok=True)
        os.makedirs(os.path.join('results', 'schema_to_operator_type'), exist_ok=True)

        # Export operator type to next operator type CSVs
        for topology_type in ['Dot', 'Chain', 'Tree', 'DAG', 'Misc']:
            filename = os.path.join('results', 'operators_type_to_operators_type', f'{topology_type.lower()}.csv')
            with open(filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Topology', 'OperatorType', 'NextOperatorType', 'Count'])

                for stats, operator_type_map in self.operatorTypeToNextOperatorType.items():
                    if stats.GetTopologyType().lower() == topology_type.lower():
                        for operator_type, next_operator_dict in operator_type_map.items():
                            for next_operator_type, count in next_operator_dict.items():
                                writer.writerow([topology_type, operator_type, next_operator_type, count])

        # Export schema to operator type CSVs
        for topology_type in ['Dot', 'Chain', 'Tree', 'DAG', 'Misc']:
            filename = os.path.join('results', 'schema_to_operator_type', f'{topology_type.lower()}.csv')
            with open(filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Topology', 'DataSchema', 'NextOperatorType', 'Count'])

                for stats, schema_map in self.schemaToNextOperatorType.items():
                    if stats.GetTopologyType().lower() == topology_type.lower():
                        for schema, next_operator_dict in schema_map.items():
                            for next_operator_type, count in next_operator_dict.items():
                                writer.writerow([topology_type, str(schema), next_operator_type, count])
    def incrementStats(self, workflow: TexeraWorkflow | None, is_schema_propagated: bool):
        if workflow is None:
            self.num_of_broken_json += 1
            return

        dag = workflow.GetDAG()
        workflowStat = TexeraWorkflowStats(
            wid=workflow.GetWorkflowId(),
            is_schema_propagated=is_schema_propagated,
            is_empty=isEmpty(dag),
            is_dot=isSingleDot(dag),
            is_chain=isSingleChain(dag),
            is_tree=isSingleTree(dag),
            is_dag=isSingleDAG(dag),
            num_edges=dag.number_of_edges(),
            num_operators=dag.number_of_nodes()
        )
        self.workflowIdToStat[workflow.GetWorkflowId()] = workflowStat

        # Get the operator and schema mappings from the workflow
        operatorTypeToNextOperatorTypeMapping = workflow.GetOperatorTypeToNextOperatorDistributionMapping()
        # Merge operatorTypeToNextOperatorTypeMapping into the corresponding mapping
        if workflowStat not in self.operatorTypeToNextOperatorType:
            self.operatorTypeToNextOperatorType[workflowStat] = {}
        self.mergeOperatorTypeToNextOperatorType(self.operatorTypeToNextOperatorType[workflowStat], operatorTypeToNextOperatorTypeMapping)

        # Merge schemaToNextOperatorTypeMapping into the corresponding mapping (if schema is propagated)
        if is_schema_propagated:
            schemaToNextOperatorTypeMapping = workflow.GetSchemaToNextOperatorDistributionMapping()
            if workflowStat not in self.schemaToNextOperatorType:
                self.schemaToNextOperatorType[workflowStat] = {}
            self.mergeSchemaToNextOperatorType(self.schemaToNextOperatorType[workflowStat], schemaToNextOperatorTypeMapping)

    # Method to merge another operatorTypeToNextOperatorType dict into this one
    def mergeOperatorTypeToNextOperatorType(self, existing_mapping: Dict[str, Dict[str, int]], other: Dict[str, Dict[str, int]]):
        for operator_type, next_operator_dict in other.items():
            if operator_type not in existing_mapping:
                existing_mapping[operator_type] = {}
            for next_operator_type, count in next_operator_dict.items():
                if next_operator_type not in existing_mapping[operator_type]:
                    existing_mapping[operator_type][next_operator_type] = 0
                existing_mapping[operator_type][next_operator_type] += count

    # Method to merge another schemaToNextOperatorType dict into this one
    def mergeSchemaToNextOperatorType(self, existing_mapping: Dict[DataSchema, Dict[str, int]], other: Dict[DataSchema, Dict[str, int]]):
        for schema, next_operator_dict in other.items():
            if schema not in existing_mapping:
                existing_mapping[schema] = {}
            for next_operator_type, count in next_operator_dict.items():
                if next_operator_type not in existing_mapping[schema]:
                    existing_mapping[schema][next_operator_type] = 0
                existing_mapping[schema][next_operator_type] += count

    def __str__(self) -> str:
        return (
            f"TexeraStats(\n"
            f"  Total Workflows={self.total_workflows},\n"
            f"  Operator Type to Next Operator Type={self.operatorTypeToNextOperatorType},\n"
            f"  Schema to Next Operator Type={self.schemaToNextOperatorType}\n"
            f")"
        )