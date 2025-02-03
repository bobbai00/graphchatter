import json
from typing import List, Dict, Any, Optional


def create_port_identity(port_id: int, internal: bool = False) -> Dict[str, Any]:
    """Creates a PortIdentity dictionary."""
    return {"id": port_id, "internal": internal}


def create_input_port(port_id: int, display_name: str = "", allow_multi_links: bool = True,
                      dependencies: Optional[List[Dict]] = None) -> Dict[str, Any]:
    """Creates an InputPort dictionary."""
    return {
        "id": create_port_identity(port_id),
        "displayName": display_name,
        "allowMultiLinks": allow_multi_links,
        "dependencies": dependencies or [],
    }


def create_output_port(port_id: int, display_name: str = "", blocking: bool = False, mode: str = "SET_SNAPSHOT") -> \
Dict[str, Any]:
    """Creates an OutputPort dictionary."""
    return {
        "id": create_port_identity(port_id),
        "displayName": display_name,
        "blocking": blocking,
        "mode": {
            "value": 0 if mode == "SET_SNAPSHOT" else 1 if mode == "SET_DELTA" else 2,
            "index": 0,
            "name": mode,
            "setSnapshot": mode == "SET_SNAPSHOT",
            "unrecognized": False,
            "setDelta": mode == "SET_DELTA",
            "singleSnapshot": mode == "SINGLE_SNAPSHOT",
        },
    }


def create_operator_property(
        name: str,
        prop_type: str,
        title: str,
        description: str = "",
        default: Any = None,
        required: bool = False,
        property_order: int = 1,
        enum: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Creates a dictionary defining an operator property."""
    property_dict = {
        "propertyOrder": property_order,
        "type": prop_type,
        "title": title,
        "description": description,
    }

    if default is not None:
        property_dict["default"] = default

    if enum:
        property_dict["enum"] = enum

    return name, property_dict, required


def create_operator_metadata(
        operator_type: str,
        user_friendly_name: str,
        operator_description: str,
        operator_group_name: str,
        input_ports: List[Dict[str, Any]],
        output_ports: List[Dict[str, Any]],
        properties: List[Dict[str, Any]],
        dynamic_input_ports: bool = False,
        dynamic_output_ports: bool = False,
        support_reconfiguration: bool = False,
        allow_port_customization: bool = False,
        operator_version: str = "N/A",
) -> Dict[str, Any]:
    """Creates an Operator Metadata dictionary."""
    properties_dict = {}
    required_properties = []

    # Process properties list and separate required properties
    for name, prop_data, is_required in properties:
        properties_dict[name] = prop_data
        if is_required:
            required_properties.append(name)

    return {
            "operatorType": operator_type,
            "jsonSchema": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "additionalProperties": False,
                "properties": properties_dict,
                "required": required_properties,
            },
            "additionalMetadata": {
                "userFriendlyName": user_friendly_name,
                "operatorDescription": operator_description,
                "operatorGroupName": operator_group_name,
                "inputPorts": input_ports,
                "outputPorts": output_ports,
                "dynamicInputPorts": dynamic_input_ports,
                "dynamicOutputPorts": dynamic_output_ports,
                "supportReconfiguration": support_reconfiguration,
                "allowPortCustomization": allow_port_customization,
            },
            "operatorVersion": operator_version,
    }


def create_group(group_name: str, children: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """Creates a Group dictionary."""
    return {"groupName": group_name, "children": children}