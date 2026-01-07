from __future__ import annotations
from dataclasses import fields
import os

#----------------------------------------------------------------------------------
# env
#----------------------------------------------------------------------------------
def required_value(name: str) -> str:
    """
    Fetch required environment variable or fail fast with a clear error.
    """
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

def to_connector_kwarg_utils(self)-> dict:          
    att_dict = {}
    for field in fields(self):
        name = field.name
        value = getattr(self, name)
        att_dict[f'{name}'] = f'{value}'
    return att_dict