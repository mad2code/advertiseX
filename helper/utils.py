import os
import avro.schema
import importlib.util

def get_env_var(var_name, default=None):
    """
    Get the value of an environment variable.

    :param var_name: Name of the environment variable
    :param default: Default value if the environment variable is not set
    :return: Value of the environment variable or the default value
    """
    return os.getenv(var_name, default)

def read_avro_schema(file_path):
    """
    Read an Avro schema file and return it as a string.

    :param file_path: Path to the Avro schema file
    :return: Avro schema as a string
    """
    with open(file_path, 'r') as file:
        schema = avro.schema.Parse(file.read())
    return str(schema)

def module_from_file(file_path, module_name, object_name=None):
    """ 
    Load a module from a file path. 
    
    Args:
        file_path (str): The path to the file containing the module. eg. 'path/to/module.py'
        module_name (str): The name of the module to load. eg. 'module'
        object_name (str, optional): The name of the object to return from the module. eg. 'Class'. Defaults to None.

    Returns:
        module: The loaded module.

    Check the usage in the wig2__ingestion.py DAG for reference.
    """
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if object_name and hasattr(module, object_name):
        return getattr(module, object_name)() 
    return module
