import yaml

def load_config(file_path='config/constants.yaml'):
    """
    Load the YAML configuration file.

    :param file_path: Path to the YAML configuration file
    :return: Parsed configuration dictionary
    """
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config
