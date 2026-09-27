from pathlib import Path

def get_project_path():
    """
    returns projectPath
    .../aws/
    :return: path as String
    """
    p = Path(__file__)
    plist = str(p).split('aws')
    project_base_path = plist[0].replace("\\","/") + 'aws/'
    return project_base_path

def add_path_to_project_path(path_to_be_added):
    """
    add String path to Prohect path
    :param path_to_be_added: String path to be added
    :return: new path
    """
    return get_project_path()+path_to_be_added
