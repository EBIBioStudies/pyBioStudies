import os

BASE_DIRECTORY = ''
LOAD_DIR = ''
SUBMISSION_TOOL_PATH = os.path.abspath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'resources', 'BS_SubmissionTool.jar'))
ARRAY_DIR = ''
EXPORTER_SCRIPT = ''

SUBMISSION_USERNAME = ''
SUBMISSION_PASSWORD = ''
SUBMISSION_SERVER = ''
AE2 = {
    'name': '',
    'host': '',
    'port': '',
    'username': '',
    'password': '',

}
ENA = {
    'name': 'ERAPRO',
    'host': 'ora-vm-009.ebi.ac.uk',
    'port': '1541',
    'username': '',
    'password': ''

}