from sys import argv

from .base import *

try:
    from .local import *
except ImportError:
    pass

if 'nosetests' in argv[0]:
    from .test import *