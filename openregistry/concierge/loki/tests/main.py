import os
import pytest

ROOT = '/'.join(os.path.dirname(__file__).split('/')[:-3])
COVER_PACKAGE = '.'.join(__package__.split('.')[:-1])


def suite(*args):
    source = [ROOT, '--cov={}'.format(COVER_PACKAGE)]
    for arg in args:
        source.append(arg)
    pytest.main(source)
