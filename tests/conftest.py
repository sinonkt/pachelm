import pytest
from pachydelm.core import PachydermAdminContext
from pachydelm.cli import VERSION

@pytest.fixture(scope='module', autouse=True)
def ctx():
    return PachydermAdminContext(VERSION, './tests_stuff/migrations', './tests_stuff/configs')

@pytest.fixture(scope='module', autouse=True)
def tests_stuff():
    ctx = PachydermAdminContext(VERSION, './tests_stuff/migrations', './tests_stuff/configs')
    print('initializing...(tests_stuff)')
    try:
      ctx.pfs.create_repo('test', 'Created by pachydelm for tests purpose only.')
      ctx.create_pipeline_from_file('../../tests_stuff/configs/test-pipeline.json')
    except Exception:
      print('Something went wrong with initializing, may be because of connection issues or pipeline already exists.')