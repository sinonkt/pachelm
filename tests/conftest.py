import pytest
from pachydelm.core import PachydermAdminContext
from pachydelm.migration import PachydermMigration
from pachydelm.cli import VERSION

testPipelineConfig = './tests_stuff/configs/2019_06_04_221735_test-pipeline_pipeline_test-pipeline.json'
migrationsDir = './tests_stuff/migrations'
configsDir = './tests_stuff/configs'
testRepo = [ 'test', 'Created by pachydelm for tests purpose only.' ] 

@pytest.fixture(scope='module')
def ctx(request):
    ctx = PachydermAdminContext(VERSION, migrationsDir, configsDir)
    emptyMigration = PachydermMigration(ctx)
    print('initializing...(tests_stuff)')
    # try:
    emptyMigration.create_repo(*testRepo)
    emptyMigration.create_pipeline_from_file(testPipelineConfig)
    # except Exception:
    #   print('Something went wrong with initializing, may be because of connection issues or pipeline already exists.')
    def finalizer():
        print("Teardown test stuffs...")
        emptyMigration.delete_pipeline_from_file(testPipelineConfig)
        emptyMigration.delete_repo(testRepo[0])
    request.addfinalizer(finalizer)
    return ctx 