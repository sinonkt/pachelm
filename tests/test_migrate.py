import pytest
from pachydelm.migration import PachydermMigration

updated_config_path = './tests/updated_configs/2019_06_04_221735_test-pipeline_pipeline_test-pipeline.json'

def test_get_pipeline(ctx):
    emptyMigration = PachydermMigration(ctx)
    assert emptyMigration.get_pipeline('test-pipeline') != None

def test_not_exist_pipeline(ctx):
    emptyMigration = PachydermMigration(ctx)
    assert emptyMigration.get_pipeline('not-existed-pipeline') == None

def test_diff(ctx):
    emptyMigration = PachydermMigration(ctx)
    testPipeline = emptyMigration.get_pipeline('test-pipeline')
    diff = emptyMigration.diff('test-pipeline', updated_config_path)
    expected_changed = {
        'values_changed': {
            "root['parallelism_spec']['constant']": {
                'new_value': 3,
                'old_value': 1
            },
            "root['version']": {
                'new_value': 2,
                'old_value': 1
            }
        }
    }
    assert diff == expected_changed

def test_none_diff_compare_to_current_config(ctx):
    emptyMigration = PachydermMigration(ctx)
    diff = emptyMigration.diff('test-pipeline')
    assert diff == {}
