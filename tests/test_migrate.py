import pytest
from pachydelm.migration import PachydermMigration

def test_answer(ctx):
    # print(ctx)
    # print(ctx.migrations)
    migration = PachydermMigration(ctx)
    # print(ctx.pachydermConfigs)
    # print(ctx.find_pipeline_config('test-pipeline'))
    # migration.diff('test-pipeline', ctx.find_pipeline_config('test-pipeline'))
    # diff(ctx)
    assert 5 == 5
    # print(
    # print(verify_is_pipeline_exists(ctx, 'test-pipeline'))
    # jobs = list(ctx.pps.list_job('test-pipeline').job_info)
    # list(map(lambda x: x, jobs))
    # pipelines = list(ctx.pps.list_pipeline().pipeline_info)
    # print(jobs)
    # print(pipelines)
    
    # print(jobs[0].state)
    # for job in jobs:
    #     print(job)
    # print(list(map(jobs.job_info, lambda x: x.state)))
    # print((jobs, lambda x: x.state)))
    # for job in enumerate(jobs):
    #     print(job)

    # filePath = '%s/eiei.json' % (ctx.pachydermConfigsDir)
    # migration.create_pipeline_from_file(filePath)