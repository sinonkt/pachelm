import click
import json
import python_pachyderm
import python_pachyderm.client.pps.pps_pb2 as proto

from pachydelm.core import PachydermMigration

from google.protobuf import json_format

# fields = [field.name for field in proto.PipelineInfo.DESCRIPTOR.fields]
# deprecated: scale_down_threshold
# no usecases/docs: hashtree_spec

fields = [
    "transform", "parallelism_spec", "egress", "update",
    "output_branch", "resource_requests",
    "resource_limits", "input", "description", "cache_size", "enable_stats",
    "reprocess", "batch", "max_queue_size", "service", "chunk_spec",
    "datum_timeout", "job_timeout", "salt", "standby", "datum_tries",
    "scheduling_spec", "pod_spec", "pod_patch"
]

def verify_is_pipeline_exists(ctx, pipeline):
    try:
        ctx.pps.inspect_pipeline()
        return True
    except Exception:
        return False

@click.command()
@click.pass_obj
def test(ctx):
    """ test module"""
    click.echo('test something')
    migration = PachydermMigration(ctx)

    print(verify_is_pipeline_exists(ctx, 'test-pipeline'))

    jobs = list(ctx.pps.list_job('test-pipeline').job_info)
    # print(jobs[0].state)
    list(map(lambda x: x, jobs))
    pipelines = list(ctx.pps.list_pipeline().pipeline_info)
    print(jobs)
    print(pipelines)
    # for job in jobs:
    #     print(job)
    # print(list(map(jobs.job_info, lambda x: x.state)))
    # print((jobs, lambda x: x.state)))
    # for job in enumerate(jobs):
    #     print(job)

    # filePath = '%s/eiei.json' % (ctx.pachydermConfigsDir)
    # migration.create_pipeline_from_file(filePath)

# def create_pipeline(	self,
#  pipeline_name,
#   transform=None,
#   parallelism_spec=None,
#   hashtree_spec=None,
#   egress=None,
#   update=None,
#   output_branch=None,
#   scale_down_threshold=None,
#   resource_requests=None,
#   resource_limits=None,
#   input=None,
#   description=None,
#   cache_size=None,
#   enable_stats=None,
#   reprocess=None,
#   batch=None,
#   max_queue_size=None,
#   service=None,
#   chunk_spec=None,
#   datum_timeout=None,
#   job_timeout=None,
#   salt=None,
#   standby=None,
#   datum_tries=None,
#   scheduling_spec=None,
#   pod_spec=None,
#   pod_patch=None
# def create_pipeline(	self, pipeline_name, transform=None, parallelism_spec=None, hashtree_spec=None, egress=None, update=None, output_branch=None, scale_down_threshold=None, resource_requests=None, resource_limits=None, input=None, description=None, cache_size=None, enable_stats=None, reprocess=None, batch=None, max_queue_size=None, service=None, chunk_spec=None, datum_timeout=None, job_timeout=None, salt=None, standby=None, datum_tries=None, scheduling_spec=None, pod_spec=None, pod_patch=None)
# json_format.Parse(json.loads(json.dumps(response).get('status')), apiai_pb2.Status, ignore_unknown_fields=False)
