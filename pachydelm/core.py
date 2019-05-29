import abc
import json
import subprocess
from python_pachyderm import PfsClient, PpsClient
import python_pachyderm.client.pps.pps_pb2 as proto
from jinja2 import Environment, PackageLoader, FileSystemLoader, select_autoescape
from pachydelm.utils import list_files, extract_filename_to_migration

from google.protobuf import json_format

fields = [
    "transform", "parallelism_spec", "egress", "update",
    "output_branch", "resource_requests",
    "resource_limits", "input", "description", "cache_size", "enable_stats",
    "reprocess", "batch", "max_queue_size", "service", "chunk_spec",
    "datum_timeout", "job_timeout", "salt", "standby", "datum_tries",
    "scheduling_spec", "pod_spec", "pod_patch"
]
  
# don't forget to sort .sort(key='time')
def from_dir_to_sorted_migrations(dirPath):
  files = list_files(dirPath)
  prepend_dir_path_to_migration = lambda m: { **m, 'path': '%s/%s' % (dirPath, m['path']) }
  migrations = map(extract_filename_to_migration, files)
  prepended_migrations = map(prepend_dir_path_to_migration, migrations)
  return list(prepended_migrations)

class PachydermAdminContext(object):
   'Singleton PachydermClient'
   def __init__(self, version, migrationsdir, pachydermconfigsdir):
      self.pfs = PfsClient()
      self.pps = PpsClient()
      self.version = version
      self.migrationsDir = migrationsdir
      self.migrations = from_dir_to_sorted_migrations(migrationsdir)
      self.pachydermConfigsDir = pachydermconfigsdir
      self.pachydermConfigs = from_dir_to_sorted_migrations(pachydermconfigsdir)
      self.env = Environment(
        loader=FileSystemLoader('pachydelm/templates', followlinks=True),
        autoescape=select_autoescape(['py'])
      )

class PachydermMigration(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, ctx):
      self.pfs = ctx.pfs
      self.pps = ctx.pps

    @abc.abstractmethod
    def up(self):
      """ Setup """
      return

    @abc.abstractmethod
    def down(self):
      """ Teardown """
      return

    def create_pipeline_from_file(self, filePath):
      """ Create Pipeline from pachyderm pipeline json config file. """
      with open(filePath) as f:
          pipelineConfig = json.load(f)

      parsed = json_format.ParseDict(pipelineConfig, proto.PipelineInfo())
      pipelineName = parsed.pipeline.name
      configDict = {}
      for field in fields:
          if hasattr(parsed, field) and getattr(parsed, field):
            configDict[field] = getattr(parsed, field)
      try:
        self.pps.create_pipeline(pipelineName, **configDict)
      except Exception:
        print('Something went wrong...(May be pipeline `%s` already exists)' % (pipelineName))