import re
import json
import subprocess
from os.path import join
from operator import itemgetter
from python_pachyderm import PfsClient, PpsClient
import python_pachyderm.client.pps.pps_pb2 as proto
from jinja2 import Environment, PackageLoader, FileSystemLoader, select_autoescape
from importlib.util import spec_from_file_location, module_from_spec
from pachydelm.utils import list_files, strToTimestamp, to_class_name, convert, force_number, map_nested_dicts_modify

from google.protobuf.json_format import ParseDict, MessageToDict

# Don't forget to adapt negative lookhead instead of hard code ^(?!.*bar).*$
EXTRACT_PATTERN = '(\d*)_(\d*)_(\d*)_(\d*)_(.*)_(pipeline|repo|seed)_([^.]*).(py|json)'

fields = [
    "transform", "parallelism_spec", "egress", "update",
    "output_branch", "resource_requests",
    "resource_limits", "input", "description", "cache_size", "enable_stats",
    "reprocess", "batch", "max_queue_size", "service", "chunk_spec",
    "datum_timeout", "job_timeout", "salt", "standby", "datum_tries",
    "scheduling_spec", "pod_spec", "pod_patch"
]

def extract_filename_to_migration(filename):
    matched = re.match(EXTRACT_PATTERN, filename)
    [year, month, day, time, resource, resource_type, migration, extension] = [ matched.group(i) for i in range(1, 9)  ]
    return {
      "time": strToTimestamp("%s_%s_%s_%s" % (year, month, day, time)),
      "resource": resource,
      "resource_type": resource_type,
      "migration": migration,
      "class_name": to_class_name(migration),
      "extension": extension,
      "path": filename
    }

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

    def get_pipeline_path(self, baseFileName):
        return (join(self.migrationsDir, baseFileName) + ".py", join(self.pachydermConfigsDir, baseFileName) + ".json")

    def get_migrations_by_resource_type(self, reverse=False):
        def onlyType(resource_type):
            filtered = list(filter(lambda x : x['resource_type'] == resource_type, self.migrations))
            return sorted(filtered, key=itemgetter('time'), reverse=reverse)
        return (onlyType('repo'), onlyType('pipeline'), onlyType('seed'))

    def instantiate_module_from_migration(self, migration):
        spec = spec_from_file_location(migration['migration'], migration['path'])
        module = module_from_spec(spec)
        spec.loader.exec_module(module)
        return getattr(module, migration['class_name'])(self)


class PachydermMigration(object):

    def __init__(self, ctx):
        self.pfs = ctx.pfs
        self.pps = ctx.pps

    def up(self):
        """ Setup """
        return

    def down(self):
        """ Teardown """
        return

    def create_pipeline_from_file(self, filePath):
        """ Create Pipeline from pachyderm pipeline json config file. """
        with open(filePath) as f:
            pipelineConfig = json.load(f)

        parsed = ParseDict(pipelineConfig, proto.PipelineInfo(), ignore_unknown_fields=True)
        pipelineName = parsed.pipeline.name
        configDict = MessageToDict(parsed, including_default_value_fields=False)
        onlyPythonPachydermKeysConfigDict = { convert(oldKey): value for oldKey, value in configDict.items() if convert(oldKey) in fields }
        # try:
        
        map_nested_dicts_modify(onlyPythonPachydermKeysConfigDict, force_number)
        print(onlyPythonPachydermKeysConfigDict)
        self.pps.create_pipeline(pipelineName, **onlyPythonPachydermKeysConfigDict)
        # except Exception:
        #   print('Something went wrong...(May be pipeline `%s` already exists)' % (pipelineName))

    def delete_pipeline_from_file(self, filePath):
        with open(filePath, 'r') as f:
            obj = json.load(f)
        self.pps.delete_pipeline(obj.get('pipeline').get('name'))

# verify is pipeline/repo already exists.
# inspect status of existing deployment
# for pipeline check integrity of pipeline config.json is there any change on those field.

# migration till specified migration, all migrations before these was migrate.

# rollback to specified migration all migrations after that was rollback


# error getting pipelineInfo: could not read existing PipelineInfo from PFS: commit 0186370b401a477394e4adc816bb64cf not found in repo __spec__