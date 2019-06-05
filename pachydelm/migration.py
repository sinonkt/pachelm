import json
from pprint import pprint
from deepdiff import DeepDiff
import python_pachyderm.client.pps.pps_pb2 as proto
from google.protobuf.json_format import ParseDict, MessageToDict
from pachydelm.utils import convert, force_number, map_nested_dicts_modify

IGNORED_FROM_DIFF_FIELDS = [ 'created_at', 'salt', 'spec_commit', 'state']

# fields = [field.name for field in proto.PipelineInfo.DESCRIPTOR.fields]
# deprecated: scale_down_threshold
# no usecases/docs: hashtree_spec
FIELDS = [
    "transform", "parallelism_spec", "egress", "update",
    "output_branch", "resource_requests",
    "resource_limits", "input", "description", "cache_size", "enable_stats",
    "reprocess", "batch", "max_queue_size", "service", "chunk_spec",
    "datum_timeout", "job_timeout", "salt", "standby", "datum_tries",
    "scheduling_spec", "pod_spec", "pod_patch"
]

class PachydermMigration(object):

    def __init__(self, ctx):
        self.pfs = ctx.pfs
        self.pps = ctx.pps
        self.ctx = ctx

    def up(self):
        """ Setup """
        return

    def down(self):
        """ Teardown """
        return

    def create_repo(self, *args, **kwargs):
        self.pfs.create_repo(*args, **kwargs)
    
    def delete_repo(self, *args, **kwargs):
        self.pfs.delete_repo(*args, **kwargs)

    def create_pipeline_from_file(self, filePath):
        """ Create Pipeline from pachyderm pipeline json config file. """
        pipelineConfig = self.__load_json_config(filePath)
        parsed = ParseDict(pipelineConfig, proto.PipelineInfo(), ignore_unknown_fields=True)
        pipelineName = parsed.pipeline.name
        configDict = MessageToDict(parsed, including_default_value_fields=False)
        onlyPythonPachydermKeysConfigDict = { convert(oldKey): value for oldKey, value in configDict.items() if convert(oldKey) in FIELDS }
        # try:
        
        map_nested_dicts_modify(onlyPythonPachydermKeysConfigDict, force_number)
        # print(onlyPythonPachydermKeysConfigDict)
        self.pps.create_pipeline(pipelineName, **onlyPythonPachydermKeysConfigDict)
        # except Exception:
        #   print('Something went wrong...(May be pipeline `%s` already exists)' % (pipelineName))

    def delete_pipeline_from_file(self, filePath):
        obj = self.__load_json_config(filePath)
        self.pps.delete_pipeline(obj.get('pipeline').get('name'))

    def verify_is_pipeline_exists(self, pipeline):
        try:
            self.pps.inspect_pipeline(pipeline)
            return True
        except Exception:
            return False

    def get_pipeline(self, pipeline):
        inspected = self.pps.inspect_pipeline(pipeline)
        messageDict = MessageToDict(inspected, including_default_value_fields=False)
        pipelineInfo = { convert(k) : v for k, v in messageDict.items() if convert(k) not in IGNORED_FROM_DIFF_FIELDS }
        map_nested_dicts_modify(pipelineInfo, force_number)
        return pipelineInfo

    def __load_json_config(self, filePath):
        with open(filePath, 'r') as f:
            loaded = json.loads(f.read())
        return loaded

    def diff(self, pipeline, jsonConfigPath):
        pipelineInfo = self.get_pipeline(pipeline)
        loaded = self.__load_json_config(jsonConfigPath)
        print("********************************* Inspect PipelineInfo *********************************")
        print(pipelineInfo)
        print("****************************************************************************************")
        print("************************************ load ConfigJson ***********************************")
        print(loaded)
        print("****************************************************************************************")
        print("*************************************** Diff *******************************************")
        ddiff = DeepDiff(pipelineInfo, loaded, verbose_level=2)
        pprint(ddiff, indent=2)
        print("****************************************************************************************")
        print("****************************************************************************************")


# verify is pipeline/repo already exists.
# inspect status of existing deployment
# for pipeline check integrity of pipeline config.json is there any change on those field.

# migration till specified migration, all migrations before these was migrate.

# rollback to specified migration all migrations after that was rollback


# error getting pipelineInfo: could not read existing PipelineInfo from PFS: commit 0186370b401a477394e4adc816bb64cf not found in repo __spec__