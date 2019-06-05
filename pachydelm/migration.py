import json
from pprint import pprint
from deepdiff import DeepDiff
import python_pachyderm.client.pps.pps_pb2 as proto
from google.protobuf.json_format import ParseDict, MessageToDict
from pachydelm.utils import convert, force_number, map_nested_dicts_modify

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
        onlyPythonPachydermKeysConfigDict = { convert(oldKey): value for oldKey, value in configDict.items() if convert(oldKey) in FIELDS }
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

def diff(ctx):
    inspected = ctx.pps.inspect_pipeline('huhu')
    invalidKey = MessageToDict(inspected, including_default_value_fields=False)
    pipelineInfo = { convert(k) : v for k, v in invalidKey.items() if convert(k) not in IGNORED_FROM_DIFF_FIELDS }
    map_nested_dicts_modify(pipelineInfo, force_number)
    with open('./configs/2019_06_04_221735_huhu_pipeline_oh_my_god.json', 'r') as f:
        loaded = json.loads(f.read())
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

# verify is pipeline/repo already exists.
# inspect status of existing deployment
# for pipeline check integrity of pipeline config.json is there any change on those field.

# migration till specified migration, all migrations before these was migrate.

# rollback to specified migration all migrations after that was rollback


# error getting pipelineInfo: could not read existing PipelineInfo from PFS: commit 0186370b401a477394e4adc816bb64cf not found in repo __spec__