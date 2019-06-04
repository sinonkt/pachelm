import click
from os.path import join, dirname
from pachydelm.utils import getDateTimestampAndString, mkdir_p, overwritePrompt, to_class_name

entities = ['repo', 'pipeline']

templateByResource = {
  "repo": "repo_migration.py.tmpl",
  "pipeline": "pipeline_migration.py.tmpl",
  "pipeline_config": "pachyderm.json.tmpl",
  "seed": "seeds.py.tmpl"
}

fns = {
  'to_class_name': to_class_name
}

def renderResource(ctx, args, filePath, resource_type):
  template = ctx.env.get_template(templateByResource[resource_type])
  rendered = template.render(ctx=ctx, args=args, fns=fns)
  mkdir_p(dirname(filePath))
  if overwritePrompt([filePath]):
    fileObj = open(filePath, "w+")
    fileObj.write(rendered)
    fileObj.close()


@click.command()
@click.argument('resource')
@click.option('--name', '-n', help='Resource name...', required=True)
@click.option('--migration', '-m', help='Migration file name...')
@click.pass_obj
def create(ctx, **kwargs):
  """ Create Migrations & Seeds & corresponding pachyderm json config"""
  (timestamp, datetimeStr) = getDateTimestampAndString()
  combined_migration_name = '%s_%s_%s' % (kwargs['name'], kwargs['resource'], kwargs['migration']) if kwargs['migration'] else '%s_%s_%s' % (kwargs['name'], kwargs['resource'], kwargs['name'])
  baseFileName = '%s_%s' %(datetimeStr, combined_migration_name)
  (migrationFilePath, pachydermConfigFilePath) = ctx.get_pipeline_path(baseFileName)
  kwargs = {
    **kwargs,
    "migration": kwargs['migration'] or kwargs['name'],
    "migrationFilePath": migrationFilePath,
    "pachydermConfigFilePath": pachydermConfigFilePath
  }
  try:
    renderResource(ctx, kwargs, migrationFilePath, kwargs['resource'])
    if kwargs['resource'] == 'pipeline':
      renderResource(ctx, kwargs, pachydermConfigFilePath, kwargs['resource'] + '_config')
  except KeyError:
    raise Exception('Unknown resource...')
  # except:
  #   raise Exception('Something went wrong...')

# 2015_03_13_195049_create_testtable2_table
# 2015_03_13_195050_create_testtable_table