import click
from importlib import import_module
from importlib.util import spec_from_file_location, module_from_spec
from pachydelm.utils import extract_filename_to_migration

## Credit about dynamic module in python: https://stackoverflow.com/questions/67631/how-to-import-a-module-given-the-full-path
@click.command()
@click.pass_obj
def migrate(ctx):
  """ Migrate module"""
  # print(ctx.migrations)
  for migration in ctx.migrations:
    spec = spec_from_file_location(migration['migration'], migration['path'])
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    getattr(module, migration['class_name']).up()
  # print(list(map(extract_filename_to_migration, ctx.migrations)))
  click.echo('Migrate something')