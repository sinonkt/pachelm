import click

## Credit about dynamic module in python: https://stackoverflow.com/questions/67631/how-to-import-a-module-given-the-full-path
@click.command()
@click.pass_obj
def migrate(ctx):
  """ Migrate module"""
  # print(ctx.migrations)
  (repos, pipelines, seeds) = ctx.get_migrations_by_resource_type()
  for migration in repos + pipelines + seeds:
    ctx.instantiate_module_from_migration(migration).up()
  # print(list(map(extract_filename_to_migration, ctx.migrations)))
  click.echo('Migrate something')