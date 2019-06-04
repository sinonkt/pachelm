import click

## Credit about dynamic module in python: https://stackoverflow.com/questions/67631/how-to-import-a-module-given-the-full-path
@click.command()
@click.pass_obj
def rollback(ctx):
  """ Rollback module"""
  # print(ctx.migrations)
  (repos, pipelines, seeds) = ctx.get_migrations_by_resource_type(reverse=True)
  for migration in seeds + pipelines + repos:
    print(migration)
    ctx.instantiate_module_from_migration(migration).down()
  # print(list(map(extract_filename_to_migration, ctx.migrations)))
  click.echo('rollback something')