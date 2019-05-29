import shutil
import click
from pachadm.utils import deletePrompt

@click.command()
@click.pass_obj
def clean(ctx):
  """ Clean all migrations & config """
  try:
    if deletePrompt([ctx.migrationsDir, ctx.pachydermConfigsDir]):
      shutil.rmtree(ctx.migrationsDir)
      shutil.rmtree(ctx.pachydermConfigsDir)
      click.echo('deleteing...\n%s\n%s' % (ctx.migrationsDir, ctx.pachydermConfigsDir))
  except FileNotFoundError:
    click.echo('Directory Not exist...')