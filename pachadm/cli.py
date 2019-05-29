import click
from pachadm.create import create
from pachadm.migrate import migrate
from pachadm.rollback import rollback
from pachadm.clean import clean
from pachadm.test import test
from pachadm.core import PachydermAdminContext

VERSION='0.0.1'
# packageLoader = PackageLoader('pachadm', 'templates')

# packageLoader = PackageLoader('pachadm', 'templates')

@click.group(invoke_without_command=True)
@click.option('--migrationsDir', '-m', default='./migrations', help="specify directory to keep migrations.")
@click.option('--pachydermConfigsDir', '-p', default='./configs', help="specify directory to keep pachderm json configs.")
@click.pass_context
def entry(ctx, **kwargs):
  """ Hi, How are u? """
  ctx.obj = PachydermAdminContext(**{ "version": VERSION, **kwargs })
  if ctx.invoked_subcommand is None:
    click.echo('I was invoked without subcommand')
  else:
    click.echo('Invoke %s' % ctx.invoked_subcommand)

entry.add_command(create)
entry.add_command(migrate)
entry.add_command(rollback)
entry.add_command(clean)
entry.add_command(test)

if __name__ == '__main__':
    entry()