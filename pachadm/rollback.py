import click

@click.command()
def rollback():
  """ rollback module"""
  click.echo('rollback something')