#!/usr/bin/env python

from uuid import UUID

import requests
from requests.packages import urllib3
import click

from .. import (
    try_verify_by_system_ca_bundle,
    xyz_parser_iterator,
    )

def json_pretty_dumps(orig):
    import json
    return json.dumps(orig, sort_keys=True,
                      indent=4, separators=(',', ': '))


def get_table_instance(table_data):
    from sys import stdout
    from terminaltables import SingleTable, AsciiTable

    if stdout.isatty():
        return SingleTable(table_data)
    else:
        return AsciiTable(table_data)


def bool2str(value):
    return u'\N{check mark}' if value else u'\N{heavy multiplication x}'


@click.group()
@click.option('--url', type=str,
              default='https://tctdb.chem.uzh.ch/fatman', show_default=True,
              help="The URL where FATMAN is running")
@click.option('--ssl-verify/--no-ssl-verify', required=False,
              default=True, show_default=True,
              help="verify the servers SSL certificate")
@click.pass_context
def cli(ctx, url, ssl_verify):
    if ctx.obj is None:
        ctx.obj = {}

    ctx.obj['url'] = url

    ctx.obj['session'] = requests.Session()
    if ssl_verify:
        ctx.obj['session'].verify = try_verify_by_system_ca_bundle()
    else:
        ctx.obj['session'].verify = False
        urllib3.disable_warnings()


from . import basis, calc, deltatest, struct, task, testresult


@cli.group(invoke_without_command=True)
@click.option('--code', type=UUID, required=True)
@click.option('--machine', type=UUID, required=True)
@click.pass_context
def command(ctx, code, machine):
    """Manage Code Commands"""
    ctx.obj['command_url'] = '{url}/api/v2/codes/{code}/commands/{machine}'.format(
        code=code, machine=machine, **ctx.obj)

    # action is going to happen in the subcommand
    if ctx.invoked_subcommand:
        return

    req = ctx.obj['session'].get(ctx.obj['command_url'])
    req.raise_for_status()
    cmd_content = req.json()

    click.echo("Commands:")
    for cmd in cmd_content['commands']:
        click.echo("  - {name}:".format(**cmd))
        click.echo("      cmd: {cmd}".format(**cmd))
        click.echo("      args: {args}".format(**cmd))

    click.echo("Environment:")

    click.echo("  Modules:")
    for module in cmd_content['environment'].get('modules', []):
        click.echo("    - {}".format(module))

    click.echo("  Variables:")
    for name, content in cmd_content['environment'].get('variables', {}).items():
        click.echo("    {}: {}".format(name, content))


@command.command('set-cmd')
@click.argument('name', type=str)
@click.argument('cmd', type=str)
@click.pass_context
def cmd_set_cmd(ctx, name, cmd):
    """
    Set the commandline for the given sub-command
    """

    req = ctx.obj['session'].get(ctx.obj['command_url'])
    req.raise_for_status()
    cmd_content = req.json()

    for ccmd in cmd_content['commands']:
        if ccmd['name'] == name:
            ccmd['cmd'] = cmd
            break
    else:
        raise RuntimeError("Command '{}' not found".format(name))

    click.echo("Setting command line for '{}' to '{}'..".format(name, cmd), nl=False)
    req = ctx.obj['session'].post(ctx.obj['command_url'], json=cmd_content)
    req.raise_for_status()
    click.echo("done")

