#!/usr/bin/env python

import json

import requests
from requests.packages import urllib3
import click

from six.moves.urllib.parse import urlparse  # pylint: disable=import-error

from . import try_verify_by_system_ca_bundle


def validate_basis_set_families(ctx, param, values):
    """Convert and validate basis set families arguments"""
    try:
        parsed = {k: v for k, v in (v.split(':', 2) for v in values)}
        assert all(parsed.keys()) and all(parsed.values())
        return parsed
    except (ValueError, AssertionError):
        raise click.BadParameter(
            "basis set family must be in format type:name")


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

    parsed_uri = urlparse(url)
    ctx.obj['server'] = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)

    ctx.obj['session'] = requests.Session()
    if ssl_verify:
        ctx.obj['session'].verify = try_verify_by_system_ca_bundle()
    else:
        ctx.obj['session'].verify = False
        urllib3.disable_warnings()


@cli.group()
@click.pass_context
def calc(ctx):
    """Manage calculations"""
    ctx.obj['calc_url'] = '{url}/api/v2/calculations'.format(**ctx.obj)


@calc.command('add')
@click.option('--collection', type=str, required=True)
@click.option('--test', type=str, required=True)
@click.option('--structure', type=str, required=True)
@click.option('--pseudo-family', type=str, required=True)
@click.option('--basis-set-family', type=str, required=True, multiple=True,
              callback=validate_basis_set_families)
@click.option('--code', type=str, required=True,
              default="CP2K", show_default=True)
@click.option('--task/--no-task',
              default=True, show_default=True,
              help="also create a task for this calculation")
@click.pass_context
def calc_add(ctx, **data):
    """Create a new calculation on FATMAN.

    Examples:

    \b
    # for a new deltatest point calculation
    fclient calc add \\
        --collection CP2K-Deltatest \\
        --test deltatest_H \\
        --structure deltatest_H_1.00 \\
        --pseudo-family GTH-PBE \\
        --basis-set-family default:DZVP-MOLOPT-GTH \\
        --code CP2K

    \b
    # for a new GW100 calculation
    fclient calc add \\
        --collection GW100-20161026 \\
        --test GW100 \\
        --structure Benzene-GW100 \\
        --pseudo-family GTH-PBE \\
        --basis-set-family default:cc-QZV3P-GTH \\
        --basis-set-family ri_aux:RI_QZ_opt_basis \\
        --code CP2K

    """

    click.echo("Creating calculation..")

    try:
        req = ctx.obj['session'].post(ctx.obj['calc_url'], json=data)
        req.raise_for_status()
        click.echo(json.dumps(req.json(), sort_keys=True,
                              indent=2, separators=(',', ': ')))

    except requests.exceptions.HTTPError as exc:
        try:
            msgs = exc.response.json()
            attr, msg = list(msgs['errors'].items())[0]
            raise click.BadParameter(str(msg[0] if isinstance(msg, list) else msg), param_hint=attr)
        except (ValueError, KeyError):
            click.echo(exc.response.text, err=True)

    click.echo("Creating task for calculation..")
    req = ctx.obj['session'].post(ctx.obj['server'] + req.json()['_links']['tasks'])
    req.raise_for_status()
    click.echo(json.dumps(req.json(), sort_keys=True,
                          indent=2, separators=(',', ': ')))
