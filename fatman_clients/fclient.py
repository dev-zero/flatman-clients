#!/usr/bin/env python

import json

import requests
import click

from six.moves.urllib.parse import urlparse  # pylint: disable=import-error


CALCULATION_URL = '{}/api/v2/calculations'


def validate_basis_set_families(ctx, param, values):
    """Convert and validate basis set families arguments"""
    try:
        parsed = {k: v for k, v in (v.split(':', 2) for v in values)}
        assert all(parsed.keys()) and all(parsed.values())
        return parsed
    except (ValueError, AssertionError):
        raise click.BadParameter(
            "basis set family must be in format type:name")


@click.command()
@click.option('--url', type=str,
              default='https://tctdb.chem.uzh.ch/fatman', show_default=True,
              help="The URL where FATMAN is running")
@click.option('--collection', type=str, required=True)
@click.option('--test', type=str, required=True)
@click.option('--structure', type=str, required=True)
@click.option('--pseudo-family', type=str, required=True)
@click.option('--basis-set-family', type=str, required=True, multiple=True,
              callback=validate_basis_set_families)
@click.option('--code', type=str, required=True,
              default="cp2k", show_default=True)
@click.option('--task/--no-task',
              default=True, show_default=True,
              help="also create a task for this calculation")
def add_calc(url, **data):
    """Create a new calculation on FATMAN.

    Examples:

    # for a new deltatest point calculation
    fadd_calc \
        --collection CP2K-Deltatest \
        --test deltatest_H \
        --structure Benzene-GW100 \
        --pseudo-family GTH-PBE \
        --basis-set-family default:cc-QZV3P-GTH \
        --basis-set-family ri_aux:RI_QZ_opt_basis \
        --code cp2k

    # for a new GW100 calculation
    fadd_calc \
        --collection GW100-20161026 \
        --test GW100 \
        --structure deltatest_H_1.00 \
        --pseudo-family GTH-PBE \
        --basis-set-family default:DZVP-MOLOPT-SR-GTH \
        --code cp2k

    """

    try:
        parsed_uri = urlparse(url)
        server = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)

        sess = requests.Session()
        sess.verify = False  # required to ignore the self-signed cert

        click.echo("Creating calculation..")
        req = sess.post(CALCULATION_URL.format(url), json=data)
        req.raise_for_status()
        click.echo(json.dumps(req.json(), sort_keys=True,
                              indent=2, separators=(',', ': ')))

        click.echo("Creating task for calculation..")
        req = sess.post(server + req.json()['_links']['tasks'])
        req.raise_for_status()
        click.echo(json.dumps(req.json(), sort_keys=True,
                              indent=2, separators=(',', ': ')))

    except requests.exceptions.HTTPError as exc:
        try:
            msgs = exc.response.json()
            attr, msg = list(msgs['errors'].items())[0]
            raise click.BadParameter(msg, param_hint=attr)
        except (ValueError, KeyError):
            click.echo(exc.response.text, err=True)
