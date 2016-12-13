#!/usr/bin/env python

import json
import re
from io import BytesIO

import requests
from requests.packages import urllib3
import click

from . import (
    try_verify_by_system_ca_bundle,
    xyz_parser_iterator,
    )


def validate_basis_set_families(ctx, param, values):
    """Convert and validate basis set families arguments"""
    try:
        parsed = {k: v for k, v in (v.split(':', 2) for v in values)}
        assert all(parsed.keys()) and all(parsed.values())
        return parsed
    except (ValueError, AssertionError):
        raise click.BadParameter(
            "basis set family must be in format type:name")


def json_pretty_dumps(orig):
    return json.dumps(orig, sort_keys=True,
                      indent=4, separators=(',', ': '))


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
        click.echo(json_pretty_dumps(req.json()))

    except requests.exceptions.HTTPError as exc:
        try:
            msgs = exc.response.json()
            attr, msg = list(msgs['errors'].items())[0]
            raise click.BadParameter(str(msg[0] if isinstance(msg, list) else msg), param_hint=attr)
        except (ValueError, KeyError):
            click.echo(exc.response.text, err=True)

    click.echo("Creating task for calculation..")
    req = ctx.obj['session'].post(req.json()['_links']['tasks'])
    req.raise_for_status()
    click.echo(json_pretty_dumps(req.json()))


@cli.group()
@click.pass_context
def basis(ctx):
    """Manage basis sets"""
    ctx.obj['basis_url'] = '{url}/api/v2/basissets'.format(**ctx.obj)


@basis.command('add')
@click.argument('basisset_file', type=click.File(mode='r'))
@click.option('--dump-basis/--no-dump-basis',
              default=False, show_default=True,
              help="Dump also the basis during parsing")
@click.pass_context
def basis_add(ctx, basisset_file, dump_basis):
    """Upload new basis sets from a file"""
    basissets = {}
    current_basis = None

    for line in basisset_file:
        if re.match(r'\s*#.*', line):
            # ignore comment lines
            continue

        match = re.match(r'\s*(?P<element>[a-zA-Z]{1,2})\s+(?P<family>\S+).*', line)

        if match:
            if current_basis and dump_basis:
                click.echo(basissets[current_basis].getvalue().decode('utf-8'))

            click.echo(("Found basis set for element '{element}'"
                        " and family '{family}'").format(**match.groupdict()))
            current_basis = (match.group('element'), match.group('family'))

            if current_basis in basissets.keys():
                ValueError(("duplicated basis set for element '{element}'"
                            " and family '{family}' found").format(**match.groupdict()))

            basissets[current_basis] = BytesIO()
            # we don't want this line to end up in our uploaded file
            continue

        if not current_basis:
            raise ValueError("invalid basis set file")

        basissets[current_basis].write(line.encode('utf-8'))

    click.confirm("Do you want to upload the basis sets (total: {})?".format(len(basissets)), abort=True)

    for (element, family), basis_data in basissets.items():
        click.echo("Uploading basis set for '{}' and family '{}'.. ".format(element, family), nl=False)

        # rewind to the beginning
        basis_data.seek(0)

        req = ctx.obj['session'].post(
            ctx.obj['basis_url'],
            data={'element': element, 'family': family},
            files={'basis': basis_data})

        try:
            req.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            click.echo("failed")
            try:
                msgs = exc.response.json()

                # try to extract the error message
                if isinstance(msgs, dict) and 'errors' in msgs.keys():
                    ctx.fail(json_pretty_dumps(msgs['errors']))

                ctx.fail(json_pretty_dumps(msgs))
            except (ValueError, KeyError):
                ctx.fail(exc.response.text)
        except:
            click.echo("failed")
            raise

        click.echo("succeeded")


@cli.group()
@click.pass_context
def struct(ctx):
    """Manage structures"""
    ctx.obj['struct_url'] = '{url}/api/v2/structures'.format(**ctx.obj)


@struct.command('add')
@click.argument('xyzfile', type=click.File(mode='r'))
@click.option('--name', type=str,
              help="Use the given name instead of trying to extract from the XYZ comment")
@click.option('--name-prefix', type=str,
              default="", show_default=True,
              help="Prefix the name parsed from the XYZ file")
@click.option('--name-field', type=int,
              default=0, show_default=True,
              help="Which field in the XYZ comment should be used as the name")
@click.option('--set', 'sets', type=str, multiple=True, required=True,
              help="Place the structure in the given structure set(s)")
@click.option('--pbc/--no-pbc', default=True,
              show_default=True, help="Use periodic boundary conditions")
@click.option('--dump/--no-dump', default=False,
              show_default=True, help="Dump the parsed out structure")
@click.pass_context
def struct_add(ctx, xyzfile, name, name_prefix, name_field, sets, pbc, dump):
    """Upload a structure (in XYZ format)"""

    structures = {}

    complete_input = xyzfile.read()

    for (_, comment, _, match) in xyz_parser_iterator(complete_input, True):
        if name:
            if len(structures) >= 1:
                raise click.BadParameter("more than one structure found in XYZ file", param_hint='name')

            structure_name = name
        else:
            structure_name = name_prefix + comment.split(';')[name_field]

        if name in structures.keys():
            raise click.UsageError("duplicated name found for structure {}".format(name))

        structures[structure_name] = match.span()

        click.echo("Found structure {}".format(structure_name))

        if dump:
            click.echo(complete_input[match.span()[0]:match.span()[1]])

    click.confirm("Do you want to upload the structures (total: {})?".format(len(structures)), abort=True)

    for name, (spos, epos) in structures.items():
        click.echo("Uploading structure '{}'.. ".format(name), nl=False)

        data = {
            'name': name,
            'sets': sets,
            'pbc': pbc,
            'format': 'xyz',
            }

        structure_file = BytesIO(complete_input[spos:epos].encode('utf-8'))

        try:
            req = ctx.obj['session'].post(ctx.obj['struct_url'], data=data,
                                          files={'geometry': structure_file})
            req.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            click.echo("failed")
            try:
                msgs = exc.response.json()

                # try to extract the error message
                if isinstance(msgs, dict) and 'errors' in msgs.keys():
                    ctx.fail(json_pretty_dumps(msgs['errors']))

                ctx.fail(json_pretty_dumps(msgs))
            except (ValueError, KeyError):
                ctx.fail(exc.response.text)
        except:
            click.echo("failed")
            raise

        click.echo("succeeded")
