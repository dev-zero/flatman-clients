#!/usr/bin/env python

import json
import re
from io import BytesIO
from uuid import UUID
import os
from os import path
import sys
import csv
from collections import OrderedDict

import requests
from requests.packages import urllib3
import click
from terminaltables import SingleTable, AsciiTable
import dpath

import six

from . import (
    try_verify_by_system_ca_bundle,
    xyz_parser_iterator,
    )


# the maximal number of calculations to fetch details for
MAX_CALC_DETAILS = 200


def validate_basis_set_families(ctx, param, values):
    """Convert and validate basis set families arguments"""
    try:
        parsed = {k: v for k, v in (v.split(':', 1) for v in values)}
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
    ctx.obj['structureset_url'] = '{url}/api/v2/structuresets'.format(**ctx.obj)


@calc.command('add')
@click.option('--collection', type=str, required=True)
@click.option('--test', type=str, required=True)
@click.option('--structure', type=str, required=False)
@click.option('--structure-set', type=str, required=False)
@click.option('--pseudo-family', type=str, required=True)
@click.option('--basis-set-family', type=str, required=True, multiple=True,
              callback=validate_basis_set_families,
              help="To be specified as <type>:<default family>")
@click.option('--basis-set-family-fallback', type=str, multiple=True,
              callback=validate_basis_set_families)
@click.option('--code', type=str, required=True,
              default="CP2K", show_default=True)
@click.option('--task/--no-task', 'create_task',
              default=True, show_default=True,
              help="also create a task for this calculation")
@click.pass_context
def calc_add(ctx, structure_set, create_task, **data):
    """Create a new calculation on FATMAN.

    Examples:

    \b
    # for a new deltatest point calculation
    fclient calc add \\
        --collection CP2K-Deltatest \\
        --test deltatest \\
        --structure deltatest_H_1.00 \\
        --pseudo-family GTH-PBE \\
        --basis-set-family default:DZVP-MOLOPT-GTH \\
        --code CP2K

    \b
    # for a new deltatest calculation
    fclient calc add \\
        --collection CP2K-Deltatest \\
        --test deltatest \\
        --structure-set DELTATEST \\
        --pseudo-family GTH-PBE \\
        --basis-set-family default:DZVP-MOLOPT-GTH \\
        --basis-set-family-fallback default:DZVP-MOLOPT-SR-GTH \\
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

    if structure_set and data['structure']:
        raise click.BadOptionUsage("structure and structure-set can not be specified together")

    if structure_set:
        click.echo("Creating calculations.. ", nl=False)

        req = ctx.obj['session'].get(ctx.obj['structureset_url'] + '/' + structure_set)
        req.raise_for_status()

        try:
            url = req.json()['_links']['calculations']

            req = ctx.obj['session'].post(url, json={k: v for k, v in data.items() if k != 'structure'})
            req.raise_for_status()

            calculations = req.json()

            click.echo("succeeded")

            for calculation in calculations:
                click.echo(".. created calculation '{id}' for structure '{structure}'".format(**calculation))

            if create_task:
                for calculation in calculations:
                    click.echo(".. creating task for calculation '{id}'.. ".format(**calculation), nl=False)
                    req = ctx.obj['session'].post(calculation['_links']['tasks'])
                    req.raise_for_status()
                    click.echo("succeeded")
            else:
                click.echo("skipping task creation..")

        except requests.exceptions.HTTPError as exc:
            click.echo("failed")

            try:
                msgs = exc.response.json()
                errors = msgs['errors']
                attr, msg = six.next(six.iteritems(errors))
                if attr in list(data.keys()) + ['structure_set']:
                    raise click.BadParameter(
                        '; '.join([str(m) for m in msg]) if isinstance(msg, list) else str(msg),
                        param_hint=attr)
                else:
                    click.echo(exc.response.text, err=True)
                    ctx.abort()
            except (ValueError, KeyError):
                click.echo(exc.response.text, err=True)
                ctx.abort()

    else:
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

        if create_task:
            click.echo("Creating task for calculation..")
            req = ctx.obj['session'].post(req.json()['_links']['tasks'])
            req.raise_for_status()
            click.echo(json_pretty_dumps(req.json()))
        else:
            click.echo("skipping task creation..")


@calc.command('list')
@click.option('--collection', type=str, help="filter by collection")
@click.option('--test', type=str, help="filter by test ('GW100, 'deltatest', ..)")
@click.option('--structure', type=str, help="filter by structure ('GW100 Hydrogen peroxide', 'deltatest_H_1.00', ..)")
@click.option('--code', type=str, help="filter by used code ('CP2K', 'QE', ..)")
@click.option('--status', type=str, help="filter by status ('done', 'new', 'running', ..)")
@click.option('--show-ids/--no-show-ids',
              default=False, show_default=True,
              help="add columns with calculation and task ids")
@click.option('--column', '-c', 'columns', type=str, multiple=True,
              help="specify paths into the calculation object to be used as column")
@click.option('--csv-output', is_flag=True,
              default=False, show_default=True,
              help="output in CSV format")
@click.option('--with-details/--without-details',
              default=False, show_default=True,
              help="fetch details for selected calculations")
@click.pass_context
def calc_list(ctx, show_ids, columns, csv_output, with_details, **filters):
    """
    List calculations. Use the parameters to limit the list to certain subsets of calculations
    """

    # filter out filters not specified
    params = {k: v for k, v in filters.items() if v is not None}

    req = ctx.obj['session'].get(ctx.obj['calc_url'], params=params)
    req.raise_for_status()
    calcs = req.json()

    if with_details:
        if len(calcs) > MAX_CALC_DETAILS:
            raise click.UsageError("The number of returned calculations is too high to fetch details")

        click.echo('Please wait, fetching details..', err=True)

        with click.progressbar(calcs, file=sys.stderr) as bar:
            for cal in bar:
                req = ctx.obj['session'].get(cal['_links']['self'])
                req.raise_for_status()
                cal.update(req.json())

    table_data = []

    if not columns:
        table_data.append(['test', 'structure', 'code', 'collection', 'last modified', 'status', 'result_avail?'])

        if show_ids:
            table_data[0] += ['calc_id', 'current_task_id']

        for cal in calcs:
            table_data.append([
                cal['test'], cal['structure'], cal['code'], cal['collection'],
                cal.get('current_task', {}).get('mtime', "(unavail)"),
                cal.get('current_task', {}).get('status', "(unavail)"),
                cal['results_available'],
                ] + ([cal['id'], cal.get('current_task', {}).get('id', "(unavail)")] if show_ids else []))
    else:
        # so, a '--column a=b/c --column d=e --column =g/h/i' results in a header 'a,d,' with contents of b/c, e, g/h/i
        header, paths = zip(*[p.split('=', 1) if '=' in p else (p.split('/')[-1], p) for p in columns])

        table_data.append(header)

        table_data += [[dpath.util.get(c, p) for p in paths] for c in calcs]

    if csv_output:
        writer = csv.writer(sys.stdout)
        # when printing CSV we don't print an empty header
        writer.writerows(table_data if any(h for h in table_data[0]) else table_data[1:])
    else:
        if sys.stdout.isatty():
            table_instance = SingleTable(table_data)
        else:
            table_instance = AsciiTable(table_data)
        click.echo(table_instance.table)


@calc.command('generate-results')
@click.option('--update/--no-update', default=False, show_default=True,
              help="Rewrite the result even if already present")
@click.option('--id', 'ids', type=UUID, required=False, multiple=True,
              help="restrict action to specified calculation ids")
@click.pass_context
def calc_generate_results(ctx, update, ids):
    """Parse results from artifacts and write them to the calculation"""

    if ids:
        for cid in ids:
            click.echo("Trigger result generation for calculation {}".format(cid))
            req = ctx.obj['session'].post(ctx.obj['calc_url'] + '/{}/action'.format(cid),
                                          json={'generateResults': {'update': update}})
            req.raise_for_status()
    else:
        click.echo("Trigger result generation for all calculations")
        req = ctx.obj['session'].post(ctx.obj['calc_url'] + '/action',
                                      json={'generateResults': {'update': update}})
        req.raise_for_status()

    # TODO: implement result parsing and waiting for finish


@calc.command('retry')
@click.argument('ids', metavar='<ID 1> [<ID 2>..]', type=UUID, nargs=-1, required=True)
@click.pass_context
def calc_retry(ctx, ids):
    """Re-run specified calculation(s)"""

    for cid in ids:
        req = ctx.obj['session'].get(ctx.obj['calc_url'] + '/{}'.format(cid))
        req.raise_for_status()
        calc_content = req.json()

        req = ctx.obj['session'].post(calc_content['_links']['tasks'])
        req.raise_for_status()


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
@click.option('--cubic-cell/--no-cubic-cell', default=False,
              show_default=True, help="Whether to generate a cubic cell")
@click.option('--dump/--no-dump', default=False,
              show_default=True, help="Dump the parsed out structure")
@click.pass_context
def struct_add(ctx, xyzfile, name, name_prefix, name_field, sets, pbc, cubic_cell, dump):
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
            'cubic_cell': cubic_cell,
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


@cli.group()
@click.pass_context
def task(ctx):
    """Manage tasks"""
    ctx.obj['task_url'] = '{url}/api/v2/tasks'.format(**ctx.obj)


@task.command('list-files')
@click.argument('task_id', type=UUID)
@click.pass_context
def task_list_files(ctx, task_id):
    """List all files associated with the specified task"""

    req = ctx.obj['session'].get(ctx.obj['task_url'] + '/{}'.format(task_id))
    req.raise_for_status()
    task_content = req.json()

    for infile in task_content['infiles']:
        click.echo("-> {}".format(infile['name']))

    for outfile in task_content['outfiles']:
        click.echo("<- {}".format(outfile['name']))


@task.command('download-files')
@click.argument('task_id', type=UUID)
@click.pass_context
def task_download_files(ctx, task_id):
    """
    Creates a new directory task_<task_id> in the
    current working directory and downloads all files
    """

    target_dir = "task_{}".format(task_id)

    req = ctx.obj['session'].get(ctx.obj['task_url'] + '/{}'.format(task_id))
    req.raise_for_status()
    task_content = req.json()

    os.mkdir(target_dir)
    os.mkdir(path.join(target_dir, 'infiles'))
    os.mkdir(path.join(target_dir, 'outfiles'))

    for direction in ['infiles', 'outfiles']:
        for artifact in task_content[direction]:
            target_fn = path.join(target_dir, direction, artifact['name'])
            click.echo("downloading {} to {}..".format(artifact['name'], target_fn), nl=False)

            req = ctx.obj['session'].get(artifact['_links']['download'], stream=True)
            req.raise_for_status()

            with open(target_fn, 'wb') as fhandle:
                for chunk in req.iter_content(1024):
                    fhandle.write(chunk)

            click.echo(" done")


@task.command('upload-artifact')
@click.argument('task_id', type=UUID)
@click.argument('filename', type=click.File(mode='rb'))
@click.argument('name', type=str)
@click.pass_context
def task_upload_artifact(ctx, task_id, filename, name):
    """Upload artifacts for given task using the specified name"""

    req = ctx.obj['session'].get(ctx.obj['task_url'] + '/{}'.format(task_id))
    req.raise_for_status()
    task_content = req.json()

    req = ctx.obj['session'].post(task_content['_links']['uploads'],
                                  data={'name': name}, files={'data': filename})
    req.raise_for_status()


@cli.group()
@click.pass_context
def testresult(ctx):
    """Manage test results"""
    ctx.obj['testresult_url'] = '{url}/api/v2/testresults'.format(**ctx.obj)


@testresult.command('list')
@click.pass_context
def testresult_list(ctx):
    """
    List test results
    """

    req = ctx.obj['session'].get(ctx.obj['testresult_url'])
    req.raise_for_status()
    testresults = req.json()

    table_data = [
        ['id', 'test', 'calculations', 'data'],
        ]

    for tresult in testresults:
        data = OrderedDict()

        if tresult['test'] == 'deltatest':
            data['status'] = tresult['data']['status']

        data.update({'check.%s' % k: str(v) for k, v in tresult['data'].get('checks', {}).items()})

        table_data.append([
            tresult['id'],
            tresult['test'],
            '\n'.join([c['id'] for c in tresult['calculations']]),
            '\n'.join(': '.join(t) for t in data.items())])

    if sys.stdout.isatty():
        table_instance = SingleTable(table_data)
    else:
        table_instance = AsciiTable(table_data)
    click.echo(table_instance.table)


@testresult.command('generate-results')
@click.option('--update/--no-update', default=False, show_default=True,
              help="Rewrite the testresult even if already present")
@click.option('--id', 'ids', type=UUID, required=False, multiple=True,
              help="restrict action to specified testresult")
@click.pass_context
def testresult_generate_results(ctx, update, ids):
    """Read results from calculations and generate respective test results"""

    if ids:
        for tid in ids:
            click.echo("Trigger test result (re-)generation for test result {}".format(tid))
            req = ctx.obj['session'].post(ctx.obj['testresult_url'] + '/{}/action'.format(tid),
                                          json={'generate': {'update': update}})
            req.raise_for_status()
    else:
        click.echo("Trigger test result (re-)generation for all calculations, resp. test results")
        req = ctx.obj['session'].post(ctx.obj['testresult_url'] + '/action',
                                      json={'generate': {'update': update}})
        req.raise_for_status()

    # TODO: implement result parsing and waiting for finish


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
