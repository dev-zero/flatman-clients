
import sys
import json
import csv
import textwrap

from uuid import UUID

import click
import requests
from requests.utils import parse_header_links
import six
import dpath

from . import cli, json_pretty_dumps, get_table_instance


# the maximal number of calculations to fetch details for
MAX_CALC_DETAILS = 200
# the maximal number of calculations the server gives us per-page
MAX_CALC_PER_PAGE = 200


def validate_basis_set_families(ctx, param, values):
    """Convert and validate basis set families arguments"""
    try:
        parsed = {k: v for k, v in (v.split(':', 1) for v in values)}
        assert all(parsed.keys()) and all(parsed.values())
        return parsed
    except (ValueError, AssertionError):
        raise click.BadParameter(
            "basis set family must be in format type:name")


@cli.group()
@click.pass_context
def calc(ctx):
    """Manage calculations"""
    ctx.obj['calc_url'] = '{url}/api/v2/calculations'.format(**ctx.obj)
    ctx.obj['calc_coll_url'] = '{url}/api/v2/calculationcollections'.format(**ctx.obj)
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
@click.option('--deferred-task/--no-deferred-task',
              default=False, show_default=True,
              help="whether the task is created as deferred")
@click.option('--settings', type=str,
              help="pass additional settings for the calculation (to be specified as a string of JSON)")
@click.option('--settings-file', type=click.File(mode='r'),
              help="pass additional settings for the calculation using the given JSON file")
@click.option('--ignore-failed/--no-ignore-failed',
              default=False, show_default=True,
              help="Ignore failure in creation of single calculations (likely caused by missing basis set or pseudo)")
@click.pass_context
def calc_add(ctx, structure_set, create_task, deferred_task, settings_file, **data):
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

    if settings_file and data['settings']:
        raise click.BadOptionUsage("settings and settings-file can not be specified together")

    if data['settings']:
        # if settings are specified, load the JSON from the string
        data['settings'] = json.loads(data['settings'])
    else:
        # .. or remove the key completely, since the API does not allow None
        del data['settings']

    if settings_file:
        data['settings'] = json.load(settings_file)


    task_creation_data = {'status': 'new'}
    if deferred_task:
        task_creation_data['status'] = 'deferred'

    req = ctx.obj['session'].get(ctx.obj['calc_coll_url'])
    req.raise_for_status()
    calc_colls = req.json()

    if data['collection'] not in [c['name'] for c in calc_colls]:
        click.confirm("The specified collection '{collection}' does not exist. Do you want to create it?".format(**data),
                      abort=True)
        coll_desc = click.prompt("Please enter a description for the collection", type=str)
        req = ctx.obj['session'].post(ctx.obj['calc_coll_url'], json={'name': data['collection'], 'desc': coll_desc})
        req.raise_for_status()
        # since we pass the collection name when creating the calculation,
        # we can forget about the id of the created collection

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
                click.echo(".. created calculation '{id}' for structure '{structure[name]}'".format(**calculation))

            if create_task:
                for calculation in calculations:
                    click.echo(".. creating task for calculation '{id}'.. ".format(**calculation), nl=False)
                    req = ctx.obj['session'].post(calculation['_links']['tasks'], json=task_creation_data)
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
            req = ctx.obj['session'].post(req.json()['_links']['tasks'], json=task_creation_data)
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
@click.option('--sorted-by', type=str, help="sort by the specified column (a posteriori)")
@click.option('--fetch-all/--no-fetch-all', default=False, show_default=True,
              help="fetch all entries instead of the first N returned by the server")
@click.pass_context
def calc_list(ctx, show_ids, columns, csv_output, with_details, sorted_by, fetch_all, **filters):
    """
    List calculations. Use the parameters to limit the list to certain subsets of calculations
    """

    # filter out filters not specified
    params = {k: v for k, v in filters.items() if v is not None}

    if fetch_all:  # reduce the number of requests by maxing the number of entries per page
        params['per_page'] = MAX_CALC_PER_PAGE

    req = ctx.obj['session'].get(ctx.obj['calc_url'], params=params)
    req.raise_for_status()
    calcs = req.json()

    if fetch_all and (len(calcs) < int(req.headers['X-total-count'])):
        while True:
            try:
                next_link = [l['url'] for l in parse_header_links(req.headers['Link']) if l['rel'] == 'next'][0]
            except IndexError:
                break

            req = ctx.obj['session'].get(next_link, params=params)
            req.raise_for_status()
            calcs += req.json()

    if with_details:
        if len(calcs) > MAX_CALC_DETAILS:
            raise click.UsageError("The number of returned calculations is too high to fetch details")

        click.echo('Please wait, fetching details..', err=True)

        with click.progressbar(calcs, file=sys.stderr) as bar:
            for cal in bar:
                req = ctx.obj['session'].get(cal['_links']['self'])
                req.raise_for_status()
                cal.update(req.json())

    header = []
    table_data = []

    if not columns:
        header = ['test', 'structure', 'code', 'collection', 'last modified', 'status', 'result_avail?']

        if show_ids:
            header += ['calc_id', 'current_task_id']

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
        table_data += [[dpath.util.get(c, p) for p in paths] for c in calcs]


    if sorted_by:
        try:
            column_idx = header.index(sorted_by)
        except ValueError:
            raise click.BadParameter("specified column name not found: '{}'".format(sorted_by), param_hint='sorted_by')

        table_data.sort(key=lambda l: l[column_idx])

    if csv_output:
        writer = csv.writer(sys.stdout)
        # when printing CSV we don't print an empty header
        writer.writerows([header] + table_data if any(h for h in header) else table_data)
    else:
        table_instance = get_table_instance([header] + table_data)
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
def ccollections(ctx):
    """Manage calculation collectionss"""
    ctx.obj['ccollections_url'] = '{url}/api/v2/calculationcollections'.format(**ctx.obj)


@ccollections.command('list')
@click.pass_context
def ccollections_list(ctx):
    """
    List calculation collections
    """

    req = ctx.obj['session'].get(ctx.obj['ccollections_url'])
    req.raise_for_status()
    ccolls = req.json()

    table_data = [
        ['id', 'name', 'description'],
        ]

    for ccoll in ccolls:
        table_data.append([
            ccoll['id'],
            "\n".join(textwrap.wrap(ccoll['name'], width=20)),
            "\n".join(textwrap.wrap(ccoll['desc'], width=40)),
            ])

    table_instance = get_table_instance(table_data)
    click.echo(table_instance.table)
