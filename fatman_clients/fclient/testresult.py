
import textwrap

from uuid import UUID
from collections import OrderedDict

import click

from . import cli, get_table_instance

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

    table_instance = get_table_instance(table_data)
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


@cli.group()
@click.pass_context
def trcollections(ctx):
    """Manage test result collectionss"""
    ctx.obj['trcollections_url'] = '{url}/api/v2/testresultcollections'.format(**ctx.obj)


@trcollections.command('list')
@click.pass_context
def trcollections_list(ctx):
    """
    List test result collections
    """

    req = ctx.obj['session'].get(ctx.obj['trcollections_url'])
    req.raise_for_status()
    trcolls = req.json()

    table_data = [
        ['id', 'name', 'number of results', 'description'],
        ]

    for trcoll in trcolls:
        table_data.append([
            trcoll['id'],
            "\n".join(textwrap.wrap(trcoll['name'], width=20)),
            trcoll['testresult_count'],
            "\n".join(textwrap.wrap(trcoll['desc'], width=40)),
            ])

    table_instance = get_table_instance(table_data)
    click.echo(table_instance.table)


@trcollections.command('show')
@click.argument('id', type=UUID, required=True)
@click.option('--extended-info/--no-extended-info', default=False, show_default=True,
              help="Whether to fetch and show extended calculation info")
@click.pass_context
def trcollections_show(ctx, extended_info, id):
    """
    Show details for the specified collection
    """

    req = ctx.obj['session'].get(ctx.obj['trcollections_url'] + '/%s' % id)
    req.raise_for_status()
    trcoll = req.json()

    click.echo("Name: {name}".format(**trcoll))
    click.echo("Description:\n{desc}\n".format(**trcoll))

    click.echo("Testresults ({testresult_count}):\n".format(**trcoll))

    table_data = [
        ['id', 'test', 'data'],
        ]

    if extended_info:
        table_data[0].append("calc collections")

    for tr in trcoll['testresults']:
        entry = [tr['id'], tr['test']]

        if 'element' in tr['data']:
            entry.append("element: {element}".format(**tr['data']))
        else:
            entry.append("(unavail.)")

        if extended_info:
            req = ctx.obj['session'].get(tr['_links']['self'])
            req.raise_for_status()
            fulltr = req.json()

            calcs = fulltr['calculations']

            entry.append("\n".join(set(calc['collection'] for calc in fulltr['calculations'])))


        table_data.append(entry)

    table_instance = get_table_instance(table_data)
    click.echo(table_instance.table)


@trcollections.command('create')
@click.option('--name', type=str, required=True, prompt=True)
@click.option('--desc', type=str, required=True, prompt=True)
@click.option('--copy-from', type=UUID, required=False,
              help="copy test results from another collection")
@click.option('--copy-from-exclude', type=UUID, required=False, multiple=True,
              help="exclude the specified test result(s) from the collection to copy from")
@click.option('--include', type=UUID, required=False, multiple=True,
              help="include the specified test result(s) in the new collection")
@click.pass_context
def trcollections_create(ctx, name, desc, copy_from, copy_from_exclude, include):
    """
    Create a test result collection
    """

    # populate the results to be added by the include list,
    # converting the UUID objects back to strings while at it
    results = [str(i) for i in include] if include else []

    if copy_from:
        req = ctx.obj['session'].get(ctx.obj['trcollections_url'] + '/%s' % copy_from)
        req.raise_for_status()
        trcoll = req.json()

        excludes = [str(i) for i in copy_from_exclude] if copy_from_exclude else []
        results += [tr['id'] for tr in trcoll['testresults'] if tr['id'] not in excludes]

    payload = {
        'name': name,
        'desc': desc,
        'testresults': results,
        }

    req = ctx.obj['session'].post(ctx.obj['trcollections_url'], json=payload)
    req.raise_for_status()
    trcoll = req.json()

    click.echo("done, the assigned ID for the new collection: {id}".format(**trcoll))


@trcollections.command('delete')
@click.argument('id', type=UUID, required=True)
@click.pass_context
def trcollections_delete(ctx, id):
    """
    Delete a test result collection (does not remove test results)
    """

    req = ctx.obj['session'].delete(ctx.obj['trcollections_url'] + '/%s' % id)
    req.raise_for_status()

    click.echo("done")
