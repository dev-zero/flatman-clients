
import re
from uuid import UUID
from io import BytesIO

import click
import requests

from . import cli, json_pretty_dumps, get_table_instance


@cli.group()
@click.pass_context
def basis(ctx):
    """Manage basis sets"""
    ctx.obj['basis_url'] = '{url}/api/v2/basissets'.format(**ctx.obj)


@basis.command('add')
@click.argument('basisset_file', type=click.File(mode='r'))
@click.option('family_filter', '--filter', type=str,
              required=False, default='.*', show_default=True,
              help="Python regex to filter by name")
@click.option('--dump-basis/--no-dump-basis',
              default=False, show_default=True,
              help="Dump also the basis during parsing")
@click.pass_context
def basis_add(ctx, basisset_file, family_filter, dump_basis):
    """Upload new basis sets from a file"""
    basissets = {}
    current_basis = None

    EMPTY_LINE = re.compile(r'^(\s*|\s*#.*)$')
    BLOCK_DEFINITION = re.compile(r'^\s*(?P<element>[a-zA-Z]{1,2})\s+(?P<family>\S+).*\n')
    FAMILY_FILTER = re.compile(family_filter)

    ignore_lines = False

    for line in basisset_file:
        if EMPTY_LINE.match(line):
            # ignore empty and comment lines
            continue

        match = BLOCK_DEFINITION.match(line)

        if match:
            if current_basis and dump_basis:
                click.echo(basissets[current_basis].getvalue().decode('utf-8'))

            if not FAMILY_FILTER.match(match.group('family')):
                click.echo(("Ignoring basis set for element '{element}'"
                            " and family '{family}' (filter does not match)").format(**match.groupdict()))
                ignore_lines = True
                continue
            else:
                ignore_lines = False

            click.echo(("Found basis set for element '{element}'"
                        " and family '{family}'").format(**match.groupdict()))
            current_basis = (match.group('element'), match.group('family'))

            if current_basis in basissets.keys():
                ValueError(("duplicated basis set for element '{element}'"
                            " and family '{family}' found").format(**match.groupdict()))

            basissets[current_basis] = BytesIO()
            # we don't want this line to end up in our uploaded file
            continue

        if ignore_lines:
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


@basis.command('list')
@click.option('--element', type=str,
              help="filter by element")
@click.option('--family', type=str,
              help="filter by Basis Set family name")
@click.pass_context
def basis_list(ctx, **filters):
    """List available basis sets"""

    # filter out filters not specified
    params = {k: v for k, v in filters.items() if v is not None}

    req = ctx.obj['session'].get(ctx.obj['basis_url'], params=params)
    req.raise_for_status()

    basis_sets = req.json()

    table_data = [['id', 'element', 'family', ], ]
    for basis_set in basis_sets:
        table_data.append([basis_set[f] for f in table_data[0]])

    click.echo(get_table_instance(table_data).table)


@basis.command('download')
@click.argument('basis_set_ids', type=UUID, nargs=-1, required=False)
@click.option('--element', type=str,
              help="filter by element")
@click.option('--family', type=str,
              help="filter by Basis Set family name")
@click.pass_context
def basis_download(ctx, basis_set_ids, **filters):
    """
    Download basis sets to individual files 'BASIS_<family>-<element>' into the current working directory.
    Either specify explicitly the IDs or filter by element or family."""

    if not basis_set_ids and not (filters['element'] or filters['family']):
        raise click.UsageError("you have to specify either an ID or filter by family and/or element")

    # filter out filters not specified
    params = {k: v for k, v in filters.items() if v is not None}

    req = ctx.obj['session'].get(ctx.obj['basis_url'], params=params)
    req.raise_for_status()

    basis_sets = req.json()

    filename = "BASIS_{family}-{element}"

    for basis_set in basis_sets:
        click.echo("Fetching basis set for {family}-{element}...".format(**basis_set))

        req = ctx.obj['session'].get(basis_set['_links']['self'])
        req.raise_for_status()
        basis_set = req.json()

        with open(filename.format(**basis_set), 'w') as fhandle:
            fhandle.write("{element} {family}\n".format(**basis_set))
            fhandle.write(basis_set['basis'])
