
import sys
import json

from io import BytesIO
from uuid import UUID
from collections import OrderedDict

import click
import requests

from . import cli, json_pretty_dumps, get_table_instance
from .. import xyz_parser_iterator


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
@click.option('--replace-existing/--no-replace-existing', default=False,
              show_default=True, help="Replace an existing structure with the same name")
@click.option('--dump/--no-dump', default=False,
              show_default=True, help="Dump the parsed out structure")
@click.option('--edit/--no-edit', default=False,
              show_default=True, help="Edit the structures manually before upload")
@click.pass_context
def struct_add(ctx, xyzfile, name, name_prefix, name_field, sets, pbc, cubic_cell, replace_existing, dump, edit):
    """Upload a structure (in XYZ format)"""

    structures = OrderedDict()

    complete_input = xyzfile.read()

    def unmatched_content_cb(unmatched):
        click.echo("UNMATCHED CONTENT =>", file=sys.stderr)
        click.echo(unmatched, file=sys.stderr)
        click.echo("<= UNMATCHED CONTENT", file=sys.stderr)

    for (_, comment, _, match) in xyz_parser_iterator(complete_input, True, unmatched_content_cb):
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

    overrides = OrderedDict()

    if edit:
        for name, (spos, epos) in structures.items():
            click.echo("\nStructure:")
            click.echo(complete_input[spos:epos])

            override = {}

            new_name = click.prompt("Name (leave empty to use default):", default=name)
            if new_name and new_name != name:
                override['name'] = new_name

            charges = click.prompt("Initial charges (comma-separated list, leave empty for none):", default="")
            magmoms = click.prompt("Initial magnetic moments (comma-separated list, leave empty for none):", default="")

            # we are not sending JSON here, directly pass along the string and leave validation to the server
            # .. otherwise Requests will send one charges field per list entry
            if charges:
                override['charges'] = charges
            if magmoms:
                override['magmoms'] = magmoms

            if override:
                overrides[name] = override

        click.echo("Overridden/extended infos:")
        for name, attributes in overrides.items():
            click.echo("for structure '{}':".format(name))
            for key, value in sorted(attributes.items()):
                click.echo("    {}: {}".format(key, value))

    click.confirm("Do you want to upload the structures (total: {})?".format(len(structures)), abort=True)

    for name, (spos, epos) in structures.items():
        click.echo("Uploading structure '{}'.. ".format(name), nl=False)

        data = {
            'name': name,
            'sets': sets,
            'pbc': pbc,
            'cubic_cell': cubic_cell,
            'replace_existing': replace_existing,
            'format': 'xyz',
            }

        if name in overrides:
            data.update(overrides[name])

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

        click.echo("succeeded (id: {id})".format(**req.json()))


@struct.command('list')
@click.option('--include-replaced', is_flag=True,
              default=False, show_default=True,
              help="show also replaced structures")
@click.pass_context
def struct_list(ctx, **filters):
    """List structures"""

    # filter out filters not specified
    params = {k: v for k, v in filters.items() if v is not None}

    req = ctx.obj['session'].get(ctx.obj['struct_url'], params=params)
    req.raise_for_status()
    structs = req.json()

    table_header = ['id', 'name', 'sets']

    if filters['include_replaced']:
        table_header += ['replaced_by']

    table_data = [table_header]

    for struc in structs:
        data = [struc['id'], struc['name'], ', '.join(struc['sets']), ]

        if filters['include_replaced']:
            if struc['replaced_by']:
                data += [struc['replaced_by']['id']]
            else:
                data += ['']

        table_data.append(data)

    table_instance = get_table_instance(table_data)
    click.echo(table_instance.table)


@struct.command('show')
@click.argument('structure_id', metavar='<ID>', type=UUID, required=True)
@click.pass_context
def struct_show(ctx, structure_id):
    """Show structure"""

    from ..tools.deltatest import NUM2SYM

    req = ctx.obj['session'].get(ctx.obj['struct_url'] + '/%s' % structure_id)
    req.raise_for_status()
    struct = req.json()

    data = OrderedDict()

    data['Name'] = struct['name']
    data['Sets'] = ", ".join(struct['sets'])
    data['Replaced by'] = struct['replaced_by']

    ase_struct = json.loads(struct['ase_structure'])

    if 'pbc' in ase_struct:
        data['PBC'] = "".join(axis*enabled for axis, enabled in zip("XYZ", ase_struct['pbc']))
        if not data['PBC']:
            data['PBC'] = None

    data['Cell'] = "\n".join(["{:.4f} {:8.4f} {:8.4f}".format(*v) for v in ase_struct['cell']])

    data['Atoms'] = "\n".join(
        "{:4} {:.4f} {:8.4f} {:8.4f}".format(NUM2SYM[e[0]], *e[1])
        for e in zip(ase_struct['numbers'], ase_struct['positions']))

    if 'initial_magmoms' in ase_struct:
        data['Init. Magn. Moments'] = " ".join(map("{:.2}".format, ase_struct['initial_magmoms']))
    else:
        data['Init. Magn. Moments'] = None

    if 'kpoints' in ase_struct['key_value_pairs']:
        data['K-Points'] = " ".join(map(str, ase_struct['key_value_pairs']['kpoints']))
    else:
        data['K-Points'] = None

    table = get_table_instance([[k, v] for k, v in data.items()])
    table.inner_heading_row_border = False
    click.echo(table.table)


@struct.command('delete')
@click.argument('struct_ids', metavar='<ID 1> [<ID 2>..]', type=UUID, nargs=-1, required=True)
@click.pass_context
def struct_rm(ctx, struct_ids):
    """Delete specified structures (if not referenced by any calculation)"""
    for struct_id in struct_ids:
        req = ctx.obj['session'].delete(ctx.obj['struct_url'] + '/{}'.format(struct_id))
        req.raise_for_status()


@cli.group()
@click.pass_context
def structureset(ctx):
    """Manage structure sets"""
    ctx.obj['structureset_url'] = '{url}/api/v2/structuresets'.format(**ctx.obj)


@structureset.command('add')
@click.argument('name', type=str)
@click.option('--desc', type=str,
              help="Optional description")
@click.option('--superset', type=str,
              help="Optional name of a super set")
@click.pass_context
def structureset_add(ctx, **data):
    """Create a new structure set"""

    try:
        req = ctx.obj['session'].post(ctx.obj['structureset_url'], data=data)
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


@structureset.command('list')
@click.pass_context
def structureset_list(ctx):
    """List structure sets"""

    req = ctx.obj['session'].get(ctx.obj['structureset_url'])
    req.raise_for_status()
    structuresets = req.json()

    table_data = [['name', 'description', 'superset']]

    for sset in structuresets:
        table_data.append([sset['name'], sset['description'], sset.get('superset', '(none)')])

    table_instance = get_table_instance(table_data)
    click.echo(table_instance.table)
