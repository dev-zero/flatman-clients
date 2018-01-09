
import sys
import csv
from uuid import UUID

import click

from . import cli, get_table_instance

@cli.command('deltatest-comparison')
@click.argument('collections', type=UUID, nargs=-1, required=True)
@click.option('--analysis', type=click.Choice(['delta', 'condition-number', 'evcurves']),
              default='delta', required=True,
              help=("use delta to get the ∆-value against reference (= the first collection),"
                    " or condition-number for the condition-number of the overlap matrix"))
@click.option('--csv-output', is_flag=True,
              default=False, show_default=True,
              help="output in CSV format")
@click.option('--plot', is_flag=True,
              default=False, show_default=True,
              help="additional generate plots")
@click.option('--hide-missing/--no-hide-missing', default=False,
              show_default=True, help=("Hide entries completely"
                                       " where one element is unavailable in at least one collection"))
@click.option('--label', 'labels', type=(UUID, str), multiple=True)
@click.option('--elements', type=str, help="Only use the specified elements, comma-sep list or range")
@click.option('--plot-measure', 'plot_measures', type=(float, str), multiple=True,
              help="Include a horizontal measure line for comparison at the given value using the label")
@click.option('--save-plot', type=click.Path(exists=False))
@click.option('--plot-ylimit', type=float, help="Limit the y-axis to the given value")
@click.option('--plot-columns', type=int, default=2, help="Number of columns for the E(V)-curve analysis")
@click.option('--plot-width', type=float, default=11.69, show_default=True, help="Plot width in inches")
@click.option('--plot-height', type=float, default=8.27, show_default=True, help="Plot height in inches")
@click.option('transparent_background', '--plot-transparent-bg', is_flag=True, default=False, show_default=True,
              help="Use a transparent background (only saving images)")
@click.pass_context
def deltatest_comparison(ctx, collections, analysis,
                         csv_output, plot, hide_missing, labels, elements,
                         plot_measures, save_plot, plot_ylimit, plot_columns,
                         plot_width, plot_height, transparent_background):
    """Do the deltatest comparison between two given Testresult Collections"""

    from ..tools.deltatest import SYM_LIST, ATOMIC_ELEMENTS

    if analysis == 'delta' and len(collections) < 2:
        raise click.BadOptionUsage("Need at least two collections (reference and comparison) to get delta values")

    if analysis == 'evcurves' and not plot:
        raise click.BadOptionUsage("The evcurve analysis consists only of plots")

    collection_ids = [str(c) for c in collections]

    selected_elements = ATOMIC_ELEMENTS.keys()

    if elements:
        if '-' in elements:
            s_element, e_element = elements.split('-', maxsplit=1)

            if (s_element not in ATOMIC_ELEMENTS or
                    e_element not in ATOMIC_ELEMENTS or
                    ATOMIC_ELEMENTS[s_element]['num'] >= ATOMIC_ELEMENTS[e_element]['num']):
                raise click.BadOptionUsage("Invalid elements specified for --elements")

            e_num_range = range(ATOMIC_ELEMENTS[s_element]['num'], ATOMIC_ELEMENTS[e_element]['num']+1)

            selected_elements = [k for k, v in ATOMIC_ELEMENTS.items() if v['num'] in e_num_range]

        else:
            # split the string by its commas, strip them of whitespaces
            selected_elements = [s.strip() for s in elements.split(',')]
            if any(ee not in ATOMIC_ELEMENTS for ee in selected_elements):
                raise click.BadOptionUsage("Invalid element specified for --elements")
        # the single element is treated as a one-elemented list in the second case

    if analysis == 'delta':
        comparison_url = '{url}/api/v2/comparisons'.format(**ctx.obj)

        reference_collection = collection_ids[0]
        comparison_collections = collection_ids[1:]

        req = ctx.obj['session'].post(comparison_url,
                                      json={'metric': "deltatest",
                                            'testresult_collections': collection_ids})
        req.raise_for_status()
        cdata = req.json()

        cid2cname = {c['id']: c['name'] for c in cdata['testresult_collections']}

        for cid, label in labels:
            cid2cname[str(cid)] = label

        header = ['element']
        for collection in comparison_collections:
            header.append("∆-value\n{}\n<->\n{}".format(cid2cname[reference_collection], cid2cname[collection]))

        ncomparisons = len(comparison_collections)

        deltas = [[el] + [None]*ncomparisons for el in cdata['elements']]

        # dict to convert element name to row number
        elrows = {v: k for k, v in enumerate(cdata['elements'])}
        # ... and the same for the columns
        colcolumns = {v: k+1 for k, v in enumerate(comparison_collections)}

        for value in cdata['values']:

            comp_collection = None

            # the API only guarantees that each comparison occurs only once,
            # but not that the order is maintained
            if value['collectionA'] == reference_collection:
                comp_collection = value['collectionB']
            elif value['collectionB'] == reference_collection:
                comp_collection = value['collectionA']
            else:
                # ignore comparisons between different comparison_collections returned by the API
                continue

            # fill out the matrix
            deltas[elrows[value['element']]][colcolumns[comp_collection]] = value['delta']

        deltas = [l for l in deltas if l[0] in selected_elements]

        sums = [0.]*ncomparisons
        available_deltas = [0]*ncomparisons
        # for each comparison collection
        for col in range(1, ncomparisons+1):
            # build a sum over the (available) deltas
            for entry in deltas:
                if entry[col] is not None:
                    sums[col-1] += entry[col]
                    available_deltas[col-1] += 1
        averages = [sums[i]/available_deltas[i] for i in range(len(sums))]

        if hide_missing:
            # remove lines containing Nones (= missing elements in some collection)
            deltas = [l for l in deltas if None not in l]

        table_data = [header] + deltas

        if csv_output:
            writer = csv.writer(sys.stdout)
            writer.writerows(table_data)
        else:
            table_instance = get_table_instance(table_data)
            click.echo(table_instance.table)

        # elements missing completely
        missing_elements_all = [", ".join([e for e in SYM_LIST if e not in cdata['elements']])]*ncomparisons
        # elements missing in respective comparisons (missing in one or the other collection)
        missing_elements = [", ".join([l[0] for l in deltas if l[i+1] is None]) for i in range(ncomparisons)]

        stats_table_data = [
            ['Stat'] + header[1:],
            ['# of available deltas'] + available_deltas,
            ['missing elements (all comparisons)'] + missing_elements_all,
            ['missing elements (this comparison)'] + missing_elements,
            ['averages'] + averages,
            ]
        stats_table_instance = get_table_instance(stats_table_data)
        click.echo(stats_table_instance.table)

        if plot:
            import matplotlib as mpl
            import matplotlib.pyplot as plt
            import matplotlib.collections as matcoll
            import matplotlib.cm as cm
            import numpy as np

            deltas = np.array(deltas)
            elements = deltas[:, 0]
            nelements = len(elements)

            plt.style.use('ggplot')
            mpl.rcParams.update({
                'xtick.labelsize': 16 if nelements < 20 else 14,
                'ytick.labelsize': 16,
                'axes.labelsize': 16,
                'lines.linewidth': 1,
                'font.weight': 'bold',
                })

            syms = ['o', '^', 's', 'v', 'p', 'D']
            linestyles = ['dotted', 'dashdot', 'dashed', 'solid']

            # the elements are already sorted by atomic number,
            # but we don't want the transition metals gap in the plot
            numbers = np.arange(1, nelements+1)

            fig = plt.figure(figsize=(plot_width, plot_height))
            ax = fig.add_subplot(111)

            if ncomparisons > 1:
                shifts = np.linspace(-0.25, 0.25, ncomparisons)
            else:
                shifts = [0.]

            cmap = plt.get_cmap("gnuplot")
            colors = [cmap(0.8*i/nelements) for i in range(nelements)]

            phandles = []

            for colnum in range(ncomparisons):
                x = numbers + shifts[colnum]
                y = deltas[:, colnum+1]

                phandle = ax.scatter(x, y, color=colors, marker=syms[colnum], s=50)
                phandles.append(phandle)

                lines = []
                for idx in range(len(x)):
                    lines.append([(x[idx], 0), (x[idx], y[idx])]) # for each datapoint add a list of pairs (start and endpoint)
                linecoll = matcoll.LineCollection(lines, colors=colors, linestyles=linestyles[colnum % len(linestyles)], linewidths=2)
                ax.add_collection(linecoll)

            additional_labels = []

            for plot_measure in plot_measures:
                phandle = ax.axhline(y=plot_measure[0])
                phandles.append(phandle)
                additional_labels.append(plot_measure[1])

            ax.grid(True, axis='y') # turn the grid on for the y axis since the plot is wide
            ax.tick_params(axis='both', which='both', length=0) # disable all ticks since we have lines and a grid

            plt.xlim(0, numbers[-1]+1) # set the minimum to 0 to get some space on the left
            plt.ylim(ymin=0) # no point in wasting space below 0

            if plot_ylimit:
                plt.ylim(ymax=plot_ylimit)

            plt.xticks(numbers, elements) # use elements instead of atomic numbers
            plt.ylabel("∆-value")
            #plt.title("Reference: {}".format(cid2cname[reference_collection]))

            plt.legend(phandles, [cid2cname[c] for c in comparison_collections] + additional_labels, loc="upper left", scatterpoints=1)

            plt.tight_layout()

            if save_plot:
                plt.savefig(save_plot, transparent=transparent_background)
            else:
                plt.show()

    elif analysis == 'condition-number':

        trcollections_url = '{url}/api/v2/testresultcollections'.format(**ctx.obj)

        req = ctx.obj['session'].get(trcollections_url)
        req.raise_for_status()
        cdata = req.json()

        cid2cname = {c['id']: c['name'] for c in cdata if c['id'] in collection_ids}

        for cid, label in labels:
            cid2cname[str(cid)] = label

        header = ['element'] + [cid2cname[cid] for cid in collection_ids]
        ncollections = len(collection_ids)

        # map collection IDs to colum numbers:
        colcolumns = {v: k+1 for k, v in enumerate(collection_ids)}

        cond_numbers = {}

        for coll_id in collection_ids:
            trcollection_url = '{url}/api/v2/testresultcollections/{trcid}'.format(trcid=coll_id, **ctx.obj)

            req = ctx.obj['session'].get(trcollection_url)
            req.raise_for_status()
            trcdata = req.json()

            for tresult in trcdata['testresults']:
                element = tresult.get('data', {}).get('element')
                condnum = tresult.get('data', {}).get('overlap_matrix_condition_number@V0')

                # ignore invalid deltatest data
                if element is None or condnum is None:
                    print("Ignoring:", tresult.get('data'))
                    continue

                # create an empty list for that element in the dictionary if not already present
                if element not in cond_numbers:
                    cond_numbers[element] = [element] + [None]*ncollections

                cond_numbers[element][colcolumns[coll_id]] = condnum['1-norm (estimate)']['Log(CN)']

        # strip the key which we only used to avoid the manual lookup
        cond_numbers = list(sorted(cond_numbers.values(), key=lambda l: ATOMIC_ELEMENTS[l[0]]['num']))

        cond_numbers = [l for l in cond_numbers if l[0] in selected_elements]

        if hide_missing:
            # remove lines containing Nones (= missing elements in some collection)
            cond_numbers = [l for l in cond_numbers if None not in l]

        table_data = [header] + cond_numbers

        if csv_output:
            writer = csv.writer(sys.stdout)
            writer.writerows(table_data)
        else:
            table_instance = get_table_instance(table_data)
            click.echo(table_instance.table)

        if plot:
            import matplotlib as mpl
            import matplotlib.pyplot as plt
            import matplotlib.collections as matcoll
            import matplotlib.cm as cm
            import numpy as np

            # unpack the cond_numbers list of lists manually to get
            # a proper NumPy array of values to get reliable conversion
            condnums = np.array([l[1:] for l in cond_numbers])
            elements = [l[0] for l in cond_numbers]
            nelements = len(elements)

            plt.style.use('ggplot')
            mpl.rcParams.update({
                'xtick.labelsize': 16 if nelements < 20 else 14,
                'ytick.labelsize': 16,
                'axes.labelsize': 16,
                'lines.linewidth': 1,
                'font.weight': 'bold',
                })

            syms = ['o', '^', 's', 'v', 'p', 'D']
            linestyles = ['dotted', 'dashdot', 'dashed', 'solid']

            # the elements are already sorted by atomic number,
            # but we don't want the transition metals gap in the plot
            numbers = np.arange(1, nelements+1)

            fig = plt.figure(figsize=(plot_width, plot_height))
            ax = fig.add_subplot(111)

            if ncollections > 1:
                shifts = np.linspace(-0.25, 0.25, ncollections)
            else:
                shifts = [0.]

            cmap = plt.get_cmap("gnuplot")
            colors = [cmap(0.8*i/nelements) for i in range(nelements)]

            phandles = []

            for colnum in range(ncollections):
                x = numbers + shifts[colnum]
                y = condnums[:, colnum]

                phandle = ax.scatter(x, y, color=colors, marker=syms[colnum], zorder=10)
                phandles.append(phandle)

                lines = []
                for idx in range(len(x)):
                    lines.append([(x[idx], 0), (x[idx], y[idx])]) # for each datapoint add a list of pairs (start and endpoint)
                linecoll = matcoll.LineCollection(lines, colors=colors, linestyles=linestyles[colnum % len(linestyles)], linewidths=2, zorder=8)
                ax.add_collection(linecoll)

            ax.grid(True, axis='y') # turn the grid on for the y axis since the plot is wide
            ax.tick_params(axis='both', which='both', length=0) # disable all ticks since we have lines and a grid

            maxcondnum = max(filter(None, condnums.flatten()))
            stable_span = ax.axhspan(0, 7., facecolor='limegreen', alpha=0.5, zorder=5)
            critical_span = ax.axhspan(7., 10., facecolor='yellow', alpha=0.5, zorder=5)
            unstable_span = ax.axhspan(10., max(12., maxcondnum*1.1), facecolor='red', alpha=0.5, zorder=5)
            ax.text(nelements+1, 3.5, "stable", ha="right", va="center", rotation=90, color="black", zorder=10)
            ax.text(nelements+1, 8.5, "critical", ha="right", va="center", rotation=90, color="black", zorder=10)
            ax.text(nelements+1, 11, "unstable", ha="right", va="center", rotation=90, color="black", zorder=10)

            plt.xlim(0, numbers[-1]+1) # set the min/max to 0/last-element+1 to get some space on the left and right
            plt.ylim(0, max(12., maxcondnum*1.1)) # no point in wasting space below 0 or above the max
            plt.xticks(numbers, elements) # use elements instead of atomic numbers
            plt.ylabel("Overlap Matrix Condition Number (log)")

            plt.legend(phandles, [cid2cname[c] for c in collection_ids], loc="upper left", scatterpoints=1)

            plt.tight_layout()

            if save_plot:
                plt.savefig(save_plot, dpi=100, transparent=transparent_background)
            else:
                plt.show()

    elif analysis == 'evcurves':
        import math
        import matplotlib.pyplot as plt
        import numpy as np
        from ..tools.deltatest import eos

        # it would be sufficient to get each testcollection instead,
        # but here we already have the list of all available elements
        comparison_url = '{url}/api/v2/comparisons'.format(**ctx.obj)

        req = ctx.obj['session'].post(comparison_url,
                                      json={'metric': "deltatest",
                                            'testresult_collections': collection_ids})
        req.raise_for_status()
        cdata = req.json()

        cid2cname = {c['id']: c['name'] for c in cdata['testresult_collections']}

        for cid, label in labels:
            cid2cname[str(cid)] = label

        ncollections = len(collections)

        trcdata = {c['id']: {r['data']['element']: r['data'] for r in  c['testresults'] if 'element' in r['data']} for c in cdata['testresult_collections']}

        elements = [e for e in cdata['elements'] if e in selected_elements]

        prows = math.ceil(len(elements)/plot_columns)

        fig, axarr = plt.subplots(prows, plot_columns, figsize=(plot_width, plot_height))

        if plot_columns == 1:
            axarr = np.array([axarr])

        axarr = axarr.flatten()

        for el_num, element in enumerate(elements):
            for cid in collection_ids:
                try:
                    coeffs = trcdata[cid][element]['coefficients']
                except KeyError:
                    continue

                xfit, yfit = eos(coeffs['V'], coeffs['B0'], coeffs['B1'])
                axarr[el_num].plot(xfit, yfit, label=cid2cname[cid])
                axarr[el_num].set_title(element)
                axarr[el_num].legend(loc="upper center")

        plt.tight_layout()

        if save_plot:
            plt.savefig(save_plot, dpi=100, transparent=transparent_background)
        else:
            plt.show()
