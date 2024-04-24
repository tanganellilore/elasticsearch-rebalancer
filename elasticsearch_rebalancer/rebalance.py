from collections import deque
import json

from . import utils
import click
import requests

import click
from . import rebalance

# create 

@click.command()
@click.argument('es_host')
@click.option(
    '--iterations',
    default=1,
    type=int,
    help='Number of iterations (swaps) to execute.',
)
@click.option(
    '--attr',
    multiple=True,
    help=(
        'Rebalance only on node with attributes specified here. '
        'Attributes is accepted in format key=value.'
    ),
)
@click.option(
    '--commit',
    is_flag=True,
    default=False,
    help='Execute the shard reroutes (default print only).',
)
@click.option(
    '--print-state',
    is_flag=True,
    default=False,
    help='Print the current nodes & weights and exit.',
)
@click.option(
    '--index-name',
    default=None,
    help='Filter the indices for swaps by name, supports wildcards.',
)
@click.option(
    '--max-node',
    default=None,
    multiple=True,
    help='Force the max node to consider for shard swaps.',
)
@click.option(
    '--min-node',
    default=None,
    multiple=True,
    help='Force the min node to consider for shard swaps.',
)
@click.option(
    '--one-way',
    is_flag=True,
    default=False,
    help=(
        'Disables shard swaps and simply moves max -> min. '
        'Note after ES rebalancing is restored ES will attempt '
        "to rebalance itself according to it's own heuristics."
    ),
)
@click.option(
    '--override-watermarks',
    help=(
        'Temporarily override the Elasticsearch low & high disk '
        'watermark settings. Makes it possible to parallel swap '
        'shards even when the most full nodes are on the limit.'
    ),
)
@click.option(
    '--use-shard-id',
    is_flag=True,
    default=False,
    help=(
        'If passed, we use the shard_id created in runtime instead '
        'index name for shard algoritms. Without this params if index '
        'of shard is in the max and min node, shard will be skipped.'
    ),
)
@click.option(
    '--skip-attr',
    multiple=True,
    help=(
        'If specified we avoid rebalance beetween node that have same '
        'attributes specified here. Attributes are in string format.'
    ),
)
@click.option(
    '--max-shard-size',
    default=None,
    type=int,
    help='Max shard size in bytes. If a shard is larger than this, it will be skipped.',
)
@click.option(
    '--node-role',
    default='data',
    help=(
        'Filter the nodes for swaps by role. Typically this are the roles '
        'defined in the elasticsearch.yml file. Generally you can use this '
        '"hot" or "warm" or "cold" to filter nodes by their role.'
        'Default is "data", which means all data are considered for rebalance.'
    )
)
@click.option(
    '--max-recovery-per-node',
    default=None,
    type=int,
    help='Max number of concurrent recoveries per node. If a node has more recoveries, it will be skipped.',
)

def rebalance_elasticsearch(
        es_host,
        iterations=1,
        attr=None,
        commit=False,
        print_state=False,
        index_name=None,
        max_node=None,
        min_node=None,
        one_way=False,
        override_watermarks=None,
        use_shard_id=False,
        skip_attr=None,
        max_shard_size=None,
        node_role="data",
        max_recovery_per_node=None,
):
        # Parse out any attrs
    attrs = {}
    if attr:
        attrs = utils.parse_attr(attr)

    # Parse out any skip_attrs
    skip_attrs = []
    if skip_attr:
        skip_attrs = skip_attr

    # Turn min/max node lists into deque instances
    if min_node:
        min_node = deque(min_node)
    if max_node:
        max_node = deque(max_node)

    click.echo()
    click.echo('# Elasticsearch Rebalancer')
    click.echo(f'> Target: {click.style(es_host, bold=True)}')
    click.echo()

    if commit:
        if print_state:
            raise click.ClickException('Cannot have --commit and --print-state!')

        # Check we have a healthy cluster
        utils.check_raise_health(es_host)

        click.echo('Disabling cluster rebalance...')
        settings_to_set = {'cluster.routing.rebalance.enable': 'none'}

        if override_watermarks:
            click.echo(f'Overriding disk watermarks to: {override_watermarks}')
            settings_to_set.update({
                'cluster.routing.allocation.disk.watermark.low': override_watermarks,
                'cluster.routing.allocation.disk.watermark.high': override_watermarks,
            })

        # Save the old value to restore later
        previous_settings = utils.get_transient_cluster_settings(es_host, settings_to_set.keys())
        utils.set_transient_cluster_settings(es_host, settings_to_set)

    try:
        click.echo('Loading nodes...')
        nodes = utils.get_nodes(es_host, role=node_role, attrs=attrs)
        if not nodes:
            raise utils.BalanceException('No nodes found!')

        click.echo(f'> Found {len(nodes)} nodes')
        click.echo()

        click.echo('Loading shards...')
        shards = utils.get_shards(
            es_host,
            attrs=attrs,
            index_name_filter=index_name,
            max_shard_size=max_shard_size
        )
        if not shards:
            raise utils.BalanceException('No shards found!')

        click.echo(f'> Found {len(shards)} shards')
        click.echo()

        if print_state:
            click.echo('Nodes ordered by weight:')
            utils.print_node_shard_states(nodes)
            return

        click.echo('Investigating rebalance options...')

        if skip_attrs:
            node_skip_attrs_map = utils.get_nodes_attributes_map(es_host)
        else:
            node_skip_attrs_map = None

        all_reroute_commands = []
        used_shards = set()

        for i in range(iterations):
            click.echo(f'> Iteration {i}')
            reroute_commands = utils.attempt_to_find_swap(
                nodes, shards,
                used_shards=used_shards,
                max_node_name=max_node[0] if max_node else None,
                min_node_name=min_node[0] if min_node else None,
                one_way=one_way,
                use_shard_id=use_shard_id,
                skip_attrs_list=skip_attrs,
                node_skip_attrs_map=node_skip_attrs_map,
                max_recovery_per_node=max_recovery_per_node,
            )

            if reroute_commands:
                all_reroute_commands.extend(reroute_commands)

            click.echo()

            if min_node:
                min_node.rotate()
            if max_node:
                max_node.rotate()

        if commit:
            utils.print_execute_reroutes(es_host, all_reroute_commands)
        else:
            click.echo('No Command will be executed. Below the POST to be executed for reroute:')
            click.echo('>Command:  \nPOST /_cluster/reroute \n{ \n"commands": \n' + json.dumps(all_reroute_commands)+'\n}')

    except requests.HTTPError as e:
        click.echo(click.style(e.response.content, 'yellow'))
        raise utils.BalanceException(f'Invalid ES response: {e.response.status_code}')

    # Always restore the previous rebalance setting
    finally:
        if commit:
            click.echo(
                f'Restoring previous settings ({previous_settings})...',
            )
            utils.set_transient_cluster_settings(es_host, previous_settings)

    if commit:
        click.echo(f'# Ended rebalanced. Executed {len(all_reroute_commands)} reroutes!')
    else:
        click.echo(f'# Ended rebalanced. Calculated {len(all_reroute_commands)} reroutes!')
    click.echo()


if __name__ == '__main__':
    rebalance_elasticsearch()
