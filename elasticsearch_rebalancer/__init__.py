from collections import deque
from time import sleep
import json

import click
import requests


from .util import (
    check_cluster_health,
    combine_nodes_and_shards,
    execute_reroute_commands,
    format_shard_size,
    get_nodes,
    get_shard_size,
    get_shards,
    get_transient_cluster_settings,
    set_transient_cluster_settings,
    wait_for_no_relocations,
    parse_attr,
    matches_attrs,
    extract_attrs,
    get_nodes_attributes_map,
)


class BalanceException(click.ClickException):
    def __init__(self, message):
        message = click.style(message, 'red')
        super(BalanceException, self).__init__(message)


def find_node(nodes, node_name=None, skip_attr_map=None, max_recovery_per_node=None):
    if not isinstance(nodes, list):
        nodes = list(nodes)

    if not node_name:
        if not skip_attr_map:
            for node in nodes:
                if max_recovery_per_node and len(node.get('recovery', [])) >= max_recovery_per_node:
                    continue
                node['recovery'].append({'shard': 'new_shard_allocated'})
                return node
        else:
            for node in nodes:
                if max_recovery_per_node and len(node.get('recovery', [])) >= max_recovery_per_node:
                    continue
                if not matches_attrs(node.get('attributes'), skip_attr_map):
                    node['recovery'].append({'shard': 'new_shard_allocated'})
                    return node
            return None

    for node in nodes:
        if node['name'] == node_name:
            if max_recovery_per_node and len(node.get('recovery', [])) >= max_recovery_per_node:
                continue
            node['recovery'].append({'shard': 'new_shard_allocated'})
            return node

    return None


def check_skip_attr(curr_node, skip_attrs_list, skip_attr_map, map_id):
    same_attr = False
    for skip_attr in skip_attrs_list:
        curr_skip_attr = skip_attr_map.get(skip_attr)
        if curr_skip_attr and not same_attr:
            for node in curr_skip_attr.get((curr_node.get('attributes', {}).get(skip_attr, "None")), []):
                if node in map_id:
                    same_attr = True
    return same_attr


def attempt_to_find_swap(
    nodes, shards, used_shards,
    max_node_name=None,
    min_node_name=None,
    format_shard_weight_function=lambda weight: weight,
    one_way=False,
    use_shard_id=False,
    skip_attrs_list=None,
    node_skip_attrs_map=None,
    max_recovery_per_node=None,

):
    ordered_nodes, node_name_to_shards, index_to_node_names, shard_id_to_node_names = (
        combine_nodes_and_shards(nodes, shards)
    )

    max_node = find_node(reversed(ordered_nodes), node_name=max_node_name, max_recovery_per_node=max_recovery_per_node)
    if not max_node:
        return None

    max_node_skip_attr_map = extract_attrs(max_node.get('attributes'), skip_attrs_list)
    min_node = find_node(ordered_nodes, node_name=min_node_name, skip_attr_map=max_node_skip_attr_map, max_recovery_per_node=max_recovery_per_node)
    if not min_node:
        return None

    min_weight = min_node['weight']
    max_weight = max_node['weight']
    spread_used = round(max_weight - min_weight, 2)

    click.echo((
        f'> Weight used over {len(nodes)} nodes: '
        f'min={format_shard_weight_function(min_weight)}, '
        f'max={format_shard_weight_function(max_weight)}, '
        f'spread={format_shard_weight_function(spread_used)}'
    ))

    max_node_shards = node_name_to_shards[max_node['name']]
    min_node_shards = node_name_to_shards[min_node['name']]

    for shard in reversed(max_node_shards):  # biggest to smallest shard
        if shard['id'] not in used_shards:

            # Find if the shard to be moved is in a node that has the same attributes to be skipped
            # e.g. if the replica shard is in a node that has the same rack as the node to be moved to, skip it
            if node_skip_attrs_map:
                if use_shard_id:
                    same_attr = check_skip_attr(min_node, skip_attrs_list, node_skip_attrs_map, shard_id_to_node_names[shard['id']])
                else:
                    same_attr = check_skip_attr(min_node, skip_attrs_list, node_skip_attrs_map, index_to_node_names[shard['index']])
            else:
                same_attr = False

            if (
                use_shard_id 
                and min_node['name'] not in shard_id_to_node_names[shard['id']]
                and not same_attr
            ):
                max_shard = shard
                break
            elif (
                not use_shard_id 
                and min_node['name'] not in index_to_node_names[shard['index']]
                and not same_attr
            ):
                max_shard = shard
                break
    else:
        raise BalanceException((
            'Could not find suitable large shard to move to '
            f'{max_node["name"]}!'
        ))

    for shard in min_node_shards:
        if shard['id'] not in used_shards:
            # Find if the shard to be moved is in a node that has the same attributes to be skipped
            # e.g. if the replica shard is in a node that has the same rack as the node to be moved to, skip it
            if node_skip_attrs_map:
                if use_shard_id:
                    same_attr = check_skip_attr(max_node, skip_attrs_list, node_skip_attrs_map, shard_id_to_node_names[shard['id']])
                else:
                    same_attr = check_skip_attr(max_node, skip_attrs_list, node_skip_attrs_map, index_to_node_names[shard['index']])
            else:
                same_attr = False

            if (
                use_shard_id 
                and max_node['name'] not in shard_id_to_node_names[shard['id']]
            ):
                min_shard = shard
                break
            elif (
                not use_shard_id 
                and max_node['name'] not in index_to_node_names[shard['index']]
            ):
                min_shard = shard
                break
    else:
        raise BalanceException((
            'Could not find suitable small shard to move to '
            f'{min_node["name"]}!'
        ))

    # Update shard + node info according to the reroutes
    used_shards.add(max_shard['id'])
    max_shard['node'] = min_node['name']
    min_node['weight'] += max_shard['weight']
    max_node['weight'] -= max_shard['weight']

    if not one_way:
        used_shards.add(min_shard['id'])
        min_shard['node'] = max_node['name']
        min_node['weight'] -= min_shard['weight']
        max_node['weight'] += min_shard['weight']

        if min_node['weight'] >= max_node['weight']:
            return None

    if one_way:
        click.echo((
            '> Recommended move for: '
            f'{max_shard["id"]} ({format_shard_weight_function(max_shard["weight"])})'
        ))
    else:
        click.echo((
            '> Recommended swap for: '
            f'{max_shard["id"]} ({format_shard_weight_function(max_shard["weight"])}) <> '
            f'{min_shard["id"]} ({format_shard_weight_function(min_shard["weight"])})'
        ))

    click.echo((
        f'  maxNode: {max_node["name"]} ({len(max_node_shards)} shards) '
        f'({format_shard_weight_function(max_weight)} '
        f'-> {format_shard_weight_function(max_node["weight"])})'
    ))
    click.echo((
        f'  minNode: {min_node["name"]} ({len(min_node_shards)} shards) '
        f'({format_shard_weight_function(min_weight)} '
        f'-> {format_shard_weight_function(min_node["weight"])})'
    ))

    reroute_commands = [
        {
            'move': {
                'index': max_shard['index'],
                'shard': max_shard['shard'],
                'from_node': max_node['name'],
                'to_node': min_node['name'],
            },
        },
    ]

    if not one_way:
        reroute_commands.append({
            'move': {
                'index': min_shard['index'],
                'shard': min_shard['shard'],
                'from_node': min_node['name'],
                'to_node': max_node['name'],
            },
        })

    return reroute_commands


def print_command(command):
    args = command['move']
    click.echo((
        f'> Executing reroute of {args["index"]}-{args["shard"]} '
        f'from {args["from_node"]} -> {args["to_node"]}'
    ))


def check_raise_health(es_host):
    # Check we're good to go
    try:
        check_cluster_health(es_host)
    except Exception as e:
        raise BalanceException(f'{e}')


def print_execute_reroutes(es_host, commands):
    try:
        execute_reroute_commands(es_host, commands)
    except requests.HTTPError as e:
        if e.response.status_code != 400:
            raise
        click.echo(e)
    # Parallel reroute worked - so just wait & return
    else:
        for command in commands:
            print_command(command)

        click.echo('Waiting for relocations to complete...')
        wait_for_no_relocations(es_host)
        return

    # Now try to execute the reroutes one by one - it's likely that ES rejected the
    # parallel re-route because it would push the max node over the disk threshold.
    # So now attempt to reroute one shard at a time - first the big shard off the
    # big node, which should make space for the returning shard.
    if not click.confirm(click.style(
        'Parallel rerouting failed! Attempt shard by shard?',
        'yellow',
    )):
        raise BalanceException('User exited serial rerouting!')

    cluster_update_interval = get_transient_cluster_settings(
        es_host, 'cluster.info.update.interval',
    )['cluster.info.update.interval'] or '30s'

    cluster_update_interval = int(cluster_update_interval[:-1])

    for i, command in enumerate(commands, 1):
        print_command(command)
        execute_reroute_commands(es_host, [command])

        click.echo(
            f'Waiting for relocation to complete ({i}/{len(commands)})...',
        )
        wait_for_no_relocations(es_host)
        check_raise_health(es_host)  # check the cluster is still good
        # Wait for minimum update interval or ES might still think there's not
        # enough space for the next reroute.
        sleep(cluster_update_interval + 1)


def print_node_shard_states(
    nodes, shards,
    format_shard_weight_function=format_shard_size,
):
    ordered_nodes, node_name_to_shards, _ , _= (
        combine_nodes_and_shards(nodes, shards)
    )

    for node in ordered_nodes:
        click.echo(
            f'> Node: {node["name"]}, '
            f'shards: {len(node_name_to_shards[node["name"]])}, '
            f'weight: {format_shard_weight_function(node["weight"])}'
            f' ({node["weight_percentage"]})%',
        )


def make_rebalance_elasticsearch_cli(
    get_shard_weight_function=get_shard_size,
    format_shard_weight_function=format_shard_size,
):
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
            attrs = parse_attr(attr)

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
            check_raise_health(es_host)

            click.echo('Disabling cluster rebalance...')
            settings_to_set = {'cluster.routing.rebalance.enable': 'none'}

            if override_watermarks:
                click.echo(f'Overriding disk watermarks to: {override_watermarks}')
                settings_to_set.update({
                    'cluster.routing.allocation.disk.watermark.low': override_watermarks,
                    'cluster.routing.allocation.disk.watermark.high': override_watermarks,
                })

            # Save the old value to restore later
            previous_settings = get_transient_cluster_settings(es_host, settings_to_set.keys())
            set_transient_cluster_settings(es_host, settings_to_set)

        try:
            click.echo('Loading nodes...')
            nodes = get_nodes(es_host, role=node_role, attrs=attrs)
            if not nodes:
                raise BalanceException('No nodes found!')

            click.echo(f'> Found {len(nodes)} nodes')
            click.echo()

            click.echo('Loading shards...')
            shards = get_shards(
                es_host,
                attrs=attrs,
                index_name_filter=index_name,
                max_shard_size=max_shard_size,
                get_shard_weight_function=get_shard_weight_function,
            )
            if not shards:
                raise BalanceException('No shards found!')

            click.echo(f'> Found {len(shards)} shards')
            click.echo()

            if print_state:
                click.echo('Nodes ordered by weight:')
                print_node_shard_states(
                    nodes, shards,
                    format_shard_weight_function=format_shard_weight_function,
                )
                return

            click.echo('Investigating rebalance options...')
            
            if skip_attrs:
                node_skip_attrs_map = get_nodes_attributes_map(es_host)
            else:
                node_skip_attrs_map = None
                
            all_reroute_commands = []
            used_shards = set()

            for i in range(iterations):
                click.echo(f'> Iteration {i}')
                reroute_commands = attempt_to_find_swap(
                    nodes, shards,
                    used_shards=used_shards,
                    max_node_name=max_node[0] if max_node else None,
                    min_node_name=min_node[0] if min_node else None,
                    format_shard_weight_function=format_shard_weight_function,
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
                print_execute_reroutes(es_host, all_reroute_commands)
            else:
                click.echo('No Command will be executed. Below the POST to be executed for reroute:')
                click.echo('>Command:  \nPOST /_cluster/reroute \n{ \n"commands": \n' + json.dumps(all_reroute_commands)+'\n}')

        except requests.HTTPError as e:
            click.echo(click.style(e.response.content, 'yellow'))
            raise BalanceException(f'Invalid ES response: {e.response.status_code}')

        # Always restore the previous rebalance setting
        finally:
            if commit:
                click.echo(
                    f'Restoring previous settings ({previous_settings})...',
                )
                set_transient_cluster_settings(es_host, previous_settings)

        if commit:
            click.echo(f'# Ended rebalanced. Executed {len(all_reroute_commands)} reroutes!')
        else:
            click.echo(f'# Ended rebalanced. Calculated {len(all_reroute_commands)} reroutes!')
        click.echo()
    return rebalance_elasticsearch
