from collections import defaultdict
from fnmatch import fnmatch
import time

import requests
import click
from urllib3.exceptions import InsecureRequestWarning

# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

from humanize import naturalsize


class BalanceException(click.ClickException):
    def __init__(self, message):
        message = click.style(message, 'red')
        super(BalanceException, self).__init__(message)


def matches_attrs(attrs, match_attrs):
    for key, value in match_attrs.items():
        if attrs.get(key) != value:
            return False
    return True


def es_request(es_host, endpoint, method=requests.get, **kwargs):
    response = method(
        f'{es_host}/{endpoint}',
        verify=False,
        **kwargs,
    )
    response.raise_for_status()
    return response.json()


def get_cluster_health(es_host):
    return es_request(es_host, '_cluster/health')


def get_cluster_settings(es_host):
    return es_request(es_host, '_cluster/settings')

def get_nodes_attributes_map(es_host):
    nodes_attr = es_request(es_host, '_cat/nodeattrs?v=true&format=json')
    nodes_attr_map = {}

    for item in nodes_attr:
        attr = item['attr']
        value = item['value']
        node = item['node']

        if attr not in nodes_attr_map:
            nodes_attr_map[attr] = {value: [node]}
        else:
            if value not in nodes_attr_map[attr]:
                nodes_attr_map[attr][value] = [node]
            else:
                nodes_attr_map[attr][value].append(node)
    return nodes_attr_map


def check_cluster_health(es_host):
    health = get_cluster_health(es_host)

    if health['status'] != 'green':
        raise Exception('ES is not green!')

    relocating_shards = health['relocating_shards']
    if relocating_shards > 0:
        raise Exception(f'ES is already relocating {relocating_shards} shards!')


def wait_for_no_relocations(es_host):
    while True:
        health = get_cluster_health(es_host)

        relocating_shards = health['relocating_shards']
        if not relocating_shards:
            break

        time.sleep(10)


def execute_reroute_commands(es_host, commands):
    es_request(es_host, '_cluster/reroute', method=requests.post, json={
        'commands': commands,
    })


def get_transient_cluster_settings(es_host, paths):
    settings = get_cluster_settings(es_host)
    path_to_value = {}

    for path in paths:
        attrs = path.split('.')

        value = settings['transient']
        for attr in attrs:
            value = value.get(attr)
            if not value:
                path_to_value[path] = None
                break
        path_to_value[path] = value

    return path_to_value


def set_transient_cluster_settings(es_host, path_to_value):
    es_request(es_host, '_cluster/settings', method=requests.put, json={
        'transient': path_to_value,
    })


def get_nodes(es_host, role="data", attrs=None):
    nodes = es_request(es_host, '_nodes/stats/fs')['nodes']
    filtered_nodes = []

    recoveries = get_recovery(es_host)

    for node_id, node_data in nodes.items():
        if not matches_attrs(node_data.get('attributes'), attrs) or role not in node_data.get('roles', []):
            continue

        node_data['id'] = node_id
        node_data['recovery'] = []
        for recovery in recoveries:
            if recovery['source_node'] == node_data['name'] or recovery['target_node'] == node_data['name']:
                node_data['recovery'].append(recovery)

        filtered_nodes.append(node_data)

    return filtered_nodes


def get_shard_size(shard):
    if shard['store']:
        return int(shard['store'])
    return 0


def format_shard_size(weight):
    return naturalsize(weight, binary=True)


def get_recovery(es_host):
    # _cat/recovery?v&active_only=true&h=index,shard,source_node,target_node,stage,bytes_percent,translog_ops_percent,time&s=source_node
    return es_request(es_host, '_cat/recovery', params={
        'active_only': 'true',
        'h': 'index,shard,source_node,target_node,stage,bytes_percent,translog_ops_percent,time',
        's': 'source_node',
        'format': 'json',
    })


def get_shards(
    es_host,
    attrs=None,
    index_name_filter=None,
    max_shard_size=None,
    get_shard_weight_function=get_shard_size,
):
    indices = es_request(es_host, '_settings')

    filtered_index_names = []

    for index_name, index_data in indices.items():
        index_settings = index_data['settings']['index']
        index_attrs = (
            index_settings
            .get('routing', {})
            .get('allocation', {})
            .get('require', {})
        )
        if not matches_attrs(index_attrs, attrs):
            continue

        if index_name_filter and not fnmatch(index_name, index_name_filter):
            continue

        filtered_index_names.append(index_name)

    shards = es_request(
        es_host, '_cat/shards',
        params={
            'format': 'json',
            'bytes': 'b',
        },
    )

    filtered_shards = []

    for shard in shards:
        if (
            shard['state'] != 'STARTED'
            or shard['index'] not in filtered_index_names
            or (max_shard_size and get_shard_weight_function(shard) > max_shard_size)
        ):
            # logger.debug(f"skip shard: {shard['index']} {shard['shard']} {shard['node']} {shard['store']}")
            continue

        shard['id'] = f'{shard["index"]}-{shard["shard"]}'
        shard['weight'] = get_shard_weight_function(shard)

        filtered_shards.append(shard)
    # logger.debug(f"Tot shards: {len(shards)}")
    # logger.debug(f"Tot filtered shards: {len(filtered_shards)}")
    return filtered_shards


def combine_nodes_and_shards(nodes, shards):
    node_name_to_shards = defaultdict(list)
    index_to_node_names = defaultdict(list)
    shard_id_to_node_names = defaultdict(list)

    for shard in shards:
        node_name_to_shards[shard['node']].append(shard)
        index_to_node_names[shard['index']].append(shard['node'])
        shard_id_to_node_names[shard['id']].append(shard['node'])

    node_name_to_shards = {
        node_name: sorted(shards, key=lambda shard: shard['weight'])
        for node_name, shards in node_name_to_shards.items()
    }

    ordered_nodes = []

    # logger.debug(f"Total node shards: {total_shards}")
    for node in nodes:
        if node['name'] not in node_name_to_shards:
            continue

        node['weight'] = sum(
            shard['weight'] for shard in node_name_to_shards[node['name']]
        )

        ordered_nodes.append(node)

    ordered_nodes = sorted(ordered_nodes, key=lambda node: node['weight'])

    # min_weight = ordered_nodes[0]['weight']
    max_weight = ordered_nodes[-1]['weight']

    for node in ordered_nodes:
        node['weight_percentage'] = round((node['weight'] / max_weight) * 100, 2)

    return ordered_nodes, node_name_to_shards, index_to_node_names, shard_id_to_node_names


def parse_attr(attributes):
    attr_map = {}
    for a in attributes:
        try:
            key, value = a.split('=', 1)
        except ValueError:
            raise BalanceException('Invalid attr, specify as key=value!')
        attr_map[key] = value
    return attr_map

def extract_attrs(attrs, skip_attrs):
    extract_attr = {}
    if not attrs or not skip_attrs:
        return extract_attr

    for skip_attr in skip_attrs:
        if attrs.get(skip_attr):
            extract_attr[skip_attr] = attrs.get(skip_attr)
    return extract_attr

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
        time.sleep(cluster_update_interval + 1)


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
